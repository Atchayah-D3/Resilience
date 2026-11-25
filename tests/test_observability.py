"""L4 Observability (Arch §9): the run's markers on Grafana, its metrics copied from Prometheus,
the dashboard link -- all context, never a pass/fail source (Arch §7.1). Prometheus and Grafana
are faked; nothing here reaches the network."""

from __future__ import annotations

import asyncio
import io
import json
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from resilience_tests.control import orchestrator as orch
from resilience_tests.control.profile import Observability, load_profile
from resilience_tests.observability import dashboard, scrape_config
from resilience_tests.observability.event_stream import EventStream
from resilience_tests.observability.grafana import GrafanaClient
from resilience_tests.observability.http_client import HttpError
from resilience_tests.observability.metrics import SNAPSHOT_QUERIES, render
from resilience_tests.observability.observer import (
    METRICS_FILE,
    NoObserver,
    StackObserver,
    observer_for,
)
from resilience_tests.observability.prometheus import PrometheusClient
from tests.test_orchestrator import run as run_scenario
from tests.test_orchestrator import why
from tests.test_orchestrator import env  # noqa: F401  (fixture)

LAB = load_profile("e2-dedicated-vm-u22-2")
NODE = LAB.nodes[0]
REPO = Path(__file__).resolve().parents[1]


class FakeGrafana(GrafanaClient):
    def __init__(self, healthy: bool = True, store: bool = True) -> None:
        super().__init__("http://grafana:3000", "token")
        self.healthy, self.store, self.notes = healthy, store, []

    async def health(self) -> dict[str, Any]:
        if not self.healthy:
            raise HttpError("GET http://grafana:3000/api/health: URLError: refused")
        return {"database": "ok"}

    async def annotate(self, *, time_s, text, tags, end_s=None) -> int:
        if not self.store:
            raise HttpError("POST /api/annotations: HTTPError 403")
        self.notes.append({"time_s": time_s, "text": text, "tags": list(tags), "end_s": end_s})
        return len(self.notes)


class FakePrometheus(PrometheusClient):
    def __init__(self, ready: bool = True) -> None:
        super().__init__("http://localhost:9090")
        self.is_ready, self.queries = ready, []

    async def ready(self) -> None:
        if not self.is_ready:
            raise HttpError("GET http://localhost:9090/-/ready: URLError: refused")

    async def query_range(self, query, start_s, end_s, step_s):
        self.queries.append((query, start_s, end_s, step_s))
        if "pg_stat_checkpointer" in query:
            return []                                            # a series this target does not have
        return [{"metric": {"node": NODE.name}, "values": [[start_s, "1"], [end_s, "2"]]}]


def observer(grafana=None, prometheus=None, save_metrics=True) -> StackObserver:
    assert LAB.observability is not None
    config = LAB.observability.model_copy(update={"save_metrics_snapshot": save_metrics})
    return StackObserver(config, env=LAB.name, node=NODE, run_id="NL-N-I03a-run1", scenario="NL-N-I03a",
                         grafana=grafana or FakeGrafana(), prometheus=prometheus or FakePrometheus())


def run(coro):
    return asyncio.run(coro)


async def a_run(obs: StackObserver, run_dir: Path) -> tuple[list[str], dict[str, Any]]:
    """What the orchestrator does: start, a run's events, finish."""
    stream = EventStream(run_dir / "events.jsonl")
    disclosures = await obs.start(stream)
    stream.emit("orchestrator", "run_start", run_id="NL-N-I03a-run1", scenario="NL-N-I03a")
    stream.emit("orchestrator", "phase_start", phase="baseline")
    stream.emit("orchestrator", "warmup", seconds=300)
    stream.emit("workload", "sample", tps=60.0)                  # not a milestone: no marker
    stream.emit("injector", "t0", t0_mono_ns=1)
    stream.emit("orchestrator", "phase_end", phase="fault_inject")
    stream.emit("orchestrator", "fault_milestone", text="schema change issued: it waits for the table's lock")
    stream.emit("orchestrator", "t1", action="lock queue cleared")
    await asyncio.sleep(0.05)
    facts = await obs.finish(run_dir, 1_000.0, 1_600.0, "passed")
    stream.close()
    return disclosures, facts


# --- the event stream's hook ----------------------------------------------------------------

def test_a_subscriber_sees_every_event_and_cannot_break_the_record(tmp_path):
    stream = EventStream(tmp_path / "e.jsonl")
    seen: list[str] = []
    stream.subscribe(lambda e: seen.append(e.kind))
    stream.subscribe(lambda e: 1 / 0)                            # a broken observer
    stream.emit("orchestrator", "run_start")
    stream.emit("injector", "t0")
    stream.close()
    assert seen == ["run_start", "t0"]
    assert [json.loads(line)["kind"] for line in (tmp_path / "e.jsonl").read_text().splitlines()] == ["run_start", "t0"]


# --- the env profile ----------------------------------------------------------------------------

def test_the_lab_profile_names_its_stack_and_the_others_have_none():
    assert LAB.observability is not None and LAB.observability.grafana_token_file.startswith("~")
    assert LAB.observability.exporters == {"node": 9100, "postgres": 9187}
    assert load_profile("e2-dedicated-vm").observability is None
    assert isinstance(observer_for(load_profile("e2-dedicated-vm"), NODE, run_id="r", scenario="s"), NoObserver)
    assert isinstance(observer_for(LAB, NODE, run_id="r", scenario="s"), StackObserver)


def test_an_address_that_is_not_http_is_refused():
    with pytest.raises(ValidationError):
        Observability(prometheus_url="localhost:9090", grafana_url="http://g:3000", grafana_token_file="t")


# --- the observer ---------------------------------------------------------------------------

def test_the_runs_milestones_become_markers_and_nothing_else(tmp_path):
    grafana = FakeGrafana()
    disclosures, facts = run(a_run(observer(grafana=grafana), tmp_path))
    assert disclosures == [] and "problems" not in facts
    texts = [n["text"] for n in grafana.notes]
    assert texts == ["run start: NL-N-I03a (NL-N-I03a-run1)", "phase: baseline", "warm-up 300 s (not measured)",
                     "T0: fault injected", "schema change issued: it waits for the table's lock",
                     "T1: lock queue cleared", "NL-N-I03a: passed (NL-N-I03a-run1)"]
    assert all({"resilience", "NL-N-I03a-run1", "NL-N-I03a", LAB.name, NODE.name} <= set(n["tags"]) for n in grafana.notes)
    region = grafana.notes[-1]
    assert (region["time_s"], region["end_s"]) == (1_000.0, 1_600.0) and "run" in region["tags"]
    assert facts["annotations"] == {"sent": 7, "failed": 0}


def test_the_report_link_opens_the_runs_window_on_its_node(tmp_path):
    _, facts = run(a_run(observer(), tmp_path))
    link = facts["grafana_link"]
    assert link.startswith("http://grafana:3000/d/resilience-run?")
    assert "from=970000" in link and "var-env=e2-dedicated-vm-u22-2" in link and f"var-node={NODE.name}" in link
    assert f"var-db={NODE.db.dbname}" in link          # was missing: the dashboard opened on database postgres


def test_the_run_windows_metrics_are_copied_into_the_run_folder(tmp_path):
    prometheus = FakePrometheus()
    _, facts = run(a_run(observer(prometheus=prometheus), tmp_path))
    doc = json.loads((tmp_path / METRICS_FILE).read_text())
    assert facts["metrics_file"] == METRICS_FILE and facts["metrics_series"] == len(SNAPSHOT_QUERIES)
    assert set(doc["series"]) == {name for name, _ in SNAPSHOT_QUERIES}
    expected = render(dict(SNAPSHOT_QUERIES)["commits_per_s"], env=LAB.name, node=NODE.name, db=NODE.db.dbname)
    assert doc["series"]["commits_per_s"]["query"] == expected and 'datname="resilience"' in expected
    assert facts["metrics_empty"] == ["checkpoints_per_min"]    # reported, not hidden
    assert "never used in the verdict" in doc["note"]


def test_by_default_the_metrics_stay_on_the_dashboards_and_no_copy_is_written(tmp_path):
    _, facts = run(a_run(observer(save_metrics=False), tmp_path))
    assert not (tmp_path / METRICS_FILE).exists() and "metrics_file" not in facts
    assert facts["grafana_link"].startswith("http://grafana:3000/d/resilience-run?")
    assert LAB.observability is not None and LAB.observability.save_metrics_snapshot is False


def test_grafana_down_is_disclosed_and_the_metrics_are_still_kept(tmp_path):
    disclosures, facts = run(a_run(observer(grafana=FakeGrafana(healthy=False)), tmp_path))
    assert "Grafana (http://10.11.21.13:3000) not used" in disclosures[0] and "judged exactly as without it" in disclosures[0]
    assert "grafana_link" not in facts and (tmp_path / METRICS_FILE).exists()


def test_prometheus_down_is_disclosed_and_nothing_is_copied(tmp_path):
    disclosures, facts = run(a_run(observer(prometheus=FakePrometheus(ready=False)), tmp_path))
    assert "Prometheus (http://localhost:9090) not used" in disclosures[0]
    assert "metrics_file" not in facts and not (tmp_path / METRICS_FILE).exists()
    assert "grafana_link" in facts                               # the dashboard still opens


def test_markers_grafana_refuses_are_counted_and_disclosed(tmp_path):
    _, facts = run(a_run(observer(grafana=FakeGrafana(store=False)), tmp_path))
    assert facts["annotations"]["failed"] == 7 and facts["annotations"]["sent"] == 0
    assert "were not stored in Grafana" in facts["disclosure"]


def test_a_missing_token_file_is_disclosed(tmp_path):
    assert LAB.observability is not None
    config = LAB.observability.model_copy(update={"grafana_token_file": str(tmp_path / "no-such-token")})
    obs = StackObserver(config, env=LAB.name, node=NODE, run_id="r", scenario="s", prometheus=FakePrometheus())
    disclosures, _ = run(a_run(obs, tmp_path))
    assert "Grafana" in disclosures[0] and "FileNotFoundError" in disclosures[0]


def test_a_marker_sits_where_the_moment_happened_not_when_it_was_announced():
    """Was: T1 was drawn when the harness announced it, 6 s after the queue had cleared (it is
    announced after the read's release). An event recording its moment is drawn there."""
    from resilience_tests.observability.event_stream import Event
    from resilience_tests.observability.observer import label, marker_time
    t1 = Event(t_mono_ns=100 * 10**9, t_wall=5_000.0, source="orchestrator", kind="t1",
               data={"t1_mono_ns": 94 * 10**9, "action": "lock queue cleared"})
    assert marker_time(t1) == pytest.approx(4_994.0)
    issued = Event(50 * 10**9, 2_000.0, "orchestrator", "fault_milestone", {"text": "schema change issued", "at_mono_ns": 49 * 10**9})
    assert marker_time(issued) == pytest.approx(1_999.0) and label(issued) == "schema change issued"
    plain = Event(10 * 10**9, 1_000.0, "orchestrator", "phase_start", {"phase": "baseline"})
    assert marker_time(plain) == 1_000.0


# --- generated configuration ------------------------------------------------------------------

def test_prometheus_targets_come_from_the_env_profile():
    config = scrape_config.build([LAB, load_profile("e2-dedicated-vm")])        # the second has none
    jobs = {j["job_name"]: j for j in config["scrape_configs"]}
    assert config["global"]["scrape_interval"] == "1s"
    assert jobs["node"]["static_configs"] == [{"targets": [f"{NODE.ssh.host}:9100"],
                                               "labels": {"env": LAB.name, "node": NODE.name}}]
    assert jobs["postgres"]["static_configs"][0]["targets"] == [f"{NODE.ssh.host}:9187"]
    with pytest.raises(ValueError, match="observability"):
        scrape_config.build([load_profile("e2-dedicated-vm")])


def test_the_committed_dashboard_is_the_generated_one():
    committed = (REPO / "infra/monitoring/grafana/dashboards/resilience-run.json").read_text()
    assert committed == dashboard.render(), "regenerate it: python -m resilience_tests.observability.dashboard --out ..."


def test_every_kind_of_marker_has_its_own_coloured_switchable_layer():
    """One layer per kind (each selects one tag), so phases, T0/T1, milestones and the whole run
    can be told apart by colour and switched on or off; a kind without a layer would vanish."""
    from resilience_tests.observability.observer import MARKED
    layers = dashboard.build()["annotations"]["list"]
    tags = [layer["target"]["tags"][3] for layer in layers]
    assert len(tags) == len(set(tags)) and set(tags) == {kind for _, kind in MARKED} | {"run"}
    colour = {layer["target"]["tags"][3]: layer["iconColor"] for layer in layers}
    assert colour["phase_start"] == "blue" and colour["t0"] == colour["t1"] == "red"
    assert colour["fault_milestone"] == "orange" and colour["run"] == "#8e8e8e"
    assert all(layer["enable"] and not layer["hide"] for layer in layers)


def test_the_dashboard_shows_every_series_a_run_keeps_for_the_selected_node():
    board = dashboard.build()
    graphs = [p for p in board["panels"] if p["type"] == "timeseries"]
    shown = {name for _, items in dashboard.ROWS for name, _, _ in items}
    assert shown == {name for name, _ in SNAPSHOT_QUERIES}
    assert all('env="$env",node="$node"' in g["targets"][0]["expr"] for g in graphs)
    assert board["uid"] == LAB.observability.grafana_dashboard_uid       # the report's link opens it
    layers = board["annotations"]["list"]
    assert all(layer["target"]["tags"][:3] == ["resilience", "$env", "$node"] for layer in layers)
    db = next(v for v in board["templating"]["list"] if v["name"] == "db")
    assert db["current"]["value"] == "resilience" and "template" in db["regex"]     # not "postgres" by default


# --- a whole run: context only ----------------------------------------------------------------

def test_a_run_against_an_unreachable_stack_is_judged_exactly_as_before(env, monkeypatch, tmp_path):
    def unreachable(profile, node, *, run_id, scenario):
        assert LAB.observability is not None
        return StackObserver(LAB.observability, env=profile.name, node=node, run_id=run_id, scenario=scenario,
                             grafana=FakeGrafana(healthy=False), prometheus=FakePrometheus(ready=False))
    monkeypatch.setattr(orch, "observer_for", unreachable)
    results = run_scenario(env, "NL-C-01")
    assert results["status"] == "passed", why(results)
    assert any("Observability (context only" in d for d in results["disclosures"])
    assert results["facts"]["observability"]["problems"]


def test_a_run_with_the_stack_gets_markers_metrics_and_a_report_link(env, monkeypatch):
    grafana, prometheus = FakeGrafana(), FakePrometheus()

    def stack(profile, node, *, run_id, scenario):
        assert LAB.observability is not None
        return StackObserver(LAB.observability, env=profile.name, node=node, run_id=run_id, scenario=scenario,
                             grafana=grafana, prometheus=prometheus)
    monkeypatch.setattr(orch, "observer_for", stack)
    results = run_scenario(env, "NL-C-01")
    assert results["status"] == "passed", why(results)
    texts = [n["text"] for n in grafana.notes]
    assert texts[0].startswith("run start: NL-C-01") and "T0: fault injected" in texts
    assert sum(t.startswith("phase: ") for t in texts) >= 8 and texts[-1].startswith("NL-C-01: passed")
    obs = results["facts"]["observability"]
    assert "metrics_file" not in obs and not Path(results["evidence_dir"], METRICS_FILE).exists()   # off by default
    from resilience_tests.reporting.html import render_run
    page = render_run(results["evidence_dir"]).read_text()
    assert "open this run in Grafana" in page and obs["grafana_link"].split("?")[0] in page


# --- the live watcher (observability/watch.py) ---------------------------------------------

def test_the_watcher_narrates_a_run_in_plain_words(tmp_path):
    from resilience_tests.observability import watch
    run_dir = tmp_path / "NL-N-I03a-run1"
    stream = EventStream(run_dir / "events.jsonl")
    stream.emit("orchestrator", "run_start", scenario="NL-N-I03a", run_id="NL-N-I03a-run1")
    stream.emit("orchestrator", "phase_start", phase="baseline")
    stream.emit("workload", "start", profile="tpcc", concurrency=64, rate_tps=60.0)
    for _ in range(3):                                           # single slow writes: not an outage
        stream.emit("write_prober", "write_probe", ok=False, error="TimeoutError")
        stream.emit("write_prober", "write_probe", ok=True)
    for _ in range(watch.OUTAGE_PROBES):                         # several in a row: an outage
        stream.emit("write_prober", "write_probe", ok=False, error="ConnectionRefusedError")
    stream.emit("write_prober", "write_probe", ok=True)
    for i in range(30):
        stream.emit("log_tailer", "log_line", line=f"LOG:  process {i} still waiting for RowExclusiveLock on relation 1")
    stream.emit("workload", "sample", tps=60.0, p99_ms=900.0, errors=0, indeterminate=0)
    stream.emit("injector", "t0", fault="lock_pileup", blocker="long_read")
    issued = stream.emit("orchestrator", "phase_end", phase="baseline", outcome="ok", error=None).t_mono_ns
    stream.emit("orchestrator", "fault_milestone", text="schema change issued", at_mono_ns=issued - 2 * 10**9)
    stream.emit("orchestrator", "validate_step", text="integrity check started on the whole database (at most 1800 s)")
    stream.emit("orchestrator", "validate_step", text="integrity check done: 0 structural errors, 0 checksum failures",
                took_s=574.4)
    stream.emit("orchestrator", "run_end")
    stream.close()
    (run_dir / "results.json").write_text(json.dumps({
        "status": "passed", "verdict": {"results": [{"predicate": "rpo_txn == 0", "outcome": "pass", "values": {"rpo_txn": 0}}]},
        "facts": {"observability": {"grafana_link": "http://grafana:3000/d/resilience-run?from=1"}}}))
    out = io.StringIO()
    watch.follow(run_dir, every_s=0.0, out=out)
    text = out.getvalue()
    assert "RUN START  NL-N-I03a" in text and ">> PHASE baseline started" in text and "<< PHASE baseline ok" in text
    assert "WORKLOAD started: profile tpcc, 64 workers, target 60.0 TPS" in text
    assert text.count("OUTAGE") == 1 and "WRITES ACCEPTED AGAIN" in text            # one outage, not every slow write
    assert "slow write probes 3" in text and "lock-wait log lines 30" in text          # counted, not flooded
    assert text.count("still waiting") == 1
    assert "T0  FAULT INJECTED  fault=lock_pileup, blocker=long_read" in text and "FAULT  schema change issued" in text
    assert "validate: integrity check started on the whole database (at most 1800 s)" in text
    assert "validate: integrity check done: 0 structural errors, 0 checksum failures  (574 s)" in text
    assert "STATUS: PASSED" in text and "Grafana: http://grafana:3000/d/resilience-run" in text


def test_the_watcher_prints_a_moment_when_it_happened_not_when_it_was_announced():
    from resilience_tests.observability.watch import happened_at
    e = {"t_mono_ns": 100 * 10**9, "t_wall": 5_000.0, "source": "orchestrator", "kind": "t1",
         "data": {"t1_mono_ns": 94 * 10**9}}
    assert happened_at(e) == pytest.approx(4_994.0)
    assert happened_at({**e, "data": {}}) == 5_000.0


def test_the_watcher_follows_scenario_runs_not_the_common_workloads_folder(tmp_path):
    from resilience_tests.observability import watch
    workload = tmp_path / "workload-attach-check-1"
    scenario = tmp_path / "NL-N-I03a-run1"
    for d in (scenario, workload):                      # the workload's folder is written to last
        d.mkdir()
        (d / "events.jsonl").write_text("{}\n")
    (workload / "service.log").write_text("")             # all it has at its start (no service.json yet)
    assert watch.newest(tmp_path) == scenario
