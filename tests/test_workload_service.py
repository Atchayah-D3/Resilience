"""The common long-running workload (specs/007-common-oltp-workload), without a database.

The settings file, the metrics and baseline rules (pure), the guards that protect a running
workload from scenarios and the TPC-C commands, and the service's run itself: the real TPC-C
engine against a scripted session, for a few seconds.
"""

from __future__ import annotations

import asyncio
import fcntl
import json
import os
from pathlib import Path

import pytest

from catalog.schema import Workload
from resilience_tests.adapters.base import DatabaseSession
from resilience_tests.control.profile import load_profile
from resilience_tests.execution.workload import contract as c
from resilience_tests.execution.workload.markers import read_journal
from resilience_tests.execution.workload.service import baseline as bl
from resilience_tests.execution.workload.service import runner as runner_mod
from resilience_tests.execution.workload.service import state as st
from resilience_tests.execution.workload.service.config import BaselineRules, ServiceConfig, load_config
from resilience_tests.execution.workload.service.metrics import IntervalSums, OutageTracker, Second, period_summary
from resilience_tests.execution.workload.service.runner import ServiceRunner
from resilience_tests.execution.workload.tpcc import engine as engine_mod
from resilience_tests.execution.workload.tpcc.load import Refused, check_target
from resilience_tests.execution.workload.tpcc.profile import TpccProfile
from resilience_tests.execution.workload.tpcc.samples import merge_raws
from resilience_tests.observability.event_stream import EventStream
from tests.test_adapter_seam import NODE, FakeAdapter
from tests.test_orchestrator import env, run  # noqa: F401  (pytest fixture)

REPO = Path(__file__).resolve().parents[1]
ENV = load_profile("e2-dedicated-vm")


def _workload(**extra):
    return Workload(profile="tpcc", concurrency=4, transaction_markers=True, rate_tps=200,
                    tpcc={"warehouses": 8}, **extra)


# --- the settings file ---------------------------------------------------------------------------


def test_the_shipped_settings_file_is_valid():
    cfg = load_config(REPO / "workloads" / "oltp-common.yaml")
    assert cfg.workload.profile == "tpcc" and cfg.workload.transaction_markers
    assert cfg.duration_s == 7200 and cfg.warmup_s < cfg.duration_s
    assert cfg.workload.rate_fraction_of_capacity == 0.6 and cfg.workload.latency_from == "intended_start"


def test_any_scenario_workload_profile_can_be_the_common_workload():
    for profile in ("oltp_write_heavy", "mixed"):
        cfg = ServiceConfig(id="t", duration_s=100, warmup_s=10,
                            workload=Workload(profile=profile, concurrency=4, transaction_markers=True, rate_tps=10))
        assert cfg.workload.profile == profile


@pytest.mark.parametrize("change, message", [
    ({"workload": {"profile": "idle", "concurrency": 4, "transaction_markers": True}}, "idle"),
    ({"workload": {"profile": "oltp_write_heavy", "concurrency": 4, "transaction_markers": True, "rate_tps": 10,
                   "history": "list_append"}}, "Elle history"),
    ({"workload": {"profile": "tpcc", "concurrency": 4, "transaction_markers": False, "rate_tps": 10,
                   "tpcc": {"warehouses": 8}}}, "transaction_markers"),
    ({"warmup_s": 100}, "shorter than duration_s"),
    ({"worker_processes": 5}, "more than the clerks"),
    ({"unknown_field": 1}, "unknown_field"),
])
def test_a_bad_settings_file_is_refused_before_anything_starts(change, message):
    raw = {"id": "t", "duration_s": 100, "warmup_s": 10, "workload": _workload().model_dump(mode="json"), **change}
    with pytest.raises(ValueError, match=message):
        ServiceConfig.model_validate(raw)


# --- metrics (pure) ------------------------------------------------------------------------------


def sec(t, commits=100, p99=10.0, errors=0, unknown=0, drops=0, connect_failures=0, interval=1.0):
    return Second(elapsed_s=t, interval_s=interval, commits=commits, p99_ms=p99, errors=errors, indeterminate=unknown,
                  drops=drops, reconnects=0, connect_failures=connect_failures, business_rollbacks=1)


def test_an_outage_is_seconds_without_a_commit_with_start_alert_and_end():
    tr = OutageTracker(rate_tps=100, alert_s=3)
    assert tr.update(sec(1), 1001.0) == []
    kinds = [k for k, _ in tr.update(sec(2, commits=0, connect_failures=40), 1002.0)]
    assert kinds == ["outage_start"] and tr.open_outage()["connect_failures"] == 40
    assert tr.update(sec(3, commits=0), 1003.0) == []
    assert [k for k, _ in tr.update(sec(4, commits=0), 1004.0)] == ["outage_long"]       # 3 s: alert once
    assert tr.update(sec(5, commits=0), 1005.0) == []
    (kind, data), = tr.update(sec(6, commits=50), 1006.0)
    assert kind == "outage_end" and data["seconds"] == 4.0 and data["started_wall"] == 1001.0
    assert tr.open_outage() is None and len(tr.outages) == 1


def test_no_outage_detection_at_a_rate_where_empty_seconds_are_normal():
    tr = OutageTracker(rate_tps=2, alert_s=3)
    assert not tr.enabled and tr.update(sec(1, commits=0), 1.0) == []


def test_interval_sums_add_error_kinds_business_rollbacks_and_commits_per_type():
    sums = IntervalSums()
    sample = {"error_kinds": {"server_40P01": 2, "timeout": 1},
              c.BY_TYPE: {c.NEW_ORDER: {"commits": 5, "business_rollbacks": 1}, c.PAYMENT: {"commits": 4}}}
    sums.add(sample)
    sums.add(sample)
    out = sums.take()
    assert out["error_kinds"] == {"server_40P01": 4, "timeout": 2} and out["business_rollbacks"] == 2
    assert out["commits_by_type"][c.NEW_ORDER] == 10 and out["samples"] == 2
    assert sums.take()["samples"] == 0                       # the next interval starts empty


def test_period_summary_uses_exact_sums_and_labels_the_p99_figures():
    secs = [sec(t, commits=10, p99=float(t)) for t in range(1, 21)]
    p = period_summary(secs, period_s=10)
    assert [x["commits"] for x in p] == [100, 100]         # seconds ending at 1..10 | 11..20
    assert p[0]["tps"] == 10.0 and p[0]["p99_max_1s_ms"] == 10.0 and p[0]["p99_median_1s_ms"] == 5.5


# --- baseline rules (pure) -----------------------------------------------------------------------

RULES = BaselineRules(tps_window_s=10, tps_share_min=0.95, p99_window_s=5, p99_limit_ms=100, error_share_max=0.001,
                      memory_growth_max=0.10, memory_from_s=0)
GOOD_END = {"rpo_txn": 0, "phantom": 0, "partial_commits": 0, "aborted_markers_present": 0,
            "tpcc_consistency_violations": 0}


def judge(seconds, end=GOOD_END, memory=((0, 100_000_000), (60, 105_000_000)), **kw):
    args = dict(seconds=seconds, measure_start_s=0.0, rate_tps=100, rules=RULES, memory=memory, end=end,
                driver_limited=False, full_duration=True, ran_s=60, duration_s=60)
    args.update(kw)
    checks = bl.evaluate(**args)
    return {ch["name"].split(":")[0]: ch["status"] for ch in checks}, bl.passed(checks)


def test_a_steady_run_is_a_valid_baseline():
    statuses, ok = judge([sec(t) for t in range(1, 61)])
    assert ok, statuses


def test_one_slow_minute_window_fails_the_throughput_rule():
    seconds = [sec(t, commits=50 if 21 <= t <= 30 else 100) for t in range(1, 61)]
    statuses, ok = judge(seconds)
    assert statuses["throughput"] == bl.FAIL and not ok


def test_a_single_slow_second_passes_the_p99_window_rule_but_a_slow_stretch_fails_it():
    spike = [sec(t, p99=900.0 if t == 12 else 10.0) for t in range(1, 61)]
    assert judge(spike)[0]["latency"] == bl.PASS                # median of 5 seconds ignores one spike
    stretch = [sec(t, p99=900.0 if 11 <= t <= 13 else 10.0) for t in range(1, 61)]
    assert judge(stretch)[0]["latency"] == bl.FAIL


def test_errors_unknown_outcomes_and_dropped_connections_fail():
    statuses, ok = judge([sec(t, errors=10 if t == 5 else 0) for t in range(1, 61)])     # 10 of ~6,070: 0.16%
    assert statuses["errors at or under 0.10% of all transactions"] == bl.FAIL and not ok
    statuses, ok = judge([sec(t, drops=1 if t == 5 else 0) for t in range(1, 61)])
    assert statuses["errors at or under 0.10% of all transactions"] == bl.PASS        # a drop is not an error
    assert statuses["no dropped connections"] == bl.FAIL and not ok


def test_missing_evidence_is_not_measured_and_never_passes():
    statuses, ok = judge([sec(t) for t in range(1, 61)], end={"rpo_txn": 0})     # phantom, TPC-C checks unknown
    assert statuses["no lost commit, no phantom"] == bl.NOT_MEASURED and not ok
    statuses, ok = judge([sec(t) for t in range(1, 61)], memory=())
    assert statuses["harness memory grows at most 10% from 0 s to the end"] == bl.NOT_MEASURED and not ok
    assert not judge([])[1]


def test_a_lost_commit_a_short_run_or_memory_growth_fails():
    seconds = [sec(t) for t in range(1, 61)]
    assert not judge(seconds, end={**GOOD_END, "rpo_txn": 1})[1]
    assert not judge(seconds, full_duration=False)[1]
    assert not judge(seconds, memory=((0, 100_000_000), (60, 130_000_000)))[1]
    assert not judge(seconds, driver_limited=True)[1]


# --- errors by kind in the engine's samples ------------------------------------------------------


def test_samples_merge_error_kinds_across_worker_processes():
    raw = {"elapsed": 1.0, "totals": {f: 0 for f in ("commits", "errors", "indeterminate", "drops", "reconnects",
                                                     "connect_failures")},
           "latencies": [], "journal_ms": [], "types": {}, "groups": {}}
    a = {**raw, "process": 0, "error_kinds": {"server_40P01": 1, "timeout": 2}}
    b = {**raw, "process": 1, "error_kinds": {"timeout": 1, "connection_lost": 3}}
    assert merge_raws([a, b])["error_kinds"] == {"connection_lost": 3, "server_40P01": 1, "timeout": 3}


def test_the_engine_counts_errors_by_kind(monkeypatch, tmp_path):
    from tests.test_tpcc_engine import ScriptedAdapter, ScriptedSession
    from collections import Counter

    ScriptedSession.log, ScriptedSession.counts = [], Counter()
    monkeypatch.setattr(engine_mod, "SAMPLE_INTERVAL_S", 0.2)
    profile = TpccProfile(_workload())
    profile.set_run_constants(100)

    async def go():
        stream = EventStream(tmp_path / "events.jsonl")
        from resilience_tests.execution.workload.markers import MarkerJournals
        journals = MarkerJournals(tmp_path)
        drv = profile.make_driver(ScriptedAdapter(NODE), journals, stream)
        await drv.start()
        await asyncio.sleep(1.2)
        await drv.stop()
        journals.close()
        return [e.data for e in stream.events() if e.kind == "sample"]

    samples = asyncio.run(go())
    kinds: Counter = Counter()
    for s in samples:
        kinds.update(s["error_kinds"])
    # the scripted session: Delivery every 3rd a deadlock (40P01), Payment every 15th connection lost
    assert kinds["server_40P01"] > 0 and kinds["connection_lost"] > 0
    assert sum(s["errors"] for s in samples) == kinds["server_40P01"]
    assert sum(s["indeterminate"] for s in samples) == kinds["connection_lost"]


def test_a_file_only_event_stream_keeps_nothing_in_memory(tmp_path):
    stream = EventStream(tmp_path / "e.jsonl", keep_in_memory=False)
    for i in range(3):
        stream.emit("workload", "sample", n=i)
    assert stream._events == [] and [e.data["n"] for e in stream.events()] == [0, 1, 2]
    stream.close()


# --- the guards ------------------------------------------------------------------------------------


def _env(tmp_path):
    return ENV.model_copy(update={"driver_host": ENV.driver_host.model_copy(update={"run_dir": str(tmp_path)})})


def test_a_live_pointer_or_a_fresh_target_lock_means_a_workload_is_running(tmp_path, monkeypatch):
    env = _env(tmp_path)
    node = env.nodes[0]
    assert asyncio.run(st.running_workload(env, node)) is None
    st.write_pointer(env, node, tmp_path / "workload-x", os.getpid())
    with pytest.raises(st.WorkloadRunning, match="common workload is running"):
        asyncio.run(st.refuse_if_running(env, node, "scenario NL-C-01"))
    st.write_pointer(env, node, tmp_path / "workload-x", 2**22 + 12345)          # a process that is gone
    assert asyncio.run(st.running_workload(env, node)) is None

    async def held(n, path, stale):
        return {"run_id": "workload-y", "harness_host": "other-host", "heartbeat_age_s": 3}
    monkeypatch.setattr(st, "remote_lock_holder", held)
    with pytest.raises(st.WorkloadRunning, match="other-host"):
        asyncio.run(st.refuse_if_running(env, node, "scenario NL-C-01"))


def test_a_target_that_cannot_be_checked_is_refused_not_assumed_free(tmp_path, monkeypatch):
    async def unreachable(n, path, stale):
        raise OSError("ssh: connect timed out")
    monkeypatch.setattr(st, "remote_lock_holder", unreachable)
    with pytest.raises(st.WorkloadRunning, match="could not check"):
        asyncio.run(st.refuse_if_running(_env(tmp_path), NODE, "this command"))


def test_tpcc_commands_that_write_are_refused_while_the_workload_runs(tmp_path):
    env = _env(tmp_path)
    st.write_pointer(env, env.nodes[0], tmp_path / "workload-x", os.getpid())
    with pytest.raises(Refused, match="common workload is running"):
        asyncio.run(check_target(env, env.nodes[0], FakeAdapter(NODE)))


def test_a_scenario_run_is_refused_while_the_workload_runs(env):  # noqa: F811
    st.write_pointer(env, env.nodes[0], Path(env.driver_host.run_dir) / "workload-x", os.getpid())
    results = run(env, "NL-C-01")
    assert results["status"] == "aborted"
    assert "common workload is running" in results["error"]


def test_a_scenario_holding_the_target_is_seen_through_its_lock_file(tmp_path):
    env = _env(tmp_path)
    node = env.nodes[0]
    assert asyncio.run(st.scenario_holding_target(env, node)) is None
    path = tmp_path / f".lock-{env.name}-{node.name}"
    with path.open("a+") as fh:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        fh.write("NL-C-01-run pid=1\n")
        fh.flush()
        assert "NL-C-01-run" in asyncio.run(st.scenario_holding_target(env, node))
    assert asyncio.run(st.scenario_holding_target(env, node)) is None             # released


# --- the service's run -----------------------------------------------------------------------------


class Shop:
    """A database that commits everything; it can be 'down' (connections refused, open ones lost)."""
    committed: set = set()
    down = False


class ShopSession(DatabaseSession):
    def __init__(self) -> None:
        self._closed = False

    async def commit_marker(self, seq, marker_id):
        raise AssertionError("not used by the TPC-C engine")

    async def tpcc_execute(self, txn_type, inputs, marker):
        await asyncio.sleep(0.001)
        if Shop.down:
            self._closed = True
            return ("unknown", {}, None)
        if marker is not None:
            Shop.committed.add(marker[1])
        return ("committed", {}, None)

    async def try_write(self):
        return True

    async def ping(self):
        return True

    async def close(self):
        self._closed = True

    @property
    def is_closed(self):
        return self._closed


class ShopAdapter(FakeAdapter):
    lose_one = False

    async def session(self, endpoint=None, timeout_s=5.0, **kwargs):
        if Shop.down:
            raise ConnectionRefusedError("down")
        return ShopSession()

    async def marker_ids(self):
        ids = set(Shop.committed)
        if self.lose_one and ids:
            ids.pop()
        return ids


class ShopProfile(TpccProfile):
    """The TPC-C profile without its data steps (they need a database)."""

    calls: list = []

    async def reset_data(self, adapter, extend_phase=None, fresh=False):
        ShopProfile.calls.append(("reset_data", fresh))
        return {"tpcc_reset": {"reloaded": False}, "_disclosures": ["data used as it is"]}

    async def prepare(self, adapter):
        ShopProfile.calls.append(("prepare",))
        self.set_run_constants(100)
        if self.workload.rate_fraction_of_capacity is not None:      # as resolve_rate: a share of 400 TPS
            rate = self.workload.rate_fraction_of_capacity * 400
            self.workload = self.workload.model_copy(update={"rate_tps": rate, "rate_fraction_of_capacity": None})
            self.rate = {"fraction": 0.5, "capacity_tps": 400, "rate_tps": rate}
        return {"tpcc_data": {"warehouses": 8}, "_disclosures": []}

    async def consistency_checks(self, adapter, run_dir):
        return {"partial_commits": 0, "aborted_markers_present": 0, "tpcc_consistency_violations": 0,
                "_facts": {}, "_not_measured": {}}


class FakeLock:
    def __init__(self):
        self.acquired = self.released = False
        self.lost = None

    async def acquire(self):
        self.acquired = True
        return {"path": "/var/tmp/resilience-workload-5433.lock", "acquired": True}

    def start_heartbeat(self):
        pass

    async def release(self):
        self.released = True
        return {"released": True}


def _config(**kw):
    rules = BaselineRules(tps_window_s=1, tps_share_min=0.5, p99_window_s=1, p99_limit_ms=1000,
                          error_share_max=0.0, memory_growth_max=10.0, memory_from_s=0)
    args = dict(id="test", duration_s=3.0, warmup_s=1.0, metrics_interval_s=1, status_interval_s=0.2,
                no_commit_alert_s=1.0, workload=_workload(), baseline=rules)
    args.update(kw)
    return ServiceConfig(**args)


def service(tmp_path, monkeypatch, cfg=None, adapter=None, scenario_holder=None):
    Shop.committed, Shop.down = set(), False
    ShopProfile.calls = []
    monkeypatch.setattr(runner_mod, "LOOP_S", 0.05)
    monkeypatch.setattr(runner_mod, "MEMORY_EVERY_S", 0.3)
    env = _env(tmp_path)
    lock = FakeLock()

    async def target_check(e, n, a):
        return "BDB-QA-U22-30"

    async def scenario_check(e, n):
        return scenario_holder

    cfg = cfg or _config()
    r = ServiceRunner(env, env.nodes[0], cfg, tmp_path / "workload-test", adapter=adapter or ShopAdapter(NODE),
                      lock=lock, target_check=target_check, scenario_check=scenario_check)
    r.profile = ShopProfile(cfg.workload)
    return r, lock, env


def _lines(path):
    return [json.loads(x) for x in path.read_text().splitlines() if x.strip()]


def test_the_service_runs_its_duration_and_judges_a_valid_baseline(tmp_path, monkeypatch):
    r, lock, env = service(tmp_path, monkeypatch)
    summary = asyncio.run(r.run())
    folder = tmp_path / "workload-test"
    assert summary["state"] == st.FINISHED, summary
    assert summary["baseline_valid"], json.dumps(summary["baseline_checks"], indent=1)
    # the data steps ran once, at the start
    assert ShopProfile.calls == [("reset_data", False), ("prepare",)]
    # what it wrote while it ran
    seconds = _lines(folder / st.METRICS_1S_FILE)
    assert {s["part"] for s in seconds} == {"warmup", "measured"} and all("error_kinds" in s for s in seconds)
    intervals = _lines(folder / st.metrics_interval_file(1))
    assert intervals and {"tps", "p99_ms", "error_kinds", "commits_by_type"} <= set(intervals[-1])
    assert st.read_status(folder)["state"] == st.FINISHED
    # the settings used, the end checks, the lock and pointer
    assert summary["settings"]["config"]["id"] == "test" and summary["settings"]["workload_used"]["rate_tps"] == 200
    assert summary["end_checks"]["rpo_txn"] == 0 and summary["end_checks"]["markers"]["acked"] > 0
    assert lock.acquired and lock.released and st.read_pointer(env, env.nodes[0]) is None
    assert (folder / st.SUMMARY_TEXT_FILE).read_text().count("PASS") == len(summary["baseline_checks"])
    assert sum(len(read_journal(p).records) for p in folder.glob("acked*.jrnl")) == len(Shop.committed)
    info = st.read_info(folder)
    assert info["settings"]["workload_used"]["rate_tps"] == 200 and info["tpcc"]["run_constants"] is not None
    assert info["measure_start_planned_wall"] - info["clerks_started_wall"] == pytest.approx(1.0)
    assert info["files"]["intervals"] == st.metrics_interval_file(1)


def test_the_stop_command_ends_the_run_and_the_end_checks_still_run(tmp_path, monkeypatch):
    r, lock, env = service(tmp_path, monkeypatch, cfg=_config(duration_s=60.0))
    folder = tmp_path / "workload-test"

    async def go():
        task = asyncio.create_task(r.run())
        await asyncio.sleep(2.0)
        assert st.read_status(folder)["state"] in (st.WARMING_UP, st.RUNNING)
        st.request_stop(folder, "stop command")
        return await task

    summary = asyncio.run(go())
    assert summary["state"] == st.STOPPED and summary["stop_requested_by"] == "stop command"
    assert not summary["baseline_valid"]                         # not the full duration
    assert summary["end_checks"]["rpo_txn"] == 0 and lock.released


def test_a_lost_commit_is_found_at_the_end(tmp_path, monkeypatch):
    adapter = ShopAdapter(NODE)
    adapter.lose_one = True
    r, _, _ = service(tmp_path, monkeypatch, adapter=adapter)
    summary = asyncio.run(r.run())
    assert summary["end_checks"]["rpo_txn"] == 1 and not summary["baseline_valid"]
    lost = [ch for ch in summary["baseline_checks"] if ch["name"].startswith("no lost commit")][0]
    assert lost["status"] == bl.FAIL


def test_a_database_outage_does_not_stop_the_workload_and_is_reported(tmp_path, monkeypatch):
    monkeypatch.setattr(engine_mod, "RECONNECT_PAUSE_S", 0.05)
    r, _, _ = service(tmp_path, monkeypatch, cfg=_config(duration_s=6.0))
    folder = tmp_path / "workload-test"

    async def go():
        task = asyncio.create_task(r.run())
        await asyncio.sleep(2.5)
        Shop.down = True
        await asyncio.sleep(2.2)
        Shop.down = False
        return await task

    summary = asyncio.run(go())
    assert summary["state"] == st.FINISHED                        # the clerks reconnected and went on
    assert summary["outages"] and summary["outages"][0]["seconds"] >= 1.0
    kinds = [e["kind"] for e in _lines(folder / st.EVENTS_FILE) if e["source"] == "service"]
    assert "outage_start" in kinds and "outage_end" in kinds and "outage_long" in kinds
    assert summary["totals"]["whole_run"]["connect_failures"] > 0 and not summary["baseline_valid"]


def test_a_failing_workload_ends_the_run_as_failed(tmp_path, monkeypatch):
    r, lock, _ = service(tmp_path, monkeypatch, cfg=_config(duration_s=60.0))

    async def go():
        task = asyncio.create_task(r.run())
        await asyncio.sleep(1.5)
        r.driver._fail("tpcc clerk 0: OSError: No space left on device (journal)")
        return await task

    summary = asyncio.run(go())
    assert summary["state"] == st.FAILED and "No space left" in summary["failure"]
    assert "rpo_txn" in summary["end_checks"] and lock.released     # the evidence is still checked


def test_the_service_refuses_to_start_under_a_running_scenario(tmp_path, monkeypatch):
    r, lock, _ = service(tmp_path, monkeypatch, scenario_holder="run NL-N-I03a-20261118 on this harness host")
    summary = asyncio.run(r.run())
    assert summary["state"] == st.FAILED and "scenario run holds" in summary["failure"]
    assert not lock.acquired and ShopProfile.calls == []            # nothing emptied, nothing loaded
    assert summary["end_checks"] == {}


def test_with_a_share_of_capacity_the_rate_and_outage_detection_follow_the_stored_maximum(tmp_path, monkeypatch):
    w = Workload(profile="tpcc", concurrency=4, transaction_markers=True, rate_fraction_of_capacity=0.5,
                 tpcc={"warehouses": 8})
    r, _, _ = service(tmp_path, monkeypatch, cfg=_config(workload=w))
    assert not r.outages.enabled                                   # the rate is not known before the start
    summary = asyncio.run(r.run())
    assert summary["settings"]["workload_used"]["rate_tps"] == 200 and summary["settings"]["rate"]["capacity_tps"] == 400
    assert r.outages.enabled and summary["baseline_valid"], json.dumps(summary["baseline_checks"], indent=1)


# --- any workload profile; the harness tables split; the client functions ---------------------------


def test_the_service_runs_the_custom_marker_workload_of_most_scenarios(tmp_path, monkeypatch):
    w = Workload(profile="oltp_write_heavy", concurrency=4, transaction_markers=True, rate_tps=200)
    # 5 s: the custom driver writes no part-second sample at its stop, so a 3 s run would leave a
    # single measured second
    r, lock, _ = service(tmp_path, monkeypatch, cfg=_config(workload=w, duration_s=5.0), adapter=FakeAdapter(NODE))
    from resilience_tests.execution.workload.hooks import hooks_for
    r.profile = hooks_for(w)                                  # the custom workload's (empty) hooks
    summary = asyncio.run(r.run())
    assert summary["state"] == st.FINISHED and summary["end_checks"]["rpo_txn"] == 0
    names = [ch["name"] for ch in summary["baseline_checks"]]
    assert not any("TPC-C" in n for n in names)              # TPC-C checks only for the TPC-C workload
    assert summary["baseline_valid"], json.dumps(summary["baseline_checks"], indent=1)
    assert st.read_info(tmp_path / "workload-test")["tpcc"] is None


class RecordingConn:
    def __init__(self, log):
        self.log = log

    async def execute(self, sql, *args):
        self.log.append(" ".join(sql.split()))

    async def close(self):
        pass


@pytest.mark.parametrize("method", ["prepare_harness_state", "prepare_workload_state", "prepare_scenario_state"])
def test_the_harness_step_splits_into_the_workloads_part_and_the_scenarios_part(monkeypatch, method):
    from resilience_tests.adapters.postgresql import adapter as pg

    a = pg.PostgreSQLAdapter(NODE)
    log: list[str] = []

    async def connect(*args, **kwargs):
        return RecordingConn(log)

    async def no_grant():
        log.append("GRANT")
    monkeypatch.setattr(a, "_connect", connect)
    monkeypatch.setattr(a, "_grant_harness_roles", no_grant)
    asyncio.run(getattr(a, method)())
    text = "\n".join(log)
    truncates = [x for x in log if x.startswith("TRUNCATE")]
    if method == "prepare_harness_state":                   # today's init: unchanged, everything
        assert truncates == [" ".join(pg.TRUNCATE.split())] and "GRANT" in log and "DROP TABLE" in text
        assert "churn" in text and "VACUUM" in text
    elif method == "prepare_workload_state":                # the workload's start: its own tables only
        assert truncates == [pg.WORKLOAD_TRUNCATE] and "DROP" not in text and "GRANT" not in log
        assert "resilience.markers" in truncates[0] and "VACUUM" in text
    else:                                                   # a scenario attached: never the workload's tables
        assert truncates == [pg.SCENARIO_TRUNCATE] and "GRANT" in log and "DROP TABLE" in text
        assert "markers" not in truncates[0] and "churn" not in "\n".join(x for x in log if "HARNESS" not in x
                                                                          and not x.startswith("CREATE"))
    # the two parts together are exactly today's step
    assert set(pg.TRUNCATE.replace("TRUNCATE ", "").replace(",", " ").split()) == \
        set((pg.WORKLOAD_TRUNCATE + " " + pg.SCENARIO_TRUNCATE).replace("TRUNCATE ", "").replace(",", " ").split())


def test_client_start_runs_the_service_in_the_background_and_refuses_a_second(tmp_path, monkeypatch):
    from resilience_tests.execution.workload.service import client

    started = []

    class FakePopen:
        def __init__(self, cmd, **kwargs):
            started.append((cmd, kwargs))
    monkeypatch.setattr(client.subprocess, "Popen", FakePopen)
    env = _env(tmp_path)
    monkeypatch.setattr(client, "load_profile", lambda name: env)
    folder = client.start("e2-dedicated-vm", REPO / "workloads" / "oltp-common.yaml")
    (cmd, kwargs), = started
    assert folder.is_dir() and folder.name.startswith("workload-oltp-common-")
    assert cmd[2:4] == ["resilience_tests.execution.workload.service.cli", "_run"] and str(folder) in cmd
    assert kwargs["start_new_session"] is True                 # outlives the caller's terminal
    st.write_pointer(env, env.nodes[0], folder, os.getpid())
    with pytest.raises(client.ServiceFailed, match="already running"):
        client.start("e2-dedicated-vm", REPO / "workloads" / "oltp-common.yaml")


def test_client_waits_until_running_and_reports_an_early_end(tmp_path):
    from resilience_tests.execution.workload.service import client

    folder = tmp_path / "w"
    st.write_status(folder, {"state": st.WARMING_UP, "step": "warm-up", "pid": os.getpid()})
    with pytest.raises(TimeoutError, match="not running"):
        client.wait_until_running(folder, 0.3, poll_s=0.1)
    st.write_status(folder, {"state": st.RUNNING, "pid": os.getpid()})
    assert client.wait_until_running(folder, 1, poll_s=0.1)["state"] == st.RUNNING
    st.write_status(folder, {"state": st.FAILED, "failure": "no capacity record", "pid": os.getpid()})
    with pytest.raises(client.ServiceFailed, match="no capacity record"):
        client.wait_until_running(folder, 1, poll_s=0.1)
    st.write_status(folder, {"state": st.WARMING_UP, "pid": 2**22 + 12345})
    with pytest.raises(client.ServiceFailed, match="process is gone"):
        client.wait_until_running(folder, 1, poll_s=0.1)


def test_client_stop_asks_once_and_returns_the_summary(tmp_path):
    from resilience_tests.execution.workload.service import client

    folder = tmp_path / "w"
    st.write_status(folder, {"state": st.FINISHED, "pid": os.getpid()})
    (folder / st.SUMMARY_FILE).write_text(json.dumps({"state": "finished", "baseline_valid": True}))
    assert client.stop(folder, wait_s=1, poll_s=0.1)["baseline_valid"] is True
    assert st.stop_requested(folder) is None                    # already finished: nothing asked
    st.write_status(folder, {"state": st.RUNNING, "pid": os.getpid()})
    assert client.stop(folder, wait_s=0.3, poll_s=0.1) is None   # still running after the wait
    assert st.stop_requested(folder)["by"] == "runner"
