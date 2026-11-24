"""The standard way to run scenarios against the common workload: one pytest session, the workload
started once, every scenario attached to it in the order given, a cooldown and a health check
between them, the workload stopped once at the end (control/common_workload.py and the session
fixture in resilience_tests/test_scenarios.py), and the whole session on index.html."""

from __future__ import annotations

from pathlib import Path

import pytest

from resilience_tests.control import common_workload as cw
from resilience_tests.control.common_workload import CommonWorkload
from resilience_tests.control.profile import load_profile

REPO = Path(__file__).resolve().parents[1]
ENV = load_profile("e2-u22-3")


@pytest.fixture
def calls(monkeypatch, tmp_path):
    log: list = []
    folder = tmp_path / "workload-1"

    def start(env, config, node=None):
        log.append(("start", config))
        return folder

    def wait_until_running(f, timeout_s):
        log.append(("wait_until_running",))

    def stop(f, by="runner"):
        log.append(("stop", by))
        return {"state": "stopped", "baseline_valid": False,
                "end_checks": {"rpo_txn": 0, "phantom": 0, "partial_commits": 0, "tpcc_consistency_violations": 0}}

    monkeypatch.setattr(cw.client, "start", start)
    monkeypatch.setattr(cw.client, "wait_until_running", wait_until_running)
    monkeypatch.setattr(cw.client, "stop", stop)
    monkeypatch.setattr(cw.client, "info", lambda f: {"settings": {"workload_used": {
        "profile": "tpcc", "concurrency": 64, "rate_tps": 60.0, "tpcc": {"warehouses": 283}}}})
    monkeypatch.setattr(cw, "wait_for_health", lambda w, t: log.append(("health",)) or [])
    monkeypatch.setattr(cw.time, "sleep", lambda s: log.append(("cooldown", s)))
    return log


def workload(**kw):
    return CommonWorkload(ENV, ENV.nodes[0], config_path="workloads/attach-check.yaml", say=lambda *a: None, **kw)


def test_started_once_checked_before_each_scenario_and_stopped_once(calls):
    w = workload(cooldown_s=60)
    w.start()
    assert w.before_scenario("NL-N-I03a") == [] and w.before_scenario("NL-N-I03b") == []
    w.stop()
    assert calls == [("start", "workloads/attach-check.yaml"), ("wait_until_running",),
                     ("health",), ("cooldown", 60), ("health",), ("stop", "pytest session")]
    s = w.summary()
    assert s["profile"] == "tpcc" and s["clerks"] == 64 and s["state"] == "stopped" and s["end_checks"]["rpo_txn"] == 0
    assert s["grafana_link"].startswith("http://10.11.21.13:3000/d/resilience-run?") and "var-env=e2-u22-3" in s["grafana_link"]


def test_a_workload_that_does_not_recover_is_named(calls, monkeypatch):
    monkeypatch.setattr(cw, "wait_for_health", lambda w, t: ["no sample for 40 s"])
    w = workload()
    w.start()
    assert w.before_scenario("NL-N-I03a") == ["no sample for 40 s"]
    assert "not healthy before NL-N-I03a: no sample for 40 s" in w.unhealthy


def test_a_workload_the_session_did_not_start_is_never_stopped(calls, monkeypatch, tmp_path):
    monkeypatch.setattr(cw, "find_folder", lambda p, n: tmp_path / "running")
    w = CommonWorkload(ENV, ENV.nodes[0], config_path=None, say=lambda *a: None)
    w.start()
    w.stop()
    assert not w.started_here and not any(c[0] in ("start", "stop") for c in calls)


# --- the whole pytest session --------------------------------------------------------------------


def test_one_pytest_session_runs_the_scenarios_in_order_on_one_workload_and_writes_index_html(
        calls, monkeypatch, pytester, tmp_path):
    """pytest test_scenarios.py --common-workload ... --scenario NL-N-I03b --scenario NL-N-I03a: the
    workload started once, the scenarios attached in the order given, stopped once, and the
    session's index.html with the common workload on it."""
    import resilience_tests.test_scenarios as ts
    ran = []

    async def run_item(item, profile, options):
        ran.append((item.scenario.id, options.attach_workload))
        run = tmp_path / f"{item.scenario.id}-run"
        run.mkdir(exist_ok=True)
        (run / "results.json").write_text('{"status": "passed", "scenario": {"id": "%s"}}' % item.scenario.id)
        return {"run_id": run.name, "status": "passed", "error": None, "evidence_dir": str(run),
                "measured": {}}
    monkeypatch.setattr(ts, "run_item", run_item)
    monkeypatch.setattr(ts, "render_run", lambda d: Path(d) / "report.html")
    pytester.chdir()
    result = pytester.runpytest_inprocess(
        str(REPO / "resilience_tests" / "test_scenarios.py"), "--rootdir", str(REPO), "-p", "no:cacheprovider",
        "--env", "e2-u22-3", "--common-workload", "workloads/attach-check.yaml", "--cooldown", "5",
        "--scenario", "NL-N-I03b", "--scenario", "NL-N-I03a", "-q")
    result.assert_outcomes(passed=2)
    assert ran == [("NL-N-I03b", True), ("NL-N-I03a", True)]                 # in the order given, attached
    assert [c[0] for c in calls].count("start") == 1 and [c[0] for c in calls].count("stop") == 1
    assert calls[-1] == ("stop", "pytest session") and ("cooldown", 5.0) in calls
    index = next(Path(pytester.path).glob("reports/*/index.html")).read_text()
    assert "Common workload (one for the whole session)" in index and "283 warehouses" in index
    assert "the whole session in Grafana" in index and "NL-N-I03a" in index and "NL-N-I03b" in index
