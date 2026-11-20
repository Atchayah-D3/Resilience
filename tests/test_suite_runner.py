"""The sequential runner (control/suite.py): one common workload for the whole run, the scenarios
one after another against it, stopped once at the end -- also when something fails."""

from __future__ import annotations

from pathlib import Path

import pytest

from resilience_tests.control import suite as suite_mod
from resilience_tests.control.matrix import RunPlanItem
from resilience_tests.control.profile import load_profile
from tests.test_orchestrator import scenario

ENV = load_profile("e2-u22-3")


def plan(*ids):
    return [RunPlanItem(scenario=scenario(i), env_class="E2", role="standalone", node=ENV.nodes[0]) for i in ids]


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
        return {"state": "stopped"}

    async def run_item(item, profile, options):
        log.append(("run", item.scenario.id, options.attach_workload))
        return {"run_id": f"{item.scenario.id}-r", "status": "passed", "error": None, "evidence_dir": str(tmp_path)}
    monkeypatch.setattr(suite_mod.client, "start", start)
    monkeypatch.setattr(suite_mod.client, "wait_until_running", wait_until_running)
    monkeypatch.setattr(suite_mod.client, "stop", stop)
    monkeypatch.setattr(suite_mod, "run_item", run_item)
    monkeypatch.setattr(suite_mod, "wait_for_health", lambda w, t: log.append(("health",)) or [])
    monkeypatch.setattr(suite_mod.time, "sleep", lambda s: log.append(("cooldown", s)))
    return log


def runner(items, **kw):
    return suite_mod.SuiteRunner(ENV, items, workload_config="workloads/oltp-common.yaml", say=lambda *a: None, **kw)


def test_the_workload_starts_once_runs_every_scenario_attached_and_stops_once(calls):
    result = runner(plan("NL-N-I03a", "NL-N-I03b"), cooldown_s=60).run()
    assert result.passed and [r["status"] for r in result.runs] == ["passed", "passed"]
    assert calls == [("start", "workloads/oltp-common.yaml"), ("wait_until_running",),
                     ("health",), ("run", "NL-N-I03a", True),
                     ("cooldown", 60), ("health",), ("run", "NL-N-I03b", True),
                     ("stop", "suite runner")]


def test_a_workload_that_does_not_recover_ends_the_suite_and_still_stops_it(calls, monkeypatch):
    answers = iter([[], ["no sample for 40 s"]])
    monkeypatch.setattr(suite_mod, "wait_for_health", lambda w, t: next(answers))
    result = runner(plan("NL-N-I03a", "NL-N-I03b", "NL-C-01")).run()
    assert [r["status"] for r in result.runs] == ["passed", "not_run", "not_run"]
    assert not result.passed and "not healthy before NL-N-I03b" in result.error
    assert calls[-1] == ("stop", "suite runner")


def test_an_interrupted_suite_still_stops_the_workload_it_started(calls, monkeypatch):
    async def boom(item, profile, options):
        raise KeyboardInterrupt
    monkeypatch.setattr(suite_mod, "run_item", boom)
    with pytest.raises(KeyboardInterrupt):
        runner(plan("NL-N-I03a")).run()
    assert calls[-1] == ("stop", "suite runner")


def test_a_workload_the_runner_did_not_start_is_never_stopped(calls, monkeypatch, tmp_path):
    monkeypatch.setattr(suite_mod, "find_folder", lambda p, n: tmp_path / "running")
    result = runner(plan("NL-N-I03a"), use_running=True).run()
    assert result.passed and not result.started_here
    assert not any(c[0] in ("start", "stop") for c in calls)


def test_the_plan_runs_in_the_order_given():
    items, _ = suite_mod.build_plan(ENV, ["NL-N-I03b", "NL-N-I03a"])
    assert [i.scenario.id for i in items] == ["NL-N-I03b", "NL-N-I03a"]


def test_an_interrupt_while_waiting_for_the_warm_up_still_stops_the_workload(calls, monkeypatch):
    """Ctrl-C during the workload's start-up (data step, warm-up): what was started is stopped."""
    def interrupted(f, timeout_s):
        raise KeyboardInterrupt
    monkeypatch.setattr(suite_mod.client, "wait_until_running", interrupted)
    with pytest.raises(KeyboardInterrupt):
        runner(plan("NL-N-I03a")).run()
    assert calls[-1] == ("stop", "suite runner")
