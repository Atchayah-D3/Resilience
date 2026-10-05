"""Behaviour shared by every scenario: what the fault lands inside is read from the catalog,
not from the scenario's name (S3); a standalone run is still guarded by a live abort (S5); and
the baseline is warm, honest about failures, and able to support the SLO it anchors (S7)."""

from __future__ import annotations

import asyncio
import inspect

from resilience_tests.analysis.rto_decomposer import Baseline, baseline_slo_check
from resilience_tests.control import orchestrator as orch
from resilience_tests.control.matrix import RunPlanItem
from resilience_tests.control.orchestrator import RunOptions, TestOrchestrator
from resilience_tests.execution.workload.driver import percentile
from resilience_tests.observability.event_stream import Event
from tests.test_orchestrator import env, scenario, why  # noqa: F401  (fixture)

S = 1_000_000_000


def run_scenario(profile, sc):
    item = RunPlanItem(scenario=sc, env_class=profile.env_class, role="standalone", node=profile.nodes[0])
    o = TestOrchestrator(item, profile, RunOptions())
    return o, asyncio.run(o.run())


# --- S3: the catalog decides, never the scenario id ------------------------------------------


def test_the_orchestrator_never_branches_on_a_scenario_id():
    source = inspect.getsource(orch)
    assert "scenario.id ==" not in source and 'id == "NL-' not in source


def test_fault_state_comes_from_the_catalog_field_not_the_scenario_name(env):
    """Any scenario declaring `during: checkpoint` gets the checkpoint treatment -- the proof
    that NL-C-02's behaviour is data, not a special case keyed on its id."""
    sc = scenario("NL-C-01")
    sc = sc.model_copy(update={"fault": sc.fault.model_copy(update={"during": "checkpoint"})})
    _, results = run_scenario(env, sc)
    assert results["status"] == "passed", why(results)
    assert results["facts"]["checkpointer_seen_active_before_kill"] is True
    assert results["facts"]["checkpoint_verification"]["in_flight"] is True


def test_without_a_declared_state_no_state_is_established(env):
    _, results = run_scenario(env, scenario("NL-C-01"))
    assert "checkpoint_injection" not in results["facts"] and "concurrent_index" not in results["facts"]


# --- S5: a live abort that applies to a standalone target -----------------------------------


class FullDisk:
    """Stands in for DiskUsageProber: the data filesystem reads 95% full."""

    def __init__(self, node, stream):
        self.stream = stream

    def start(self):
        self.stream.emit("disk_prober", "disk_usage", node="n", used_pct=95.0)

    async def stop(self):
        pass


def test_a_filling_disk_aborts_a_standalone_run(env, monkeypatch):
    """Was: the catalog's abort_if are cluster signals, NOT_APPLICABLE on a standalone node --
    nothing could stop a run however close the disk came to full."""
    monkeypatch.setattr(orch, "DiskUsageProber", FullDisk)
    _, results = run_scenario(env, scenario("NL-C-01"))
    assert results["status"] == "aborted", why(results)
    assert "data_fs_used_pct > 90" in results["error"]
    assert results["verdict"] is None


def test_a_run_says_when_its_own_abort_conditions_did_not_apply(env):
    _, results = run_scenario(env, scenario("NL-C-01"))
    text = " ".join(results["disclosures"])
    assert "None of this scenario's abort conditions apply to a standalone target" in text
    assert "data_fs_used_pct > 90" in text
    # the test fixture's prober never reads the disk, and the run must say so
    assert "standing disk abort never received a reading" in text


# --- S7: the baseline ------------------------------------------------------------------------


def sample(t_s: float, tps: float, p99: float) -> Event:
    return Event(int(t_s * S), 0.0, "workload", "sample", {"interval_s": 1.0, "tps": tps, "p99_ms": p99})


def test_a_steady_baseline_supports_the_slo():
    events = [sample(t, 200, 5.0) for t in range(1, 121)]
    check = baseline_slo_check(events, 0, 121 * S, Baseline(200, 5.0))
    assert check.sustained_window_found and check.compliant == 120


def test_a_baseline_that_stalls_every_30_s_cannot_support_the_slo():
    """Stalls the target always has would be timed as 'recovery' after the fault."""
    events = [sample(t, 200, 300.0 if t % 30 == 0 else 5.0) for t in range(1, 121)]
    check = baseline_slo_check(events, 0, 121 * S, Baseline(200, 6.0))
    assert not check.sustained_window_found and check.compliant == 116


def test_time_to_slo_is_not_measured_when_the_baseline_cannot_support_it(env):
    """The fixture's 1 s baseline is shorter than its 2 s sustain period, so it cannot hold the
    SLO by construction: time-to-SLO must then be NOT_MEASURED with the reason, never a value."""
    _, results = run_scenario(env, scenario("NL-C-01"))
    assert results["facts"]["baseline_slo_check"]["sustained_window_found"] is False
    assert str(results["measured"]["rto_to_slo_s"]) == "NOT_MEASURED"
    assert "never held the SLO" in results["facts"]["not_measured"]["rto_to_slo_s"]


def test_the_baseline_reports_p50_and_p95_beside_p99(env):
    _, results = run_scenario(env, scenario("NL-C-01"))
    b = results["baseline"]
    assert b["p50_ms"] is not None and b["p95_ms"] is not None and b["p50_ms"] <= b["p95_ms"] <= b["p99_ms"]


def test_percentile_is_nearest_rank():
    assert percentile(list(range(1, 101)), 0.50) == 50 and percentile(list(range(1, 101)), 0.95) == 95
    assert percentile([], 0.5) is None


def test_failed_attempts_count_towards_latency(env, monkeypatch):
    """Was: only committed transactions were timed, so an attempt that hung until it failed
    contributed no latency at all and the p99 read healthier than the client's experience."""
    from resilience_tests.adapters.base import TransactionOutcome
    from tests.test_orchestrator import OutageSession

    calls = {"n": 0}
    real = OutageSession.commit_marker

    async def every_tenth_hangs_then_fails(self, seq, marker_id):
        calls["n"] += 1
        if calls["n"] % 10 == 0:
            await asyncio.sleep(0.25)
            return TransactionOutcome.DEFINITELY_ABORTED
        return await real(self, seq, marker_id)

    monkeypatch.setattr(OutageSession, "commit_marker", every_tenth_hangs_then_fails)
    _, results = run_scenario(env, scenario("NL-C-01"))
    assert results["baseline"]["p99_ms"] >= 250


def test_warm_up_waits_for_every_worker(env, monkeypatch):
    """The window opens only once the declared concurrency is actually connected."""
    from resilience_tests.execution.workload.driver import WorkloadDriver

    seen: list[int] = []
    real = WorkloadDriver.begin_window

    def begin(self):
        seen.append(self.connected_workers)
        return real(self)

    monkeypatch.setattr(WorkloadDriver, "begin_window", begin)
    o, results = run_scenario(env, scenario("NL-C-01"))
    assert seen == [o.scenario.workload.concurrency], why(results)
