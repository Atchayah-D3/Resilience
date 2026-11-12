"""The shared hooks for faults held under load (Framework §10.6 NL-N, NL-R pressure faults):
hold-and-release recovery, the hold window and its load ratios, baseline and injection hooks,
the fault's own samplers, phase bounds that include the hold, and the alert source.

Runs the real orchestrator against the fake engine of test_orchestrator, with a stand-in
behaviour registered for a fault type no scenario implements yet.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from resilience_tests.analysis.degradation import P99_RATIO, TPS_RATIO, load_ratios
from resilience_tests.analysis.predicates import NOT_MEASURED
from resilience_tests.control.faults import base as faults_base
from resilience_tests.control.faults.base import HeldFault
from resilience_tests.control.killswitch import ledger_for
from resilience_tests.control.matrix import RunPlanItem
from resilience_tests.control.orchestrator import RunOptions, TestOrchestrator
from resilience_tests.control.profile import load_profile
from resilience_tests.observability.alerts import alert_source_for
from tests.test_orchestrator import CATALOG, FakeFault, env, why  # noqa: F401  (env is a fixture)

HOLD_S = 1.5


class RecordingSampler:
    def __init__(self, calls: list[str], stream: Any) -> None:
        self.calls, self.stream = calls, stream

    def start(self) -> None:
        self.calls.append("sampler_start")
        self.stream.emit("os_sampler", "sample", steal_pct=0.0)

    async def stop(self) -> None:
        self.calls.append("sampler_stop")


class StandInHold(HeldFault):
    """What an NL-N behaviour looks like: snapshots at each edge, its own sampler, an alert."""

    fault_type = "resource_limit"
    calls: list[str] = []

    def samplers(self):
        return [RecordingSampler(self.calls, self.ctx.stream)]

    async def baseline_started(self):
        self.calls.append("baseline_started")

    async def baseline_ended(self):
        self.calls.append("baseline_ended")

    async def before_inject(self):
        self.calls.append("before_inject")
        await super().before_inject()

    async def after_inject(self, detail):
        self.calls.append("after_inject")

    async def on_hold_start(self):
        self.calls.append("hold_start")

    async def on_hold_end(self):
        self.calls.append("hold_end")

    async def measure(self, events):
        m = await super().measure(events)
        hold = self.ctx.facts["hold"]
        m["degradation_alerted"] = await self.ctx.alert_measure(
            "degradation_alerted", "DatabaseDegraded", hold["start_wall_s"], hold["end_wall_s"])
        return m


@pytest.fixture
def held(monkeypatch):
    StandInHold.calls = []
    monkeypatch.setattr(faults_base, "_FAULTS", dict(faults_base._FAULTS, resource_limit=StandInHold))


def held_scenario(duration: float | str = HOLD_S, accept: list[str] | None = None):
    sc = CATALOG.scenarios["NL-R-04"]
    return sc.model_copy(update={
        "steady_state": sc.steady_state.model_copy(update={"duration_s": 1, "tps_min": 1}),
        "fault": sc.fault.model_copy(update={"type": "resource_limit", "duration": duration}),
        "measure": ["rpo_txn", "structural_integrity_errors", TPS_RATIO, P99_RATIO, "degradation_alerted"],
        "accept": accept or ["rpo_txn == 0", "structural_integrity_errors == 0", f"{TPS_RATIO} >= 0.01"],
    })


def run_held(profile, sc):
    item = RunPlanItem(scenario=sc, env_class=profile.env_class, role="standalone", node=profile.nodes[0])
    return asyncio.run(TestOrchestrator(item, profile, RunOptions()).run())


def test_a_held_fault_is_held_measured_released_and_judged(env, held):
    results = run_held(env, held_scenario())
    assert results["status"] == "passed", why(results)

    # every hook, once, in phase order; the sampler runs from baseline to the end of the load
    assert StandInHold.calls == ["sampler_start", "baseline_started", "baseline_ended", "before_inject",
                                 "after_inject", "hold_start", "hold_end", "sampler_stop"]

    # held for the scenario's duration with the workload measured in its own window
    hold = results["facts"]["hold"]
    assert hold["planned_s"] == HOLD_S
    assert (hold["end_mono_ns"] - hold["start_mono_ns"]) / 1e9 >= HOLD_S
    assert hold["load"]["commits"] > 0
    measured = results["measured"]
    assert isinstance(measured[TPS_RATIO], float) and measured[TPS_RATIO] > 0
    assert isinstance(measured[P99_RATIO], float)

    # released by the harness at T1, after the hold; the ledger entry is closed by that
    # release, so cleanup has nothing left to revert
    timing = results["timing"]
    assert (timing["t1_mono_ns"] - timing["t0_mono_ns"]) / 1e9 >= HOLD_S
    assert [r["fault_type"] for r in FakeFault.reverts] == ["resource_limit"]
    assert results["facts"]["slo_t0_mono_ns"] == timing["t1_mono_ns"]   # time-to-SLO from release

    # no alert source in the lab: "alerted" is not measured, with the reason
    assert measured["degradation_alerted"] == "NOT_MEASURED"
    assert "alerting.source: none" in results["facts"]["not_measured"]["degradation_alerted"]


def test_a_hold_longer_than_the_fault_inject_bound_is_not_cut_off(env, held):
    """Was: every phase was capped at the profile bound, so a 300 s hold under a 120 s bound
    timed out. The hold is added to the bound of the phase it is held in: fault_inject."""
    short = env.model_copy(update={"phase_timeouts_s": dict(env.phase_timeouts_s, fault_inject=1.0)})
    results = run_held(short, held_scenario(duration=2.0))
    inject = next(p for p in results["phases"] if p["phase"] == "fault_inject")
    assert inject["outcome"] == "ok", why(results)
    assert results["status"] == "passed", why(results)


def test_the_hold_is_part_of_the_fault_inject_phase_not_of_recovery(env, held):
    """Was: the fault was held inside the recovery phase, so the report's recovery time
    included the hold. Held and released in fault_inject, recovery measures only what follows
    the release (T1)."""
    results = run_held(env, held_scenario(duration=2.0))
    assert results["status"] == "passed", why(results)
    phases = {p["phase"]: p for p in results["phases"]}
    assert phases["fault_inject"]["duration_s"] >= 2.0
    assert phases["fault_inject"]["detail"]["hold"]["released"] == {"action": "revert"}
    assert phases["recovery"]["detail"]["held_s"] >= 2.0          # reported, not spent again
    # the hold opens and the release (T1) happens inside fault_inject, before recovery starts
    events = [json.loads(line) for line in (Path(results["evidence_dir"]) / "events.jsonl").read_text().splitlines()]
    order = [e["kind"] if e["kind"] not in ("phase_start", "phase_end") else f"{e['kind']}:{e['data']['phase']}"
             for e in events if e["kind"] in ("phase_start", "phase_end", "hold_start", "hold_end", "t1")]
    i = order.index
    assert (i("phase_start:fault_inject") < i("hold_start") < i("hold_end") < i("t1")
            < i("phase_end:fault_inject") < i("phase_start:recovery"))
    ledger = [e for e in ledger_for(env).entries() if e.fault_type == "resource_limit"]
    assert ledger and ledger[-1].state == "reverted"              # released by the hold itself


def test_a_repeated_held_fault_is_refused_before_anything_is_applied(env, held):
    sc = held_scenario()
    sc = sc.model_copy(update={"repeat": CATALOG.scenarios["NL-C-05"].repeat})
    results = run_held(env, sc)
    assert results["status"] == "aborted"
    assert "repeating a held fault is not built yet" in results["error"]
    assert "after_inject" not in StandInHold.calls and FakeFault.reverts == []


def test_a_held_fault_without_a_duration_is_refused_before_anything_is_applied(env, held):
    results = run_held(env, held_scenario(duration="permanent"))
    assert results["status"] == "aborted"
    assert "must set fault.duration" in results["error"]
    assert "after_inject" not in StandInHold.calls and FakeFault.reverts == []


def test_a_failing_ratio_fails_the_verdict(env, held):
    results = run_held(env, held_scenario(accept=["rpo_txn == 0", f"{TPS_RATIO} >= 1000"]))
    assert results["status"] == "failed", why(results)


# ----------------------------------------------------------------------- the ratios on their own

def window(tps: float, p99: float | None, duration_s: float = 10.0):
    return SimpleNamespace(tps=tps, p99_ms=p99, duration_s=duration_s)


def test_ratios_compare_the_hold_with_the_baseline():
    m, why_ = load_ratios(window(140.0, 30.0), window(200.0, 10.0))
    assert m == {TPS_RATIO: 0.7, P99_RATIO: 3.0} and why_ == {}


def test_a_hold_that_committed_nothing_has_a_zero_tps_ratio_and_no_latency_ratio():
    m, why_ = load_ratios(window(0.0, None), window(200.0, 10.0))
    assert m[TPS_RATIO] == 0.0                       # a real, failing value
    assert m[P99_RATIO] is NOT_MEASURED and "no transaction completed" in why_[P99_RATIO]


def test_no_hold_window_is_not_measured_never_one():
    m, why_ = load_ratios(None, window(200.0, 10.0))
    assert m[TPS_RATIO] is NOT_MEASURED and m[P99_RATIO] is NOT_MEASURED
    assert why_[TPS_RATIO] == "no measurement window was taken during the hold"


def test_the_lab_profiles_have_no_alert_source():
    for name in ("e2-dedicated-vm", "local-lab"):
        answer = asyncio.run(alert_source_for(load_profile(name)).fired("X", 0.0, 1.0))
        assert answer["fired"] is None
