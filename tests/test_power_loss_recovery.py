"""Host power loss (Framework §10.1): the harness switches the power back on and does nothing
else; the database must start unattended. T1 = power restored.

Runs the real orchestrator against the fake engine of test_orchestrator, with a fake power
switch: power off takes the engine down until power on brings it back.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from resilience_tests.control import killswitch
from resilience_tests.control import orchestrator as orch
from resilience_tests.control.killswitch import ledger_for
from resilience_tests.control.matrix import RunPlanItem
from resilience_tests.control.orchestrator import RunOptions, TestOrchestrator
from resilience_tests.execution.injectors.base import FaultInjector
from tests.test_orchestrator import OUTAGE_S, Engine, env, scenario, why  # noqa: F401  (env is a fixture)


class FakePowerSwitch(FaultInjector):
    fault_types = frozenset({"host_power_loss"})
    driver_name = "libvirt"
    power_on_fails = False
    power_on_at: list[int] = []

    async def preflight(self, node):
        return {"domain": "fake"}

    async def inject(self, node):
        Engine.down = True                      # stays down: nothing restarts a machine with no power
        return {"action": "power off", "t0_mono_ns": time.monotonic_ns()}

    async def revert(self, node, detail=None):
        if FakePowerSwitch.power_on_fails:
            raise RuntimeError("hypervisor refused to start the domain")
        FakePowerSwitch.power_on_at.append(time.monotonic_ns())
        # the machine boots and the database starts on its own a moment after power returns
        asyncio.get_running_loop().call_later(OUTAGE_S, lambda: setattr(Engine, "down", False))
        return {"action": "power on"}


@pytest.fixture
def power(env, monkeypatch):
    FakePowerSwitch.power_on_fails, FakePowerSwitch.power_on_at = False, []
    monkeypatch.setattr(orch, "resolve", lambda fault, prof: FakePowerSwitch(prof, fault.type))
    monkeypatch.setattr(killswitch, "resolve_by_name",
                        lambda section, driver, prof, fault_type="": FakePowerSwitch(prof, fault_type))
    return env


def run_power_loss(profile) -> dict[str, Any]:
    sc = scenario("NL-C-01")
    sc = sc.model_copy(update={"fault": sc.fault.model_copy(update={"type": "host_power_loss",
                                                                     "section": "power_control"})})
    item = RunPlanItem(scenario=sc, env_class=profile.env_class, role="standalone", node=profile.nodes[0])
    return asyncio.run(TestOrchestrator(item, profile, RunOptions()).run())


def test_power_is_restored_by_the_harness_and_t1_is_the_moment_it_came_back(power):
    results = run_power_loss(power)
    assert results["status"] == "passed", why(results)

    # the harness switched the power on exactly once, in recovery -- cleanup found nothing left
    assert len(FakePowerSwitch.power_on_at) == 1
    entries = ledger_for(power).entries()
    assert [e.state for e in entries] == ["intent", "applied", "reverted"]
    assert entries[-1].detail["by"] == "orchestrator"

    # T1 is the power-on, not the fault and not the first write
    t1 = results["timing"]["t1_mono_ns"]
    assert 0 <= t1 - FakePowerSwitch.power_on_at[0] < 1e9
    assert t1 > results["timing"]["t0_mono_ns"]
    recovery = next(p for p in results["phases"] if p["phase"] == "recovery")
    assert recovery["detail"]["power_on"] == {"action": "power on"}

    # the outage was seen and the database came back on its own
    assert results["facts"]["outage_observed"] is True
    assert results["measured"]["starts_unattended"] is True


def test_power_that_does_not_come_back_is_never_a_pass(power):
    FakePowerSwitch.power_on_fails = True
    results = run_power_loss(power)
    assert results["status"] != "passed", why(results)
    assert results["verdict"] is None
    # the failed power-on is on record for the kill switch, never marked reverted
    assert ledger_for(power).entries()[-1].state == "revert_failed"
