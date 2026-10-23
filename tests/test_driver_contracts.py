"""What every fault driver may and may not do, beyond injecting its own fault.

- A driver's fallback for an unknown fault type only reverts; it never injects.
- The driver base class carries no fault's settings: each driver takes its own from the
  scenario (`configure`), and a driver that cannot aim at one process refuses to be aimed.
- The process driver requires the unit's Restart policy only for the fault whose recovery
  relies on it (a kill); it records the policy for every fault.
"""

from __future__ import annotations

import asyncio

import pytest

from catalog.schema import load_catalog
from resilience_tests.control.profile import load_profile
from resilience_tests.execution import injectors  # noqa: F401  (registers drivers)
from resilience_tests.execution.injectors import process as process_mod
from resilience_tests.execution.injectors.base import DriverNotAvailable
from resilience_tests.execution.injectors.corruption import OsSshCorruptionDriver
from resilience_tests.execution.injectors.power import LibvirtPowerDriver
from resilience_tests.execution.injectors.process import OsSshProcessDriver
from resilience_tests.execution.remote import RemoteResult

PROFILE = load_profile("e2-dedicated-vm")
NODE = PROFILE.nodes[0]
CATALOG = load_catalog()


def fake_host(monkeypatch, *, restart: str = "on-failure") -> list[str]:
    """SSH to a running unit with the given Restart policy; every command is recorded."""
    calls: list[str] = []

    class Host:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return None

        async def run(self, command, *, timeout_s, check=True):
            calls.append(command)
            if "is-active" in command:
                return RemoteResult(0, "active", "")
            if "postmaster.pid" in command:
                return RemoteResult(0, "4242", "")
            if "show -p Restart " in command:
                return RemoteResult(0, restart, "")
            return RemoteResult(0, "", "")

    monkeypatch.setattr(process_mod, "RemoteHost", Host)
    return calls


# --- the fallback for an unknown fault type ----------------------------------------------


def test_the_fallback_for_an_unknown_fault_never_injects(monkeypatch):
    """Was: the fallback inherited the process kill, so anything reaching `inject` with an
    empty or unknown fault type killed the database."""
    calls = fake_host(monkeypatch)
    driver = OsSshProcessDriver(PROFILE, "")
    with pytest.raises(DriverNotAvailable, match="nothing was applied"):
        asyncio.run(driver.inject(NODE))
    assert calls == []                                   # not one command reached the host
    with pytest.raises(DriverNotAvailable, match="has no fault ''"):
        asyncio.run(driver.preflight(NODE))
    assert not any("kill" in c for c in calls)


def test_the_fallback_still_reverts_by_making_sure_the_service_runs(monkeypatch):
    """The kill switch uses it for a ledger entry that names no fault type."""
    calls = fake_host(monkeypatch)
    assert asyncio.run(OsSshProcessDriver(PROFILE, "").revert(NODE, {}))["state"] == "active"
    assert any("is-active" in c for c in calls) and not any("kill" in c for c in calls)


# --- each driver keeps its own settings -------------------------------------------------


def test_the_driver_base_class_carries_no_fault_settings():
    """Was: FaultInjector held kill_target, corruption_kind, repeat_plan and duration_s for
    every driver, so the base class knew what its subclasses needed."""
    power = LibvirtPowerDriver(PROFILE, "host_power_loss")
    for name in ("kill_target", "corruption_kind", "repeat_plan", "duration_s"):
        assert not hasattr(power, name), name


def test_each_driver_takes_its_own_settings_from_the_scenario():
    flood = CATALOG.scenarios["NL-R-04"]
    driver = OsSshProcessDriver(PROFILE, "connection_exhaustion")
    driver.configure(flood.fault, flood.repeat, None)
    assert driver.duration_s == float(flood.fault.duration) and driver.repeat_plan is None

    cycles = CATALOG.scenarios["NL-C-05"]
    driver = OsSshProcessDriver(PROFILE, "process_kill")
    driver.configure(cycles.fault, cycles.repeat, None)
    assert driver.repeat_plan == (cycles.repeat.cycles, cycles.repeat.interval_s)

    index = CATALOG.scenarios["NL-I-02"]
    corruption = OsSshCorruptionDriver(PROFILE, "data_corruption")
    assert corruption.corruption_kind == "heap"
    corruption.configure(index.fault, index.repeat, None)
    assert corruption.corruption_kind == "btree_index"


def test_a_driver_that_cannot_aim_refuses_to_be_aimed():
    """Aiming at one process and then hitting the whole service would test something else."""
    target = {"pid": 9001, "title_marker": "autovacuum worker"}
    power = LibvirtPowerDriver(PROFILE, "host_power_loss")
    power.aim_at(None)                                   # clearing is always allowed
    with pytest.raises(DriverNotAvailable, match="cannot aim 'host_power_loss'"):
        power.aim_at(target)
    with pytest.raises(DriverNotAvailable, match="cannot aim 'config_reload'"):
        OsSshProcessDriver(PROFILE, "config_reload").aim_at(target)
    kill = OsSshProcessDriver(PROFILE, "process_kill")
    kill.aim_at(target)
    assert kill.kill_target == target
    kill.aim_at(None)
    assert kill.kill_target is None


# --- the Restart policy -------------------------------------------------------------------


@pytest.mark.parametrize("fault_type", ["config_reload", "idle_in_transaction", "service_restart"])
def test_faults_that_do_not_rely_on_the_restart_policy_accept_a_unit_without_one(monkeypatch, fault_type):
    """Was: every process fault refused Restart=no, although only a kill relies on the unit
    restarting itself. The policy is still recorded with the injection."""
    fake_host(monkeypatch, restart="no")
    if fault_type == "service_restart":
        monkeypatch.setattr(process_mod, "unit_stop_mode", lambda exec_stop, kill_signal: "fast")
    detail = asyncio.run(OsSshProcessDriver(PROFILE, fault_type).preflight(NODE))
    assert detail["restart_policy"] == "no"


def test_a_kill_still_refuses_a_unit_without_a_restart_policy(monkeypatch):
    fake_host(monkeypatch, restart="no")
    with pytest.raises(DriverNotAvailable, match="unattended recovery"):
        asyncio.run(OsSshProcessDriver(PROFILE, "process_kill").preflight(NODE))
