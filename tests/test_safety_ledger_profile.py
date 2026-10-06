"""Safety controller, injection ledger, environment profile and run-plan tests.

Arch §15: the safety controls are enforced by the harness, not by convention -- so each of
them has a test that proves it refuses.
"""

import asyncio
import copy
import time

import pytest
import yaml
from pydantic import ValidationError

from catalog.schema import CATALOG_ROOT, Scenario, load_catalog
from resilience_tests.analysis.predicates import NOT_APPLICABLE
from resilience_tests.control.ledger import InjectionLedger
from resilience_tests.control.matrix import RunPlanItem, expand
from resilience_tests.control.orchestrator import RunOptions, TargetBusy, TestOrchestrator
from resilience_tests.control.profile import EnvProfile, load_profile
from resilience_tests.control.safety import SafetyController, SafetyViolation
from resilience_tests.execution import injectors  # noqa: F401  (registers drivers)
from resilience_tests.execution.injectors.base import DriverNotAvailable, resolve

PROFILE = load_profile("e2-dedicated-vm")
NODE = PROFILE.nodes[0]
NLC01 = load_catalog().scenarios["NL-C-01"]
SENTINEL = {"hostname": "BDB-QA-U22-30", "inventory_tag": "resilience-lab-e2",
            "disposable": True, "created_by": "dbre-operator"}


# /proc/<pid>/stat of a running postmaster (state S, starttime 123456) and the ExecStop a
# `pg_ctl stop` unit reports through `systemctl show -p ExecStop --value`.
PROC_STAT_4242 = "4242 (postgres) S 1 4242 4242 0 -1 4194560 " + " ".join(["0"] * 12) + " 123456 0 0"
EXEC_STOP_PG_CTL = ("{ path=/usr/lib/postgresql/17/bin/pg_ctl ; argv[]=/usr/lib/postgresql/17/bin/pg_ctl "
                    "-D ${PGDATA} stop ; ignore_errors=no ; start_time=[n/a] ; stop_time=[n/a] ; pid=0 ; code=(null) ; "
                    "status=0/0 }")


def destructive_scenario() -> Scenario:
    """An NL-D scenario built from the authored row: NL-D is the destructive category, so it
    exercises the disposable-target gate that NL-C does not."""
    d = copy.deepcopy(yaml.safe_load((CATALOG_ROOT / "NL" / "NL-C-01.yaml").read_text()))
    d.update(id="NL-D-02", category="NL-D", name="Durability under host power loss",
             description="power loss under write load")
    d["fault"].update(type="host_power_loss", driver="power_control")
    d["environment"]["env_sensitive"] = True
    return Scenario.model_validate(d)


def ctl(scenario=NLC01, disposable=True):
    return SafetyController(PROFILE, scenario, target_is_disposable=disposable)


def test_destructive_gate_requires_flag():
    destructive = destructive_scenario()
    with pytest.raises(SafetyViolation, match="target-is-disposable"):
        ctl(destructive, disposable=False).check_static([NODE])
    ctl(destructive).check_static([NODE])
    # a non-destructive category does not need the flag
    ctl(NLC01, disposable=False).check_static([NODE])


def test_blast_radius_counts_resolved_targets():
    with pytest.raises(SafetyViolation, match="ceiling is 1"):
        ctl().check_static([NODE, NODE])


def test_production_profile_refused():
    prod = PROFILE.model_copy(update={"environment": "production"})
    with pytest.raises(SafetyViolation, match="may run only in"):
        SafetyController(prod, NLC01, target_is_disposable=True).check_static([NODE])


def test_allowlist_default_deny():
    with pytest.raises(SafetyViolation, match="does not match allowlist"):
        ctl().check_fingerprint(NODE, "prod-db-01", SENTINEL)
    with pytest.raises(SafetyViolation, match="no sentinel row"):
        ctl().check_fingerprint(NODE, "BDB-QA-U22-30", None)
    with pytest.raises(SafetyViolation, match="inventory_tag"):
        ctl().check_fingerprint(NODE, "BDB-QA-U22-30", {**SENTINEL, "inventory_tag": "other"})
    assert ctl().check_fingerprint(NODE, "BDB-QA-U22-30", SENTINEL)["disposable"] is True


def test_destructive_scenario_needs_a_disposable_sentinel():
    with pytest.raises(SafetyViolation, match="disposable"):
        ctl(destructive_scenario()).check_fingerprint(NODE, "BDB-QA-U22-30", {**SENTINEL, "disposable": False})


def test_abort_predicates_on_standalone_are_not_applicable_not_triggered():
    signals = {"replication_lag_s": NOT_APPLICABLE, "secondary_node_unhealthy": NOT_APPLICABLE,
               "data_fs_used_pct": 40.0}
    checks = ctl().check_abort(signals)
    assert [c.outcome for c in checks] == ["not_applicable", "not_applicable", "fail"]
    assert not any(c.triggered for c in checks)
    fired = ctl().check_abort({"replication_lag_s": 400, "secondary_node_unhealthy": False, "data_fs_used_pct": 40.0})
    assert fired[0].triggered and not fired[1].triggered


def test_the_standing_disk_abort_applies_to_a_standalone_target():
    """Was: every abort_if in the catalog is a cluster signal, so on a standalone node nothing
    could ever stop a run. The environment's own condition applies to every target."""
    assert ctl().standing_aborts() == ["data_fs_used_pct > 90"]
    checks = ctl().check_abort({"replication_lag_s": NOT_APPLICABLE, "secondary_node_unhealthy": NOT_APPLICABLE,
                                "data_fs_used_pct": 95.0})
    assert [c.predicate for c in checks if c.triggered] == ["data_fs_used_pct > 90"]
    # no reading yet is NOT_MEASURED -- never read as an empty disk, and never a trigger
    from resilience_tests.analysis.predicates import NOT_MEASURED
    unknown = ctl().check_abort({"replication_lag_s": NOT_APPLICABLE, "secondary_node_unhealthy": NOT_APPLICABLE,
                                 "data_fs_used_pct": NOT_MEASURED})
    assert unknown[-1].outcome == "not_measured" and not unknown[-1].triggered


def test_ledger_outstanding_until_reverted(tmp_path):
    ledger = InjectionLedger(tmp_path / "ledger.jsonl")
    e = ledger.intent(run_id="r", env_profile="p", fault_type="process_kill", driver="os_ssh", node="n",
                      detail={"section": "os_ssh"})
    assert [x.state for x in ledger.outstanding()] == ["intent"]  # crash mid-call is outstanding
    e = ledger.transition(e, "applied")
    assert [x.state for x in ledger.outstanding()] == ["applied"]
    ledger.transition(e, "reverted")
    assert ledger.outstanding() == []


def test_process_kill_resolves_to_the_os_ssh_driver():
    injector = resolve(NLC01.fault, PROFILE)
    assert injector.driver_name == "os_ssh" and "process_kill" in injector.fault_types


def test_fault_without_a_configured_driver_fails_closed():
    # this profile provides no power control, so a power-loss fault must refuse, never proceed
    with pytest.raises(DriverNotAvailable):
        resolve(destructive_scenario().fault, PROFILE)


def test_preflight_refuses_a_service_that_cannot_restart_itself(monkeypatch):
    from resilience_tests.execution.injectors import process as process_mod

    class FakeHost:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return None

        async def run(self, command, *, timeout_s, check=True):
            from resilience_tests.execution.remote import RemoteResult
            if "is-active" in command:
                return RemoteResult(0, "active", "")
            if "postmaster.pid" in command:
                return RemoteResult(0, "4242", "")
            return RemoteResult(0, "no", "")  # Restart policy

    monkeypatch.setattr(process_mod, "RemoteHost", FakeHost)
    with pytest.raises(DriverNotAvailable, match="unattended recovery"):
        asyncio.run(process_mod.OsSshProcessDriver(PROFILE, "process_kill").preflight(NODE))


def test_profile_rejects_driver_host_that_is_a_target():
    data = PROFILE.model_dump(by_alias=True)
    data["driver_host"]["host"] = NODE.ssh.host
    with pytest.raises(ValidationError, match="Arch §6.2"):
        EnvProfile.model_validate(data)


def test_profile_requires_every_phase_bounded():
    data = PROFILE.model_dump(by_alias=True)
    del data["phase_timeouts_s"]["recovery"]
    with pytest.raises(ValidationError, match="phase_timeouts_s"):
        EnvProfile.model_validate(data)


def test_restart_and_kill_share_the_os_ssh_driver():
    from resilience_tests.execution.injectors import process as process_mod
    assert {"process_kill", "service_restart"} <= process_mod.OsSshProcessDriver.fault_types
    catalog = load_catalog()
    for sid, sc in catalog.scenarios.items():
        if sc.fault.driver == "os_ssh":
            injector = resolve(sc.fault, PROFILE)
            assert injector.fault_type == sc.fault.type, sid


def test_every_fault_type_can_be_injected_and_reverted(monkeypatch):
    """Exercises each driver action against a fake host: catches missing imports, bad command
    construction and typos that would otherwise only surface mid-run, after the baseline."""
    from resilience_tests.execution.injectors import process as process_mod
    from resilience_tests.execution.remote import RemoteResult

    calls: list[str] = []
    killed: set[int] = set()

    class FakeHost:
        def __init__(self, *a, **k):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return None

        async def run(self, command, *, timeout_s, check=True):
            calls.append(command)
            if "postmaster.pid" in command:
                return RemoteResult(0, "4242", "")
            if "systemctl kill" in command:
                killed.add(4242)
                return RemoteResult(0, "", "")
            if "/proc/4242/stat" in command:
                return RemoteResult(0, "" if 4242 in killed else PROC_STAT_4242, "")
            if "is-active" in command:
                return RemoteResult(0, "active", "")
            if "show -p Restart" in command:
                return RemoteResult(0, "on-failure", "")
            if "show -p ExecStop" in command:
                return RemoteResult(0, EXEC_STOP_PG_CTL, "")
            return RemoteResult(0, "", "")

    monkeypatch.setattr(process_mod, "RemoteHost", FakeHost)
    # the flood itself talks to the database; it is exercised in test_connection_exhaustion.py
    from resilience_tests.adapters.postgresql.adapter import PostgreSQLAdapter

    async def flood(self, hold_s):
        calls.append(f"flood held {hold_s}s")
        return {"t0_mono_ns": time.monotonic_ns()}

    async def unflood(self):
        calls.append("flood terminated")
        return {"remaining": 0}
    monkeypatch.setattr(PostgreSQLAdapter, "exhaust_connections", flood)
    monkeypatch.setattr(PostgreSQLAdapter, "revert_exhaust_connections", unflood)
    for fault_type in sorted(process_mod.OsSshProcessDriver.fault_types):
        injector = process_mod.OsSshProcessDriver(PROFILE, fault_type)
        injector.duration_s = 0.01   # as the orchestrator sets it from fault.duration
        assert asyncio.run(injector.preflight(NODE))
        if fault_type == "idle_in_transaction":
            # injected and confirmed by the database adapter; a shell-held second session
            # could not be confirmed or found again, so the SSH driver refuses
            with pytest.raises(DriverNotAvailable, match="through the database adapter"):
                asyncio.run(injector.inject(NODE))
        else:
            detail = asyncio.run(injector.inject(NODE))
            assert detail["t0_mono_ns"] > 0, fault_type
        asyncio.run(injector.revert(NODE))
    assert any("systemctl kill -s SIGKILL" in c for c in calls)   # Arch §5: the unit's cgroup
    assert any("systemctl restart" in c for c in calls)
    assert any("pg_reload_conf" in c for c in calls)
    assert "flood held 0.01s" in calls and "flood terminated" in calls


def test_matrix_expands_every_scenario_once_on_the_reference_class():
    catalog = load_catalog()
    plan, skipped = expand(catalog, PROFILE, reference_env_class="E2")
    blocked = sorted(sid for sid, sc in catalog.scenarios.items() if sc.needs_infra)
    assert sorted(s.scenario_id for s in skipped) == blocked     # infrastructure only (NL-C-04)
    assert sorted(p.scenario.id for p in plan) == sorted(set(catalog.scenarios) - set(blocked))
    assert all(p.node.name == "shaktidb-standalone" for p in plan)


def test_env_insensitive_scenarios_run_only_on_the_reference_class():
    catalog = load_catalog()
    plan, skipped = expand(catalog, PROFILE, reference_env_class="E1")
    insensitive = {sid for sid, sc in catalog.scenarios.items() if not sc.environment.env_sensitive}
    assert {s.scenario_id for s in skipped} == insensitive
    assert {p.scenario.id for p in plan} == set(catalog.scenarios) - insensitive


def test_second_run_against_the_same_target_is_refused(tmp_path):
    """Two concurrent runs on one node destroy each other's measurements: the second run's
    init truncates the first run's marker table. The harness refuses instead."""
    profile = PROFILE.model_copy(
        update={"driver_host": PROFILE.driver_host.model_copy(update={"run_dir": str(tmp_path)})})
    item = RunPlanItem(scenario=NLC01, env_class=profile.env_class, role=NODE.role, node=NODE)
    first = TestOrchestrator(item, profile, RunOptions())
    first._acquire_target_lock()
    try:
        second = TestOrchestrator(item, profile, RunOptions())
        with pytest.raises(TargetBusy, match="already in use"):
            second._acquire_target_lock()
    finally:
        first._release_target_lock()
    third = TestOrchestrator(item, profile, RunOptions())  # released: the next run may proceed
    third._acquire_target_lock()
    third._release_target_lock()
