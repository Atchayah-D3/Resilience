"""The per-fault behaviours that replaced the orchestrator's fault-type branches.

The sets below are the orchestrator's former hard-coded lists (OUTAGE_FAULTS,
UNATTENDED_FAULTS, FAULT_FACT_KEY): moving them into registered behaviours must not change
which fault types expect an outage, recover unattended, or keep their detail under a name.
"""

import ast
import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from catalog.schema import FaultDuring, FaultType
from resilience_tests.control import faults
from resilience_tests.control.faults.base import _DURING, _FAULTS
from resilience_tests.execution.injectors.process import OsSshProcessDriver

FORMER_OUTAGE_FAULTS = {"process_kill", "service_restart", "host_power_loss"}
FORMER_UNATTENDED_FAULTS = {"process_kill", "service_restart", "config_reload", "connection_exhaustion",
                            "idle_in_transaction", "data_corruption"}
FORMER_FAULT_FACT_KEY = {"idle_in_transaction": "idle_transaction", "connection_exhaustion": "connection_exhaustion",
                         "data_corruption": "data_corruption"}
ALL_FAULT_TYPES = set(FaultType.__args__)


def behaviour(fault_type: str) -> faults.FaultBehaviour:
    run = SimpleNamespace(scenario=SimpleNamespace(fault=SimpleNamespace(type=fault_type, during=None)))
    return faults.fault_behaviour(run)


def test_outage_unattended_and_fact_key_match_the_former_lists():
    assert {t for t in ALL_FAULT_TYPES if behaviour(t).expects_outage} == FORMER_OUTAGE_FAULTS
    assert {t for t in ALL_FAULT_TYPES if behaviour(t).recovers_unattended} == FORMER_UNATTENDED_FAULTS
    assert {t: behaviour(t).fact_key for t in ALL_FAULT_TYPES if behaviour(t).fact_key} == FORMER_FAULT_FACT_KEY


def test_every_registered_fault_is_a_catalog_fault_type():
    assert set(_FAULTS) <= ALL_FAULT_TYPES


def test_every_catalog_during_value_has_a_behaviour():
    assert set(_DURING) == set(FaultDuring.__args__)


def test_an_unregistered_fault_refuses_recovery_rather_than_guessing():
    b = behaviour("network_delay")
    b.ctx.scenario.fault.duration = "permanent"
    assert type(b) is faults.FaultBehaviour
    with pytest.raises(NotImplementedError, match="not built yet"):
        asyncio.run(b.begin_recovery())


def test_a_second_registration_of_a_name_is_refused(monkeypatch):
    monkeypatch.setattr(faults.base, "_FAULTS", dict(_FAULTS))
    monkeypatch.setattr(faults.base, "_DURING", dict(_DURING))

    class AnotherCorruption(faults.FaultBehaviour):
        fault_type = "data_corruption"

    class AnotherCheckpoint(faults.base.DuringBehaviour):
        during = "checkpoint"

    with pytest.raises(ValueError, match="'data_corruption' is registered twice"):
        faults.base.register_fault(AnotherCorruption)
    with pytest.raises(ValueError, match="'checkpoint' is registered twice"):
        faults.base.register_during(AnotherCheckpoint)
    registered = faults.base._FAULTS["data_corruption"]
    assert faults.base.register_fault(registered) is registered   # the same class again is harmless


def test_a_behaviour_without_a_name_is_refused():
    class Nameless(faults.FaultBehaviour):
        pass

    with pytest.raises(ValueError, match="Nameless does not set its fault_type"):
        faults.base.register_fault(Nameless)


def test_an_unregistered_during_value_names_the_problem(monkeypatch):
    monkeypatch.setattr(faults.base, "_DURING", {})
    run = SimpleNamespace(scenario=SimpleNamespace(fault=SimpleNamespace(type="process_kill", during="checkpoint")))
    with pytest.raises(NotImplementedError, match="no behaviour is registered for fault.during 'checkpoint'"):
        faults.during_behaviour(run)


def test_behaviours_see_the_run_only_through_the_context():
    """Behaviours depend on RunContext's short list, not on how the orchestrator keeps its
    state: only context.py may reach the orchestrator, and only the factories in base.py
    hand it over."""
    offending = []
    for path in Path(faults.__file__).parent.glob("*.py"):
        if path.name == "context.py":
            continue
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Attribute) and node.attr in ("run", "_run"):
                offending.append(f"{path.name}:{node.lineno} .{node.attr}")
            if isinstance(node, ast.Name) and node.id == "run" and path.name != "base.py":
                offending.append(f"{path.name}:{node.lineno} run")
    assert not offending, f"behaviours reach the orchestrator directly: {offending}"


def test_the_context_reads_the_run_when_used_not_when_built():
    """Behaviours are built before the injector, the stream and T0 exist."""
    run = SimpleNamespace(scenario=SimpleNamespace(fault=SimpleNamespace(type="process_kill", during=None)),
                          facts={}, t0_ns=None, t1_ns=None, stream=None)
    b = faults.fault_behaviour(run)
    run.t0_ns = 42
    emitted = []
    run.stream = SimpleNamespace(emit=lambda source, kind, **data: emitted.append((source, kind, data)))
    run.facts = {"replaced": True}
    assert asyncio.run(b.begin_recovery()) == {"recovery": "unattended (no harness action)"}
    assert run.t1_ns == 42
    assert emitted == [("orchestrator", "t1", {"t1_mono_ns": 42, "action": "unattended recovery expected"})]
    assert b.ctx.facts is run.facts


def test_the_process_driver_serves_the_same_fault_types():
    assert OsSshProcessDriver.fault_types == {"process_kill", "service_restart", "config_reload",
                                              "connection_exhaustion", "idle_in_transaction"}


def test_the_orchestrator_never_compares_a_fault_type_or_during_value():
    """Fault-specific behaviour belongs in control/faults; a comparison such as
    `scenario.fault.type == "data_corruption"` in the orchestrator reintroduces the branching
    the behaviours replaced."""
    import resilience_tests.control.orchestrator as orch

    tree = ast.parse(Path(orch.__file__).read_text())
    offending = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Compare):
            for side in [node.left, *node.comparators]:
                if isinstance(side, ast.Attribute) and side.attr in ("type", "during") \
                        and isinstance(side.value, ast.Attribute) and side.value.attr == "fault":
                    offending.append(node.lineno)
    assert not offending, f"orchestrator.py compares fault.type/fault.during at lines {offending}"
