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
from tests.test_orchestrator import env  # noqa: F401  (env is a fixture)

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


def _fault_names(node: ast.AST) -> list[str]:
    names = set(FaultType.__args__) | set(FaultDuring.__args__)
    if isinstance(node, ast.Constant) and node.value in names:
        return [node.value]
    if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
        return [v for e in node.elts for v in _fault_names(e)]
    return []


def test_no_module_branches_on_a_fault_type_or_during_name():
    """Was: the adapter chose what to observe and create with `fault_type != "idle_in_transaction"`
    and `during == "checkpoint"`, which the orchestrator-only check above could not see. A fault
    or `during` name compared anywhere in the harness -- through any variable, in any module --
    is the branching the behaviour classes replaced."""
    root = Path(faults.__file__).parents[2]   # resilience_tests/
    offending = []
    for path in sorted(root.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            sides = [node.left, *node.comparators] if isinstance(node, ast.Compare) else \
                    [c.pattern.value for c in node.cases if isinstance(c.pattern, ast.MatchValue)] \
                    if isinstance(node, ast.Match) else []
            hits = [v for side in sides for v in _fault_names(side)]
            if hits:
                offending.append(f"{path.relative_to(root)}:{node.lineno} {hits}")
    assert not offending, f"fault names compared outside the behaviour registry: {offending}"


def test_a_second_driver_for_the_same_fault_type_is_refused(monkeypatch):
    """Was: `register` appended any class, so two drivers serving one fault type under the same
    profile section ran whichever was imported first."""
    from resilience_tests.execution.injectors import base as injectors

    monkeypatch.setattr(injectors, "_REGISTRY", {k: list(v) for k, v in injectors._REGISTRY.items()})

    class AnotherKill(OsSshProcessDriver):
        fault_types = frozenset({"process_kill"})

    class Unrelated(OsSshProcessDriver):
        fault_types = frozenset({"clock_skew"})

    with pytest.raises(ValueError, match=r"process_kill of os_ssh/os_ssh registered twice"):
        injectors.register("os_ssh", AnotherKill)
    assert injectors.register("os_ssh", OsSshProcessDriver) is OsSshProcessDriver   # again: harmless
    assert injectors.register("os_ssh", Unrelated) is Unrelated                     # other fault types: fine


def test_the_report_names_no_fault_and_each_fault_supplies_its_own_section():
    """Was: report.py carried the checkpoint, idle-transaction and corruption sections itself, so
    a new fault meant editing the report. Each behaviour supplies its own section, and the report
    is handed them when it is written -- it keeps no list that importing a module fills in."""
    from resilience_tests.analysis import report
    from resilience_tests.control.faults.corruption import DataCorruption
    from resilience_tests.control.faults.during import Checkpoint
    from resilience_tests.control.faults.idle_transaction import IdleTransaction

    sections = faults.summary_sections()
    assert [s.__func__ for s in sections] == [
        DataCorruption.summary_lines.__func__, IdleTransaction.summary_lines.__func__,
        Checkpoint.summary_lines.__func__]
    source = Path(report.__file__).read_text()
    for fact_key in ("checkpoint_injection", "idle_transaction", "data_corruption"):
        assert fact_key not in source, f"report.py reads the {fact_key!r} fact itself"
    imported = {n.module for n in ast.walk(ast.parse(source)) if isinstance(n, ast.ImportFrom)}
    assert not {m for m in imported if m and m.startswith("resilience_tests.control")}, \
        "analysis/report.py depends on the control layer"
    assert not hasattr(report, "SECTIONS")
    # a section concerns only its own runs: none of them adds lines to an unrelated run
    unrelated = {"facts": {"cycles": []}, "measured": {}}
    assert all(s(unrelated) == [] for s in sections)


def test_the_sections_follow_the_registry_not_import_side_effects(monkeypatch):
    """A behaviour registered later (or replaced in a test) is reported from that moment on;
    nothing is appended to a shared list that would outlive it."""
    class Extra(faults.FaultBehaviour):
        fault_type = "clock_skew"

        @classmethod
        def summary_lines(cls, r):
            return ["clock"]

    monkeypatch.setattr(faults.base, "_FAULTS", dict(_FAULTS))
    faults.base.register_fault(Extra)
    assert any(s.__func__ is Extra.summary_lines.__func__ for s in faults.summary_sections())
    monkeypatch.undo()
    assert all(s.__func__ is not Extra.summary_lines.__func__ for s in faults.summary_sections())


# --- what is not built refuses before anything is applied ---------------------------------


def test_every_fault_type_a_driver_serves_has_a_behaviour():
    """Was: only `registered behaviours are catalog fault types` was checked. A driver added
    without a behaviour would inject a fault whose recovery nobody defined."""
    from resilience_tests.execution.injectors import base as injectors

    served = {t for classes in injectors._REGISTRY.values() for cls in classes for t in cls.fault_types}
    assert served <= set(_FAULTS), f"served by a driver but no behaviour: {sorted(served - set(_FAULTS))}"


def _unbuilt_fault_scenario():
    from tests.test_orchestrator import scenario
    sc = scenario("NL-C-01")
    return sc.model_copy(update={"fault": sc.fault.model_copy(update={"type": "clock_skew"})})


def test_a_fault_type_without_a_behaviour_is_refused_in_init_before_any_injection(env):
    """Was: the run fell back to the default behaviour, injected the fault, and refused only in
    the recovery phase -- with the fault already applied."""
    from resilience_tests.control.killswitch import ledger_for
    from resilience_tests.control.matrix import RunPlanItem
    from resilience_tests.control.orchestrator import RunOptions, TestOrchestrator
    from tests.test_orchestrator import FakeFault

    item = RunPlanItem(scenario=_unbuilt_fault_scenario(), env_class=env.env_class, role="standalone",
                       node=env.nodes[0])
    results = asyncio.run(TestOrchestrator(item, env, RunOptions()).run())
    assert results["status"] == "aborted"
    assert "fault type 'clock_skew' is not built yet" in results["error"]
    assert ledger_for(env).entries() == [] and FakeFault.reverts == []
    ran = {p["phase"]: p["outcome"] for p in results["phases"]}
    assert ran["init"] == "aborted" and "fault_inject" not in ran


def test_the_run_plan_skips_a_fault_type_without_a_behaviour(env):
    from catalog.schema import Catalog
    from resilience_tests.control.matrix import expand

    plan, skipped = expand(Catalog(scenarios={"NL-C-01": _unbuilt_fault_scenario()}), env,
                           reference_env_class=env.env_class)
    assert plan == [] and len(skipped) == 1 and "is not built yet" in skipped[0].reason


# --- the orchestrator knows no fault's detail ---------------------------------------------


def test_the_orchestrator_reads_no_fault_specific_detail():
    """Was: the orchestrator read `landed` (targeted kill), set the adapter's
    `integrity_exclusions` (corruption), stored `checkpoint` evidence and listed the targeted
    kill's per-cycle keys. Each now belongs to the fault or `during` behaviour that produces it."""
    import resilience_tests.control.orchestrator as orch

    fault_keys = {"landed", "kill_target", "target_pid", "death_confirmed_s", "postmaster_pid",
                  "postmaster_survived", "integrity_exclusions", "checkpoint", "relation_kind"}
    tree = ast.parse(Path(orch.__file__).read_text())
    found = sorted({(n.lineno, n.value) for n in ast.walk(tree) if isinstance(n, ast.Constant) and n.value in fault_keys})
    attrs = sorted({(n.lineno, n.attr) for n in ast.walk(tree) if isinstance(n, ast.Attribute)
                    and n.attr in ("integrity_exclusions", "kills_landed", "kill_target")})
    assert not found and not attrs, f"orchestrator.py reads fault-specific detail: {found + attrs}"


def test_run_state_is_declared_not_created_on_first_use():
    """Was: the orchestrator created `_cycle_entries` and `_inject_detail` with getattr defaults,
    and the adapter mixins did the same. State is declared where its owner is defined; a
    getattr/hasattr on `self` with a literal name is that pattern coming back."""
    root = Path(faults.__file__).parents[2]   # resilience_tests/
    offending = []
    for path in sorted(root.rglob("*.py")):
        for node in ast.walk(ast.parse(path.read_text())):
            if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in ("getattr", "hasattr")
                    and len(node.args) >= 2 and isinstance(node.args[0], ast.Name) and node.args[0].id == "self"
                    and isinstance(node.args[1], ast.Constant)):
                offending.append(f"{path.relative_to(root)}:{node.lineno} {node.func.id}(self, {node.args[1].value!r})")
    assert not offending, offending


def test_reverting_an_injection_the_ledger_does_not_hold_names_it():
    """Was: a bare StopIteration from next() surfaced as an obscure error."""
    run = SimpleNamespace(scenario=SimpleNamespace(fault=SimpleNamespace(type="process_kill", during=None)),
                          facts={"injection_id": "inj-42"}, injector=SimpleNamespace(),
                          ledger=SimpleNamespace(outstanding=lambda: [], path="/runs/injection-ledger.jsonl"))
    with pytest.raises(RuntimeError, match="injection 'inj-42' of this run is not outstanding in the ledger"):
        asyncio.run(faults.fault_behaviour(run).ctx.revert_fault())


def test_an_engine_that_cannot_report_configuration_deviations_is_disclosed(env, monkeypatch):
    from tests.test_orchestrator import OutageAdapter, run

    async def cannot_tell(self):
        return None
    monkeypatch.setattr(OutageAdapter, "config_deviations", cannot_tell)
    results = run(env, "NL-C-01")
    assert results["facts"]["config_deviations"] is None
    assert any("cannot report configuration deviations" in d for d in results["disclosures"])
