"""Compound scenarios (CL-X) are refused until compound injection is built.

A compound scenario declares a second fault (`compound.fault_b`). Nothing injects it yet, so a
run would apply fault A alone and could pass without testing what the scenario specifies.
"""

from __future__ import annotations

import asyncio

from catalog.schema import BlastRadiusOverride, Catalog, Compound, CompoundFaultB
from resilience_tests.control.killswitch import ledger_for
from resilience_tests.control.matrix import RunPlanItem, expand
from resilience_tests.control.orchestrator import RunOptions, TestOrchestrator
from tests.test_orchestrator import FakeFault, env, scenario  # noqa: F401  (env is a fixture)


def compound_scenario():
    """NL-C-01 with a second fault, as CL-X-01 declares one (kill the sync standby once the
    cluster is promoting)."""
    fault_b = CompoundFaultB(trigger="on_state", condition="patroni_state == 'promoting'", timeout_s=60,
                             type="process_kill", target="sync_standby")
    return scenario("NL-C-01").model_copy(update={
        "compound": Compound(fault_b=fault_b, blast_radius_override=BlastRadiusOverride(max_nodes_affected=2))})


def test_a_compound_scenario_is_refused_before_any_fault_is_applied(env):
    sc = compound_scenario()
    item = RunPlanItem(scenario=sc, env_class=env.env_class, role="standalone", node=env.nodes[0])
    results = asyncio.run(TestOrchestrator(item, env, RunOptions()).run())

    assert results["status"] == "aborted"
    assert "compound faults are not built yet" in results["error"]
    assert results["verdict"] is None
    # refused in init: no injection was journalled, so fault A was never applied either
    assert ledger_for(env).entries() == [] and FakeFault.reverts == []
    ran = {p["phase"]: p["outcome"] for p in results["phases"]}
    assert ran["init"] == "aborted"
    assert not {"baseline", "fault_inject", "recovery", "validate"} & set(ran)   # report and cleanup still run


def test_the_run_plan_skips_a_compound_scenario_with_the_reason(env):
    catalog = Catalog(scenarios={"NL-C-01": compound_scenario()})
    plan, skipped = expand(catalog, env, reference_env_class=env.env_class)
    assert plan == []
    assert [(s.scenario_id, "compound faults are not built yet" in s.reason) for s in skipped] == [("NL-C-01", True)]
