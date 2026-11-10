"""TPC-C build step 3: the workload profile registry and the `tpcc` skeleton.

The registry follows the fault registry (@register_fault). The `tpcc` profile is registered
but not built, so it is skipped and refused with the reason; the custom workload is not
registered and keeps its do-nothing hooks. Every test fails for its own reason.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import get_args

import pytest

from catalog import schema as catalog_schema
from catalog.schema import Workload, load_catalog
from resilience_tests.adapters.base import BaseDatabaseAdapter, Capability, TpccSupport, capabilities_of
from resilience_tests.control.matrix import expand
from resilience_tests.control.profile import load_profile
from resilience_tests.execution.workload import hooks
from resilience_tests.execution.workload.profiles import base as profiles_base
from resilience_tests.execution.workload.profiles import (
    Request,
    Result,
    Terminal,
    WorkloadProfile,
    profile_class,
    register_profile,
    registered_profiles,
)
from resilience_tests.execution.workload.tpcc.profile import TpccProfile
from tests.test_adapter_seam import NODE, WORKLOAD, FakeAdapter

TPCC_WORKLOAD = Workload(profile="tpcc", concurrency=8, transaction_markers=True, rate_tps=200,
                         tpcc={"warehouses": 40})


# --- the registry ------------------------------------------------------------------------

def test_tpcc_is_registered_and_the_custom_workload_is_not():
    assert registered_profiles() == {"tpcc": TpccProfile}
    assert profile_class("tpcc") is TpccProfile
    for custom in ("oltp_write_heavy", "mixed", "idle"):
        assert profile_class(custom) is None          # add-only: today's shapes are not moved


def test_a_nameless_profile_is_refused():
    class Nameless(TpccProfile):
        name = ""

    with pytest.raises(ValueError, match="does not set its name"):
        register_profile(Nameless)


def test_a_second_profile_with_the_same_name_is_refused():
    class Impostor(TpccProfile):
        name = "tpcc"

    with pytest.raises(ValueError, match="registered twice"):
        register_profile(Impostor)
    assert profile_class("tpcc") is TpccProfile        # the original stays


def test_registering_the_same_class_again_is_harmless():
    assert register_profile(TpccProfile) is TpccProfile


# --- the tpcc skeleton: registered, not built, refused with the reason ----------------------

def test_tpcc_is_not_built_so_it_is_refused():
    assert TpccProfile.built is False
    assert "not built yet" in (hooks.not_built_reason(TPCC_WORKLOAD) or "")
    with pytest.raises(NotImplementedError, match="not built yet"):
        hooks.hooks_for(TPCC_WORKLOAD)


def test_every_skeleton_hook_refuses_and_names_its_build_step():
    p = TpccProfile(TPCC_WORKLOAD)
    calls = [
        (5, lambda: p.terminal(0)),
        (5, lambda: p.next_request(Terminal(0, "all"))),
        (6, lambda: asyncio.run(p.execute(None, Request("new_order"), None))),
        (7, lambda: asyncio.run(p.consistency_checks(None, Path(".")))),
    ]
    for step, call in calls:
        with pytest.raises(NotImplementedError, match=f"step {step}"):
            call()


def test_tpcc_needs_the_tpcc_capability():
    assert TpccProfile(TPCC_WORKLOAD).required_capabilities() == {Capability.TPCC_WORKLOAD}
    assert hooks.required_capabilities(TPCC_WORKLOAD) == {"tpcc_workload"}
    assert hooks.required_capabilities(WORKLOAD) == set()             # custom workload: nothing extra


# --- the run plan: a tpcc scenario is skipped on an engine without the capability ----------

def test_run_plan_skips_a_tpcc_scenario_on_postgresql_today_with_the_reason():
    catalog = load_catalog()
    sc = catalog.scenarios["NL-C-01"]
    catalog.scenarios["NL-C-01"] = sc.model_copy(update={"workload": TPCC_WORKLOAD})
    plan, skipped = expand(catalog, load_profile("e2-dedicated-vm"), reference_env_class="E2", only={"NL-C-01"})
    assert plan == []
    reasons = {s.scenario_id: s.reason for s in skipped}
    assert "tpcc_workload" in reasons["NL-C-01"]


def test_run_plan_is_unchanged_for_existing_scenarios():
    plan, _ = expand(load_catalog(), load_profile("e2-dedicated-vm"), reference_env_class="E2", only={"NL-C-01"})
    assert len(plan) == 1


# --- a built profile gets its own hooks (shown with a temporary stand-in) -------------------

class BuiltStandIn(WorkloadProfile):
    name = "tpcc"
    built = True

    async def prepare(self, adapter):
        return {"warehouses": 40}

    def terminal(self, worker_id):
        return Terminal(worker_id, "all", home=1)

    def next_request(self, terminal):
        return Request("new_order", {"w": terminal.home})

    async def execute(self, session, request, marker):
        return Result("committed", {"o_id": 3001})

    async def consistency_checks(self, adapter, run_dir):
        return {"partial_commits": 0}


def test_a_built_profile_provides_its_own_hooks(monkeypatch):
    monkeypatch.setitem(profiles_base._PROFILES, "tpcc", BuiltStandIn)
    h = hooks.hooks_for(TPCC_WORKLOAD)
    assert isinstance(h, BuiltStandIn)
    assert hooks.not_built_reason(TPCC_WORKLOAD) is None
    assert asyncio.run(h.prepare(None)) == {"warehouses": 40}
    assert asyncio.run(h.consistency_checks(None, Path("."))) == {"partial_commits": 0}


def test_the_custom_workload_keeps_its_do_nothing_hooks():
    assert isinstance(hooks.hooks_for(WORKLOAD), hooks.NoProfileHooks)


# --- the adapter: capability vocabulary and the TpccSupport interface group ----------------

def test_adapter_capabilities_and_catalog_vocabulary_are_the_same():
    assert {c.value for c in Capability} == set(get_args(catalog_schema.Capability))


def test_every_adapter_has_the_tpcc_interface_group_and_it_refuses_by_default():
    assert issubclass(BaseDatabaseAdapter, TpccSupport)
    fake = FakeAdapter(NODE)
    with pytest.raises(NotImplementedError, match="cannot run the TPC-C workload"):
        asyncio.run(fake.tpcc_data_status())
    with pytest.raises(NotImplementedError, match="cannot run the TPC-C workload"):
        asyncio.run(fake.tpcc_consistency([1, 2, 3]))


def test_postgresql_does_not_claim_the_tpcc_capability_yet():
    # claimed in step 4/6, when the adapter really has the tables, loader and functions
    assert Capability.TPCC_WORKLOAD not in capabilities_of("postgresql")
