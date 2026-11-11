"""Profile hooks the orchestrator calls for every workload (docs/tpcc-contract.md §6).

    init      prepare(adapter)                     the profile's data is present and right
    validate  consistency_checks(adapter, run_dir)  profile-specific checks after recovery

Registered profiles (profiles/, e.g. `tpcc`) provide their own hooks. The custom workload
(marker, churn, list_append) is not registered and needs neither hook, so its hooks do nothing
and a scenario that uses it runs exactly as before. A registered profile that is not built yet
is refused, before anything is applied: the run plan and init both ask `not_built_reason`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from catalog.schema import Workload
from resilience_tests.execution.workload.contract import ProfileHooks
from resilience_tests.execution.workload.profiles import WorkloadProfile, profile_class


class NoProfileHooks:
    """The hooks of the custom workload: nothing to prepare, nothing extra to check."""

    async def prepare(self, adapter: Any) -> dict[str, Any]:
        return {}

    async def consistency_checks(self, adapter: Any, run_dir: Path) -> dict[str, Any]:
        return {}


def split_checks(result: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], dict[str, str]]:
    """A profile's consistency_checks() result -> (measures, facts, not_measured reasons).

    "_facts" holds the details for the evidence; "_not_measured" maps a measure to why it could
    not be taken. Such a measure is NOT MEASURED in the verdict (never 0), whatever value the
    profile may also have returned for it."""
    measures = {k: v for k, v in result.items() if not k.startswith("_")}
    facts = dict(result.get("_facts") or {})
    reasons = {str(k): str(v) for k, v in (result.get("_not_measured") or {}).items()}
    for name in reasons:
        measures.pop(name, None)
    return measures, facts, reasons


def with_tpcc_workload(scenario: Any, warehouses: int) -> Any:
    """The scenario with the TPC-C workload in place of its own (same clerks, rate, groups and
    session settings; `warehouses` must match the loaded data). For trying a scenario on TPC-C
    before it is switched for good (TPC-C build steps 7 and 10). The run discloses it. A
    list-append history is not kept with TPC-C, so a scenario that needs Elle then fails its
    Elle criterion rather than passing without it."""
    w = scenario.workload
    data = {**w.model_dump(), "profile": "tpcc", "history": "none",
            "tpcc": {"warehouses": warehouses, "home_warehouse": {}}}
    return scenario.model_copy(update={"workload": Workload(**data)})


def tpcc_override_note(warehouses: int) -> str:
    return (f"Workload replaced for this run: the TPC-C workload (profile tpcc, {warehouses} warehouses) ran in "
            "place of the scenario's own workload, with the same clerks, rate and session settings "
            "(--tpcc-warehouses). The scenario file was not changed.")


def not_built_reason(workload: Workload) -> str | None:
    cls = profile_class(workload.profile)
    if cls is not None and not cls.built:
        return (f"workload profile {workload.profile!r} is not built yet (TPC-C build plan, "
                "docs/tpcc-build-plan.md): the scenario cannot be run to specification")
    return None


def fault_runs_reason(workload: Workload) -> str | None:
    """Why this workload may only be used for dry runs (--stop-before-fault) for now, or None."""
    cls = profile_class(workload.profile)
    if cls is not None and cls.built and not cls.ready_for_faults:
        return (f"workload profile {workload.profile!r} can run only dry runs (--stop-before-fault) until its "
                "checks after a fault are built (docs/tpcc-build-plan.md, step 7)")
    return None


def make_workload_driver(adapter: Any, workload: Workload, journals: Any, stream: Any,
                         history: Any, hooks: ProfileHooks) -> Any:
    """The engine for this workload: a registered profile's own engine, else the custom driver."""
    if isinstance(hooks, WorkloadProfile):
        return hooks.make_driver(adapter, journals, stream)
    from resilience_tests.execution.workload.driver import WorkloadDriver
    return WorkloadDriver(adapter, workload, journals, stream, history=history)


def required_capabilities(workload: Workload) -> set[str]:
    """Engine capabilities the scenario's workload profile needs (catalog vocabulary)."""
    cls = profile_class(workload.profile)
    if cls is None:
        return set()
    return {c.value for c in cls(workload).required_capabilities()}


def hooks_for(workload: Workload) -> ProfileHooks:
    reason = not_built_reason(workload)
    if reason:
        raise NotImplementedError(reason)
    cls = profile_class(workload.profile)
    return cls(workload) if cls is not None else NoProfileHooks()
