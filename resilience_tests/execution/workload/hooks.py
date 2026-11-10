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
from resilience_tests.execution.workload.profiles import profile_class


class NoProfileHooks:
    """The hooks of the custom workload: nothing to prepare, nothing extra to check."""

    async def prepare(self, adapter: Any) -> dict[str, Any]:
        return {}

    async def consistency_checks(self, adapter: Any, run_dir: Path) -> dict[str, Any]:
        return {}


def not_built_reason(workload: Workload) -> str | None:
    cls = profile_class(workload.profile)
    if cls is not None and not cls.built:
        return (f"workload profile {workload.profile!r} is not built yet (TPC-C build plan, "
                "docs/tpcc-build-plan.md): the scenario cannot be run to specification")
    return None


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
