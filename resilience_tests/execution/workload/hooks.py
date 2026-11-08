"""Profile hooks the orchestrator calls for every workload (docs/tpcc-contract.md §6).

    init      prepare(adapter)                    the profile's data is present and right
    validate  consistency_checks(adapter, run_dir) profile-specific checks after recovery

The custom workload (marker, churn, list_append) needs neither, so its hooks do nothing: a
scenario that uses it runs exactly as before. The `tpcc` profile gets real hooks when it is
built (TPC-C build steps 3-7); until then it is refused as not built, before anything is
applied (run plan and init both ask `not_built_reason`).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from catalog.schema import Workload
from resilience_tests.execution.workload.contract import ProfileHooks

# Workload profiles the catalog accepts but the harness cannot run yet.
NOT_BUILT_PROFILES: frozenset[str] = frozenset({"tpcc"})


class NoProfileHooks:
    """The hooks of the custom workload: nothing to prepare, nothing extra to check."""

    async def prepare(self, adapter: Any) -> dict[str, Any]:
        return {}

    async def consistency_checks(self, adapter: Any, run_dir: Path) -> dict[str, Any]:
        return {}


def not_built_reason(workload: Workload) -> str | None:
    if workload.profile in NOT_BUILT_PROFILES:
        return (f"workload profile {workload.profile!r} is not built yet (TPC-C build plan, "
                "docs/tpcc-build-plan.md): the scenario cannot be run to specification")
    return None


def hooks_for(workload: Workload) -> ProfileHooks:
    reason = not_built_reason(workload)
    if reason:
        raise NotImplementedError(reason)
    return NoProfileHooks()
