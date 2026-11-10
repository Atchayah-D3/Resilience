"""The `tpcc` workload profile: TPC-C 5.11-like transactions (docs/tpcc-workload-proposal.md).

A skeleton (TPC-C build step 3): it is registered, so the harness knows the name, the
capability it needs and where its hooks live, but `built = False`, so every scenario that uses
it is skipped by the run plan and refused by init, with the reason, before anything is
applied. The later steps fill it in (docs/tpcc-build-plan.md):

    step 4  prepare(): the TPC-C data is loaded, the right size and consistent
    step 5  terminal(), next_request(): clerk placement and the TPC-C inputs (pure code)
    step 6  execute(): one server-side function call per transaction
    step 7  consistency_checks(): RPO, per-order check, consistency rules after the fault
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from resilience_tests.adapters.base import Capability
from resilience_tests.execution.workload.profiles.base import (
    Request,
    Result,
    Terminal,
    WorkloadProfile,
    register_profile,
)

NOT_BUILT = "the tpcc workload profile is not built yet (docs/tpcc-build-plan.md, step {step})"


@register_profile
class TpccProfile(WorkloadProfile):
    name = "tpcc"
    built = False            # becomes True when steps 4-7 are done

    def required_capabilities(self) -> set[Capability]:
        return {Capability.TPCC_WORKLOAD}

    async def prepare(self, adapter: Any) -> dict[str, Any]:
        raise NotImplementedError(NOT_BUILT.format(step=4))

    def terminal(self, worker_id: int) -> Terminal:
        raise NotImplementedError(NOT_BUILT.format(step=5))

    def next_request(self, terminal: Terminal) -> Request:
        raise NotImplementedError(NOT_BUILT.format(step=5))

    async def execute(self, session: Any, request: Request, marker: tuple[int, str] | None) -> Result:
        raise NotImplementedError(NOT_BUILT.format(step=6))

    async def consistency_checks(self, adapter: Any, run_dir: Path) -> dict[str, Any]:
        raise NotImplementedError(NOT_BUILT.format(step=7))
