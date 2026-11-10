"""The `tpcc` workload profile: TPC-C 5.11-like transactions (docs/tpcc-workload-proposal.md).

A skeleton (TPC-C build step 3): it is registered, so the harness knows the name, the
capability it needs and where its hooks live, but `built = False`, so every scenario that uses
it is skipped by the run plan and refused by init, with the reason, before anything is
applied. The later steps fill it in (docs/tpcc-build-plan.md):

    step 4  prepare(): the TPC-C data is loaded, the right size and consistent   (done)
    step 5  terminal(), next_request(): clerk placement and the TPC-C inputs (pure code)
    step 6  execute(): one server-side function call per transaction
    step 7  consistency_checks(): RPO, per-order check, consistency rules after the fault
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from resilience_tests.adapters.base import Capability
from resilience_tests.execution.workload.tpcc import data
from resilience_tests.execution.workload.profiles.base import (
    Request,
    Result,
    Terminal,
    WorkloadProfile,
    register_profile,
)

NOT_BUILT = "the tpcc workload profile is not built yet (docs/tpcc-build-plan.md, step {step})"


class TpccDataError(RuntimeError):
    """The TPC-C data on the target cannot be used for this run; the reasons are in the message."""


@register_profile
class TpccProfile(WorkloadProfile):
    name = "tpcc"
    built = False            # becomes True when steps 4-7 are done

    def required_capabilities(self) -> set[Capability]:
        return {Capability.TPCC_WORKLOAD}

    async def prepare(self, adapter: Any) -> dict[str, Any]:
        """Init: refuse the run unless the loaded data is present, has the scenario's number of
        warehouses, was loaded by this loader, has not grown past the limit, and passes
        consistency rules 1-3 (all data) and 4 (a sample of districts). The data must be
        correct BEFORE the test (Clause 3.3.3.1); it is never reloaded silently."""
        assert self.workload.tpcc is not None      # the catalog requires it for profile tpcc
        status = await adapter.tpcc_data_status()
        problems = data.data_problems(status, self.workload.tpcc.warehouses)
        results: dict[int, int] = {}
        if not problems:
            results = await adapter.tpcc_consistency(data.PREPARE_RULES, district_sample=data.PREPARE_DISTRICT_SAMPLE)
            problems = data.consistency_problems({r: results.get(r) for r in data.PREPARE_RULES})
        if problems:
            raise TpccDataError("the TPC-C data cannot be used: " + "; ".join(problems))
        return {"tpcc_data": {
            "warehouses": status["warehouses"], "loader_version": status["loader_version"],
            "loaded_at": status.get("loaded_at"), "growth": data.growth(status),
            "size_bytes": status.get("size_bytes"),
            "consistency": {str(r): results[r] for r in data.PREPARE_RULES},
            "district_sample": data.PREPARE_DISTRICT_SAMPLE,
        }}

    def terminal(self, worker_id: int) -> Terminal:
        raise NotImplementedError(NOT_BUILT.format(step=5))

    def next_request(self, terminal: Terminal) -> Request:
        raise NotImplementedError(NOT_BUILT.format(step=5))

    async def execute(self, session: Any, request: Request, marker: tuple[int, str] | None) -> Result:
        raise NotImplementedError(NOT_BUILT.format(step=6))

    async def consistency_checks(self, adapter: Any, run_dir: Path) -> dict[str, Any]:
        raise NotImplementedError(NOT_BUILT.format(step=7))
