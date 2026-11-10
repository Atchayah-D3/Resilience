"""The `tpcc` workload profile: TPC-C 5.11-like transactions (docs/tpcc-workload-proposal.md).

A skeleton (TPC-C build step 3): it is registered, so the harness knows the name, the
capability it needs and where its hooks live, but `built = False`, so every scenario that uses
it is skipped by the run plan and refused by init, with the reason, before anything is
applied. The later steps fill it in (docs/tpcc-build-plan.md):

    step 4  prepare(): the TPC-C data is loaded, the right size and consistent   (done)
    step 5  terminal(), next_request(): clerk placement and the TPC-C inputs (pure code)   (done)
    step 6  execute(): one server-side function call per transaction
    step 7  consistency_checks(): RPO, per-order check, consistency rules after the fault
"""

from __future__ import annotations

import random
from pathlib import Path
from typing import Any

from resilience_tests.adapters.base import Capability
from resilience_tests.execution.workload.contract import (
    DELIVERY,
    NEW_ORDER,
    ORDER_STATUS,
    PAYMENT,
    STOCK_LEVEL,
    TPCC_READ_ONLY_TYPES,
)
from resilience_tests.execution.workload.tpcc import data, inputs
from resilience_tests.execution.workload.tpcc.terminals import place_clerks
from resilience_tests.execution.workload.profiles.base import (
    Request,
    Result,
    Terminal,
    WorkloadProfile,
    register_profile,
)

NOT_BUILT = "the tpcc workload profile is not built yet (docs/tpcc-build-plan.md, step {step})"

# Fixed random seed: every run of the same scenario sends the same sequence of transactions
# (proposal 7.11, "repeatable"). Each clerk draws from its own generator, seeded from this.
SEED = 51_105


class TpccDataError(RuntimeError):
    """The TPC-C data on the target cannot be used for this run; the reasons are in the message."""


@register_profile
class TpccProfile(WorkloadProfile):
    name = "tpcc"
    built = False            # becomes True when steps 4-7 are done

    def __init__(self, workload: Any, seed: int = SEED) -> None:
        super().__init__(workload)
        self.seed = seed
        self.constants: inputs.RunConstants | None = None      # set by prepare() from the loaded data
        self._terminals: list[Terminal] | None = None
        self._clerks: dict[int, tuple[random.Random, inputs.Deck]] = {}

    def set_run_constants(self, c_last_load: int) -> inputs.RunConstants:
        """The run's NURand constants, chosen against the load's C_LAST constant (Clause 2.1.6.1)."""
        self.constants = inputs.RunConstants.choose(random.Random(self.seed), c_last_load)
        return self.constants

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
        consts = self.set_run_constants(status["c_last_load"])
        return {"tpcc_data": {
            "run_constants": {"c_last": consts.c_last, "c_id": consts.c_id, "ol_i_id": consts.ol_i_id,
                              "c_last_load": status["c_last_load"]},
            "warehouses": status["warehouses"], "loader_version": status["loader_version"],
            "loaded_at": status.get("loaded_at"), "growth": data.growth(status),
            "size_bytes": status.get("size_bytes"),
            "consistency": {str(r): results[r] for r in data.PREPARE_RULES},
            "district_sample": data.PREPARE_DISTRICT_SAMPLE,
        }}

    def terminal(self, worker_id: int) -> Terminal:
        """Clerk `worker_id`: its group, home warehouse and fixed district (terminals.py)."""
        if self._terminals is None:
            self._terminals = place_clerks(self.workload)
        return self._terminals[worker_id]

    def next_request(self, terminal: Terminal) -> Request:
        """The clerk's next transaction: the next card of its deck, with TPC-C inputs (inputs.py)."""
        if self.constants is None:
            raise RuntimeError("the run's NURand constants are not set: prepare() must run first")
        assert self.workload.tpcc is not None and terminal.home is not None and terminal.district is not None
        rng, deck = self._clerks.setdefault(
            terminal.worker_id, (r := random.Random(self.seed * 1_000 + terminal.worker_id), inputs.Deck(r)))
        card = deck.draw()
        w, home = self.workload.tpcc.warehouses, terminal.home
        if card == NEW_ORDER:
            values = inputs.new_order(rng, home, w, self.constants)
        elif card == PAYMENT:
            values = inputs.payment(rng, home, w, self.constants)
        elif card == ORDER_STATUS:
            values = inputs.order_status(rng, home, self.constants)
        elif card == DELIVERY:
            values = inputs.delivery(rng, home)
        elif card == STOCK_LEVEL:
            values = inputs.stock_level(rng, home, terminal.district)
        else:                                            # pragma: no cover -- the deck holds only these
            raise ValueError(f"unknown TPC-C card {card!r}")
        return Request(txn_type=card, inputs=values, needs_marker=card not in TPCC_READ_ONLY_TYPES)

    async def execute(self, session: Any, request: Request, marker: tuple[int, str] | None) -> Result:
        raise NotImplementedError(NOT_BUILT.format(step=6))

    async def consistency_checks(self, adapter: Any, run_dir: Path) -> dict[str, Any]:
        raise NotImplementedError(NOT_BUILT.format(step=7))
