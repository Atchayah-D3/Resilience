"""The `tpcc` workload profile: TPC-C 5.11-like transactions (docs/tpcc-workload-proposal.md).

Registered in step 3; it runs since step 6; since step 7 its checks after a fault are built,
so it may be used for real fault runs. Build steps (docs/tpcc-build-plan.md):

    step 4  prepare(): the TPC-C data is loaded, the right size and consistent   (done)
    step 5  terminal(), next_request(): clerk placement and the TPC-C inputs (pure code)   (done)
    step 6  execute(): one server-side function call per transaction; the engine   (done)
    step 7  consistency_checks(): RPO, per-order check, consistency rules after the fault   (done)
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
from resilience_tests.execution.workload.markers import (
    ABORTED_JOURNAL,
    ACKED_JOURNAL,
    MARKER_JOURNAL,
    journal_paths,
    read_journal,
)
from resilience_tests.execution.workload.tpcc import checks, data, inputs
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
    built = True
    ready_for_faults = True      # step 7: the checks after a fault are built

    def __init__(self, workload: Any, seed: int = SEED) -> None:
        super().__init__(workload)
        self.seed = seed
        self.constants: inputs.RunConstants | None = None      # set by prepare() from the loaded data
        self.orders_at_prepare: int | None = None                # sum(D_NEXT_O_ID - 1) at prepare, for step 7
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
            await adapter.tpcc_install_functions()     # this harness's transaction functions
            results = await adapter.tpcc_consistency(data.PREPARE_RULES, district_sample=data.PREPARE_DISTRICT_SAMPLE)
            problems = data.consistency_problems({r: results.get(r) for r in data.PREPARE_RULES})
        if problems:
            raise TpccDataError("the TPC-C data cannot be used: " + "; ".join(problems))
        consts = self.set_run_constants(status["c_last_load"])
        self.orders_at_prepare = status.get("current_orders")
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
        """One transaction = one call through the adapter (all SQL lives there)."""
        outcome, journal, sqlstate = await session.tpcc_execute(request.txn_type, request.inputs, marker)
        return Result(outcome=outcome, journal=journal, sqlstate=sqlstate)

    def make_driver(self, adapter: Any, journals: Any, stream: Any) -> Any:
        from resilience_tests.execution.workload.tpcc.engine import TpccWorkloadDriver
        if self.constants is None:
            raise RuntimeError("the run's NURand constants are not set: prepare() must run first")
        return TpccWorkloadDriver(adapter, self.workload, journals, stream, self)

    async def consistency_checks(self, adapter: Any, run_dir: Path) -> dict[str, Any]:
        """Validate: label every write transaction from the evidence (tpcc/checks.py) and return
        the measures, plus "_facts" (the labels in detail) and "_not_measured" (why a measure
        could not be taken). A part that fails to run is NOT MEASURED, never 0."""
        written, acked, aborted, torn = {}, {}, {}, 0
        for p in journal_paths(run_dir, MARKER_JOURNAL):
            written.update({r["uuid"]: r.get("type", "") for r in read_journal(p).records})
        for p in journal_paths(run_dir, ACKED_JOURNAL):
            acked.update({r["uuid"]: r for r in read_journal(p).records})
        for p in journal_paths(run_dir, ABORTED_JOURNAL):
            j = read_journal(p)
            aborted.update({r["uuid"]: r for r in j.records})
            torn += j.torn_lines

        problems: list[str] = []

        async def attempt(what: str, call: Any) -> Any:
            try:
                return await call
            except Exception as exc:  # noqa: BLE001 -- recorded; the measure is then not measured
                problems.append(f"{what}: {type(exc).__name__}: {exc}")
                return None

        db = await attempt("markers", adapter.marker_ids())
        acked_check = lost_check = None
        if db is not None:
            acked_check = await attempt("per-order check", adapter.tpcc_order_check(
                checks.new_order_lines(acked, set(acked) & db)))
            lost_check = await attempt("per-order check of lost New-Orders", adapter.tpcc_order_check(
                checks.new_order_lines(acked, set(acked) - db)))
        status = await attempt("order count", adapter.tpcc_data_status())
        rules: dict[int, int | None] = {}
        full = await attempt("consistency rules", adapter.tpcc_consistency(checks.RULES_ALL_DATA))
        sampled = await attempt("consistency rules (sample)", adapter.tpcc_consistency(
            checks.RULES_SAMPLED, district_sample=checks.SAMPLE_DISTRICTS, seed=self.seed))
        rules.update(full or {})
        rules.update(sampled or {})

        ev = checks.Evidence(
            written=written, acked=acked, aborted=aborted, db=db,
            acked_order_check=acked_check, lost_order_check=lost_check,
            orders_before=self.orders_at_prepare,
            orders_after=(status or {}).get("current_orders"),
            rules=rules, torn_aborted_lines=torn)
        measures, facts, reasons = checks.evaluate(ev)
        if problems:
            facts["errors"] = problems
            reasons = {m: f"{why} ({'; '.join(problems)})" for m, why in reasons.items()}
        return {**measures, "_facts": facts, "_not_measured": reasons}
