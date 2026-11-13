"""The `tpcc` workload profile: TPC-C 5.11-like transactions (docs/tpcc-workload-proposal.md).

Registered in step 3; it runs since step 6; since step 7 its checks after a fault are built,
so it may be used for real fault runs. Build steps (docs/tpcc-build-plan.md):

    step 4  prepare(): the TPC-C data is loaded, the right size and consistent   (done)
    step 5  terminal(), next_request(): clerk placement and the TPC-C inputs (pure code)   (done)
    step 6  execute(): one server-side function call per transaction; the engine   (done)
    step 7  consistency_checks(): RPO, per-order check, consistency rules after the fault   (done)
"""

from __future__ import annotations

import json
import os
import random
import time
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
from resilience_tests.execution.workload.tpcc.samples import choose_processes
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
RESET_JOBS = 4               # parallel loaders for a reload before a run (as used on the lab)


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
        self.history_since: Any = None                           # database time at prepare: this run's payments
        self._terminals: list[Terminal] | None = None
        self._clerks: dict[int, tuple[random.Random, inputs.Deck]] = {}

    def set_run_constants(self, c_last_load: int) -> inputs.RunConstants:
        """The run's NURand constants, chosen against the load's C_LAST constant (Clause 2.1.6.1)."""
        self.constants = inputs.RunConstants.choose(random.Random(self.seed), c_last_load)
        return self.constants

    def required_capabilities(self) -> set[Capability]:
        return {Capability.TPCC_WORKLOAD}

    async def reset_data(self, adapter: Any, extend_phase: Any = None) -> dict[str, Any]:
        """Init, after the target checks (build step 9, option A): reload the data -- back to
        exactly the loaded state -- when it has grown past data.RESET_GROWTH, fails a consistency
        rule, was loaded by another loader version, or its tables' columns changed. Nothing is
        reset when the data is missing or has another number of warehouses: prepare() then
        refuses the run with the load command. Every reset is recorded and disclosed."""
        assert self.workload.tpcc is not None
        warehouses = self.workload.tpcc.warehouses
        status = await adapter.tpcc_data_status()
        if not data.resettable(status, warehouses):
            return {}
        columns = await adapter.tpcc_columns()
        rules = None
        if status.get("loader_version") == data.LOADER_VERSION:      # the rules' SQL comes with the loader
            rules = await adapter.tpcc_consistency(data.PREPARE_RULES, district_sample=data.PREPARE_DISTRICT_SAMPLE)
        reasons = data.reset_reasons(status, columns, rules)
        before = {"growth": data.growth(status), "size_bytes": status.get("size_bytes")}
        if not reasons:
            return {"tpcc_reset": {"reloaded": False, **before}}
        budget = data.reload_budget_s(status)
        if extend_phase is not None and budget is not None:
            extend_phase(budget, f"TPC-C data reload (twice the {status['load_seconds']:.0f} s this target's "
                                 f"last load took, plus {data.RELOAD_EXTRA_S:.0f} s)")
        capacity = status.get("capacity_tps")
        started = time.monotonic()
        load = await adapter.tpcc_load(warehouses, jobs=RESET_JOBS, replace=True)
        if capacity is not None:
            await adapter.tpcc_record_capacity(capacity)    # same target, same size: still its capacity
        checkpoint_error = await adapter.tpcc_checkpoint()
        seconds = time.monotonic() - started
        after = await adapter.tpcc_data_status()
        disclosures = [f"The TPC-C data was reloaded before this run ({'; '.join(reasons)}); the reload took "
                       f"{seconds:.0f} s and passed every row count and all 12 consistency rules."]
        if checkpoint_error:
            disclosures.append(f"After the reload the harness could not run a CHECKPOINT ({checkpoint_error}): "
                               "the baseline may include the database writing the reloaded data to disk.")
        return {"tpcc_reset": {"reloaded": True, "reasons": reasons, "seconds": round(seconds, 1),
                               "load_seconds": round(load["seconds"], 1), "before": before,
                               "after": {"growth": data.growth(after), "size_bytes": after.get("size_bytes")},
                               "checkpoint": checkpoint_error or "done", "capacity_kept": capacity},
                "_disclosures": disclosures}

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
        # history rows written from now on are this run's payments (the per-payment check after a fault)
        self.history_since = await adapter.tpcc_clock()
        return {"tpcc_data": {
            "run_constants": {"c_last": consts.c_last, "c_id": consts.c_id, "ol_i_id": consts.ol_i_id,
                              "c_last_load": status["c_last_load"]},
            "warehouses": status["warehouses"], "loader_version": status["loader_version"],
            "loaded_at": status.get("loaded_at"), "growth": data.growth(status),
            "size_bytes": status.get("size_bytes"),
            "consistency": {str(r): results[r] for r in data.PREPARE_RULES},
            "district_sample": data.PREPARE_DISTRICT_SAMPLE,
            "history_since": str(self.history_since), "load_seed": status.get("load_seed"),
        }, "_disclosures": [self.label()]}

    def label(self) -> str:
        from resilience_tests.execution.workload.hooks import tpcc_label
        assert self.workload.tpcc is not None
        return tpcc_label(self.workload.concurrency, self.workload.tpcc.warehouses, self.workload.rate_tps)

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

    def make_driver(self, adapter: Any, journals: Any, stream: Any, processes: int | None = None) -> Any:
        """One process (engine.py) up to samples.PLANNED_TPS_PER_PROCESS; above it, several
        worker processes under a coordinator (step 8). `processes` overrides the choice."""
        from resilience_tests.execution.workload.tpcc.coordinator import TpccCoordinator
        from resilience_tests.execution.workload.tpcc.engine import TpccWorkloadDriver
        if self.constants is None:
            raise RuntimeError("the run's NURand constants are not set: prepare() must run first")
        n = processes or choose_processes(self.workload.rate_tps, self.workload.concurrency, os.cpu_count() or 1)
        if n <= 1:
            return TpccWorkloadDriver(adapter, self.workload, journals, stream, self)
        return TpccCoordinator(adapter, self.workload, journals, stream, self, n)

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
        acked_check = lost_check = acked_pay = lost_pay = None
        if db is not None:
            acked_check = await attempt("per-order check", adapter.tpcc_order_check(
                checks.new_order_lines(acked, set(acked) & db)))
            lost_check = await attempt("per-order check of lost New-Orders", adapter.tpcc_order_check(
                checks.new_order_lines(acked, set(acked) - db)))
            if self.history_since is None:
                problems.append("per-payment check: the run's start time was not recorded at prepare")
            else:
                acked_pay = await attempt("per-payment check", adapter.tpcc_payment_check(
                    checks.payment_lines(acked, set(acked) & db), self.history_since))
                lost_pay = await attempt("per-payment check of lost Payments", adapter.tpcc_payment_check(
                    checks.payment_lines(acked, set(acked) - db), self.history_since))
        status = await attempt("order count", adapter.tpcc_data_status())
        rules: dict[int, int | None] = {}
        full = await attempt("consistency rules", adapter.tpcc_consistency(checks.RULES_ALL_DATA))
        # rules 10 and 12 check a sample of districts: a NEW sample every run (a fixed seed checked
        # the same 20 districts every time), recorded so the run can be checked again
        sample_seed = random.SystemRandom().randrange(1, 2**31)
        assert self.workload.tpcc is not None
        w_list, d_list = data.sample_districts(random.Random(sample_seed), self.workload.tpcc.warehouses,
                                               checks.SAMPLE_DISTRICTS)
        sample = {"seed": sample_seed, "districts": [[w, d] for w, d in zip(w_list, d_list)]}
        sampled = await attempt("consistency rules (sample)", adapter.tpcc_consistency(
            checks.RULES_SAMPLED, district_sample=checks.SAMPLE_DISTRICTS, seed=sample_seed))
        rules.update(full or {})
        rules.update(sampled or {})

        ev = checks.Evidence(
            written=written, acked=acked, aborted=aborted, db=db,
            acked_order_check=acked_check, lost_order_check=lost_check,
            orders_before=self.orders_at_prepare,
            orders_after=(status or {}).get("current_orders"),
            acked_payment_check=acked_pay, lost_payment_check=lost_pay,
            rules=rules, torn_aborted_lines=torn, sample=sample)
        measures, facts, reasons = checks.evaluate(ev)
        # context only: a driver-limited run already stopped with the workload's failure (abort)
        facts["driver_load"] = _driver_load(run_dir / "events.jsonl") or "the run's events could not be read"
        if problems:
            facts["errors"] = problems
            reasons = {m: f"{why} ({'; '.join(problems)})" for m, why in reasons.items()}
        return {**measures, "_facts": facts, "_not_measured": reasons}


def _driver_load(events: Path) -> dict[str, Any] | None:
    """From the run's events: was the harness ever the limit, and how busy were its processes."""
    try:
        lines = events.read_text().splitlines()
    except OSError:
        return None
    limited, max_cpu, max_lag, processes = False, 0.0, 0.0, 1
    for line in lines:
        try:
            e = json.loads(line)
        except json.JSONDecodeError:
            continue
        if e.get("source") != "workload":
            continue
        d = e.get("data", {})
        if e.get("kind") == "driver_load":
            max_cpu = max([max_cpu, *d.get("cpu_share", [])])
            max_lag = max([max_lag, *d.get("loop_lag_s", [])])
        elif e.get("kind") == "fatal" and d.get("driver_limited"):
            limited = True
        elif e.get("kind") == "start":
            processes = d.get("processes", 1)
    return {"driver_limited": limited, "max_cpu_share": round(max_cpu, 3), "max_loop_lag_s": round(max_lag, 3),
            "processes": processes}
