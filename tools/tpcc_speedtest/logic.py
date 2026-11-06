"""Pure logic of the TPC-C speed test: inputs, statistics, checks and the decision.

No database and no I/O here, so all of it is unit-tested (tests/test_tpcc_speedtest.py).
"""

from __future__ import annotations

import math
import random
from collections.abc import Iterable, Sequence
from dataclasses import dataclass

# --- TPC-C inputs (TPC-C 5.11) -----------------------------------------------------------

DISTRICTS_PER_WAREHOUSE = 10        # Clause 1.2
CUSTOMERS_PER_DISTRICT = 3000       # Clause 1.2
BUSINESS_ROLLBACK_SQLSTATE = "TP001"  # raised by resilience_speedtest.new_order for an unknown item


def nurand(rng: random.Random, a: int, x: int, y: int, c: int) -> int:
    """Non-uniform random number (Clause 2.1.6): NURand(A, x, y) with run constant C."""
    return (((rng.randint(0, a) | rng.randint(x, y)) + c) % (y - x + 1)) + x


@dataclass(frozen=True)
class RunConstants:
    """The run-time constants C of NURand (Clause 2.1.6), chosen once per run."""

    c_customer: int   # for NURand(1023, 1, 3000)
    c_item: int       # for NURand(8191, 1, items)

    @staticmethod
    def choose(rng: random.Random) -> RunConstants:
        return RunConstants(c_customer=rng.randint(0, 1023), c_item=rng.randint(0, 8191))


@dataclass(frozen=True)
class NewOrderInput:
    w_id: int
    d_id: int
    c_id: int
    items: tuple[int, ...]
    supply_w: tuple[int, ...]
    quantities: tuple[int, ...]
    rollback: bool               # the 1% New-Order with an unknown item (Clause 2.4.1.4)

    @property
    def ol_cnt(self) -> int:
        return len(self.items)


def new_order_input(rng: random.Random, home_w: int, warehouses: int, items: int,
                    consts: RunConstants) -> NewOrderInput:
    """The inputs of one New-Order (Clause 2.4.1).

    - district uniform 1..10, customer NURand(1023, 1, 3000)
    - 5..15 order lines; item NURand(8191, 1, items); quantity 1..10
    - each line is supplied by another warehouse with probability 1% (only when there is one)
    - 1% of New-Orders get an unknown item number as their last line -> business rollback
    - items are unique within one order, and lines are sorted by (supply warehouse, item) so
      that concurrent New-Orders lock stock rows in the same order (Clause 3.5.2, comment 2)
    """
    d_id = rng.randint(1, DISTRICTS_PER_WAREHOUSE)
    c_id = nurand(rng, 1023, 1, CUSTOMERS_PER_DISTRICT, consts.c_customer)
    ol_cnt = rng.randint(5, 15)
    rollback = rng.randint(1, 100) == 1

    chosen: set[int] = set()
    lines: list[tuple[int, int, int]] = []          # (supply_w, item, quantity)
    for _ in range(ol_cnt):
        item = nurand(rng, 8191, 1, items, consts.c_item)
        while item in chosen:
            item = nurand(rng, 8191, 1, items, consts.c_item)
        chosen.add(item)
        supply = home_w
        if warehouses > 1 and rng.randint(1, 100) == 1:
            supply = rng.randint(1, warehouses - 1)
            if supply >= home_w:
                supply += 1                          # any warehouse except the home one
        lines.append((supply, item, rng.randint(1, 10)))

    lines.sort()
    if rollback:
        supply, _, qty = lines[-1]
        lines[-1] = (supply, items + 1, qty)          # an item number that does not exist
    return NewOrderInput(
        w_id=home_w, d_id=d_id, c_id=c_id,
        items=tuple(line[1] for line in lines),
        supply_w=tuple(line[0] for line in lines),
        quantities=tuple(line[2] for line in lines),
        rollback=rollback,
    )


def home_warehouses(processes: int, clerks_per_process: int, warehouses: int) -> list[list[int]]:
    """The home warehouse of every clerk, spread evenly: clerk k of all clerks -> k mod W + 1."""
    out = []
    for p in range(processes):
        out.append([((p * clerks_per_process + c) % warehouses) + 1 for c in range(clerks_per_process)])
    return out


# --- statistics --------------------------------------------------------------------------

def percentile(values: Sequence[float], q: float) -> float | None:
    """Nearest-rank percentile, the same definition the harness uses (driver.percentile)."""
    if not values:
        return None
    ordered = sorted(values)
    rank = math.ceil(q * len(ordered))
    return ordered[max(0, rank - 1)]


def mean(values: Sequence[float]) -> float | None:
    return sum(values) / len(values) if values else None


def cpu_ms_per_transaction(cpu_s_total: float, committed: int) -> float | None:
    """Python CPU time spent per committed transaction, over all processes of a step."""
    return cpu_s_total * 1000.0 / committed if committed > 0 and cpu_s_total > 0 else None


def estimated_tps_per_core(cpu_ms: float | None) -> float | None:
    """The TPS one fully busy Python process would reach if its CPU cost per transaction stayed
    the same. An estimate: it is exact only in a step where Python really was the limit."""
    return 1000.0 / cpu_ms if cpu_ms else None


# A Python process whose CPU use stays at or above this share of one core is "busy": its
# speed is then the limit of Python, not of the database. Below it, the process was waiting
# (for the database, the network or the disk) and its TPS is only a lower bound.
PYTHON_BUSY_CPU_SHARE = 0.85


def python_limited(cpu_shares: Iterable[float]) -> bool:
    shares = list(cpu_shares)
    return bool(shares) and min(shares) >= PYTHON_BUSY_CPU_SHARE


# --- checks: is the measurement true? ----------------------------------------------------

@dataclass(frozen=True)
class StepEvidence:
    """What one step left behind: the process counters, the journals and the database."""

    committed_counted: int                   # calls that returned without an error (all processes)
    marker_uuids: frozenset[str]             # marker journal lines
    acked_uuids: frozenset[str]              # "committed" journal lines
    aborted_uuids: frozenset[str]            # "rolled back" journal lines
    db_uuids: frozenset[str]                 # marker rows in the database
    acked_lines: int                         # number of "committed" lines (duplicates would differ from the set)
    order_check_failures: int                # acknowledged New-Orders whose rows are missing or incomplete
    other_errors: int                        # errors that are not business rollbacks
    unknown_outcomes: int                    # calls whose outcome is unknown (timeout, connection lost)
    business_rollbacks: int
    new_orders_attempted: int
    fsyncs: int
    journal_lines: int                       # all lines written, all journals


@dataclass(frozen=True)
class Check:
    name: str
    ok: bool
    detail: str


# Business rollbacks are 1% of New-Orders (Clause 2.4.1.4). Below this many New-Orders the
# share is too noisy to check.
ROLLBACK_CHECK_MIN_ORDERS = 5_000
ROLLBACK_SHARE_RANGE = (0.005, 0.015)


def check_step(ev: StepEvidence) -> list[Check]:
    lost = ev.acked_uuids - ev.db_uuids
    phantom = ev.db_uuids - ev.marker_uuids
    unjournalled = ev.acked_uuids - ev.marker_uuids
    kept_rollbacks = ev.aborted_uuids & ev.db_uuids
    checks = [
        Check("counts agree", ev.committed_counted == ev.acked_lines == len(ev.acked_uuids),
              f"counted {ev.committed_counted}, committed lines {ev.acked_lines}, "
              f"unique {len(ev.acked_uuids)}"),
        Check("every committed transaction is in the database", not lost,
              f"{len(lost)} committed but missing"),
        Check("no unknown rows in the database", not phantom, f"{len(phantom)} rows with no marker line"),
        Check("every committed line has a marker line before it", not unjournalled,
              f"{len(unjournalled)} without a marker line"),
        Check("no rolled-back transaction in the database", not kept_rollbacks,
              f"{len(kept_rollbacks)} rolled back but present"),
        Check("every committed New-Order is complete", ev.order_check_failures == 0,
              f"{ev.order_check_failures} orders missing or with the wrong number of lines"),
        Check("no errors and no unknown outcomes", ev.other_errors == 0 and ev.unknown_outcomes == 0,
              f"{ev.other_errors} errors, {ev.unknown_outcomes} unknown outcomes"),
        Check("journal flushes really happen", 0 < ev.fsyncs <= ev.journal_lines,
              f"{ev.fsyncs} flushes for {ev.journal_lines} journal lines"),
    ]
    if ev.new_orders_attempted >= ROLLBACK_CHECK_MIN_ORDERS:
        share = ev.business_rollbacks / ev.new_orders_attempted
        lo, hi = ROLLBACK_SHARE_RANGE
        checks.append(Check("business rollbacks are about 1%", lo <= share <= hi,
                            f"{share:.2%} of {ev.new_orders_attempted} New-Orders"))
    return checks


# --- the decision ------------------------------------------------------------------------

HEADROOM = 1.2   # the harness must be able to push past the target's maximum (proposal 6.3)


def cores_needed(target_max_tps: float, tps_per_process: float, headroom: float = HEADROOM) -> int:
    if tps_per_process <= 0:
        raise ValueError("tps_per_process must be > 0")
    return math.ceil(target_max_tps * headroom / tps_per_process)


@dataclass(frozen=True)
class StepSummary:
    processes: int
    tps_per_process: float      # average over the step's processes
    python_limited: bool
    checks_ok: bool


@dataclass(frozen=True)
class PerProcessRate:
    tps: float
    exact: bool     # True: measured while Python was the limit. False: a lower bound only.


def tps_per_process(steps: Sequence[StepSummary]) -> PerProcessRate | None:
    """The TPS one Python process can drive, from steps whose checks passed.

    If some steps were limited by Python, the lowest of their per-process rates is used
    (processes slow each other a little as they share the machine, so the lowest is the
    safe number). If none was, the database or the network was always the limit first: the
    highest per-process rate seen is then a lower bound for Python.
    """
    valid = [s for s in steps if s.checks_ok]
    limited = [s.tps_per_process for s in valid if s.python_limited]
    if limited:
        return PerProcessRate(min(limited), exact=True)
    if valid:
        return PerProcessRate(max(s.tps_per_process for s in valid), exact=False)
    return None


def parse_proc_stat(text: str) -> tuple[int, int, int]:
    """(busy, iowait, total) jiffies from the first line of Linux /proc/stat.

    cpu  user nice system idle iowait irq softirq steal [guest guest_nice]
    guest time is already counted in user, so only the first eight fields are summed.
    """
    for line in text.splitlines():
        parts = line.split()
        if parts and parts[0] == "cpu":
            values = [int(v) for v in parts[1:9]]
            values += [0] * (8 - len(values))
            idle, iowait = values[3], values[4]
            total = sum(values)
            return total - idle - iowait, iowait, total
    raise ValueError("no 'cpu' line in /proc/stat output")
