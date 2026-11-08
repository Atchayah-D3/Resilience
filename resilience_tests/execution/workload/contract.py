"""The workload contract: the names the scenarios and the workload agree on (TPC-C build step 1).

Two developers build in parallel: one the noisy-neighbour scenarios, one the TPC-C workload
(docs/tpcc-build-plan.md, docs/tpcc-contract.md). This file is the single place where the names
both sides use are written down. It holds names and shapes only, no behaviour, so importing it
changes nothing.

What it fixes:
  1. the driver functions the orchestrator calls (they exist today; written down here)
  2. the fields of the 1-second `sample` event, today's and the two new ones (by_type, by_group)
  3. the names of clerk groups, transaction types, outcomes and measures
  4. the session settings a scenario may give each clerk connection
  5. the TPC-C table names, so faults never spell a table name themselves
  6. the hooks a workload profile offers the orchestrator (prepare, consistency_checks)

Rule: a name here changes only by agreement of both developers; CONTRACT_VERSION then goes up.
Every existing scenario keeps working: the new scenario fields are optional, the new sample
fields are extra, and the custom workload fills them with one type and one group.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol, TypedDict, runtime_checkable

CONTRACT_VERSION = "1"

# --- 1. the driver the orchestrator uses ---------------------------------------------------


@runtime_checkable
class WorkloadDriverContract(Protocol):
    """What the orchestrator calls on any workload driver (today: driver.WorkloadDriver).

    start               begin offering load (clerks connect, the sampler starts)
    wait_until_ready    return once every clerk has opened its connection, or the driver failed
    begin_window        start an exact measurement window; sets window_t0_ns
    end_window          end it; returns a MeasuredWindow (driver.MeasuredWindow)
    stop                stop the load and the sampler
    failure             None, or why the driver stopped (the run must then abort)
    """

    failure: str | None
    window_t0_ns: int

    async def start(self) -> None: ...

    async def wait_until_ready(self, poll_s: float = 0.1) -> None: ...

    def begin_window(self) -> None: ...

    def end_window(self) -> Any: ...

    async def stop(self) -> None: ...


# --- 2. the 1-second sample event (events.jsonl: source "workload", kind "sample") ----------

SAMPLE_SOURCE = "workload"
SAMPLE_KIND = "sample"

# Today's fields, unchanged in name and meaning. tps and the latencies cover ALL transaction
# types and groups together, so the existing analysis reads every profile the same way.
SAMPLE_FIELDS: tuple[str, ...] = (
    "interval_s",        # the interval actually measured (not the nominal 1 s)
    "commits",
    "tps",               # commits / interval_s
    "p50_ms", "p95_ms", "p99_ms",   # over every attempt, committed or not
    "journal_p99_ms",    # marker journal flush on the driver host
    "errors",            # sure rollbacks that are not business rollbacks
    "indeterminate",     # outcome unknown
    "drops",             # established connections lost without the client closing them
    "reconnects",
    "connect_failures",
)

# New, extra fields: one entry per transaction type and per clerk group.
BY_TYPE = "by_type"
BY_GROUP = "by_group"


class TypeStats(TypedDict):
    """One transaction type in one sample (inside `by_type`)."""

    commits: int
    tps: float
    p90_ms: float | None          # TPC-C's response-time rule is on the 90th percentile (Clause 5.2.5.3)
    p99_ms: float | None
    errors: int
    business_rollbacks: int       # expected rollbacks (TPC-C: 1% of New-Orders); never an error


class GroupStats(TypedDict):
    """One clerk group in one sample (inside `by_group`). Same names as the top-level fields."""

    commits: int
    tps: float
    p50_ms: float | None
    p95_ms: float | None
    p99_ms: float | None
    errors: int
    indeterminate: int
    drops: int


# The custom workload (marker, churn, list_append) reports one type and one group, so a
# measurement written against by_type / by_group works before TPC-C exists.
MARKER_TYPE = "marker"
DEFAULT_GROUP = "all"

# --- 3. clerk groups ---------------------------------------------------------------------

GROUP_NAME_PATTERN = r"^[a-z][a-z0-9_]{0,31}$"


class GroupSpec(TypedDict):
    """A scenario file's `workload.groups` entry: a name and how many clerks belong to it."""

    name: str
    clerks: int


def group_problems(concurrency: int, groups: Sequence[Mapping[str, Any]]) -> list[str]:
    """Why a scenario's groups are invalid (empty list: valid).

    Without groups every clerk is in DEFAULT_GROUP. With groups, every clerk belongs to exactly
    one declared group: the clerks must add up to `concurrency`, so no clerk is left in a group
    nobody named."""
    import re

    problems: list[str] = []
    if not groups:
        return problems
    names = [str(g.get("name", "")) for g in groups]
    for name in names:
        if not re.match(GROUP_NAME_PATTERN, name):
            problems.append(f"group name {name!r} must match {GROUP_NAME_PATTERN}")
    if DEFAULT_GROUP in names:
        problems.append(f"group name {DEFAULT_GROUP!r} is reserved for 'no groups declared'")
    if len(set(names)) != len(names):
        problems.append("group names must be unique")
    clerks = [g.get("clerks") for g in groups]
    if any(not isinstance(c, int) or isinstance(c, bool) or c <= 0 for c in clerks):
        problems.append("every group needs clerks > 0")
    elif sum(clerks) != concurrency:
        problems.append(f"group clerks add up to {sum(clerks)}, but workload.concurrency is {concurrency}")
    return problems


# --- 4. session settings per clerk connection --------------------------------------------

# A scenario may give each clerk connection these settings (workload.session). Each clerk sets
# them itself when it connects (SET on its own session). The workload never uses ALTER ROLE or
# changes server settings, so nothing leaks into the next scenario (constitution VI).
SESSION_SETTINGS: frozenset[str] = frozenset({
    "lock_timeout",
    "statement_timeout",
    "idle_in_transaction_session_timeout",
})

# --- 5. transaction types, outcomes and measures -----------------------------------------

# TPC-C transaction types (Clause 2): the keys of by_type for the tpcc profile.
NEW_ORDER = "new_order"
PAYMENT = "payment"
ORDER_STATUS = "order_status"
DELIVERY = "delivery"
STOCK_LEVEL = "stock_level"
TPCC_TYPES: tuple[str, ...] = (NEW_ORDER, PAYMENT, ORDER_STATUS, DELIVERY, STOCK_LEVEL)
TPCC_READ_ONLY_TYPES: frozenset[str] = frozenset({ORDER_STATUS, STOCK_LEVEL})   # no marker, no journal line

# Outcomes of one transaction. committed / definitely_aborted / unknown are the adapter's
# TransactionOutcome values (adapters/base.py); business_rollback is a definitely_aborted
# transaction the workload expected (TPC-C's 1% New-Order rollback), counted apart from errors.
OUTCOME_COMMITTED = "committed"
OUTCOME_DEFINITELY_ABORTED = "definitely_aborted"
OUTCOME_UNKNOWN = "unknown"
OUTCOME_BUSINESS_ROLLBACK = "business_rollback"

# Measures a scenario may name in `measure:` and `accept:`. Existing names keep their meaning;
# the new ones are produced once the tpcc profile is built (until then: refused as not built).
EXISTING_MEASURES: frozenset[str] = frozenset({
    "rpo_txn", "indeterminate_txn", "rto_first_write_s", "rto_to_slo_s", "corruption_count",
    "failed_transactions", "dropped_connections",
})
NEW_MEASURES: frozenset[str] = frozenset({
    "nopm",                         # committed New-Orders per minute
    "tpcc_response_time_ok",        # 90% within 5 s (Stock-Level: 20 s), Clause 5.2.5.3
    "mix_ok",                       # the mix meets Clause 5.2.3's minimums
    "business_rollbacks",           # about 1% of New-Orders, Clause 2.4.1.4
    "partial_commits",              # transactions found half-saved after the fault
    "aborted_markers_present",      # rolled-back transactions found in the database, Clause 3.5.4
    "tpcc_consistency_violations",  # TPC-C consistency rules failing after recovery, Clause 3.3.2
    "driver_limited",               # the harness could not keep the declared rate: run invalid
    "capacity_tps",                 # the target's measured maximum
})


# Per-group and per-type measure names. A measure name must be a plain identifier (letters,
# digits, "_"): the catalog checks it (catalog/schema.py IDENT_RE) and `accept:` predicates are
# parsed as expressions, where a "." would mean something else. So the parts are joined with
# "__group_" and "__type_".


def measure_by_group(measure: str, group: str) -> str:
    """The name of a measure for one clerk group, e.g. tps_ratio__group_non_colliding."""
    return f"{measure}__group_{group}"


def measure_by_type(measure: str, txn_type: str) -> str:
    """The name of a measure for one transaction type, e.g. p90_ms__type_new_order."""
    return f"{measure}__type_{txn_type}"


# --- 6. TPC-C tables ---------------------------------------------------------------------

# Every TPC-C object lives in its own schema, owned by the harness (constitution VI). Faults
# and checks use these constants, never a table name written by hand.
TPCC_SCHEMA = "resilience_tpcc"
TPCC_TABLE_NAMES: tuple[str, ...] = (
    "warehouse", "district", "customer", "history", "orders",
    "new_order", "order_line", "item", "stock",
)
TPCC_TABLES: Mapping[str, str] = {name: f"{TPCC_SCHEMA}.{name}" for name in TPCC_TABLE_NAMES}
TPCC_WAREHOUSE = TPCC_TABLES["warehouse"]
TPCC_DISTRICT = TPCC_TABLES["district"]
TPCC_CUSTOMER = TPCC_TABLES["customer"]
TPCC_HISTORY = TPCC_TABLES["history"]
TPCC_ORDERS = TPCC_TABLES["orders"]
TPCC_NEW_ORDER = TPCC_TABLES["new_order"]
TPCC_ORDER_LINE = TPCC_TABLES["order_line"]
TPCC_ITEM = TPCC_TABLES["item"]
TPCC_STOCK = TPCC_TABLES["stock"]
TPCC_META = f"{TPCC_SCHEMA}.meta"            # warehouses, loader version, capacity result

# --- 7. the hooks a workload profile offers the orchestrator ------------------------------


class ProfileHooks(Protocol):
    """Called by the orchestrator for every profile; the custom workload's hooks do nothing.

    prepare             init phase: the data the profile needs is present, the right size and
                        consistent; raises with the reason otherwise (the run is refused)
    consistency_checks  validate phase: profile-specific checks after recovery; returns
                        measure name -> value (e.g. partial_commits, tpcc_consistency_violations)
    """

    async def prepare(self, adapter: Any) -> dict[str, Any]: ...

    async def consistency_checks(self, adapter: Any, run_dir: Path) -> dict[str, Any]: ...
