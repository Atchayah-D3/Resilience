"""The checks after a fault for the TPC-C workload (TPC-C build step 7; proposal 7.6).

After recovery, from the evidence alone -- the journals on the driver host and what the
database now holds -- every write transaction gets exactly one label:

    marker in DB   its TPC-C rows        label
    yes            complete              committed and kept
    no             none                  lost (counted by the harness's rpo_txn)
    yes            missing / incomplete  PARTIAL COMMIT (atomicity violation)
    no             present               PARTIAL COMMIT (atomicity violation), never "lost"
    "rolled back" journal line, but marker in DB:  ROLLED BACK BUT KEPT (Clause 3.5.4)

How each case is found:
  acknowledged New-Orders   the per-order check on their order number (missing / incomplete)
  lost New-Orders           the same check: an order that IS there means its rows survived
                            without its marker -- a partial commit, not a loss
  unknown New-Orders        no order number is known, so a count: the orders created during the
                            run (sum of D_NEXT_O_ID - 1, after minus before) must equal the
                            New-Orders whose marker is in the database, after the two cases
                            above are accounted for. A difference is a partial commit.
  acknowledged Payments     the per-payment check: each has its history row (this customer, this
                            district, this amount, written during the run); missing = partial commit
  lost Payments             the same check: a history row that IS there survived without its
                            marker -- a partial commit, not a loss
  Payment, Delivery         and the TPC-C consistency rules (1-9 on all data, 10 and 12 on a sample)

Known limits, stated:
- Two unknown New-Orders that fail in opposite ways (one kept its marker but lost its rows, the
  other kept its rows but lost its marker) cancel out in the count. Each is an atomicity
  violation of its own; both happening in one run is not caught by the count.
- The count uses the district's order counter (D_NEXT_O_ID), so a purge of old orders (scenario
  4b) does not disturb it. An acknowledged New-Order whose order row is missing but whose counter
  update survived is counted twice. That can only over-count; the run fails either way.

A check that cannot run leaves its measure NOT MEASURED, with the reason: never 0.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from resilience_tests.execution.workload.contract import NEW_ORDER

# Consistency rules after a fault (Clause 3.3.2): 1-9 over all data; 10 and 12 compare every
# customer's balance with deliveries and payments, so they run on a sample of districts.
# Rule 11 holds only right after a load.
RULES_ALL_DATA = (1, 2, 3, 4, 5, 6, 7, 8, 9)
RULES_SAMPLED = (10, 12)
SAMPLE_DISTRICTS = 20


@dataclass
class Evidence:
    """What the checks look at. None = that part could not be read."""

    written: dict[str, str]                      # uuid -> transaction type (marker journals)
    acked: dict[str, dict[str, Any]]             # uuid -> "committed" journal line
    aborted: dict[str, dict[str, Any]]           # uuid -> "rolled back" journal line
    db: set[str] | None                          # markers in the database
    acked_order_check: Mapping[str, int] | None  # per-order check of acknowledged New-Orders, marker present
    lost_order_check: Mapping[str, int] | None   # per-order check of lost New-Orders
    orders_before: int | None                    # sum(D_NEXT_O_ID - 1) when the run was prepared
    orders_after: int | None                     # ...after recovery
    acked_payment_check: Mapping[str, int] | None    # per-payment check of acknowledged Payments, marker present
    lost_payment_check: Mapping[str, int] | None     # per-payment check of lost Payments
    rules: dict[int, int | None] = field(default_factory=dict)
    sample: dict[str, Any] | None = None             # the districts rules 10 and 12 checked, and the seed that chose them
    torn_aborted_lines: int = 0


def evaluate(ev: Evidence) -> tuple[dict[str, Any], dict[str, Any], dict[str, str]]:
    """(measures, facts, not_measured reasons). Pure."""
    measures: dict[str, Any] = {}
    reasons: dict[str, str] = {}
    facts: dict[str, Any] = {"written": len(ev.written), "acked": len(ev.acked), "aborted": len(ev.aborted)}

    if ev.db is None:
        for m in ("aborted_markers_present", "partial_commits", "indeterminate_txn"):
            reasons[m] = "the markers in the database could not be read after recovery"
    else:
        db = ev.db
        written, acked, aborted = set(ev.written), set(ev.acked), set(ev.aborted)
        unknown = written - acked - aborted
        unknown_committed = unknown & db
        kept = aborted & db
        facts.update({"lost": len(acked - db), "phantom": len(db - written), "rolled_back_but_kept": len(kept),
                      "unknown": len(unknown), "unknown_committed": len(unknown_committed)})
        # Unknown = neither committed nor surely rolled back: exact, because the TPC-C engine
        # journals sure rollbacks (the custom workload counts those as unknown too).
        measures["indeterminate_txn"] = len(unknown)
        if ev.torn_aborted_lines:
            reasons["aborted_markers_present"] = (f"{ev.torn_aborted_lines} unreadable line(s) in the rolled-back journal")
        else:
            measures["aborted_markers_present"] = len(kept)

        partial, why = _partial_commits(ev, db, unknown_committed)
        facts["partial_commit_parts"] = why
        if partial is None:
            reasons["partial_commits"] = why.get("reason", "a part of the partial-commit check could not run")
        else:
            measures["partial_commits"] = partial

    rules = {r: ev.rules.get(r) for r in RULES_ALL_DATA + RULES_SAMPLED}
    facts["consistency"] = {str(r): v for r, v in rules.items()}
    facts["consistency_sample_districts"] = ev.sample if ev.sample is not None else SAMPLE_DISTRICTS
    if any(v is None for v in rules.values()):
        missing = sorted(r for r, v in rules.items() if v is None)
        reasons["tpcc_consistency_violations"] = f"consistency rule(s) {missing} could not be evaluated after recovery"
    else:
        measures["tpcc_consistency_violations"] = sum(rules.values())
    return measures, facts, reasons


def _partial_commits(ev: Evidence, db: set[str], unknown_committed: set[str]) -> tuple[int | None, dict[str, Any]]:
    parts: dict[str, Any] = {}
    if ev.acked_order_check is None or ev.lost_order_check is None:
        return None, {"reason": "the per-order check could not run after recovery"}
    if ev.orders_before is None or ev.orders_after is None:
        return None, {"reason": "the number of orders before or after the run could not be read"}
    if ev.acked_payment_check is None or ev.lost_payment_check is None:
        return None, {"reason": "the per-payment check could not run after recovery"}
    pay_missing = int(ev.acked_payment_check.get("missing", 0))
    lost_pay_checked = int(ev.lost_payment_check.get("checked", 0))
    lost_pay_found = lost_pay_checked - int(ev.lost_payment_check.get("missing", 0))   # history row survived
    acked_missing = int(ev.acked_order_check.get("missing", 0))
    acked_incomplete = int(ev.acked_order_check.get("incomplete", 0))
    lost_checked = int(ev.lost_order_check.get("checked", 0))
    lost_found = lost_checked - int(ev.lost_order_check.get("missing", 0))       # rows survived, marker did not
    new_orders_with_marker = sum(1 for u in db if ev.written.get(u) == NEW_ORDER)
    created = ev.orders_after - ev.orders_before
    # created = (New-Orders with marker) - (their missing orders) + (lost ones whose rows survived)
    #           + (unknown ones whose rows survived without a marker) - (unknown ones with a marker but no rows)
    unexplained = created - new_orders_with_marker + acked_missing - lost_found
    parts.update({"acknowledged_order_missing": acked_missing, "acknowledged_order_incomplete": acked_incomplete,
                  "lost_but_rows_present": lost_found, "orders_created": created,
                  "new_orders_with_marker": new_orders_with_marker, "unexplained_order_count": unexplained,
                  "unknown_committed": len(unknown_committed),
                  "payments_checked": int(ev.acked_payment_check.get("checked", 0)),
                  "acknowledged_payment_missing": pay_missing, "lost_payment_rows_present": lost_pay_found})
    return acked_missing + acked_incomplete + lost_found + abs(unexplained) + pay_missing + lost_pay_found, parts


def new_order_lines(records: dict[str, dict[str, Any]], uuids: set[str]) -> list[dict[str, Any]]:
    """The {w, d, o_id, ol_cnt} of those acknowledged New-Orders (lines that carry an order number)."""
    return [records[u] for u in sorted(uuids) if u in records and "o_id" in records[u]]


def payment_lines(records: dict[str, dict[str, Any]], uuids: set[str]) -> list[dict[str, Any]]:
    """The {w, d, c_w, c_d, c, amt} of those acknowledged Payments (lines that carry an amount)."""
    return [records[u] for u in sorted(uuids) if u in records and "amt" in records[u]]
