"""Cumulative bloat — a pure function over footprint samples (Framework NL-C-05).

Repeated crashes must not leave storage behind. Raw size is the wrong measure: the workload
inserts rows the whole time, so the database MUST grow. What must not grow is the cost of
holding a row, and the write-ahead log must stay inside the bound the server itself declares.

    bytes per live row   markers_bytes / markers_live_rows, sampled each cycle
    unexplained bytes    growth the inserted rows do not account for, priced at the
                         per-row cost measured before the first crash
    WAL against its own max_wal_size, read from pg_settings — never a number we chose
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any


# The churn table is the bloat instrument: its live row count is held constant by the
# workload, so a rise in bytes per live row is garbage that was never reclaimed and nothing
# else. The markers table is the fallback for an engine with no churn workload, where the
# measure is weaker -- an append-only table grows for legitimate reasons.
CHURN = ("churn_bytes", "churn_live_rows", "churn_dead_rows")
MARKERS = ("markers_bytes", "markers_live_rows", "markers_dead_rows")


def _table(sample: dict[str, Any]) -> tuple[str, str, str]:
    return CHURN if sample.get("churn_live_rows") else MARKERS


def bytes_per_live_row(sample: dict[str, Any]) -> float | None:
    size_key, rows_key, _ = _table(sample)
    rows = sample.get(rows_key) or 0
    size = sample.get(size_key)
    return size / rows if rows and size is not None else None


def bloat_metrics(samples: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """`samples[0]` is taken before the first fault; one more follows each cycle."""
    usable = [s for s in samples if bytes_per_live_row(s) is not None]
    if len(usable) < 2:
        return {"samples": len(usable)}
    first, last = usable[0], usable[-1]
    size_key, rows_key, dead_key = _table(last)
    bpr_first, bpr_last = bytes_per_live_row(first), bytes_per_live_row(last)
    grown_rows = (last.get(rows_key) or 0) - (first.get(rows_key) or 0)
    grown_bytes = (last.get(size_key) or 0) - (first.get(size_key) or 0)
    explained = grown_rows * bpr_first
    wal_max = last.get("max_wal_size_bytes") or 0
    return {
        "samples": len(usable),
        "measured_on": "churn" if size_key == "churn_bytes" else "markers",
        "bytes_per_live_row_first": round(bpr_first, 2),
        "bytes_per_live_row_last": round(bpr_last, 2),
        # the criterion: holding a row must not become more expensive across cycles
        "bloat_ratio": round(bpr_last / bpr_first, 4) if bpr_first else None,
        "rows_added": grown_rows,
        "bytes_added": grown_bytes,
        "bytes_unexplained_by_rows": round(grown_bytes - explained, 1),
        # Reported as context only. A crash discards PostgreSQL's statistics, so after ten
        # kills this counts only the garbage made since the last one -- never gate on it.
        "dead_rows_last": last.get(dead_key),
        "wal_bytes_last": last.get("wal_bytes"),
        "wal_segments_last": last.get("wal_segments"),
        # the engine's own ceiling, not one we invented
        "wal_ratio_of_max_wal_size": round(last["wal_bytes"] / wal_max, 3) if wal_max else None,
        "database_bytes_first": first.get("database_bytes"),
        "database_bytes_last": last.get("database_bytes"),
    }
