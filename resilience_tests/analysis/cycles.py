"""Measures of a repeated fault (`repeat:` in the scenario; Framework NL-C-05 repeated crash
cycles, NL-M-03 repeated autovacuum-worker kills).

For a repeated scenario the shape across cycles is the finding: each cycle's recovery joined to
the replay work it actually faced, the trend from first to last cycle, the client-visible
disturbance inside each cycle's window, and the growth of the on-disk footprint the workload
does not explain. A figure that cannot be produced is NOT_MEASURED, with the reason by name.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from typing import Any

from resilience_tests.analysis.bloat import bloat_metrics
from resilience_tests.analysis.predicates import NOT_MEASURED
from resilience_tests.analysis.rto_decomposer import per_cycle_recovery, recovery_trend
from resilience_tests.observability.event_stream import Event


@dataclass
class CycleMeasures:
    measured: dict[str, Any] = field(default_factory=dict)
    facts: dict[str, Any] = field(default_factory=dict)
    not_measured: dict[str, str] = field(default_factory=dict)


def repeated_fault_measures(events: Sequence[Event], cycle_t0s: Sequence[int],
                            cycle_details: Sequence[dict[str, Any]], redo_at_t0: Sequence[int | None],
                            footprints: list[dict[str, Any]], footprint_errors: Sequence[str], *,
                            expect_outage: bool, evidence_keys: Sequence[str]) -> CycleMeasures:
    """`cycle_details`: what the fault-inject phase recorded for each cycle (the driver's
    injection detail, its recovery, an inter-cycle integrity check). `evidence_keys`: the keys of
    that detail the fault keeps in each cycle's row (FaultBehaviour.cycle_evidence_keys)."""
    out = CycleMeasures()
    m = out.measured
    cycles = per_cycle_recovery(events, cycle_t0s, expect_outage=expect_outage)
    trend = recovery_trend(cycles)
    bloat = bloat_metrics(footprints)
    rows = [asdict(c) for c in cycles]
    # Join each cycle's recovery to the replay work it actually faced. A recovery time on its
    # own is not comparable across cycles; bytes replayed per second is.
    for i, (row, redo) in enumerate(zip(rows, redo_at_t0)):
        row["redo_distance_bytes"] = redo
        rec = row.get("recovery_s")
        row["replay_bytes_per_s"] = (
            round(redo / rec, 1) if redo and isinstance(rec, (int, float)) and rec > 0 else None)
        if i < len(cycle_details):
            row["redo_sampling_delay_ms"] = cycle_details[i].get("redo_sampling_delay_ms")
            if "quick_integrity" in cycle_details[i]:
                row["quick_integrity"] = cycle_details[i]["quick_integrity"]
            for key in evidence_keys:
                if key in cycle_details[i]:
                    row[key] = cycle_details[i][key]
        # client-visible errors, partitioned into each cycle's window
        c_start = cycle_t0s[i]
        c_end = cycle_t0s[i + 1] if i + 1 < len(cycle_t0s) else None
        c_samples = [e for e in events if e.kind == "sample" and e.source == "workload"
                     and e.t_mono_ns > c_start and (c_end is None or e.t_mono_ns <= c_end)]
        row["failed_transactions"] = sum(e.data.get("errors", 0) for e in c_samples)
        row["dropped_connections"] = sum(e.data.get("drops", 0) for e in c_samples)
        row["connect_failures"] = sum(e.data.get("connect_failures", 0) for e in c_samples)
    out.facts["cycles"] = rows
    m["failed_transactions_max_per_cycle"] = max((r.get("failed_transactions", 0) for r in rows), default=0)
    m["dropped_connections_max_per_cycle"] = max((r.get("dropped_connections", 0) for r in rows), default=0)
    replayed = [r["redo_distance_bytes"] for r in rows if r["redo_distance_bytes"] is not None]
    if replayed:
        m["wal_replayed_bytes_max"] = max(replayed)
        m["wal_replayed_bytes_median"] = sorted(replayed)[len(replayed) // 2]
    rates = [r["replay_bytes_per_s"] for r in rows if r["replay_bytes_per_s"] is not None]
    if rates:
        m["replay_bytes_per_s_min"] = min(rates)
    out.facts["recovery_trend"] = trend
    out.facts["bloat"] = bloat
    out.facts["footprints"] = footprints
    m["cycles_run"] = len(cycle_t0s)
    m["cycles_recovered"] = sum(1 for c in cycles if isinstance(c.recovery_s, (int, float)))
    for key, name in (("first_s", "recovery_s_first"), ("last_s", "recovery_s_last"),
                      ("median_s", "recovery_s_median"), ("max_s", "recovery_s_max"),
                      ("ratio_last_over_first", "recovery_ratio_last_over_first"),
                      ("slope_s_per_cycle", "recovery_slope_s_per_cycle")):
        if trend.get(key) is not None:
            m[name] = trend[key]
    for key in ("bloat_ratio", "wal_ratio_of_max_wal_size", "bytes_unexplained_by_rows"):
        if bloat.get(key) is not None:
            m[key] = bloat[key]
    if m.get("cycles_recovered") != m.get("cycles_run"):
        out.not_measured["recovery_ratio_last_over_first"] = (
            f"only {m.get('cycles_recovered')} of {m.get('cycles_run')} cycles were seen to recover")
    # A bloat figure the engine could not produce must say so by name. Left merely absent it
    # would still fail the predicate, but with "never measured" and no cause -- and the cause
    # here is usually one missing grant, not a misbehaving database.
    errors = "; ".join(footprint_errors)
    if bloat.get("bloat_ratio") is None:
        m["bloat_ratio"] = NOT_MEASURED
        out.not_measured["bloat_ratio"] = (
            f"on-disk footprint sampled {bloat.get('samples', 0)} time(s); two are needed"
            + (f" ({errors})" if errors else ""))
    if bloat.get("wal_ratio_of_max_wal_size") is None:
        why_wal = next((s["wal_unavailable"] for s in reversed(footprints) if s.get("wal_unavailable")), None)
        m["wal_ratio_of_max_wal_size"] = NOT_MEASURED
        out.not_measured["wal_ratio_of_max_wal_size"] = (
            f"write-ahead log size not readable: {why_wal}" if why_wal else
            # the footprint failed outright: name that cause rather than imply the engine
            # simply does not report a WAL size
            f"on-disk footprint could not be sampled ({errors})" if errors else
            "write-ahead log size was not reported by this engine")
    return out
