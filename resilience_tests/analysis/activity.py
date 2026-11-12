"""Per-phase figures from the background activity samples (execution/probes/activity.py).

A pure function over the event stream: no I/O. For each window (warm-up, baseline, fault,
recovery) it reports what the database and its host did -- connections, wait events, lock
waiters, the longest transaction, autovacuum, WAL written, checkpoints, temporary files,
deadlocks, rollbacks, cache hit ratio, dead rows, CPU, iowait, memory, swap and block I/O.

Cumulative counters are summed over the positive steps between consecutive samples, so a
counter reset (a crash clears the statistics) never shows as a negative amount. A window with
no sample says so ("samples": 0) and reports no figure: never a zero it did not measure.
Evidence only: nothing here is judged.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any

TOP_WAIT_EVENTS = 5


def _in(events: Sequence[Any], source: str, start_ns: int, end_ns: int) -> list[Any]:
    return sorted((e for e in events if e.source == source and e.kind == "sample"
                   and start_ns <= e.t_mono_ns <= end_ns), key=lambda e: e.t_mono_ns)


def _get(d: Mapping[str, Any] | None, *path: str) -> Any:
    for key in path:
        if not isinstance(d, Mapping):
            return None
        d = d.get(key)
    return d


def _increase(samples: Sequence[Any], *path: str) -> float | None:
    """Sum of the positive steps of a cumulative counter across the samples (None if fewer than
    two samples carry it)."""
    values = [v for v in (_get(e.data, *path) for e in samples) if isinstance(v, (int, float))]
    if len(values) < 2:
        return None
    return float(sum(b - a for a, b in zip(values, values[1:]) if b > a))


def _peak(samples: Sequence[Any], *path: str) -> float | None:
    values = [v for v in (_get(e.data, *path) for e in samples) if isinstance(v, (int, float))]
    return max(values) if values else None


def _low(samples: Sequence[Any], *path: str) -> float | None:
    values = [v for v in (_get(e.data, *path) for e in samples) if isinstance(v, (int, float))]
    return min(values) if values else None


def _mean(samples: Sequence[Any], *path: str) -> float | None:
    values = [v for v in (_get(e.data, *path) for e in samples) if isinstance(v, (int, float))]
    return round(sum(values) / len(values), 2) if values else None


def _span_s(samples: Sequence[Any]) -> float | None:
    return (samples[-1].t_mono_ns - samples[0].t_mono_ns) / 1e9 if len(samples) >= 2 else None


def database_window(samples: Sequence[Any]) -> dict[str, Any]:
    if not samples:
        return {"samples": 0}
    span = _span_s(samples)
    wal = _increase(samples, "wal_lsn_bytes")
    hits, reads = _increase(samples, "database", "blks_hit"), _increase(samples, "database", "blks_read")
    waits: Counter[str] = Counter()
    for e in samples:
        for name, n in (e.data.get("wait_events") or {}).items():
            waits[name] += int(n)
    clients = [e.data.get("client_connections") for e in samples if isinstance(e.data.get("client_connections"), int)]
    limit = samples[-1].data.get("max_connections")
    return {
        "samples": len(samples),
        "client_connections_max": max(clients) if clients else None,
        "max_connections": limit,
        "connections_used_pct_max": (round(100.0 * max(clients) / limit, 1)
                                     if clients and isinstance(limit, int) and limit else None),
        "lock_waiters_max": _peak(samples, "lock_waiters"),
        "longest_transaction_s_max": _peak(samples, "longest_transaction_s"),
        "autovacuum_workers_max": _peak(samples, "autovacuum_workers"),
        # session-seconds spent in each wait event (one sample per second)
        "wait_events_top": dict(waits.most_common(TOP_WAIT_EVENTS)),
        "wal_bytes": wal,
        "wal_bytes_per_s": round(wal / span, 1) if wal is not None and span else None,
        "checkpoints_timed": _increase(samples, "checkpoints", "timed"),
        "checkpoints_requested": _increase(samples, "checkpoints", "requested"),
        "checkpoint_write_time_ms": _increase(samples, "checkpoints", "write_time_ms"),
        "checkpoint_sync_time_ms": _increase(samples, "checkpoints", "sync_time_ms"),
        "checkpoint_buffers_written": _increase(samples, "checkpoints", "buffers_written"),
        "temp_files": _increase(samples, "database", "temp_files"),
        "temp_bytes": _increase(samples, "database", "temp_bytes"),
        "deadlocks": _increase(samples, "database", "deadlocks"),
        "rollbacks": _increase(samples, "database", "xact_rollback"),
        "cache_hit_ratio": (round(hits / (hits + reads), 4)
                            if hits is not None and reads is not None and hits + reads > 0 else None),
        "dead_tuples_first": samples[0].data.get("dead_tuples"),
        "dead_tuples_last": samples[-1].data.get("dead_tuples"),
        "dead_tuples_max": _peak(samples, "dead_tuples"),
    }


def host_window(samples: Sequence[Any]) -> dict[str, Any]:
    if not samples:
        return {"samples": 0}
    busy = [e.data["us"] + e.data["sy"] for e in samples if "us" in e.data and "sy" in e.data]
    return {
        "samples": len(samples),
        "cpu_busy_pct_mean": round(sum(busy) / len(busy), 1) if busy else None,
        "cpu_busy_pct_max": max(busy) if busy else None,
        "iowait_pct_mean": _mean(samples, "wa"),
        "iowait_pct_max": _peak(samples, "wa"),
        "steal_pct_max": _peak(samples, "st"),
        "run_queue_max": _peak(samples, "r"),
        "blocked_processes_max": _peak(samples, "b"),
        "free_memory_kb_min": _low(samples, "free"),
        "swap_in_kb": sum(e.data.get("si", 0) for e in samples),
        "swap_out_kb": sum(e.data.get("so", 0) for e in samples),
        "blocks_in_per_s_mean": _mean(samples, "bi"),
        "blocks_out_per_s_mean": _mean(samples, "bo"),
        "blocks_out_per_s_max": _peak(samples, "bo"),
    }


def summarize(events: Sequence[Any], windows: Mapping[str, tuple[int, int]]) -> dict[str, Any]:
    """{window: {"database": {...}, "host": {...}}} for each (start_ns, end_ns) window, plus how
    often each sampler could not take a sample and its first reason."""
    out: dict[str, Any] = {}
    for name, (start, end) in windows.items():
        if end <= start:
            continue
        out[name] = {"database": database_window(_in(events, "db_activity", start, end)),
                     "host": host_window(_in(events, "host_stats", start, end))}
    for source in ("db_activity", "host_stats"):
        missing = [e for e in events if e.source == source and e.kind == "unavailable"]
        if missing:
            out.setdefault("unavailable", {})[source] = {"count": len(missing), "first_error": missing[0].data.get("error")}
    return out


def summary_lines(activity: Mapping[str, Any] | None) -> list[str]:
    """summary.txt lines: one database and one host line per window."""
    if not activity:
        return []
    lines = ["", "database and host activity per phase (evidence, not judged):"]
    for name in ("warmup", "baseline", "fault", "recovery"):
        w = activity.get(name)
        if not w:
            continue
        db, host = w.get("database") or {}, w.get("host") or {}
        if db.get("samples"):
            waits = ", ".join(f"{k}={v}" for k, v in (db.get("wait_events_top") or {}).items()) or "none"
            lines.append(
                f"  {name:<9} db: conns max {db.get('client_connections_max')}/{db.get('max_connections')}  "
                f"lock waiters max {db.get('lock_waiters_max')}  longest txn {db.get('longest_transaction_s_max')} s  "
                f"WAL {db.get('wal_bytes_per_s')} B/s  checkpoints {db.get('checkpoints_timed')}+"
                f"{db.get('checkpoints_requested')} (write {db.get('checkpoint_write_time_ms')} ms)  "
                f"temp files {db.get('temp_files')}  deadlocks {db.get('deadlocks')}  "
                f"cache hit {db.get('cache_hit_ratio')}  dead rows max {db.get('dead_tuples_max')}  "
                f"autovacuum max {db.get('autovacuum_workers_max')}  waits: {waits}")
        else:
            lines.append(f"  {name:<9} db: no sample")
        if host.get("samples"):
            lines.append(
                f"  {'':<9} host: cpu busy mean {host.get('cpu_busy_pct_mean')}% max {host.get('cpu_busy_pct_max')}%  "
                f"iowait mean {host.get('iowait_pct_mean')}% max {host.get('iowait_pct_max')}%  "
                f"run queue max {host.get('run_queue_max')}  free mem min {host.get('free_memory_kb_min')} KB  "
                f"swap in/out {host.get('swap_in_kb')}/{host.get('swap_out_kb')} KB  "
                f"blocks out mean {host.get('blocks_out_per_s_mean')}/s")
        else:
            lines.append(f"  {'':<9} host: no sample")
    for source, info in (activity.get("unavailable") or {}).items():
        lines.append(f"  {source} unavailable {info['count']} time(s): {info.get('first_error')}")
    return lines
