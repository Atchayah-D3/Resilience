"""Combining the TPC-C engine's numbers across worker processes, and the driver-limited guard
(TPC-C build step 8). Pure: no I/O, so every rule is unit-tested.

Each worker process sends, once per interval, its RAW numbers: counts and the full list of
latencies, per transaction type and per clerk group. The coordinator merges the raw numbers
of all processes into one sample. Percentiles come from the merged latencies, never from
averaging each process's percentile: the average of two p99 values is not the p99 of all
transactions (proposal 7.7, "exact merged statistics").
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Sequence
from typing import Any

from resilience_tests.execution.workload import contract
from resilience_tests.execution.workload.driver import p99, percentile

# Planned transactions per second per worker process, with a safety margin: the speed test
# measured about 4,800 per process at full use of one core (proposal 6.3, step 0).
PLANNED_TPS_PER_PROCESS = 2_000

# The harness is the limit when a worker process uses (almost) a whole core, or its event loop
# falls behind, for this many seconds in a row. The run is then stopped as invalid: it would be
# measuring the harness, not the database (proposal 7.7, driver_limited).
DRIVER_CPU_LIMIT = 0.95
DRIVER_LOOP_LAG_LIMIT_S = 0.5
DRIVER_LIMIT_SECONDS = 5

TOTAL_FIELDS = ("commits", "errors", "indeterminate", "drops", "reconnects", "connect_failures")

# Coordinated omission. Response times are measured from the moment a transaction is SENT. When
# the database stalls, a clerk waits inside one call and does not send the transactions it was
# due to send meanwhile; their waiting is in no measurement, so percentiles look better than what
# users would feel. The standard correction (HdrHistogram's recordValueWithExpectedInterval, as
# YCSB and wrk2 use): a response of L ms from a clerk due every E ms also stands for the
# transactions it missed, recorded as L - E, L - 2E, ... (while above 0). E = clerks / rate.
# Reported NEXT TO the measured values (p99_corrected_ms), never instead of them. Meaningful
# when the declared rate is reachable; at an unreachable rate every response above E counts as late.
MAX_CORRECTION_SAMPLES = 10_000      # per response: bounds memory after a very long stall


def corrected_latencies(latency_ms: float, expected_ms: float | None) -> list[float]:
    """The response itself plus the responses it hid (L - E, L - 2E, ... > 0)."""
    out = [latency_ms]
    if not expected_ms or expected_ms <= 0:
        return out
    hidden = latency_ms - expected_ms
    while hidden > 0 and len(out) <= MAX_CORRECTION_SAMPLES:
        out.append(hidden)
        hidden -= expected_ms
    return out


def choose_processes(rate_tps: float | None, concurrency: int, cores: int) -> int:
    """How many worker processes: one per PLANNED_TPS_PER_PROCESS of rate, at least 1, at most
    one per clerk, and leaving one core for the coordinator and the system."""
    wanted = 1 if not rate_tps else math.ceil(rate_tps / PLANNED_TPS_PER_PROCESS)
    return max(1, min(wanted, concurrency, max(1, cores - 1)))


def split_workers(concurrency: int, processes: int) -> list[list[int]]:
    """Clerk numbers per process, round robin: process p gets clerks p, p+N, p+2N, ... So each
    process gets a fair share of every group (groups take clerks in declaration order)."""
    return [list(range(p, concurrency, processes)) for p in range(processes)]


def _rate(raws: Sequence[dict[str, Any]], commits_of: Any) -> float:
    """Per process: its commits over the time its intervals covered; summed over processes."""
    per: dict[Any, list[float]] = {}
    for r in raws:
        c = per.setdefault(r.get("process", 0), [0, 0.0])
        c[0] += commits_of(r)
        c[1] += r["elapsed"]
    return sum(commits / secs for commits, secs in per.values() if secs > 0)


def merge_raws(raws: Sequence[dict[str, Any]], elapsed: float | None = None) -> dict[str, Any]:
    """One sample (contract.SAMPLE_FIELDS + by_type + by_group) from the raw numbers of one
    interval of one or more processes. tps = the sum of each process's commits over its own
    measured interval; `elapsed` defaults to the longest interval."""
    if not raws:
        raise ValueError("no raw numbers to merge")
    interval = elapsed if elapsed is not None else max(r["elapsed"] for r in raws)
    totals = {f: sum(r["totals"][f] for r in raws) for f in TOTAL_FIELDS}
    lat = [v for r in raws for v in r["latencies"]]
    journal = [v for r in raws for v in r["journal_ms"]]
    tps = _rate(raws, lambda r: r["totals"]["commits"])
    corrected = [v for r in raws for v in r.get("corrected", r["latencies"])]

    by_type: dict[str, Any] = {}
    for t in contract.TPCC_TYPES:
        parts = [r["types"][t] for r in raws if t in r["types"]]
        tl = [v for part in parts for v in part["latencies"]]
        commits = sum(part["commits"] for part in parts)
        by_type[t] = {"commits": commits,
                      "tps": _rate(raws, lambda r, t=t: r["types"].get(t, {}).get("commits", 0)),
                      "p50_ms": percentile(tl, 0.50), "p90_ms": percentile(tl, 0.90), "p99_ms": p99(tl),
                      "max_ms": max(tl) if tl else None,
                      "errors": sum(part["errors"] for part in parts),
                      "business_rollbacks": sum(part["business_rollbacks"] for part in parts)}
    by_group: dict[str, Any] = {}
    for g in dict.fromkeys(g for r in raws for g in r["groups"]):
        parts = [r["groups"][g] for r in raws if g in r["groups"]]
        gl = [v for part in parts for v in part["latencies"]]
        by_group[g] = {"commits": sum(part["commits"] for part in parts),
                       "tps": _rate([r for r in raws if g in r["groups"]], lambda r, g=g: r["groups"][g]["commits"]),
                       "p50_ms": percentile(gl, 0.50), "p95_ms": percentile(gl, 0.95), "p99_ms": p99(gl),
                       "errors": sum(part["errors"] for part in parts),
                       "indeterminate": sum(part["indeterminate"] for part in parts),
                       "drops": sum(part["drops"] for part in parts)}
    return {"interval_s": interval, **totals, "tps": tps,
            "p99_ms": p99(lat), "journal_p99_ms": p99(journal),
            "p50_ms": percentile(lat, 0.50), "p95_ms": percentile(lat, 0.95),
            "p99_corrected_ms": p99(corrected),
            contract.BY_TYPE: by_type, contract.BY_GROUP: by_group}


def merge_window(raws: Iterable[dict[str, Any]], duration_s: float) -> dict[str, Any]:
    """The totals and percentiles of every raw interval inside a measurement window. tps is
    each process's commits over the time ITS intervals really covered, summed over processes:
    dividing by the window length would count the first interval's part before the window."""
    raws = list(raws)
    lat = [v for r in raws for v in r["latencies"]]
    journal = [v for r in raws for v in r["journal_ms"]]
    totals = {f: sum(r["totals"][f] for r in raws) for f in TOTAL_FIELDS}
    tps = _rate(raws, lambda r: r["totals"]["commits"])
    corrected = [v for r in raws for v in r.get("corrected", r["latencies"])]
    return {"duration_s": duration_s, **totals, "tps": tps,
            "p99_ms": p99(lat), "journal_p99_ms": p99(journal),
            "p50_ms": percentile(lat, 0.50), "p95_ms": percentile(lat, 0.95),
            "p99_corrected_ms": p99(corrected)}


class DriverLoadWatch:
    """Raises the alarm when the harness itself is the limit: some worker process at or above
    DRIVER_CPU_LIMIT of a core, or its event loop late by DRIVER_LOOP_LAG_LIMIT_S, for
    DRIVER_LIMIT_SECONDS intervals in a row. A database that is slow makes the clerks WAIT,
    which uses no CPU -- so this does not fire for a slow database, only for a busy harness."""

    def __init__(self) -> None:
        self._streak = 0
        self.max_cpu = 0.0
        self.max_lag_s = 0.0

    def update(self, cpu_shares: Sequence[float], loop_lags_s: Sequence[float]) -> str | None:
        cpu = max(cpu_shares, default=0.0)
        lag = max(loop_lags_s, default=0.0)
        self.max_cpu, self.max_lag_s = max(self.max_cpu, cpu), max(self.max_lag_s, lag)
        if cpu >= DRIVER_CPU_LIMIT or lag >= DRIVER_LOOP_LAG_LIMIT_S:
            self._streak += 1
        else:
            self._streak = 0
        if self._streak >= DRIVER_LIMIT_SECONDS:
            return (f"driver limited: a worker process used {cpu:.0%} of a core and its loop was {lag:.2f} s late "
                    f"for {self._streak} s in a row; the harness, not the database, is the limit (add worker "
                    "processes or harness machines)")
        return None
