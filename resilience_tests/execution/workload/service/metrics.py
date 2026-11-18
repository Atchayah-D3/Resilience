"""The common workload's metrics, from the engine's 1-second samples (pure: no I/O).

  Second          the few numbers of one 1-second sample the run keeps in memory (the full
                  sample is in metrics-1s.jsonl), for the baseline rules and the end summary
  IntervalSums    what the interval lines add to the engine's exact window figures: errors by
                  kind, business rollbacks and commits per transaction type
  OutageTracker   a stretch of seconds with no commit at all -- the database down, frozen or
                  unreachable -- as one outage with its start, length and what the clerks saw
  period_summary  the run in periods (10 minutes by default) for the end summary
"""

from __future__ import annotations

import statistics
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from typing import Any

from resilience_tests.execution.workload import contract

# Below this rate a second without a commit is normal (one transaction every few seconds), so
# outages are not detected; the status says so.
OUTAGE_MIN_RATE_TPS = 5.0


@dataclass(frozen=True)
class Second:
    elapsed_s: float            # seconds since the clerks started, at the end of this sample
    interval_s: float
    commits: int
    p99_ms: float | None
    errors: int                 # server rollbacks other than business rollbacks
    indeterminate: int          # unknown outcomes
    drops: int
    reconnects: int
    connect_failures: int
    business_rollbacks: int
    t_wall: float = 0.0         # when the sample was written (end of its second), harness wall clock

    @classmethod
    def from_sample(cls, sample: Mapping[str, Any], elapsed_s: float, t_wall: float = 0.0) -> Second:
        business = sum(t.get("business_rollbacks", 0) for t in sample.get(contract.BY_TYPE, {}).values())
        return cls(elapsed_s=elapsed_s, t_wall=t_wall, interval_s=float(sample.get("interval_s") or 0.0),
                   commits=int(sample.get("commits", 0)), p99_ms=sample.get("p99_ms"),
                   errors=int(sample.get("errors", 0)), indeterminate=int(sample.get("indeterminate", 0)),
                   drops=int(sample.get("drops", 0)), reconnects=int(sample.get("reconnects", 0)),
                   connect_failures=int(sample.get("connect_failures", 0)), business_rollbacks=business)


class IntervalSums:
    """Sums over the samples of one interval; take() returns them and starts the next."""

    def __init__(self) -> None:
        self._reset()

    def _reset(self) -> None:
        self.samples = 0
        self.error_kinds: dict[str, int] = {}
        self.business_rollbacks = 0
        self.commits_by_type: dict[str, int] = {t: 0 for t in contract.TPCC_TYPES}

    def add(self, sample: Mapping[str, Any]) -> None:
        self.samples += 1
        for kind, n in (sample.get("error_kinds") or {}).items():
            self.error_kinds[kind] = self.error_kinds.get(kind, 0) + int(n)
        for t, st in (sample.get(contract.BY_TYPE) or {}).items():
            self.commits_by_type[t] = self.commits_by_type.get(t, 0) + int(st.get("commits", 0))
            self.business_rollbacks += int(st.get("business_rollbacks", 0))

    def take(self) -> dict[str, Any]:
        out = {"samples": self.samples, "error_kinds": dict(sorted(self.error_kinds.items())),
               "business_rollbacks": self.business_rollbacks, "commits_by_type": dict(self.commits_by_type)}
        self._reset()
        return out


class OutageTracker:
    """Seconds in a row with no commit, as one outage. update() returns the events to record:
    ("outage_start", {...}), ("outage_long", {...}) once when it lasts `alert_s`, and
    ("outage_end", {...})."""

    def __init__(self, rate_tps: float | None, alert_s: float) -> None:
        self.enabled = bool(rate_tps) and rate_tps >= OUTAGE_MIN_RATE_TPS
        self.alert_s = alert_s
        self.current: dict[str, Any] | None = None
        self.outages: list[dict[str, Any]] = []

    def update(self, second: Second, t_wall: float) -> list[tuple[str, dict[str, Any]]]:
        if not self.enabled:
            return []
        events: list[tuple[str, dict[str, Any]]] = []
        if second.commits == 0:
            if self.current is None:
                self.current = {"started_wall": round(t_wall - second.interval_s, 3),
                                "started_elapsed_s": round(second.elapsed_s - second.interval_s, 3),
                                "seconds": 0.0, "connect_failures": 0, "drops": 0, "indeterminate": 0,
                                "alerted": False}
                events.append(("outage_start", {"started_wall": self.current["started_wall"],
                                                "started_elapsed_s": self.current["started_elapsed_s"]}))
            c = self.current
            c["seconds"] += second.interval_s
            c["connect_failures"] += second.connect_failures
            c["drops"] += second.drops
            c["indeterminate"] += second.indeterminate
            if not c["alerted"] and c["seconds"] >= self.alert_s:
                c["alerted"] = True
                events.append(("outage_long", {"seconds": round(c["seconds"], 1),
                                               "note": "no commit for this long; the workload keeps trying"}))
        elif self.current is not None:
            done = {k: (round(v, 3) if isinstance(v, float) else v) for k, v in self.current.items() if k != "alerted"}
            done["ended_wall"] = round(t_wall - second.interval_s, 3)
            self.outages.append(done)
            self.current = None
            events.append(("outage_end", done))
        return events

    def open_outage(self) -> dict[str, Any] | None:
        if self.current is None:
            return None
        return {k: v for k, v in self.current.items() if k != "alerted"}


def _median(values: Iterable[float | None]) -> float | None:
    vals = [v for v in values if v is not None]
    return statistics.median(vals) if vals else None


def period_summary(seconds: list[Second], period_s: float = 600.0, start_s: float = 0.0) -> list[dict[str, Any]]:
    """The run in periods of `period_s` from `start_s`: TPS from exact sums; p99 as the median
    and the largest of the 1-second p99 values (percentiles of whole periods would need every
    latency, which the run does not keep; the interval lines have exact window percentiles)."""
    buckets: dict[int, list[Second]] = {}
    for s in seconds:
        # a sample is stamped at the END of its second: one ending exactly at 600 s is in the first period
        buckets.setdefault(max(0, int((s.elapsed_s - start_s - 1e-9) // period_s)), []).append(s)
    out = []
    for i in sorted(buckets):
        b = buckets[i]
        secs = sum(s.interval_s for s in b)
        p99s = [s.p99_ms for s in b if s.p99_ms is not None]
        out.append({"from_s": round(start_s + i * period_s), "to_s": round(start_s + (i + 1) * period_s),
                    "seconds": round(secs, 1), "commits": sum(s.commits for s in b),
                    "tps": round(sum(s.commits for s in b) / secs, 1) if secs > 0 else None,
                    "p99_median_1s_ms": _median(p99s), "p99_max_1s_ms": max(p99s) if p99s else None,
                    "errors": sum(s.errors for s in b), "unknown": sum(s.indeterminate for s in b),
                    "drops": sum(s.drops for s in b), "reconnects": sum(s.reconnects for s in b),
                    "connect_failures": sum(s.connect_failures for s in b),
                    "business_rollbacks": sum(s.business_rollbacks for s in b)})
    return out


def second_dict(s: Second) -> dict[str, Any]:
    return asdict(s)
