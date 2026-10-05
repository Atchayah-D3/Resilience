"""RTO decomposer -- a pure function over the event stream (Arch §7.2, §7.3).

A decomposer bug silently produces plausible-looking wrong numbers rather than crashing,
so this module does no I/O and is unit-tested against recorded fixtures (tests/).

  RTO = T_detect + T_elect + T_promote + T_reconnect + T_warm      (Framework §6.3)

  rto_first_write_s  T0 -> first successful write probe that STARTED after the outage was
                     observed (Framework §6.2). A probe already in flight at T0 proves nothing
                     about recovery, and a fault that should interrupt service but was never
                     seen to is not measured at all -- never reported as a near-zero RTO.
  rto_to_slo_s       T0 -> start of the first window in which committed TPS >= 80% of
                     baseline and p99 <= 1.5x baseline hold for 60 s (Framework §6.2)
  t_detect/t_elect/t_promote, mttd: consensus-store and HA-agent signals; not applicable
                     to a standalone target (no HA agent, no consensus store).
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import re

from resilience_tests.analysis.predicates import NOT_APPLICABLE, NOT_MEASURED
from resilience_tests.observability.event_stream import Event

# Framework §6.2 RTO-to-SLO definition
SLO_TPS_FRACTION = 0.80
SLO_P99_MULTIPLIER = 1.5
SLO_SUSTAIN_S = 60.0
# A sample covering much more than the driver's 1 s sampling interval is an AVERAGE over that
# span: it cannot show a dip inside it, so it cannot evidence sustained compliance. Samples
# stretch when the event loop is starved -- exactly during a recovery storm -- so one is
# treated as non-compliant rather than allowed to certify the window it hides.
MAX_SAMPLE_INTERVAL_S = 1.5

# Leader detection, election and promotion exist only in a replicated group; on a standalone
# instance they are NOT_APPLICABLE. MTTD is different: detection time is meaningful for any
# target (Framework §6.2, "detection is ours"), so it is measured where a signal exists.
CLUSTER_ONLY_COMPONENTS = ("t_detect_s", "t_elect_s", "t_promote_s")


@dataclass(frozen=True)
class Baseline:
    tps: float
    p99_ms: float


@dataclass(frozen=True)
class RtoDecomposition:
    t0_ns: int
    rto_first_write_s: float | None | Any  # NOT_MEASURED when an expected outage was not seen
    rto_to_slo_s: float | None
    slo_window_end_s: float | None  # start + 60 s: when the sustained window was confirmed
    components: dict[str, Any]
    outage_observed: bool = False  # a write probe failed after T0

    def as_measured(self) -> dict[str, Any]:
        # Framework §6.3: RTO = T_detect + T_elect + T_promote + T_reconnect + T_warm. The
        # components are DURATIONS that sum to the RTO, so T_warm is the warm-up that follows
        # the first write -- not the cumulative time from T0, which would double-count
        # T_reconnect. On a standalone target the first three do not exist, so
        # T_reconnect + T_warm = rto_to_slo_s.
        return {
            "rto_first_write_s": self.rto_first_write_s,
            "rto_to_slo_s": self.rto_to_slo_s,
            "t_reconnect_s": self.rto_first_write_s,
            "t_warm_s": self.t_warm_s,
            **self.components,
        }

    @property
    def t_warm_s(self) -> Any:
        """Warm-up: first write -> sustained SLO. NOT_MEASURED if either end is unknown."""
        if self.rto_to_slo_s is None:
            return None
        if not isinstance(self.rto_first_write_s, (int, float)):
            return NOT_MEASURED
        return self.rto_to_slo_s - self.rto_first_write_s


def attempt_start_ns(e: Event) -> int:
    """When a probe attempt began. Events are stamped on completion; streams recorded before
    probes carried `t_start_mono_ns` fall back to completion minus the recorded latency."""
    start = e.data.get("t_start_mono_ns")
    if start is not None:
        return int(start)
    latency_ms = e.data.get("latency_ms")
    return e.t_mono_ns - int(latency_ms * 1e6) if latency_ms is not None else e.t_mono_ns


def first_write_after(events: Sequence[Event], t_ns: int) -> float | None:
    """Seconds from `t_ns` to the completion of the first successful write probe whose attempt
    STARTED at or after `t_ns`. An attempt in flight at `t_ns` may have committed before it."""
    for e in sorted(events, key=lambda e: e.t_mono_ns):
        if e.kind == "write_probe" and e.data.get("ok") and attempt_start_ns(e) >= t_ns:
            return (e.t_mono_ns - t_ns) / 1e9
    return None


@dataclass(frozen=True)
class WriteRecovery:
    outage_observed: bool
    first_write_s: float | None  # T0 -> first write after the outage (after T0 if none was seen)


def write_recovery(events: Sequence[Event], t0_ns: int) -> WriteRecovery:
    """The availability gap as the write prober saw it. The outage starts with the first
    failed probe completing after T0; recovery is the first successful probe that started
    after that failure. Without an observed failure the first write started after T0 is
    returned and `outage_observed` is False -- whether that is acceptable is the caller's
    decision (a config reload should cause no outage; a process kill must)."""
    probes = sorted((e for e in events if e.kind == "write_probe"), key=lambda e: e.t_mono_ns)
    # The failure that opens the outage must be attributable to the fault: its attempt has to
    # have STARTED at or after T0. A probe already in flight at T0 (or one that was failing
    # before it) says nothing about this fault -- counting it would both start the clock too
    # early, reporting a long outage as a fraction of a second, and satisfy the
    # "was an outage seen at all" guard for a fault that did nothing.
    first_fail = next((e for e in probes if not e.data.get("ok") and attempt_start_ns(e) >= t0_ns), None)
    if first_fail is None:
        return WriteRecovery(False, first_write_after(probes, t0_ns))
    for e in probes:
        if e.data.get("ok") and attempt_start_ns(e) >= first_fail.t_mono_ns:
            return WriteRecovery(True, (e.t_mono_ns - t0_ns) / 1e9)
    return WriteRecovery(True, None)


def sample_meets_slo(e: Event, baseline: Baseline, max_sample_interval_s: float = MAX_SAMPLE_INTERVAL_S) -> bool:
    """One 1 s workload sample against the Framework §6.2 SLO: committed TPS >= 80% of
    baseline and p99 <= 1.5x baseline, over an interval short enough to show a dip."""
    return (
        e.data["interval_s"] <= max_sample_interval_s
        and e.data["tps"] >= SLO_TPS_FRACTION * baseline.tps
        and e.data.get("p99_ms") is not None
        and e.data["p99_ms"] <= SLO_P99_MULTIPLIER * baseline.p99_ms
    )


@dataclass(frozen=True)
class BaselineSloCheck:
    """Can this baseline support an RTO-to-SLO measurement at all?

    rto_to_slo_s asks when the service is back to SLO -- 60 s straight of 1 s samples within
    80% TPS / 1.5x p99 of the baseline. If the UNDISTURBED service, during the baseline itself,
    never holds that for 60 s, the measurement after a fault would time stalls the database
    always has, not recovery. Derived entirely from the Framework's own SLO definition."""

    samples: int
    compliant: int
    sustained_window_found: bool

    @property
    def compliant_fraction(self) -> float | None:
        return self.compliant / self.samples if self.samples else None


def baseline_slo_check(events: Sequence[Event], window_t0_ns: int, window_end_ns: int,
                       baseline: Baseline) -> BaselineSloCheck:
    samples = [e for e in sorted(events, key=lambda e: e.t_mono_ns)
               if e.kind == "sample" and e.source == "workload"
               and e.t_mono_ns - int(e.data["interval_s"] * 1e9) >= window_t0_ns and e.t_mono_ns <= window_end_ns]
    start, _ = slo_recovery(samples, window_t0_ns, baseline)
    return BaselineSloCheck(len(samples), sum(1 for e in samples if sample_meets_slo(e, baseline)), start is not None)


def slo_recovery(events: Sequence[Event], t0_ns: int, baseline: Baseline,
                 max_sample_interval_s: float = MAX_SAMPLE_INTERVAL_S) -> tuple[float | None, float | None]:
    """Earliest run of consecutive 1 s workload samples after T0, spanning SLO_SUSTAIN_S, in
    which every sample meets the SLO. Returns (start, end) relative to T0 in seconds.

    A sample is attributed to the interval that ENDS at its timestamp. Two things break a run,
    because neither can show what happened inside them: a gap in the sample stream (samples
    missing from the record), and a sample whose own interval is far longer than the sampling
    period (a starved sampler averaging over the dip it should have exposed).
    """
    run_start_ns: int | None = None
    prev_end_ns: int | None = None
    for e in events:
        if e.kind != "sample" or e.source != "workload":
            continue
        interval_ns = int(e.data["interval_s"] * 1e9)
        start_ns = e.t_mono_ns - interval_ns
        if start_ns < t0_ns:
            continue
        ok = sample_meets_slo(e, baseline, max_sample_interval_s)
        contiguous = prev_end_ns is not None and start_ns - prev_end_ns <= interval_ns // 2
        if not ok:
            run_start_ns = None
        elif run_start_ns is None or not contiguous:
            run_start_ns = start_ns
        prev_end_ns = e.t_mono_ns
        if run_start_ns is not None and (e.t_mono_ns - run_start_ns) / 1e9 >= SLO_SUSTAIN_S - 1e-9:
            return (run_start_ns - t0_ns) / 1e9, (e.t_mono_ns - t0_ns) / 1e9
    return None, None


@dataclass(frozen=True)
class CycleRecovery:
    """One crash-and-recover cycle of a repeated scenario (Framework NL-C-05)."""

    cycle: int
    t0_ns: int
    outage_observed: bool
    recovery_s: float | None          # T0 -> first write after the outage, this cycle only


def per_cycle_recovery(events: Sequence[Event], t0s: Sequence[int],
                       expect_outage: bool = True) -> list[CycleRecovery]:
    """Recovery time for each cycle, from one continuous probe stream.

    A cycle's window ends where the next cycle's fault begins, so a probe belonging to cycle
    i+1 can never be credited to cycle i. Each window is then measured by exactly the same
    rule as a single-fault run: the first successful write that STARTED after the first
    failure following that cycle's T0.

    When the fault must cause an outage -- a crash cycle does -- a cycle in which no probe
    ever failed has no recovery to time. Reporting the first write after T0 there would turn
    a kill that landed on nothing into the fastest recovery of the run."""
    ordered = sorted(events, key=lambda e: e.t_mono_ns)
    out: list[CycleRecovery] = []
    for i, t0 in enumerate(t0s):
        end = t0s[i + 1] if i + 1 < len(t0s) else None
        window = [e for e in ordered if e.t_mono_ns > t0 and (end is None or e.t_mono_ns <= end)]
        r = write_recovery(window, t0)
        recovery = None if expect_outage and not r.outage_observed else r.first_write_s
        out.append(CycleRecovery(i + 1, t0, r.outage_observed, recovery))
    return out


def recovery_trend(cycles: Sequence[CycleRecovery]) -> dict[str, Any]:
    """Whether repeated crashes get slower. The Framework compares the last cycle with the
    first; that alone passes trivially if cycle 1 happened to be slow, so the slope over every
    cycle and the ratio against the median of the first three are reported beside it."""
    got = [c for c in cycles if isinstance(c.recovery_s, (int, float))]
    # named, not merely absent: a ratio computed over the cycles that did come back would
    # read as a healthy trend while some cycle in the middle never recovered at all
    missing = [c.cycle for c in cycles if not isinstance(c.recovery_s, (int, float))]
    if len(got) < 2:
        return {"measured_cycles": len(got), "unrecovered_cycles": missing}
    xs = [float(c.cycle) for c in got]
    ys = [float(c.recovery_s) for c in got]
    n = len(xs)
    mx, my = sum(xs) / n, sum(ys) / n
    var = sum((x - mx) ** 2 for x in xs)
    slope = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / var if var else 0.0
    ordered_y = sorted(ys)
    med = (ordered_y[n // 2] if n % 2 else (ordered_y[n // 2 - 1] + ordered_y[n // 2]) / 2)
    first3 = sorted(ys[:3])
    med3 = first3[len(first3) // 2]
    return {
        "measured_cycles": n,
        "unrecovered_cycles": missing,
        "first_s": ys[0], "last_s": ys[-1],
        "min_s": min(ys), "max_s": max(ys), "median_s": med,
        "ratio_last_over_first": ys[-1] / ys[0] if ys[0] else None,
        "ratio_last_over_median_of_first_three": ys[-1] / med3 if med3 else None,
        "slope_s_per_cycle": slope,
    }


def mttd_from_log(events: Sequence[Event], t0_ns: int, patterns: Sequence[str]) -> float | None:
    """Time from T0 to the first log line evidencing the fault, on the harness clock (the
    moment the line reached the harness). Returns None when no such line arrived."""
    if not patterns:
        return None
    matchers = [re.compile(p, re.IGNORECASE) for p in patterns]
    for e in events:
        if e.kind != "log_line" or e.t_mono_ns <= t0_ns:
            continue
        line = str(e.data.get("line", ""))
        if any(m.search(line) for m in matchers):
            return (e.t_mono_ns - t0_ns) / 1e9
    return None


def decompose(events: Sequence[Event], t0_ns: int, baseline: Baseline, *, clustered: bool,
              detection_patterns: Sequence[str] = (), recovery_patterns: Sequence[str] = (),
              expect_outage: bool = False) -> RtoDecomposition:
    """`expect_outage`: the fault must interrupt writes (kill, restart, power loss). If the
    probes never saw it do so, rto_first_write_s is NOT_MEASURED -- the outage was shorter
    than the probe interval or the fault did not take effect, and either way the harness
    has no gap to report."""
    ordered = sorted(events, key=lambda e: e.t_mono_ns)
    slo_start, slo_end = slo_recovery(ordered, t0_ns, baseline)
    components: dict[str, Any]
    if clustered:
        # Patroni / etcd signals arrive with the Tier-2 probes (Phase 4). Until then a
        # clustered target must not report fabricated values.
        raise NotImplementedError("clustered RTO decomposition needs the etcd watcher and Patroni prober (Phase 4)")
    components: dict[str, Any] = {name: NOT_APPLICABLE for name in CLUSTER_ONLY_COMPONENTS}
    mttd = mttd_from_log(ordered, t0_ns, detection_patterns)
    # measured, or honestly absent -- never "not applicable", which would claim the question
    # is meaningless for this target
    components["mttd_s"] = mttd if mttd is not None else NOT_MEASURED
    # When the replacement announced recovery. Useful, and NOT detection: reported separately
    # so neither number is read as the other.
    recovery_started = mttd_from_log(ordered, t0_ns, recovery_patterns)
    components["recovery_started_s"] = recovery_started if recovery_started is not None else NOT_MEASURED
    recovery = write_recovery(ordered, t0_ns)
    rto_first_write: Any = recovery.first_write_s
    if expect_outage and not recovery.outage_observed:
        rto_first_write = NOT_MEASURED
    return RtoDecomposition(
        t0_ns=t0_ns,
        rto_first_write_s=rto_first_write,
        rto_to_slo_s=slo_start,
        slo_window_end_s=slo_end,
        components=components,
        outage_observed=recovery.outage_observed,
    )
