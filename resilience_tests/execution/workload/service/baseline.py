"""Is the common workload's run a valid baseline? (pure: no I/O)

Judged on the measured part of the run (after the warm-up) and the checks after the stop. A
check whose evidence is missing is NOT_MEASURED, which fails the baseline -- never a pass
without evidence (constitution III). The thresholds come from the settings file
(config.BaselineRules) and are NEEDS SIGN-OFF until the lead agrees them.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from resilience_tests.execution.workload.service.config import BaselineRules
from resilience_tests.execution.workload.service.metrics import Second

PASS, FAIL, NOT_MEASURED = "PASS", "FAIL", "NOT_MEASURED"


def _check(name: str, ok: bool | None, detail: str) -> dict[str, Any]:
    return {"name": name, "status": NOT_MEASURED if ok is None else (PASS if ok else FAIL), "detail": detail}


# The p99 of each window, given the windows' start times and their length: from the samples'
# latency histograms (windows.window_p99s reads metrics-1s.jsonl); None when they carry none.
WindowP99 = Callable[[Sequence[float], float], "Sequence[float | None] | None"]


def window_starts(end_s: float, start_s: float, length_s: float,
                  excluded: Sequence[tuple[float, float]] = ()) -> list[float]:
    """Starts of the full, back-to-back windows (t, t + length_s] from `start_s` up to `end_s` (a
    last partial window is left out); a window that overlaps an excluded span (a scenario's) is
    left out too."""
    out: list[float] = []
    t = start_s
    while t + length_s <= end_s + 1e-6:
        if not any(a < t + length_s and t < b for a, b in excluded):
            out.append(t)
        t += length_s
    return out


def windows(seconds: Sequence[Second], start_s: float, length_s: float,
            excluded: Sequence[tuple[float, float]] = ()) -> list[list[Second]]:
    """The seconds of each window of window_starts()."""
    if not seconds:
        return []
    starts = window_starts(max(s.elapsed_s for s in seconds), start_s, length_s, excluded)
    return [[s for s in seconds if t < s.elapsed_s <= t + length_s] for t in starts]


def throughput_check(seconds: Sequence[Second], start_s: float, rate_tps: float | None,
                     rules: BaselineRules, excluded: Sequence[tuple[float, float]] = ()) -> dict[str, Any]:
    name = f"throughput: every {rules.tps_window_s} s window at or above {rules.tps_share_min:.0%} of the rate"
    if not rate_tps:
        return _check(name, None, "the run's rate is not known")
    ws = windows(seconds, start_s, rules.tps_window_s, excluded)
    if not ws:
        return _check(name, None, f"no full {rules.tps_window_s} s window was measured")
    tps = []
    for i, w in enumerate(ws):
        secs = sum(s.interval_s for s in w)
        tps.append((sum(s.commits for s in w) / secs if secs > 0 else 0.0, i))
    floor = rules.tps_share_min * rate_tps
    low = [(v, i) for v, i in tps if v < floor]
    worst, at = min(tps)
    detail = (f"{len(ws)} windows; lowest {worst:.1f} TPS (window from {start_s + at * rules.tps_window_s:.0f} s) "
              f"against {floor:.1f} ({rules.tps_share_min:.0%} of {rate_tps:g})")
    if low:
        detail += f"; {len(low)} window(s) below"
    return _check(name, not low, detail)


def latency_check(seconds: Sequence[Second], start_s: float, rules: BaselineRules,
                  excluded: Sequence[tuple[float, float]] = (), window_p99: WindowP99 | None = None) -> dict[str, Any]:
    """The p99 of ALL the transactions of each window (their latency histograms summed: the upper
    bound of a 2% bucket, never below the exact value), at or under the limit. A stall of a second
    or two inside a window shows in its p99; a median of 1-second p99 values would hide it."""
    name = (f"latency: p99 of every {rules.p99_window_s} s window (all its transactions) at or under "
            f"{rules.p99_limit_ms:g} ms")
    starts = window_starts(max(s.elapsed_s for s in seconds), start_s, rules.p99_window_s, excluded) if seconds else []
    if not starts:
        return _check(name, None, f"no full {rules.p99_window_s} s window was measured")
    values = window_p99(starts, float(rules.p99_window_s)) if window_p99 is not None else None
    if values is None:
        return _check(name, None, "the samples carry no latency histograms, so no window's p99 can be computed")
    if len(values) != len(starts):
        return _check(name, None, f"{len(values)} window p99 values for {len(starts)} windows")
    empty = [i for i, v in enumerate(values) if v is None]
    over = [(v, i) for i, v in enumerate(values) if v is not None and v > rules.p99_limit_ms]
    known = [(v, i) for i, v in enumerate(values) if v is not None]
    detail = f"{len(starts)} windows"
    if known:
        worst, at = max(known)
        detail += f"; highest p99 {worst:.1f} ms (window from {starts[at]:.0f} s)"
    if over:
        detail += f"; {len(over)} window(s) over the limit"
    if empty:
        detail += f"; {len(empty)} window(s) without a single transaction"
    return _check(name, not over and not empty, detail)


def error_check(seconds: Sequence[Second], rules: BaselineRules) -> dict[str, Any]:
    name = f"errors at or under {rules.error_share_max:.2%} of all transactions"
    errors = sum(s.errors + s.indeterminate for s in seconds)
    total = sum(s.commits + s.errors + s.indeterminate + s.business_rollbacks for s in seconds)
    if total == 0:
        return _check(name, None, "no transaction was measured")
    share = errors / total
    return _check(name, share <= rules.error_share_max,
                  f"{errors} of {total} ({share:.4%}): server rollbacks {sum(s.errors for s in seconds)}, "
                  f"unknown outcomes {sum(s.indeterminate for s in seconds)}")


def connection_check(seconds: Sequence[Second]) -> dict[str, Any]:
    drops = sum(s.drops for s in seconds)
    failed = sum(s.connect_failures for s in seconds)
    if not seconds:
        return _check("no dropped connections", None, "nothing was measured")
    return _check("no dropped connections", drops == 0 and failed == 0,
                  f"dropped {drops}, failed connection attempts {failed}, reconnects {sum(s.reconnects for s in seconds)}")


def memory_check(memory: Sequence[tuple[float, int]], rules: BaselineRules) -> dict[str, Any]:
    name = f"harness memory grows at most {rules.memory_growth_max:.0%} from {rules.memory_from_s:.0f} s to the end"
    later = [(t, b) for t, b in memory if t >= rules.memory_from_s and b]
    if len(later) < 2:
        return _check(name, None, f"fewer than 2 memory readings after {rules.memory_from_s:.0f} s")
    first, last = later[0][1], later[-1][1]
    growth = (last - first) / first
    return _check(name, growth <= rules.memory_growth_max,
                  f"{first / 1e6:.0f} MB at {later[0][0]:.0f} s, {last / 1e6:.0f} MB at {later[-1][0]:.0f} s "
                  f"({growth:+.1%}); largest {max(b for _, b in later) / 1e6:.0f} MB")


def end_checks(end: Mapping[str, Any], tpcc: bool = True) -> list[dict[str, Any]]:
    """The checks after the stop: lost commits, phantoms and (TPC-C workload) the TPC-C checks."""
    def known(key: str) -> Any:
        v = end.get(key)
        return v if isinstance(v, int) else None

    # with scenarios in the run: the commits acknowledged outside their windows (a scenario's own
    # losses are reported with that scenario); without: every lost commit
    outside = "rpo_outside_scenarios" in end
    lost, phantom = known("rpo_outside_scenarios" if outside else "rpo_txn"), known("phantom")
    detail = (f"lost {lost if lost is not None else 'not measured'}"
              + (f" outside the scenarios (all lost: {end.get('rpo_txn')})" if outside else "")
              + f", phantom {phantom if phantom is not None else 'not measured'}")
    out = [_check("no lost commit, no phantom", None if lost is None or phantom is None else lost == 0 and phantom == 0,
                  detail)]
    tpcc_checks = (("partial_commits", "no half-saved transaction"),
                   ("aborted_markers_present", "no rolled-back transaction kept"),
                   ("tpcc_consistency_violations", "TPC-C consistency rules hold"))
    for key, label in (tpcc_checks if tpcc else ()):
        v = known(key)
        why = end.get("_not_measured", {}).get(key) if isinstance(end.get("_not_measured"), Mapping) else None
        out.append(_check(label, None if v is None else v == 0,
                          f"{key} = {v}" if v is not None else f"not measured{f': {why}' if why else ''}"))
    return out


def evaluate(*, seconds: Sequence[Second], measure_start_s: float, rate_tps: float | None, rules: BaselineRules,
             memory: Sequence[tuple[float, int]], end: Mapping[str, Any], driver_limited: bool,
             full_duration: bool, ran_s: float, duration_s: float | None, tpcc: bool = True,
             excluded: Sequence[tuple[float, float]] = (), window_p99: WindowP99 | None = None) -> list[dict[str, Any]]:
    """`excluded`: the scenarios' spans (seconds since the clerks started): not judged as the
    workload's baseline (a scenario's fault breaks these rules by design). `window_p99`: the p99
    of each latency window from the samples' histograms (without it the latency rule is NOT
    MEASURED)."""
    measured = [s for s in seconds if s.elapsed_s > measure_start_s
                and not any(a < s.elapsed_s <= b for a, b in excluded)]
    duration = (_check("ran the full duration", full_duration, f"{ran_s:.0f} s of {duration_s:.0f} s")
                if duration_s is not None else
                _check("ran until it was stopped", full_duration, f"{ran_s:.0f} s, until the stop command"))
    return [
        duration,
        throughput_check(measured, measure_start_s, rate_tps, rules, excluded),
        latency_check(measured, measure_start_s, rules, excluded, window_p99),
        error_check(measured, rules),
        connection_check(measured),
        _check("the harness was not the limit", not driver_limited,
               "driver-limited guard fired" if driver_limited else "the guard did not fire"),
        memory_check(memory, rules),
        *end_checks(end, tpcc),
    ]


def passed(checks: Sequence[Mapping[str, Any]]) -> bool:
    return bool(checks) and all(c["status"] == PASS for c in checks)
