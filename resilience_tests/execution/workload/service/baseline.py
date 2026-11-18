"""Is the common workload's run a valid baseline? (pure: no I/O)

Judged on the measured part of the run (after the warm-up) and the checks after the stop. A
check whose evidence is missing is NOT_MEASURED, which fails the baseline -- never a pass
without evidence (constitution III). The thresholds come from the settings file
(config.BaselineRules) and are NEEDS SIGN-OFF until the lead agrees them.
"""

from __future__ import annotations

import statistics
from collections.abc import Mapping, Sequence
from typing import Any

from resilience_tests.execution.workload.service.config import BaselineRules
from resilience_tests.execution.workload.service.metrics import Second

PASS, FAIL, NOT_MEASURED = "PASS", "FAIL", "NOT_MEASURED"


def _check(name: str, ok: bool | None, detail: str) -> dict[str, Any]:
    return {"name": name, "status": NOT_MEASURED if ok is None else (PASS if ok else FAIL), "detail": detail}


def windows(seconds: Sequence[Second], start_s: float, length_s: float) -> list[list[Second]]:
    """Full, back-to-back windows of `length_s` from `start_s` (a last partial window is left out)."""
    if not seconds:
        return []
    end = max(s.elapsed_s for s in seconds)
    out: list[list[Second]] = []
    t = start_s
    while t + length_s <= end + 1e-6:
        out.append([s for s in seconds if t < s.elapsed_s <= t + length_s])
        t += length_s
    return out


def throughput_check(seconds: Sequence[Second], start_s: float, rate_tps: float | None,
                     rules: BaselineRules) -> dict[str, Any]:
    name = f"throughput: every {rules.tps_window_s} s window at or above {rules.tps_share_min:.0%} of the rate"
    if not rate_tps:
        return _check(name, None, "the run's rate is not known")
    ws = [w for w in windows(seconds, start_s, rules.tps_window_s)]
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


def latency_check(seconds: Sequence[Second], start_s: float, rules: BaselineRules) -> dict[str, Any]:
    name = (f"latency: median of the 1-second p99 values in every {rules.p99_window_s} s window at or under "
            f"{rules.p99_limit_ms:g} ms")
    ws = windows(seconds, start_s, rules.p99_window_s)
    if not ws:
        return _check(name, None, f"no full {rules.p99_window_s} s window was measured")
    medians = []
    for i, w in enumerate(ws):
        vals = [s.p99_ms for s in w if s.p99_ms is not None]
        medians.append((statistics.median(vals) if vals else None, i))
    empty = [i for v, i in medians if v is None]
    over = [(v, i) for v, i in medians if v is not None and v > rules.p99_limit_ms]
    known = [(v, i) for v, i in medians if v is not None]
    detail = f"{len(ws)} windows"
    if known:
        worst, at = max(known)
        detail += f"; highest median {worst:.1f} ms (window from {start_s + at * rules.p99_window_s:.0f} s)"
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

    lost, phantom = known("rpo_txn"), known("phantom")
    out = [_check("no lost commit, no phantom", None if lost is None or phantom is None else lost == 0 and phantom == 0,
                  f"lost {lost if lost is not None else 'not measured'}, phantom "
                  f"{phantom if phantom is not None else 'not measured'}")]
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
             full_duration: bool, ran_s: float, duration_s: float, tpcc: bool = True) -> list[dict[str, Any]]:
    measured = [s for s in seconds if s.elapsed_s > measure_start_s]
    return [
        _check("ran the full duration", full_duration, f"{ran_s:.0f} s of {duration_s:.0f} s"),
        throughput_check(measured, measure_start_s, rate_tps, rules),
        latency_check(measured, measure_start_s, rules),
        error_check(measured, rules),
        connection_check(measured),
        _check("the harness was not the limit", not driver_limited,
               "driver-limited guard fired" if driver_limited else "the guard did not fire"),
        memory_check(memory, rules),
        *end_checks(end, tpcc),
    ]


def passed(checks: Sequence[Mapping[str, Any]]) -> bool:
    return bool(checks) and all(c["status"] == PASS for c in checks)
