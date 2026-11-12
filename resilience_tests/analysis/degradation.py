"""How far the workload degraded while a fault was held, against the undisturbed baseline.

Contention faults (Framework §10.6 NL-N, and NL-R pressure faults) are judged on the load the
database sustained DURING the fault -- "OLTP TPS >= 70% baseline and p99 <= 2 x baseline" --
not on an outage. Both windows are the workload's own exact measurement windows (every
transaction counted, p99 over every latency), never a mean of per-second figures.

A ratio that cannot be formed is NOT_MEASURED with the reason, never 0 or 1.
"""

from __future__ import annotations

from typing import Any

from resilience_tests.analysis.predicates import NOT_MEASURED

# measure names; a scenario lists the ones it judges
TPS_RATIO = "oltp_tps_ratio"   # hold TPS / baseline TPS: 0.7 means 70% of baseline
P99_RATIO = "oltp_p99_ratio"   # hold p99 / baseline p99: 2.0 means twice the baseline latency


def load_ratios(hold: Any, baseline: Any) -> tuple[dict[str, Any], dict[str, str]]:
    """(measures, not-measured reasons) comparing two MeasuredWindows: the hold and the
    baseline. TPS and p99 are judged separately: a hold that committed nothing has a TPS ratio
    of 0 -- a real, failing value -- but no latency to compare."""
    m: dict[str, Any] = {}
    why: dict[str, str] = {}
    if hold is None or baseline is None:
        reason = "no measurement window was taken during the hold" if hold is None else "no baseline was measured"
        for name in (TPS_RATIO, P99_RATIO):
            m[name], why[name] = NOT_MEASURED, reason
        return m, why

    if baseline.tps > 0 and hold.duration_s > 0:
        m[TPS_RATIO] = round(hold.tps / baseline.tps, 4)
    else:
        m[TPS_RATIO] = NOT_MEASURED
        why[TPS_RATIO] = (f"baseline TPS {baseline.tps} or hold length {hold.duration_s:.1f} s "
                          "leaves no ratio to form")

    if not baseline.p99_ms:
        m[P99_RATIO], why[P99_RATIO] = NOT_MEASURED, "the baseline has no p99 latency to compare against"
    elif hold.p99_ms is None:
        m[P99_RATIO] = NOT_MEASURED
        why[P99_RATIO] = "no transaction completed during the hold, so it has no latency to compare"
    else:
        m[P99_RATIO] = round(hold.p99_ms / baseline.p99_ms, 4)
    return m, why
