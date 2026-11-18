"""Latency histograms for the common workload's 1-second samples (specs/007). Pure.

Used by both workload engines (driver.py, tpcc/) when the common workload turns them on, and by
service/windows.py to compute any window's percentiles from the samples.
"""
from __future__ import annotations

import math
from collections.abc import Iterable

# With them a reader of the 1-second samples
# can compute the percentiles of ANY window -- summing histograms is exact, averaging per-second
# percentiles is not. Bucket i holds the values in (B * G^(i-1), B * G^i]; a percentile read from
# merged buckets is the bucket's UPPER bound: never below the true nearest-rank value and at most
# (G - 1) = 2% above it, so a rule judged on it can fail early but never pass falsely.
HIST_BASE_MS = 0.001
HIST_GROWTH = 1.02


def hist_bucket(value_ms: float) -> int:
    if value_ms <= HIST_BASE_MS:
        return 0
    i = math.ceil(math.log(value_ms / HIST_BASE_MS) / math.log(HIST_GROWTH))
    while HIST_BASE_MS * HIST_GROWTH ** i < value_ms:      # floating point: the upper bound covers the value
        i += 1
    return i


def hist_upper_ms(bucket: int) -> float:
    return HIST_BASE_MS * HIST_GROWTH ** bucket


def latency_histogram(values: Iterable[float]) -> dict[str, int]:
    """Sparse histogram {bucket: count}; keys are strings (JSON)."""
    out: dict[int, int] = {}
    for v in values:
        b = hist_bucket(v)
        out[b] = out.get(b, 0) + 1
    return {str(b): n for b, n in sorted(out.items())}


def merge_histograms(hists: Iterable[dict[str, int] | None]) -> dict[str, int]:
    out: dict[int, int] = {}
    for h in hists:
        for b, n in (h or {}).items():
            out[int(b)] = out.get(int(b), 0) + int(n)
    return {str(b): n for b, n in sorted(out.items())}


def hist_percentile(hist: dict[str, int] | None, q: float) -> float | None:
    """Nearest-rank percentile from a histogram, as the upper bound of its bucket (see above)."""
    items = sorted((int(b), int(n)) for b, n in (hist or {}).items() if int(n) > 0)
    total = sum(n for _, n in items)
    if total == 0:
        return None
    rank = max(1, math.ceil(q * total))
    seen = 0
    for b, n in items:
        seen += n
        if seen >= rank:
            return hist_upper_ms(b)
    return hist_upper_ms(items[-1][0])
