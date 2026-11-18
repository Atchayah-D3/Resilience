"""Windows over the common workload's 1-second samples, and the scenarios' windows (pure: no I/O
except `window_from_file`).

  window              the figures of any time window, read from metrics-1s.jsonl: the same fields
                      as a scenario's MeasuredWindow (driver.MeasuredWindow), so a harness attached
                      to the running workload judges a window as it does today. A sample belongs to
                      the window when it ENDED inside it (t_wall in (from, to]); windows therefore
                      move in whole samples (1 s). Percentiles come from the summed latency
                      histograms: the upper bound of a 2% bucket (histogram.py), never below the
                      exact value.
  scenario_windows    the scenarios' time spans from scenario-marks.jsonl (run_start .. run_end, with
                      T0 and T1), so the service judges its own baseline outside them and reports
                      each one separately. A scenario without run_end is taken to last to the end.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from resilience_tests.execution.workload.histogram import HIST_GROWTH, hist_percentile, merge_histograms
from resilience_tests.execution.workload.service import state as st

PERCENTILE_NOTE = (f"percentiles from summed latency histograms: the upper bound of a {HIST_GROWTH - 1:.0%} bucket, "
                   "never below the exact nearest-rank value")


class NoHistograms(ValueError):
    """The samples carry no latency histograms (not written by the common workload)."""


def window(samples: Sequence[Mapping[str, Any]], from_wall: float, to_wall: float) -> dict[str, Any]:
    """The window (from_wall, to_wall] over 1-second samples (metrics-1s.jsonl lines). Fields as
    driver.MeasuredWindow, plus `samples`, `first_sample_start_wall`, `last_sample_end_wall`,
    `max_ms` and `percentiles` (how they were computed)."""
    inside = [s for s in samples if from_wall < float(s["t_wall"]) <= to_wall]
    missing = [s for s in inside if "latency_hist" not in s]
    if missing:
        raise NoHistograms(f"{len(missing)} sample(s) in the window carry no latency histogram")
    duration = sum(float(s.get("interval_s") or 0.0) for s in inside)
    commits = sum(int(s.get("commits", 0)) for s in inside)
    lat = merge_histograms(s.get("latency_hist") for s in inside)
    journal = merge_histograms(s.get("journal_hist") for s in inside)
    corrected = (merge_histograms(s.get("corrected_hist") for s in inside)
                 if inside and all("corrected_hist" in s for s in inside) else None)
    missed = sum(int(s.get("missed", 0)) for s in inside) if inside and all("missed" in s for s in inside) else None
    maxes = [s["max_ms"] for s in inside if s.get("max_ms") is not None]
    return {
        "duration_s": duration, "commits": commits, "tps": commits / duration if duration > 0 else 0.0,
        "p99_ms": hist_percentile(lat, 0.99), "journal_p99_ms": hist_percentile(journal, 0.99),
        "errors": sum(int(s.get("errors", 0)) for s in inside),
        "indeterminate": sum(int(s.get("indeterminate", 0)) for s in inside),
        "drops": sum(int(s.get("drops", 0)) for s in inside),
        "reconnects": sum(int(s.get("reconnects", 0)) for s in inside),
        "connect_failures": sum(int(s.get("connect_failures", 0)) for s in inside),
        "p50_ms": hist_percentile(lat, 0.50), "p95_ms": hist_percentile(lat, 0.95),
        "p99_corrected_ms": hist_percentile(corrected, 0.99) if corrected is not None else None,
        "missed": missed,
        # beyond MeasuredWindow
        "samples": len(inside), "max_ms": max(maxes) if maxes else None,
        "first_sample_start_wall": (float(inside[0]["t_wall"]) - float(inside[0].get("interval_s") or 0.0)
                                    if inside else None),
        "last_sample_end_wall": float(inside[-1]["t_wall"]) if inside else None,
        "percentiles": PERCENTILE_NOTE,
    }


MEASURED_WINDOW_FIELDS = ("duration_s", "commits", "tps", "p99_ms", "journal_p99_ms", "errors", "indeterminate",
                          "drops", "reconnects", "connect_failures", "p50_ms", "p95_ms", "p99_corrected_ms", "missed")


def window_from_file(folder: Path, from_wall: float, to_wall: float) -> dict[str, Any]:
    """window() over the running workload's metrics-1s.jsonl (whole lines only)."""
    return window(st.read_complete_lines(folder / st.METRICS_1S_FILE), from_wall, to_wall)


def scenario_windows(marks: Sequence[Mapping[str, Any]], end_wall: float, settle_s: float = 0.0) -> list[dict[str, Any]]:
    """One entry per scenario run (by run_id): start (run_start), end (run_end + settle_s, or
    `end_wall` when it never ended), T0, T1. Ordered by start."""
    runs: dict[str, dict[str, Any]] = {}
    for m in marks:
        r = runs.setdefault(str(m["run_id"]), {"run_id": str(m["run_id"]), "scenario": m.get("scenario")})
        r[f"{m['kind']}_wall"] = float(m["t_wall"])
    out = []
    for r in runs.values():
        start = r.get("run_start_wall", r.get("t0_wall"))
        if start is None:
            continue                                   # only a run_end/t1 without a start: nothing to place
        ended = "run_end_wall" in r
        end = min(end_wall, r["run_end_wall"] + settle_s) if ended else end_wall
        out.append({"run_id": r["run_id"], "scenario": r.get("scenario"), "start_wall": start, "end_wall": end,
                    "t0_wall": r.get("t0_wall"), "t1_wall": r.get("t1_wall"), "ended": ended})
    return sorted(out, key=lambda w: w["start_wall"])


def in_any(t_wall: float, spans: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    for w in spans:
        if w["start_wall"] <= t_wall <= w["end_wall"]:
            return w
    return None
