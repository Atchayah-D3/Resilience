"""Back to normal before the next scenario's baseline (common workload, specs/009).

A scenario judges its fault against its own baseline. When scenarios run one after another on one
common workload, the next baseline may be taken while the database is still slow from the previous
fault: the baseline's p99 is then too high, every "how much slower" ratio comes out too small, and
"back to normal" too easy -- a possible false pass. Measured on e2-u22-3 (26 Nov 2026): NL-N-I01a's
baseline p99 11 ms, NL-N-I01b's 61 s after NL-N-I01a ended 40 ms.

The rule is the harness's own "back to normal" (Framework §6.2, rto_decomposer): over the last
60 s, the customers' p99 is at most 1.5x and their TPS at least 80% of the workload's REFERENCE.

  reference   the workload's own figures before ANY fault ran on it: the last `REFERENCE_S` of its
              measured 1-second samples (after its warm-up) before the first scenario's T0, leaving
              out each scenario's start-up (run_start .. its baseline mark: init may restart the
              database). The p99 of every transaction in it (summed latency histograms, like a
              scenario's baseline) and its TPS. Fixed once the first fault has run.
  check       needed only once a fault has run on the workload (a T0 mark); before that the next
              baseline is undisturbed by definition. The p99 over the last 60 s from the summed
              histograms, never a per-second value (one slow transaction would fail a second).

Pure, except `read_reference` / `read_recent`, which read the workload's files.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from resilience_tests.analysis.rto_decomposer import MAX_SAMPLE_INTERVAL_S, SLO_P99_MULTIPLIER, SLO_SUSTAIN_S, SLO_TPS_FRACTION
from resilience_tests.execution.workload.histogram import hist_percentile, merge_histograms

# Framework §6.2 (the harness's back-to-normal rule, rto_decomposer)
P99_FACTOR = SLO_P99_MULTIPLIER          # 1.5
TPS_SHARE = SLO_TPS_FRACTION             # 0.80
WINDOW_S = SLO_SUSTAIN_S                 # 60 s
# The reference: as long as a scenario's baseline (10 minutes), and never shorter than half of it.
# NEEDS SIGN-OFF (the length of the reference, not the rule).
REFERENCE_S = 600.0
REFERENCE_MIN_S = 300.0
CITATION = "Framework §6.2: p99 <= 1.5x and TPS >= 80% of the reference over 60 s"


@dataclass(frozen=True)
class Reference:
    p99_ms: float
    tps: float
    seconds: float
    samples: int
    from_wall: float
    to_wall: float
    first_fault_wall: float

    def as_dict(self) -> dict[str, Any]:
        return {k: round(v, 3) if isinstance(v, float) else v for k, v in asdict(self).items()}


class NoReference(ValueError):
    """The workload has no usable reference (too few undisturbed measured seconds before its first
    fault, or samples without latency histograms): back to normal cannot be judged."""


# --- which seconds are undisturbed ------------------------------------------------------------------


def first_fault_wall(marks: Sequence[Mapping[str, Any]]) -> float | None:
    """The first T0 on this workload, or None when no fault has run on it."""
    t0s = [float(m["t_wall"]) for m in marks if m.get("kind") == "t0"]
    return min(t0s) if t0s else None


def startup_spans(marks: Sequence[Mapping[str, Any]]) -> list[tuple[float, float]]:
    """Each scenario's start-up, from run_start to its baseline mark (init may restart the
    database). A run without a baseline mark: to its run_end, or open (never counted)."""
    runs: dict[str, dict[str, float]] = {}
    for m in marks:
        runs.setdefault(str(m.get("run_id")), {})[str(m.get("kind"))] = float(m["t_wall"])
    spans = []
    for r in runs.values():
        if "run_start" not in r:
            continue
        end = r.get("baseline", r.get("run_end", float("inf")))
        spans.append((r["run_start"], end))
    return spans


def _undisturbed(sample: Mapping[str, Any], before_wall: float, spans: Sequence[tuple[float, float]]) -> bool:
    t = float(sample["t_wall"])
    start = t - float(sample.get("interval_s") or 0.0)
    return (sample.get("part") == "measured" and t <= before_wall
            and not any(a < t and start < b for a, b in spans))


# --- the reference and the check ----------------------------------------------------------------------


def reference(samples: Iterable[Mapping[str, Any]], marks: Sequence[Mapping[str, Any]],
              length_s: float = REFERENCE_S, min_s: float = REFERENCE_MIN_S) -> Reference:
    """The workload's reference: the last `length_s` of undisturbed measured samples before the
    first fault. Raises NoReference (never a made-up figure)."""
    fault = first_fault_wall(marks)
    if fault is None:
        raise NoReference("no fault has run on the workload yet")
    spans = startup_spans(marks)
    kept = [s for s in samples if _undisturbed(s, fault, spans)]
    window: list[Mapping[str, Any]] = []
    seconds = 0.0
    for s in reversed(kept):
        if seconds >= length_s - 1e-9:
            break
        window.append(s)
        seconds += float(s.get("interval_s") or 0.0)
    window.reverse()
    if seconds < min_s:
        raise NoReference(f"only {seconds:.0f} s of undisturbed measured samples before the first fault, "
                          f"at least {min_s:.0f} s needed")
    if any("latency_hist" not in s for s in window):
        raise NoReference("the samples carry no latency histograms")
    p99 = hist_percentile(merge_histograms(s["latency_hist"] for s in window), 0.99)
    if p99 is None:
        raise NoReference("no transaction in the reference")
    commits = sum(int(s.get("commits", 0)) for s in window)
    return Reference(p99_ms=float(p99), tps=commits / seconds, seconds=seconds, samples=len(window),
                     from_wall=float(window[0]["t_wall"]) - float(window[0].get("interval_s") or 0.0),
                     to_wall=float(window[-1]["t_wall"]), first_fault_wall=fault)


def check(recent: Sequence[Mapping[str, Any]], ref: Reference, now: float, window_s: float = WINDOW_S) -> dict[str, Any]:
    """The last `window_s` against the reference. `ok` is True only when every part was measured
    and holds; `problems` names what does not."""
    inside = [s for s in recent if now - window_s < float(s["t_wall"]) <= now]
    seconds = sum(float(s.get("interval_s") or 0.0) for s in inside)
    out: dict[str, Any] = {"rule": CITATION, "reference": ref.as_dict(), "window_s": window_s,
                           "p99_bound_ms": round(P99_FACTOR * ref.p99_ms, 3),
                           "tps_min": round(TPS_SHARE * ref.tps, 3), "seconds": round(seconds, 3),
                           "p99_ms": None, "tps": None, "ok": False, "problems": []}
    problems: list[str] = out["problems"]
    if seconds < window_s - MAX_SAMPLE_INTERVAL_S:
        problems.append(f"only {seconds:.0f} s of samples in the last {window_s:.0f} s")
        return out
    long = [float(s.get("interval_s") or 0.0) for s in inside if float(s.get("interval_s") or 0.0) > MAX_SAMPLE_INTERVAL_S]
    if long:
        problems.append(f"a sample covers {max(long):.1f} s (a starved sampler cannot show a dip)")
    if any("latency_hist" not in s for s in inside):
        problems.append("samples without latency histograms in the last 60 s")
        return out
    p99 = hist_percentile(merge_histograms(s["latency_hist"] for s in inside), 0.99)
    tps = sum(int(s.get("commits", 0)) for s in inside) / seconds
    out["p99_ms"] = None if p99 is None else round(float(p99), 3)
    out["tps"] = round(tps, 3)
    if p99 is None:
        problems.append("no transaction committed in the last 60 s")
    elif p99 > P99_FACTOR * ref.p99_ms:
        problems.append(f"the customers' delay is not back to normal: p99 {p99:.1f} ms over the last {window_s:.0f} s, "
                        f"above {P99_FACTOR:g} x the workload's reference {ref.p99_ms:.1f} ms = "
                        f"{P99_FACTOR * ref.p99_ms:.1f} ms (Framework §6.2)")
    if tps < TPS_SHARE * ref.tps:
        problems.append(f"the customers' rate is not back to normal: {tps:.0f} TPS over the last {window_s:.0f} s, "
                        f"under {TPS_SHARE:.0%} of the workload's reference {ref.tps:.0f} TPS (Framework §6.2)")
    out["ok"] = not problems
    return out


def describe(result: Mapping[str, Any]) -> str:
    """One line for the run's disclosures."""
    if result.get("needed") is False:
        return f"Back to normal before the baseline: not checked, {result.get('why')}."
    if result.get("error"):
        return f"Back to normal before the baseline: NOT JUDGED ({result['error']})."
    ref = result.get("reference") or {}
    state = "back to normal" if result.get("ok") else "NOT back to normal: " + "; ".join(result.get("problems") or [])
    return (f"Back to normal before the baseline ({CITATION}): p99 {result.get('p99_ms')} ms and "
            f"{result.get('tps')} TPS over the last {result.get('window_s'):g} s, against the workload's reference "
            f"p99 {ref.get('p99_ms')} ms and {ref.get('tps')} TPS ({ref.get('seconds')} s before its first fault) -- {state}.")


# --- reading the workload's files -----------------------------------------------------------------------


def read_samples_until(path: Path, until_wall: float) -> list[dict[str, Any]]:
    """The 1-second samples (metrics-1s.jsonl) that ended at or before `until_wall`, read from the
    start and stopped at the first later one, so a long-running workload's file is not read whole.
    A last line without its newline (still being written) is left out."""
    out: list[dict[str, Any]] = []
    try:
        with path.open("rb") as fh:
            for raw in fh:
                if not raw.endswith(b"\n"):
                    break
                if not raw.strip():
                    continue
                s = json.loads(raw)
                if float(s["t_wall"]) > until_wall:
                    break
                out.append(s)
    except FileNotFoundError:
        return []
    return out
