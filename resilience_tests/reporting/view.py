"""Run evidence -> what a run report shows (data-model E2-E4). Pure: no HTML, no I/O beyond
listing the run directory, so every rule about honest reporting is unit-testable.

The report never decides anything. The run's status and per-rule outcomes are shown exactly as
the run recorded them (spec FR-009); NOT_MEASURED and NOT_APPLICABLE are labels with reasons,
never numbers or passes (Constitution III)."""

from __future__ import annotations

import datetime as dt
import math
from typing import Any

from resilience_tests.reporting.evidence import RunEvidence

NOT_MEASURED = "NOT_MEASURED"
NOT_APPLICABLE = "NOT_APPLICABLE"
MISSING = "<missing>"                 # threshold_eval's marker for a value never produced
MAX_POINTS = 1200                     # per chart series; long soaks are bucketed down
CHART_W, CHART_H = 1000, 170
REPORT_FILE = "report.html"
# statuses that are not a pass, and therefore can never be shown or counted as one
NON_PASS = frozenset({"failed", "aborted", "error", "no_result"})
NO_VERDICT = frozenset({"aborted", "error", "no_result"})
# per-cycle columns worth showing, in this order, when present
CYCLE_COLUMNS = ("cycle", "recovery_s", "outage_observed", "target_pid", "death_confirmed_s",
                 "postmaster_survived", "missed_attempts", "redo_distance_bytes",
                 "replay_bytes_per_s", "failed_transactions", "dropped_connections")


# ------------------------------------------------------------------ values and statuses

def classify(value: Any) -> dict[str, str]:
    """{kind, display}: the only way a measured value reaches a page."""
    if value == NOT_MEASURED:
        return {"kind": "not_measured", "display": "not measured"}
    if value == NOT_APPLICABLE:
        return {"kind": "not_applicable", "display": "not applicable"}
    if value is None or value == MISSING:
        return {"kind": "missing", "display": "not produced"}
    if isinstance(value, bool):
        return {"kind": "bool", "display": "true" if value else "false"}
    if isinstance(value, int):
        return {"kind": "number", "display": f"{value:,}"}
    if isinstance(value, float):
        return {"kind": "number", "display": _fmt_float(value)}
    return {"kind": "text", "display": str(value)}


def _fmt_float(v: float) -> str:
    if math.isnan(v) or math.isinf(v):
        return str(v)
    if v == 0 or abs(v) >= 100:
        return f"{v:,.1f}"
    return f"{v:.3g}" if abs(v) < 0.01 else f"{v:.3f}".rstrip("0").rstrip(".")


def case_status(results: dict[str, Any] | None) -> str:
    """passed | failed | aborted | error | stopped_before_fault | no_result. Anything the
    harness did not record as `passed` is never mapped to it."""
    if not results:
        return "no_result"
    status = str(results.get("status") or "")
    return status if status in {"passed", "failed", "aborted", "error", "stopped_before_fault"} else "error"


def stopped_phase(results: dict[str, Any] | None) -> str | None:
    for p in (results or {}).get("phases", []):
        if p.get("outcome") not in ("ok", None):
            return p.get("phase")
    return None


def non_pass_rules(results: dict[str, Any] | None) -> list[str]:
    """One line per acceptance rule that did not pass: predicate, outcome, values, reason."""
    lines = []
    for r in ((results or {}).get("verdict") or {}).get("results", []):
        if r.get("outcome") == "pass":
            continue
        values = ", ".join(f"{k}={classify(v)['display']}" for k, v in (r.get("values") or {}).items())
        line = f"{r.get('predicate')} -> {r.get('outcome')} (values: {values})"
        if r.get("reason"):
            line += f"; {r['reason']}"
        lines.append(line)
    return lines


# ------------------------------------------------------------------ the run view

def build_run_view(ev: RunEvidence, now: dt.datetime | None = None) -> dict[str, Any]:
    r = ev.results or {}
    status = case_status(ev.results)
    facts = r.get("facts") or {}
    not_measured = facts.get("not_measured") or {}
    notes: list[str] = []
    if ev.results is None:
        notes.append("No result was recorded for this run (results.json missing or unreadable): "
                     "the harness stopped before it could write one. Nothing here is a verdict.")
    if ev.events_missing:
        notes.append("events.jsonl is missing: no timeline or charts can be shown.")
    if ev.malformed_event_lines:
        notes.append(f"{ev.malformed_event_lines} unreadable line(s) in events.jsonl were skipped.")

    scenario = r.get("scenario") or {}
    phases = [{"phase": p.get("phase"), "outcome": p.get("outcome"),
               "duration_s": classify(p.get("duration_s"))["display"], "error": p.get("error")}
              for p in r.get("phases", [])]
    rules = []
    for res in (r.get("verdict") or {}).get("results", []):
        rules.append({
            "predicate": res.get("predicate"), "outcome": res.get("outcome"),
            "values": [{"name": k, **classify(v)} for k, v in (res.get("values") or {}).items()],
            "margin": None if res.get("margin") is None else _fmt_float(float(res["margin"])),
            "reason": res.get("reason"),
        })
    measures = []
    for name, value in sorted((r.get("measured") or {}).items()):
        c = classify(value)
        measures.append({"name": name, **c,
                         "reason": not_measured.get(name) if c["kind"] in ("not_measured", "missing") else None})
    for name in facts.get("declared_not_produced") or []:
        measures.append({"name": name, "kind": "missing", "display": "not produced",
                         "reason": "declared by the scenario but not produced by this run"})

    timeline, charts = build_timeline_and_charts(ev)
    cycles = facts.get("cycles") or []
    cols = [c for c in CYCLE_COLUMNS if any(c in row for row in cycles)]
    cycle_rows = [[_cell(row.get(c)) for c in cols] for row in cycles]

    files = sorted(p.name for p in ev.run_dir.iterdir() if p.is_file() and p.name != REPORT_FILE) \
        if ev.run_dir.is_dir() else []
    total_s = sum(float(p.get("duration_s") or 0) for p in r.get("phases", []))

    return {
        "scenario": {"id": scenario.get("id") or ev.run_dir.name.split("-2")[0],
                     "name": scenario.get("name", ""), "priority": scenario.get("priority", "?"),
                     "category": scenario.get("category", "")},
        "run_id": r.get("run_id") or ev.run_dir.name,
        "environment": r.get("environment") or {}, "target": r.get("target") or {},
        "status": status, "is_pass": status == "passed", "has_verdict": status in ("passed", "failed"),
        "error": r.get("error"), "stopped_phase": stopped_phase(ev.results),
        "duration": classify(total_s)["display"] if total_s else "",
        "phases": phases, "rules": rules,
        "rules_failed": sum(1 for x in rules if x["outcome"] != "pass"),
        "measures": measures, "timeline": timeline, "charts": charts,
        "cycle_columns": cols, "cycle_rows": cycle_rows,
        "during": facts.get("during"), "during_verification": facts.get("during_verification"),
        "disclosures": list(r.get("disclosures") or []),
        "files": files, "notes": notes,
        "generated_at": (now or dt.datetime.now(dt.UTC)).strftime("%Y-%m-%d %H:%M:%S UTC"),
    }


def _cell(v: Any) -> str:
    if isinstance(v, list):
        return str(len(v)) if v else "0"
    if isinstance(v, dict):
        return ", ".join(f"{k}={classify(x)['display']}" for k, x in v.items())
    return classify(v)["display"]


# ------------------------------------------------------------------ timeline and charts

def _t(ev: dict[str, Any]) -> int:
    return int(ev.get("t_mono_ns") or 0)


def build_timeline_and_charts(ev: RunEvidence) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    events = ev.events
    if not events:
        return [], []
    start = next((_t(e) for e in events if e.get("kind") == "run_start"), min(_t(e) for e in events))
    end = max(_t(e) for e in events)
    span_s = max((end - start) / 1e9, 1.0)

    def sec(ns: int) -> float:
        return round((ns - start) / 1e9, 2)

    markers: list[dict[str, Any]] = []
    for e in events:
        k, d = e.get("kind"), e.get("data") or {}
        if k == "run_start":
            markers.append({"t_s": 0.0, "kind": "run_start", "label": "run start", "cycle": None})
        elif k == "phase_start":
            markers.append({"t_s": sec(_t(e)), "kind": "phase", "label": f"phase: {d.get('phase')}", "cycle": None})
        elif k == "t0":
            markers.append({"t_s": sec(int(d.get("t0_mono_ns") or _t(e))), "kind": "fault",
                            "label": f"FAULT: {d.get('action') or d.get('fault')}", "cycle": d.get("cycle")})
        elif k == "t1":
            markers.append({"t_s": sec(int(d.get("t1_mono_ns") or _t(e))), "kind": "t1",
                            "label": f"T1: {d.get('action', '')}", "cycle": None})
        elif k == "run_end":
            markers.append({"t_s": sec(_t(e)), "kind": "run_end", "label": "run end", "cycle": None})

    # Outage and recovery exactly as the run measured them (analysis/rto_decomposer.py) -- the
    # report never applies a rule of its own. Outage seen: the first failed write probe whose
    # attempt started at or after the fault, at the moment it failed. Recovered: the fault time
    # plus the run's own recovery time (per cycle, or rto_first_write_s for a single fault).
    r0 = ev.results or {}
    faults = sorted(((int((e.get("data") or {}).get("t0_mono_ns") or _t(e)), (e.get("data") or {}).get("cycle"))
                     for e in events if e.get("kind") == "t0"), key=lambda f: f[0])
    probes = sorted((e for e in events if e.get("kind") == "write_probe"), key=_t)
    recovery_by_cycle = {(e.get("data") or {}).get("cycle"): (e.get("data") or {}).get("recovery_s")
                         for e in events if e.get("kind") == "cycle_recovered"}
    single = (r0.get("measured") or {}).get("rto_first_write_s")
    for i, (t0, cyc) in enumerate(faults):
        nxt = faults[i + 1][0] if i + 1 < len(faults) else float("inf")
        down = next((e for e in probes if not (e.get("data") or {}).get("ok")
                     and t0 <= int((e.get("data") or {}).get("t_start_mono_ns") or _t(e)) < nxt), None)
        if down is None:
            markers.append({"t_s": sec(t0), "kind": "note", "cycle": cyc,
                            "label": "no failed write probe after this fault (outage not observed)"})
        else:
            markers.append({"t_s": sec(_t(down)), "kind": "outage_start", "cycle": cyc,
                            "label": "outage seen (first write probe that failed)"})
        rec = recovery_by_cycle.get(cyc) if cyc is not None else single
        if _is_num(rec):
            markers.append({"t_s": sec(int(t0 + rec * 1e9)), "kind": "recovered", "cycle": cyc,
                            "label": f"{'cycle ' + str(cyc) + ': ' if cyc is not None else ''}"
                                     f"first write accepted, {classify(rec)['display']} s after the fault"})
        else:
            markers.append({"t_s": sec(t0), "kind": "note", "cycle": cyc,
                            "label": "no recovery time recorded for this fault"})

    slo = (r0.get("measured") or {}).get("rto_to_slo_s")
    slo_t0 = (r0.get("facts") or {}).get("slo_t0_mono_ns") or (r0.get("timing") or {}).get("t0_mono_ns")
    if _is_num(slo) and slo_t0:
        markers.append({"t_s": sec(int(slo_t0 + slo * 1e9)), "kind": "slo_recovered",
                        "label": f"back to SLO ({classify(slo)['display']} s)", "cycle": None})
    markers.sort(key=lambda m: (m["t_s"], m["kind"]))

    samples = [(sec(_t(e)), e.get("data") or {}) for e in events
               if e.get("kind") == "sample" and e.get("source") == "workload"]
    charts = []
    if samples:
        charts.append(_chart("Throughput (committed transactions / s)", span_s, markers, [
            _series("TPS", "tps", [(t, d.get("tps")) for t, d in samples], "mean", "#2563eb")]))
        charts.append(_chart("Latency (ms, p99 per second)", span_s, markers, [
            _series("commit p99", "ms", [(t, d.get("p99_ms")) for t, d in samples], "max", "#d97706"),
            _series("journal flush p99 (driver host)", "ms",
                    [(t, d.get("journal_p99_ms")) for t, d in samples], "max", "#7c3aed")]))
    return markers, charts


def _series(name: str, unit: str, points: list[tuple[float, Any]], agg: str, color: str) -> dict[str, Any]:
    pts = [(t, float(v)) for t, v in points if isinstance(v, (int, float)) and not isinstance(v, bool)]
    downsampled = False
    if len(pts) > MAX_POINTS:
        k = math.ceil(len(pts) / MAX_POINTS)
        out = []
        for i in range(0, len(pts), k):
            chunk = pts[i:i + k]
            vals = [v for _, v in chunk]
            out.append((chunk[0][0], max(vals) if agg == "max" else sum(vals) / len(vals)))
        pts, downsampled = out, True
    return {"name": name, "unit": unit, "points": pts, "downsampled": downsampled, "color": color,
            "max": max((v for _, v in pts), default=0.0)}


def _chart(title: str, span_s: float, markers: list[dict[str, Any]], series: list[dict[str, Any]]) -> dict[str, Any]:
    ymax = max((s["max"] for s in series), default=0.0) or 1.0
    ymax *= 1.1

    def x(t: float) -> float:
        return round(t / span_s * CHART_W, 1)

    def y(v: float) -> float:
        return round(CHART_H - v / ymax * CHART_H, 1)

    for s in series:
        s["svg_points"] = " ".join(f"{x(t)},{y(v)}" for t, v in s["points"])
    # Only two kinds of vertical line on a chart -- the fault (red) and recovery (green); the
    # rest of the timeline is in the table under the charts.
    marks = [{"x": x(m["t_s"]), "kind": m["kind"], "label": m["label"]} for m in markers
             if m["kind"] in ("fault", "recovered")]
    ticks = [{"x": x(t), "label": f"{int(t)}s"} for t in _ticks(span_s)]
    yticks = [{"y": y(v), "label": _tick_label(v)} for v in _nice_ticks(ymax)]
    return {"title": title, "series": series, "markers": marks, "ticks": ticks, "yticks": yticks,
            "ymax": classify(ymax)["display"], "width": CHART_W, "height": CHART_H}


def _nice_ticks(ymax: float) -> list[float]:
    """0 and evenly spaced round values (1/2/5 x 10^n) up to ymax."""
    raw = ymax / 5
    mag = 10 ** math.floor(math.log10(raw)) if raw > 0 else 1
    step = next(m * mag for m in (1, 2, 5, 10) if m * mag >= raw)
    return [i * step for i in range(int(ymax // step) + 1)]


def _tick_label(v: float) -> str:
    return f"{v:,.0f}" if v >= 10 or v == int(v) else f"{v:g}"


def _is_num(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _ticks(span_s: float) -> list[float]:
    step = next(s for s in (10, 30, 60, 120, 300, 600, 1800, 3600, 7200) if span_s / s <= 12) \
        if span_s <= 12 * 7200 else 14400
    return [float(t) for t in range(0, int(span_s) + 1, step)]
