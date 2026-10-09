"""One scenario case of a pytest invocation (data-model E5): a run, or a skip."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from resilience_tests.reporting.evidence import load_run
from resilience_tests.reporting.view import case_status, non_pass_rules, stopped_phase

SCENARIO_ID_RE = re.compile(r"\b(?:NL|CL|DX)-[A-Z]-\d{2}\b")


@dataclass
class ReportCase:
    case_id: str
    scenario_id: str
    priority: str
    name: str = ""
    status: str = "no_result"     # passed|failed|aborted|error|stopped_before_fault|skipped|no_result
    run_id: str | None = None
    evidence_dir: str | None = None
    report_path: str | None = None
    report_error: str | None = None
    message: str = ""
    detail: list[str] = field(default_factory=list)
    duration_s: float = 0.0

    @property
    def tier(self) -> str:
        return self.scenario_id.split("-")[0] if "-" in self.scenario_id else "NL"

    @property
    def category(self) -> str:
        return "-".join(self.scenario_id.split("-")[:2])


def scenario_id_of(text: str) -> str:
    m = SCENARIO_ID_RE.search(text or "")
    return m.group(0) if m else "unknown"


def case_from_run(case_id: str, evidence_dir: str | Path, *, report_path: str | None = None,
                  report_error: str | None = None) -> ReportCase:
    ev = load_run(evidence_dir)
    r = ev.results or {}
    sc = r.get("scenario") or {}
    status = case_status(ev.results)
    case = ReportCase(case_id=case_id, scenario_id=sc.get("id") or scenario_id_of(case_id),
                      priority=sc.get("priority") or "?", name=sc.get("name", ""), status=status,
                      run_id=r.get("run_id"), evidence_dir=str(evidence_dir),
                      report_path=report_path, report_error=report_error,
                      duration_s=sum(float(p.get("duration_s") or 0) for p in r.get("phases", [])))
    if status == "failed":
        case.detail = non_pass_rules(ev.results)
        total = len((r.get("verdict") or {}).get("results", []))
        case.message = f"{len(case.detail)} of {total} acceptance rules did not pass"
    elif status in ("aborted", "error"):
        phase = stopped_phase(ev.results)
        case.message = f"{status} without a verdict{f' in phase {phase}' if phase else ''}: {r.get('error') or 'no reason recorded'}"
    elif status == "no_result":
        case.message = "no result recorded for this run (the harness stopped before writing results.json)"
    elif status == "stopped_before_fault":
        case.message = "dry run: stopped before the fault, no verdict"
    return case


def case_from_skip(case_id: str, reason: str, catalog_lookup: dict[str, dict[str, Any]]) -> ReportCase:
    sid = scenario_id_of(case_id)
    info = catalog_lookup.get(sid, {})
    return ReportCase(case_id=case_id, scenario_id=sid, priority=info.get("priority", "?"),
                      name=info.get("name", ""), status="skipped", message=reason)


def case_without_evidence(case_id: str, message: str, catalog_lookup: dict[str, dict[str, Any]]) -> ReportCase:
    """The test failed before the harness returned a result (a harness exception)."""
    sid = scenario_id_of(case_id)
    info = catalog_lookup.get(sid, {})
    return ReportCase(case_id=case_id, scenario_id=sid, priority=info.get("priority", "?"),
                      name=info.get("name", ""), status="no_result",
                      message=f"no result recorded: {message}")
