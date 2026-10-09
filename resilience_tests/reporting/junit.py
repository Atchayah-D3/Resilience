"""Scenario JUnit XML for the CI gate (Arch §10.4; contracts/junit-format.md).

Written by the harness, not by pytest's --junitxml: pytest reports every exception in a test
as <failure>, while a run that ended without a verdict (aborted, error, no result) must be an
<error> -- nothing was certified -- and a verdict failure must list the rules that failed."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from collections import defaultdict
from pathlib import Path

from resilience_tests.reporting.cases import ReportCase

FAILURE_STATUSES = frozenset({"failed"})
ERROR_STATUSES = frozenset({"aborted", "error", "no_result"})
SKIP_STATUSES = frozenset({"skipped", "stopped_before_fault"})


def kind_of(status: str) -> str:
    """pass | failure | error | skipped -- an unknown status is an error, never a pass."""
    if status == "passed":
        return "pass"
    if status in FAILURE_STATUSES:
        return "failure"
    if status in SKIP_STATUSES:
        return "skipped"
    return "error"


def build_junit(cases: list[ReportCase], suite_name: str = "resilience-scenarios") -> ET.Element:
    root = ET.Element("testsuites", name=suite_name)
    by_tier: dict[str, list[ReportCase]] = defaultdict(list)
    for c in cases:
        by_tier[c.tier].append(c)
    totals = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0, "time": 0.0}
    for tier in sorted(by_tier):
        tier_cases = by_tier[tier]
        counts = {k: sum(1 for c in tier_cases if kind_of(c.status) == k) for k in ("failure", "error", "skipped")}
        t = sum(c.duration_s for c in tier_cases)
        suite = ET.SubElement(root, "testsuite", name=tier, tests=str(len(tier_cases)),
                              failures=str(counts["failure"]), errors=str(counts["error"]),
                              skipped=str(counts["skipped"]), time=f"{t:.3f}")
        for c in tier_cases:
            tc = ET.SubElement(suite, "testcase", classname=f"scenarios.{c.category}", name=c.case_id,
                               time=f"{c.duration_s:.3f}")
            props = ET.SubElement(tc, "properties")
            for name, value in (("scenario_id", c.scenario_id), ("priority", c.priority), ("status", c.status),
                                ("name", c.name), ("run_id", c.run_id), ("evidence_dir", c.evidence_dir),
                                ("report", c.report_path), ("report_error", c.report_error)):
                if value:
                    ET.SubElement(props, "property", name=name, value=str(value))
            kind = kind_of(c.status)
            if kind == "failure":
                el = ET.SubElement(tc, "failure", message=c.message, type="verdict")
                el.text = "\n".join(c.detail)
            elif kind == "error":
                el = ET.SubElement(tc, "error", message=c.message, type=c.status)
                el.text = c.message
            elif kind == "skipped":
                ET.SubElement(tc, "skipped", message=c.message or c.status)
        totals["tests"] += len(tier_cases)
        totals["failures"] += counts["failure"]
        totals["errors"] += counts["error"]
        totals["skipped"] += counts["skipped"]
        totals["time"] += t
    for k in ("tests", "failures", "errors", "skipped"):
        root.set(k, str(totals[k]))
    root.set("time", f"{totals['time']:.3f}")
    return root


def write_junit(cases: list[ReportCase], path: Path | str) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tree = ET.ElementTree(build_junit(cases))
    ET.indent(tree)
    tree.write(path, encoding="utf-8", xml_declaration=True)
    return path
