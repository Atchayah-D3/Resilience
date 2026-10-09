"""pytest plugin: `--report-dir DIR` writes the session's reports (Arch §10.4).

Scenario sessions (resilience_tests/test_scenarios.py) -> DIR/scenarios-junit.xml (the CI
gate input), DIR/gate.txt (the P0 decision), DIR/index.html (links to each run's report.html).
Other tests (the harness unit tests) -> DIR/unit-summary.html; their JUnit XML is pytest's own
--junitxml. Reporting never changes a test outcome; a reporting failure is printed."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

SCENARIO_TEST = "test_scenarios.py::test_scenario"


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.getgroup("reporting").addoption(
        "--report-dir", action="store", default=None,
        help="write JUnit XML / HTML reports for this session into DIR (Arch §10.4)")


def pytest_configure(config: pytest.Config) -> None:
    if config.getoption("--report-dir") and not hasattr(config, "workerinput"):
        config.pluginmanager.register(ReportCollector(config), "resilience-report-collector")


class ReportCollector:
    def __init__(self, config: pytest.Config) -> None:
        self.config = config
        self.dir = Path(config.getoption("--report-dir"))
        self.records: dict[str, dict[str, Any]] = {}
        self.lines: list[str] = []

    # one record per test, merged across setup / call / teardown
    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        rec = self.records.setdefault(report.nodeid, {"nodeid": report.nodeid, "outcome": None,
                                                      "duration_s": 0.0, "props": {}, "message": ""})
        rec["duration_s"] += float(report.duration or 0.0)
        rec["props"].update(dict(report.user_properties))
        if report.skipped and rec["outcome"] is None:
            rec["outcome"] = "skipped"
            lr = report.longrepr
            rec["message"] = (lr[2] if isinstance(lr, tuple) and len(lr) == 3 else str(lr)).removeprefix("Skipped: ")
        elif report.failed:
            rec["outcome"] = "failed" if report.when == "call" else "error"
            rec["message"] = (report.longreprtext or "")[-4000:]
        elif report.when == "call" and rec["outcome"] is None:
            rec["outcome"] = "passed"

    @pytest.hookimpl(tryfirst=True)
    def pytest_sessionfinish(self, session: pytest.Session, exitstatus: int) -> None:
        scenario = [r for r in self.records.values() if SCENARIO_TEST in r["nodeid"]]
        unit = [r for r in self.records.values() if SCENARIO_TEST not in r["nodeid"]]
        try:
            self.dir.mkdir(parents=True, exist_ok=True)
            if scenario:
                self._scenario_reports(scenario)
            if unit:
                self._unit_report(unit)
        except Exception as exc:  # noqa: BLE001 -- never changes test outcomes
            self.lines.append(f"REPORTS NOT WRITTEN: {type(exc).__name__}: {exc}")

    def _scenario_reports(self, records: list[dict[str, Any]]) -> None:
        from resilience_tests.reporting import gate
        from resilience_tests.reporting.cases import case_from_run, case_from_skip, case_without_evidence
        from resilience_tests.reporting.html import render_index
        from resilience_tests.reporting.junit import write_junit

        lookup = _catalog_lookup()
        cases = []
        for r in records:
            case_id = r["nodeid"].split("[", 1)[1][:-1] if "[" in r["nodeid"] else r["nodeid"]
            props = r["props"]
            if props.get("results"):
                cases.append(case_from_run(case_id, props["results"], report_path=props.get("report"),
                                           report_error=props.get("report_error")))
            elif r["outcome"] == "skipped":
                cases.append(case_from_skip(case_id, r["message"], lookup))
            else:
                last = (r["message"].strip().splitlines() or ["the test did not complete"])[-1]
                cases.append(case_without_evidence(case_id, last, lookup))
        xml = write_junit(cases, self.dir / "scenarios-junit.xml")
        decision = gate.decide(gate.read_cases(xml))
        gate_text = gate.render(decision)
        (self.dir / "gate.txt").write_text(gate_text + "\n")
        index = render_index(cases, self.dir / "index.html", gate_text=gate_text)
        self.lines += [f"scenario JUnit XML: {xml}", f"scenario index:     {index}",
                       f"run reports:        {sum(1 for c in cases if c.report_path)} of {len(cases)} cases",
                       gate_text.splitlines()[0]]

    def _unit_report(self, records: list[dict[str, Any]]) -> None:
        from resilience_tests.reporting.html import render_unit_summary
        rows = [{"nodeid": r["nodeid"], "outcome": r["outcome"] or "error",
                 "duration_s": r["duration_s"], "message": r["message"]} for r in records]
        self.lines.append(f"unit summary:       {render_unit_summary(rows, self.dir / 'unit-summary.html')}")

    def pytest_terminal_summary(self, terminalreporter: Any) -> None:
        if self.lines:
            terminalreporter.section("resilience reports")
            for line in self.lines:
                terminalreporter.write_line(line)


def _catalog_lookup() -> dict[str, dict[str, Any]]:
    """Priority and name for cases that never ran (skipped by the run plan)."""
    try:
        from catalog.schema import load_catalog
        return {sid: {"priority": sc.priority, "name": sc.name} for sid, sc in load_catalog().scenarios.items()}
    except Exception:  # noqa: BLE001 -- a skip still appears, with priority "?"
        return {}
