---
description: "Task list for Test Reports (HTML and JUnit XML)"
---

# Tasks: Test Reports (HTML and JUnit XML)

**Input**: Design documents from `specs/002-test-reports-html-junit/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/, quickstart.md

**Tests**: REQUIRED -- spec FR-014 / Constitution III: aborted never shown as passed,
NOT_MEASURED never shown as a value, gate exit codes.

**Organization**: US1 CI gate (P1), US2 run report (P2), US3 unit-test reports (P3).

## Format: `[ID] [P?] [Story] Description`

---

## Phase 1: Setup

- [X] T001 Add `Jinja2>=3.1` to `[project].dependencies` and the templates as package data (`[tool.setuptools.package-data] "resilience_tests.reporting" = ["templates/*.j2"]`) in pyproject.toml
- [X] T002 Create package resilience_tests/reporting/ (`__init__.py`, `templates/`)

## Phase 2: Foundational

- [X] T003 Implement resilience_tests/reporting/evidence.py: `load_run(run_dir)` -> RunEvidence {run_dir, results (None if `results.json` missing), events (list; malformed lines skipped and counted)}
- [X] T004 Implement resilience_tests/reporting/view.py `classify(value)` -> {kind: number|bool|text|not_measured|not_applicable|missing, display} where the strings "NOT_MEASURED"/"NOT_APPLICABLE" are never formatted as numbers or booleans; and `case_status(results)` -> passed|failed|aborted|error|stopped_before_fault|no_result (data-model E5)

**Checkpoint**: foundation importable.

---

## Phase 3: User Story 1 - CI gate (P1) 🎯 MVP

**Independent Test**: tests below; quickstart §4 produces scenarios-junit.xml and a gate exit code.

### Tests (write first)

- [X] T005 [P] [US1] tests/test_reporting.py: JUnit mapping for every status per contracts/junit-format.md -- passed (no child), failed (`<failure type="verdict">` listing each non-pass rule with values and reason), aborted/error/no_result (`<error>` with reason and phase), stopped_before_fault/skipped (`<skipped>`); properties scenario_id/priority/status always present; suite counts match children
- [X] T006 [P] [US1] tests/test_reporting.py: gate -- P0 failed -> exit 1; P0 aborted -> exit 1; P1 failed only -> exit 0 (listed); P0 skipped -> exit 0 (listed); unreadable XML -> exit 2

### Implementation

- [X] T007 [US1] Implement resilience_tests/reporting/junit.py `write_junit(cases, path)` per contracts/junit-format.md (one testsuite per tier prefix, ElementTree, UTF-8)
- [X] T008 [US1] Implement resilience_tests/reporting/gate.py: `decide(cases)` -> GateDecision (data-model E6); `main(argv)` reads the XML, prints non-passes P0-first and skips, exit 0/1/2
- [X] T009 [US1] Implement resilience_tests/reporting/plugin.py: option `--report-dir`; collect scenario cases from `test_scenarios.py` reports (user_properties incl. `results` path; skip reasons; priority from the catalog for skipped cases); at session end write `scenarios-junit.xml` and `gate.txt`
- [X] T010 [US1] Create root conftest.py registering `pytest_plugins = ["resilience_tests.reporting.plugin"]`
- [X] T011 [US1] In resilience_tests/test_scenarios.py record properties `scenario_id`, `priority`, `status`, `run_id` (user_properties) for every run, and `scenario_id`/`priority` for skips

**Checkpoint**: quickstart §4 gives a correct JUnit XML and gate exit code.

---

## Phase 4: User Story 2 - Run report (P2)

**Independent Test**: quickstart §2 regenerates pages for a passed and an aborted real run.

### Tests

- [X] T012 [P] [US2] tests/test_reporting.py: page for an aborted run shows the abort reason and no "passed"; NOT_MEASURED measure shows the label and its reason, never "0"; every rule appears with outcome/values/margin/reason; all disclosures present
- [X] T013 [P] [US2] tests/test_reporting.py: timeline from synthetic events -- fault per cycle, outage start = first failed probe after T0, first write = first ok probe after it, cycle recovered, SLO recovered from measured rto_to_slo_s; missing events -> "not observed"; series downsampled to <= 1200 points; missing results.json -> "no result recorded"; same evidence + fixed clock -> identical HTML

### Implementation

- [X] T014 [US2] Implement in resilience_tests/reporting/view.py `build_run_view(evidence, now)` -> RunView (data-model E2): header, rules, measures with not-measured reasons, phases, cycles, during, disclosures, evidence links, notes
- [X] T015 [US2] Implement in resilience_tests/reporting/view.py timeline markers (research R5) and series with bucket downsampling and SVG point strings (research R4)
- [X] T016 [US2] Create resilience_tests/reporting/templates/run_report.html.j2 (inline CSS, inline SVG charts with markers, no JS/CDN) and templates/index.html.j2
- [X] T017 [US2] Implement resilience_tests/reporting/html.py: Jinja2 environment (autoescape on, package loader), `render_run(run_dir)` writes `report.html`, `render_index(cases, path)`, CLI `python -m resilience_tests.reporting.html RUN_DIR... [--index OUT]` exit 0/1
- [X] T018 [US2] In resilience_tests/test_scenarios.py render `report.html` after each run inside try/except; on failure record property `report_error` and print it; status unchanged (FR-011)
- [X] T019 [US2] In plugin.py write `index.html` at session end when `--report-dir` is given

---

## Phase 5: User Story 3 - Unit-test reports (P3)

- [X] T020 [P] [US3] tests/test_reporting.py: unit summary counts (passed/failed/skipped/errors) and failure messages rendered
- [X] T021 [US3] templates/unit_summary.html.j2 and plugin support: non-scenario tests collected; `unit-summary.html` written with `--report-dir`

---

## Phase 6: Polish

- [X] T022 [P] README.md and CLAUDE.md: how to produce reports, the gate, Jenkins snippet (contracts/cli-and-pytest.md)
- [X] T023 (2026-10-07: tests/test_reporting.py 22/22; quickstart §2 OK -- 4 real-run pages + index in 0.4 s; §3 unit suite with --report-dir: 312 pass, 4 fail only on the VM env file (max_data_fs_used_pct missing), unit-junit.xml + unit-summary.html written; §4 dry run NL-C-01 + NL-C-04: both skipped, scenarios-junit.xml + index.html + gate.txt written, gate PASS exit 0) Validate on the harness VM: `pytest tests` + catalog checks; quickstart §2 on real runs (NL-M-03 passed 20261006T075615Z, NL-M-03 aborted 20261006T103739Z); quickstart §3; quickstart §4 dry-run variant

## Dependencies & Execution Order

Setup -> Foundational -> US1 (MVP) -> US2 -> US3 -> Polish. US2's per-run page (T014-T018) is
independent of US1's JUnit; T019 needs T009.

## Parallel Opportunities

T005/T006, T012/T013, T020 (tests); T007 and T014-T016 (different files).

## Implementation Strategy

MVP = US1 (Jenkins-ready JUnit + P0 gate). Then US2 (the HTML engineers read), then US3.
