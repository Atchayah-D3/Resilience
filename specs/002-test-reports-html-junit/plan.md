# Implementation Plan: Test Reports (HTML and JUnit XML)

**Branch**: `002-test-reports-html-junit` (spec directory; work happens on the team branch) |
**Date**: 2026-10-07 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/002-test-reports-html-junit/spec.md`

## Summary

Arch §10.4's outputs for the two test suites, generated only from evidence the runs already
write:

- **Scenario runs**: a per-run HTML page (`report.html` beside the evidence) rendered with
  Jinja2 -- verdict, every rule, measures with NOT_MEASURED/NOT_APPLICABLE labelled, an
  annotated timeline with inline-SVG charts, cycles, disclosures, evidence links; an index
  page per invocation; a JUnit XML written by the harness itself (full control of
  `<failure>` vs `<error>` vs `<skipped>` and of per-case properties); and a **P0 gate**
  command whose exit code is the build decision.
- **Unit tests**: pytest's built-in JUnit XML plus a one-page HTML summary from the same
  plugin.

Everything is opt-in through one pytest option, `--report-dir`, provided by a small pytest
plugin; nothing changes for runs that don't ask for reports, except that every scenario run
now also writes its own `report.html`.

## Technical Context

**Language/Version**: Python 3.11

**Primary Dependencies**: pytest (existing), Jinja2 3.1 (new; Arch §12 -- installed on the VM
by the user), Python stdlib `xml.etree.ElementTree` for JUnit XML. No JavaScript, no CDN:
charts are inline SVG so pages open offline (SC-004).

**Storage**: files only -- inputs `results.json`, `events.jsonl` in each run directory; outputs
`report.html` per run, and `<report-dir>/` (index, JUnit XML, gate summary, unit summary).

**Testing**: pytest unit tests with synthetic evidence; validation on the harness VM against
real run directories from 2026-10-06 (NL-M-03 passed, NL-M-03 aborted, NL-C-02 aborted).

**Target Platform**: harness VM (Ubuntu 22.04) and Jenkins agents (Arch §10.4, §13)

**Project Type**: reporting module + pytest plugin + CLI in the existing harness

**Performance Goals**: render a page from a 4-hour soak (~15k samples, ~70k probe events)
in a few seconds; charts downsampled to <= 1,200 points per series.

**Constraints**: never recompute a verdict (FR-009); a rendering failure never changes a run's
status (FR-011); no credentials in reports (FR-013); deterministic output for regeneration
(SC-006) apart from an explicit "generated at" line.

**Scale/Scope**: new package `resilience_tests/reporting/` (~6 modules, 3 templates), a root
`conftest.py` registering the plugin, small edits to `resilience_tests/test_scenarios.py` and
`pyproject.toml`, ~20 unit tests.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Check | Status |
|---|---|---|
| I. A Test Is Data | Reports read the run's recorded catalog fields (id, priority, accept) from results.json; no scenario knowledge in templates | PASS |
| II. Anything Unbuilt Refuses | Missing results.json / events -> the page and JUnit say "no result recorded", never omitted | PASS |
| III. Never a False Pass | Only status `passed` maps to a JUnit pass; NOT_MEASURED/NOT_APPLICABLE rendered as labels with reasons, never numbers; aborted/error -> `<error>`; tests for each path | PASS |
| IV. Framework Source of Truth | Pages show predicates exactly as cataloged (with citations in the scenario file); the report adds no thresholds | PASS |
| V. Adapter Seam | Reporting reads engine-neutral evidence only; no PostgreSQL knowledge | PASS |
| VI. Leave Target as Found | Reporting never touches the target | PASS (n/a) |
| VII. Evidence Integrity | Verdict shown as recorded; disclosures always rendered in full; evidence files linked | PASS |

No violations.

## Project Structure

### Documentation (this feature)

```text
specs/002-test-reports-html-junit/
├── spec.md, checklists/requirements.md
├── plan.md                    # this file
├── research.md                # Phase 0 decisions
├── data-model.md              # run view, timeline markers, report case, gate decision
├── quickstart.md              # how to produce and check the reports
└── contracts/
    ├── cli-and-pytest.md      # --report-dir option, regenerate CLI, gate CLI + exit codes
    └── junit-format.md        # exact JUnit XML shape and status mapping
```

### Source Code (repository root)

```text
conftest.py                               # NEW: pytest_plugins = ["resilience_tests.reporting.plugin"]
pyproject.toml                            # + Jinja2 dependency, template package data
resilience_tests/
├── test_scenarios.py                     # record properties; render report.html after each run
└── reporting/                            # NEW
    ├── __init__.py
    ├── evidence.py                       # load results.json + events.jsonl (tolerant of missing)
    ├── view.py                           # pure: evidence -> view model (rules, measures, timeline, series, svg)
    ├── html.py                           # Jinja2 rendering: run page, index, unit summary; CLI to regenerate
    ├── junit.py                          # scenario JUnit XML writer (failure/error/skipped mapping)
    ├── gate.py                           # P0 gate CLI: exit 0/1, prints summary
    ├── plugin.py                         # pytest plugin: --report-dir; collects cases; writes outputs
    └── templates/
        ├── run_report.html.j2
        ├── index.html.j2
        └── unit_summary.html.j2
tests/
└── test_reporting.py                     # NEW
README.md, CLAUDE.md                      # how to produce reports
```

**Structure Decision**: a new `reporting` package next to `analysis`; `analysis/report.py`
(results.json + summary.txt) stays as it is -- the HTML is an additional view of the same file.

## Complexity Tracking

Not applicable -- no constitution violations.
