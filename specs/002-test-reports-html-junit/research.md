# Research: Test Reports (HTML and JUnit XML)

## R1. Who writes the scenario JUnit XML

- **Decision**: the harness writes its own `scenarios-junit.xml` (stdlib ElementTree) from the
  collected cases, at the end of the pytest session.
- **Rationale**: pytest's built-in `--junitxml` turns every exception in a test body into
  `<failure>`; it cannot emit `<error>` for an aborted run (spec FR-006) or attach the run's
  failed rules cleanly. Writing it ourselves gives exact control. `--junitxml` stays usable
  (unit tests use it as is).
- **Alternatives**: post-processing pytest's XML (fragile); pytest-html / Allure (Allure
  rejected in Arch §12; pytest-html shows pass/fail only, no timeline).

## R2. The P0 gate

- **Decision**: `python -m resilience_tests.reporting.gate <scenarios-junit.xml>` exits 1 if any
  P0 case is failure or error, 0 otherwise, and prints a table of every non-pass (P0 first).
  Skipped cases (blocked by infrastructure, dry runs, not selected) are listed but do not fail
  the gate: nothing ran, and a scenario blocked on missing infrastructure (NL-C-04) would
  otherwise keep every build red.
- **Rationale**: Jenkins' `junit` step marks a build UNSTABLE on ANY failure; the clarified rule
  is "only P0 fails the build" (spec FR-005). So Jenkins publishes the XML with
  `skipMarkingBuildUnstable: true` and the gate's exit code decides the result.
- **Alternatives**: custom Jenkins plugin (out of scope); pytest exit code (fails on P1 too).

## R3. Status mapping (applies to JUnit, index and gate)

| Run status | JUnit | Gate (P0) |
|---|---|---|
| passed | pass | pass |
| failed (verdict) | `<failure>` with every non-pass rule | fails |
| aborted / error (no verdict) | `<error>` with reason + phase | fails |
| stopped_before_fault (dry run) | `<skipped>` | not counted |
| skipped by run plan | `<skipped>` with reason | not counted |
| no results.json (harness died) | `<error>` "no result recorded" | fails |

## R4. Charts without JavaScript

- **Decision**: the view model builds SVG polylines in Python (scaled coordinates); the template
  inlines them. Two charts: TPS, and commit p99 + journal flush p99 (1/s samples), each with
  y-axis numbers and gridlines. Downsample by bucket (max for latencies, mean for TPS) to
  <= 1,200 points. Only two kinds of vertical line: fault (red) and recovered (green).
  (Revised 2026-10-07 after user review: a third "failed write probes per second" chart and the
  orange outage line made the page hard to read; the outage stays in the timeline table.)
- **Rationale**: offline single file (SC-004), no CDN, no JS dependency; long soaks stay small.

## R5. Timeline markers

From events: run_start; phase boundaries; each `injector/t0` (cycle, action); outage seen =
the first failed write probe whose attempt started at or after T0, at the moment it failed
(the harness's own rule, analysis/rto_decomposer.py); recovered = T0 + the run's own recovery
time (`cycle_recovered.recovery_s` per cycle, `measured.rto_first_write_s` for one fault); `t1`;
SLO recovered = T0 + `measured.rto_to_slo_s` when numeric. (Revised 2026-10-07: the report
first applied its own looser "first successful probe" rule and drew recovery when the harness
NOTICED it -- two green lines per fault that disagreed with the run's numbers. The report
never computes its own version of a measurement.)
Times are seconds from run start (monotonic clock of the event stream). Missing pieces are
listed as "not observed", never invented.

## R6. NOT_MEASURED / NOT_APPLICABLE

results.json stores them as the strings `NOT_MEASURED` / `NOT_APPLICABLE` (repr). The view
classifies those strings and renders them as labelled badges with the reason from
`facts.not_measured[name]`; they are never formatted as numbers or as pass.

## R7. When pages are produced

- Every scenario run: `test_scenarios.py` renders `report.html` into the run directory right
  after the run (inside try/except; failure is recorded as a test property and printed,
  never changes the status -- FR-011).
- With `--report-dir DIR`: at session end the plugin writes `DIR/scenarios-junit.xml`,
  `DIR/index.html`, `DIR/gate.txt` (scenario sessions) and `DIR/unit-summary.html` (other
  tests). Unit JUnit: `--junitxml=DIR/unit-junit.xml` (pytest built-in).
- Regenerate: `python -m resilience_tests.reporting.html <run_dir> [...]`.

## R8. Determinism and safety

Pages render from evidence only; a single "generated at" line is the only time-dependent text
(tests inject a fixed clock). Jinja2 autoescaping on (log lines and errors can contain
markup). Evidence links are relative within the run directory. No env/profile secrets exist
in results.json (passwords come from ~/.pgpass), so nothing sensitive is rendered.

## R9. Dependency

`Jinja2>=3.1` added to `pyproject.toml` dependencies; templates shipped as package data. The
user has installed Jinja2 3.1.6 in both VM venvs (2026-10-07).
