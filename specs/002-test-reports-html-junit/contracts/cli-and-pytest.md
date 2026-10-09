# Contract: pytest option and command-line tools

## pytest option (both suites)

| Option | Effect |
|---|---|
| `--report-dir DIR` | At session end write into DIR: scenario sessions -> `scenarios-junit.xml`, `index.html`, `gate.txt`; any session with other tests -> `unit-summary.html`. Absent -> no session reports (per-run `report.html` is still written). |

Unit-test JUnit uses pytest's built-in `--junitxml=DIR/unit-junit.xml`.

## Per-run page

Every scenario run writes `<run_dir>/report.html` right after the run. A rendering error is
printed and recorded as the test property `report_error`; the run's status is unchanged.

## Regenerate pages

```
python -m resilience_tests.reporting.html RUN_DIR [RUN_DIR ...] [--index OUT.html]
```
Writes `RUN_DIR/report.html` for each; with `--index`, also an index page over those runs.
Exit 0 if every page rendered, 1 otherwise (each failure printed).

## P0 gate

```
python -m resilience_tests.reporting.gate DIR/scenarios-junit.xml
```
Prints every non-pass (P0 first) and skipped case. Exit codes: 0 = no P0 failure/error;
1 = at least one P0 failure or error (aborted, error, no result count); 2 = the XML could not
be read.

## Jenkins (reference)

```groovy
sh '.venv/bin/pytest resilience_tests/test_scenarios.py --env e2-dedicated-vm --report-dir reports || true'
junit testResults: 'reports/scenarios-junit.xml', skipMarkingBuildUnstable: true
archiveArtifacts artifacts: 'reports/**'
sh '.venv/bin/python -m resilience_tests.reporting.gate reports/scenarios-junit.xml'
```
