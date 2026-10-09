# Quickstart: produce and check the reports

Run on the harness VM in `~/resilience` (Jinja2 installed, see research R9).

## 1. Unit tests for the feature

```bash
.venv/bin/python -m pytest tests/test_reporting.py -q
```
Must cover: each status -> JUnit element (contracts/junit-format.md); aborted never a pass;
NOT_MEASURED rendered as a label; gate exit codes 0/1/2; skipped P0 not failing the gate;
missing results.json -> "no result recorded"; timeline markers from events; downsampling;
regeneration gives identical pages with a fixed clock.

## 2. Regenerate pages from existing evidence (no database touched)

```bash
.venv/bin/python -m resilience_tests.reporting.html \
  ~/resilience-runs/NL-M-03-20261006T075615Z-5bd741 \
  ~/resilience-runs/NL-M-03-20261006T103739Z-5e93fa \
  --index reports/index.html
```
Expected: two `report.html` files; the passing run shows 5 cycles on the timeline; the aborted
run shows the abort reason ("steady state did not hold ...") and no verdict.

## 3. Unit-test reports

```bash
.venv/bin/python -m pytest tests -q --report-dir reports --junitxml=reports/unit-junit.xml
```
Expected: `reports/unit-junit.xml` and `reports/unit-summary.html` with matching counts.

## 4. A scenario invocation with reports

```bash
.venv/bin/pytest resilience_tests/test_scenarios.py --env e2-dedicated-vm \
  --scenario NL-C-01 --scenario NL-C-04 --report-dir reports
.venv/bin/python -m resilience_tests.reporting.gate reports/scenarios-junit.xml; echo "gate exit: $?"
```
Expected: `scenarios-junit.xml` with NL-C-01 (pass, or error if the baseline aborts) and NL-C-04
(skipped: blocked by infrastructure); `index.html` linking the run page; gate exit 0 if NL-C-01
passed, 1 if it aborted or failed.

## 5. Open the pages

Copy `reports/` and the run's `report.html` to a workstation and open in a browser: single
files, no network needed.
