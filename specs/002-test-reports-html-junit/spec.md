# Feature Specification: Test Reports (HTML and JUnit XML)

**Feature Branch**: `002-test-reports-html-junit`

**Created**: 2026-10-06

**Status**: Draft

**Input**: User description: "Generate an HTML report and a JUnit XML report for the harness's
tests, per the Architecture document (Arch §10.4 Outputs, §12 Technology Stack, §13 CI). Scope:
both test suites -- scenario runs and the harness's own unit tests."

**Architecture requirements (Arch §10.4 Outputs; §12; §13)**

| Artefact | Format | Consumer | Source |
|---|---|---|---|
| CI gate | JUnit XML | Jenkins -- "fails the build on any P0 failure" | Arch §10.4 |
| Run report | HTML (Jinja2), "with an annotated timeline" | Engineering | Arch §10.4, §12 (Allure rejected) |
| Per-commit gate | harness unit tests + catalog checks | CI | Arch §13 |

## User Scenarios & Testing *(mandatory)*

Readers: the CI system (Jenkins), the engineer investigating a run, and the reviewer of a
team's scenario results.

### User Story 1 - The CI gate understands scenario results (Priority: P1)

After a set of scenario runs, the CI system reads one machine-readable report and decides
the build: any P0 scenario that did not pass fails it. Each entry names the scenario, its
priority, the run, its outcome and where its evidence is; a failure says which acceptance
rules failed and why.

**Why this priority**: it is the Architecture's CI gate (Arch §10.4). Without it, a failed
P0 scenario can only be found by reading logs.

**Independent Test**: run two scenarios (one passing, one made to fail) with the report
enabled; the report holds two entries with the fields above, and the gate decision derived
from it is "fail".

**Acceptance Scenarios**:

1. **Given** a scenario run that passed, **When** the report is produced, **Then** its entry
   is a pass and carries scenario id, priority, run id and evidence location.
2. **Given** a run that failed its acceptance rules, **When** the report is produced, **Then**
   its entry is a failure whose message lists every failed rule with its measured value and
   reason (including "not measured: <why>").
3. **Given** a run that ended without a verdict (aborted or error), **When** the report is
   produced, **Then** its entry is never a pass, and states the abort reason.
4. **Given** a scenario skipped by the run plan (e.g. blocked by infrastructure) or a dry run,
   **When** the report is produced, **Then** its entry is a skip with the reason.
5. **Given** any non-pass entry for a P0 scenario, **When** the CI gate is evaluated, **Then**
   the build fails; a non-pass of a P1 or P2 scenario is reported as such (visible in the
   reports and listed in the gate summary) but does not fail the build (Arch §10.4).

---

### User Story 2 - An engineer understands one run from one page (Priority: P2)

For each scenario run, a self-contained HTML page shows the verdict, every acceptance rule
with its measured value, margin and reason, and an annotated timeline of what happened
(fault injected, outage seen, first write, return to service level, per-cycle events for
repeated scenarios), with throughput and latency over time, all disclosures, and links to
the run's evidence files. An index page lists all runs of one invocation.

**Why this priority**: the Architecture's run report for engineering (Arch §10.4). Today an
engineer reads raw JSON and event logs to understand a failure.

**Independent Test**: open the page of an existing run's evidence folder in a browser,
offline, and answer "what failed, when, and why" without opening any other file.

**Acceptance Scenarios**:

1. **Given** a finished run, **When** its report is opened, **Then** it shows the run status
   exactly as the run recorded it, and every acceptance rule with outcome, measured values,
   margin and reason.
2. **Given** a measure that was not measured or not applicable, **When** it is shown, **Then**
   it is labelled as such with its reason, and never shown as 0, true, or a pass.
3. **Given** a crash scenario, **When** the timeline is shown, **Then** fault time, outage
   start, first successful write and service-level recovery are marked on it; for repeated
   scenarios, every cycle's fault and recovery is marked.
4. **Given** an aborted run, **When** its report is opened, **Then** the abort reason and the
   phase it stopped in are shown prominently and no verdict is implied.
5. **Given** a run's evidence folder from an earlier date, **When** the report is generated
   again, **Then** the same page is produced from that evidence alone.

---

### User Story 3 - Unit-test results for the per-commit gate (Priority: P3)

The harness's own unit tests produce a machine-readable report for the per-commit CI gate,
and a simple readable summary page.

**Why this priority**: Arch §13's per-commit gate; simpler than scenario reports and largely
standard.

**Independent Test**: run the unit tests with reporting enabled; both files exist and the
counts match the run.

**Acceptance Scenarios**:

1. **Given** a unit-test run, **When** it finishes, **Then** a machine-readable report and a
   summary page exist with total, passed, failed, skipped counts and each failure's message.

### Edge Cases

- A run that crashed the harness and left no result file: the report states that no result
  was recorded for that run, never omits it silently.
- A run with an empty or partial event log: the timeline shows what exists and says what is
  missing.
- A very long run (hours, e.g. soak scenarios): the page stays readable (aggregated series).
- Disclosures: always shown in full, never collapsed out of sight.
- The run's verdict must never be recomputed or changed by the report: the report shows what
  the run recorded.
- Reporting must not change a run's outcome: a failure to render the HTML is reported, but
  the run's own result and the machine-readable report still stand.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: Scenario invocations MUST be able to produce one machine-readable CI report
  covering every scenario case of the invocation, including skipped ones.
- **FR-002**: Each scenario entry MUST carry: scenario id, name, priority, run id, run status
  (passed / failed / aborted / error / stopped before fault / skipped), and the evidence
  location.
- **FR-003**: A failed entry's message MUST list every non-passing acceptance rule with its
  measured values and reason; an aborted or error entry MUST give the reason and phase.
- **FR-004**: No run without a "passed" status may appear as a pass in any report.
- **FR-005**: The CI gate MUST fail on any non-pass of a P0 scenario (Arch §10.4). Non-pass
  outcomes of P1/P2 scenarios MUST be reported (in every report and in the gate summary) but
  MUST NOT fail the build.
- **FR-006**: An aborted or error run (no verdict) MUST count as a non-pass for the gate --
  nothing was certified -- and MUST be distinguishable from a verdict failure in the
  machine-readable report (an "error", not a "failure"), with its reason.
- **FR-007**: Each scenario run MUST produce a self-contained HTML page beside its evidence,
  readable offline, showing: header (scenario, run, environment, target, status), every
  acceptance rule (outcome, values, margin, reason), all measures (with NOT_MEASURED /
  NOT_APPLICABLE labelled and explained), the annotated timeline, throughput/latency over
  time, per-cycle details for repeated scenarios, the `during` operation evidence where
  present, all disclosures, and links to evidence files.
- **FR-008**: Each invocation MUST produce an index page listing its runs with status and a
  link to each run page.
- **FR-009**: Reports MUST be generated only from the run's recorded evidence and MUST NOT
  recompute or alter any verdict.
- **FR-010**: It MUST be possible to (re)generate a run's HTML page later from its evidence
  folder alone.
- **FR-011**: A failure to produce a report MUST be visible and MUST NOT change the run's
  status.
- **FR-012**: Unit-test runs MUST be able to produce a machine-readable report and a summary
  page (counts, failures with messages).
- **FR-013**: Generated reports MUST NOT be committed (they are outputs) and MUST NOT contain
  credentials.
- **FR-014**: Each new behaviour MUST have unit tests including the failing paths (aborted
  never shown as passed; NOT_MEASURED never shown as a value) (Constitution III).

### Key Entities

- **Scenario case**: one scenario x environment x target in an invocation; maps to one run or
  a skip.
- **Run record**: what the run wrote -- status, verdict per rule, measures, facts,
  disclosures, phases, events.
- **Timeline marker**: fault (T0), outage start, first write, SLO recovery, per-cycle events,
  phase boundaries.
- **CI gate decision**: pass/fail for the invocation, derived from entries and priorities.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100% of scenario cases in an invocation appear in the machine-readable report,
  including skips and runs that left no result.
- **SC-002**: 0 runs whose status is not "passed" appear as passes in any report (verified by
  tests for failed, aborted, error and skipped).
- **SC-003**: For any failed run, an engineer can name every failed rule and its reason from
  the run page alone, without opening another file.
- **SC-004**: A run page opens offline as a single file with all its content.
- **SC-005**: The CI gate fails whenever a P0 scenario does not pass (Arch §10.4).
- **SC-006**: Re-generating a page from an existing evidence folder produces the same content.

## Assumptions

- Run evidence already contains what the reports need: `results.json` (status, verdict per
  rule with values/margin/reason, measures, facts, disclosures, phases) and the event stream.
- The CI system is Jenkins (Arch §10.4), which reads JUnit XML natively.
- HTML templating uses Jinja2 (Arch §12); it is a new dependency the user installs on the
  harness VM.
- Reports go under `reports/` (already ignored by git) and the per-run page beside the run's
  evidence in the run directory.
- Clarified 2026-10-06: only P0 non-passes fail the build (P1/P2 reported, build stays green);
  aborted/error runs count as non-passes, reported as errors rather than verdict failures.
- Out of scope: trend store, evidence bundle upload (tar.zst to object storage) and the
  signed EQS qualification report -- other Arch §10.4 artefacts, later features.
