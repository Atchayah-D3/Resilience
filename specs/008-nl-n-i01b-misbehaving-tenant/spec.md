# Feature Specification: NL-N-I01b Misbehaving Tenant (and NL-N-I01 renamed NL-N-I01a)

**Feature Branch**: `scenario1-attach-workload` (spec directory `008-nl-n-i01b-misbehaving-tenant`)

**Created**: 2026-11-26

**Status**: Draft

**Input**: User description: "Scenario 1b of Resilience_Testing_ShaktiDB_v2 (section 5.1,
scenario 1): NL-N-I01b, the misbehaving tenant. Rename NL-N-I01 to NL-N-I01a (behaviour
unchanged). NL-N-I01b runs the same 5-minute noisy-analytics fault as 1a against the same
customers, but every report session first overrides the role's defaults with plain session SET
commands (more helpers, a large work_mem, no statement_timeout), reads them back, and one session
tries to override temp_file_limit and records the refusal. The harness never writes the target's
configuration. A memory guard ends the reports early if the target runs short of memory. 1b is a
finding: pass/fail only on what must still hold, the rest recorded."

**Source document**: `Resilience_Testing_ShaktiDB_v2.docx`, section 5.1 (scenario 1): *"It runs
twice: 1a with well-behaved report sessions (judged pass or fail) and 1b with a misbehaving tenant
that overrides its own settings (recorded as a finding)."* Section 4: *"These role settings are
defaults, not enforcement ... Only two limits are hard caps that the role cannot override:
CONNECTION LIMIT, and temp_file_limit."* Section 9: *"Run 1b (a finding, not pass or fail). Record
the helpers per report, the temporary files written, how long limit test 2 ran, the customers' TPS
and p99, and their cache hit ratio. Limit test 1 must still stop with error 53400."*

**Framework position**: like 1a, it follows the Framework's NL-N-07 row (resource limits enforced
if configured; Framework §10.6). Internal IDs: `NL-N-I01a` (run 1a) and `NL-N-I01b` (run 1b), in the
style of `NL-N-I03a/b/c`.

**Not to be confused with** the 1a/1b/1c split discussed on 2026-11-15 (scan / cache / limits),
which spec 006 rejected in favour of one run. Here 1a and 1b are the v2 document's runs: the same
reports, well-behaved or misbehaving.

## Clarifications

### Session 2026-11-26

- Q: Who changes the settings in 1b? → A: The report sessions themselves (login `analytics_role`),
  with session `SET`, as the document says ("each analytics session first overrides the role's
  defaults"). The customers and the operator change nothing.
- Q: The document's "removed again at cleanup (ALTER ROLE ... RESET)"? → A: Left out. `SET` ends
  with the session, so nothing is left to reset; `ALTER ROLE ... RESET` would delete the operator's
  own limits and would write the target's configuration (constitution VI). The role's settings are
  read at init and at cleanup and must be unchanged.
- Q: work_mem in 1b? → A: 1 GB, as the document. The target has 15 GB, about 13 GB available
  (at rest; to be confirmed under load). Estimate: sorts use up to work_mem per process; hash steps
  up to work_mem x hash_mem_multiplier (2), and a parallel hash join may use that per participant,
  so one Q2 with 8 helpers is allowed up to about 18 GB. Real use is bounded by the data the step
  holds (Q2's hash side, `orders`, about 0.5 GB; Q3's sort about 3-4 GB), so the expected peak is
  about 9-11 GB -- but the allowance is above the memory available, so the guard, not the
  estimate, protects the machine.
- Q: How is 1b judged? → A: Pass/fail only on what must still hold (the fault landed, the overrides
  took effect, the hard limits held, no customer stopped by a limit, the role unchanged, data safe,
  cleanup done). The customers' figures, helpers, temp files and limit test 2's run time are
  recorded as findings without a pass rule. No change to the shared verdict, report or gate code.
- Q: IDs? → A: `NL-N-I01` renamed `NL-N-I01a`; the new run is `NL-N-I01b`.
- Q: Try to override temp_file_limit too? → A: Yes, in one session; the refusal is recorded and
  judged.
- Q: Does 1a change? → A: No. The harness only reads the role's limits in 1a; the operator set them.

### Review 2026-11-26 (accepted)

- The memory guard reads the target's available memory (MemAvailable, which counts reclaimable
  cache; vmstat's "free" does not) and swap activity, at least every 0.5 s, is armed before the
  first override, and triggers at 2 GB available (NEEDS SIGN-OFF) or on swap-out above a bound.
- A pilot of Q2, Q3 and limit test 1 with the 1b settings, watching memory, comes before the first
  real 1b run.
- A refused override is a finding ("enforced", with the database's error), not a failure. Only a
  missing answer makes the overrides check not measured (the run is invalid).
- The crash check (document check 12, standing rule 5: "must hold in every run") is a pass rule:
  no crash restart in the engine's log after the fault starts.
- The write prober's longest gap is recorded (standing rule 7: in a finding run, recorded only).
- 1a's validity rules apply to 1b unchanged.
- Limit test 1 keeps its one-process form under the 1b settings; the pilot confirms it still
  passes temp_file_limit with work_mem 1 GB.
- The temp_file_limit attempt is its own statement outside a transaction, is not counted as a
  report, and, if accepted, the summary says whether the login was granted that setting.

## User Scenarios & Testing *(mandatory)*

The "user" is the database reliability engineer who must tell the product team which of the report
login's limits a tenant can switch off by itself, and which ones the database really enforces.

### User Story 1 - Show which role limits a tenant can override (Priority: P1)

The report sessions run the same reports as in 1a, but each one first raises its own helper count,
memory per sort and time limit with plain session commands. The run records what the database
allowed.

**Why this priority**: it is the purpose of 1b and the evidence for the resource-group decision
(document, open point 6). Without it, 1a's helper and timeout checks only show that defaults were
applied, not that anything is enforced.

**Independent Test**: Run NL-N-I01b on the lab target. The run is valid only if every report
session has an answer from the database for every override: accepted and read back, or refused
with an error. The summary then lists, from the database's own answers, the settings the login
was allowed to change ("overridden"), the ones it was refused ("enforced"), and the most helpers
any report used.

**Acceptance Scenarios**:

1. **Given** the report login with the operator's limits, **When** each report session runs its
   override commands, **Then** each session's settings read back as raised, and the run records
   this per session.
2. **Given** a report session whose override the database refuses, **When** the run is judged,
   **Then** that setting is recorded as "enforced" with the database's error (a finding), and the
   run is judged on its other rules.
3. **Given** a report session for which the harness got no answer for an override (the command or
   the read-back failed to run), **When** the run is judged, **Then** the overrides check is not
   measured and the run cannot pass.
4. **Given** the reports running with raised helpers, **When** the hold is sampled, **Then** the
   most helpers per report and the total helpers are recorded as findings (no limit applied).

### User Story 2 - Prove the hard limits still hold (Priority: P1)

**Why this priority**: the document requires limit test 1 to stop in 1b too, and the result is
only trustworthy if the database itself refuses to lift the temporary-file limit.

**Independent Test**: In the same run, one session tries to lift `temp_file_limit` and is refused
with SQLSTATE 42501; limit test 1 ends with SQLSTATE 53400; no customer transaction fails with
53300, 53400 or 57014.

**Acceptance Scenarios**:

1. **Given** a report session, **When** it tries to lift its temporary-file limit (one statement
   on its own, outside a transaction, before its reports, not counted as a report), **Then** the
   database refuses with 42501, the run records the refusal, and the session goes on with its
   reports.
2. **Given** limit test 1 in 1b, **When** it runs, **Then** it runs as one process (no helpers,
   for that one transaction, although the session raised its helpers) and ends with 53400.
3. **Given** limit test 2 in 1b (the session turned its time limit off), **When** the fault ends,
   **Then** the harness cancels it and records how long it ran; it is not judged.

### User Story 3 - Leave the target as found, and protect the shared machine (Priority: P1)

**Why this priority**: the target is shared (a second cluster and other software run on it), and
the harness must never write the operator's configuration.

**Independent Test**: The role's settings read at cleanup equal those read at init; the server's
settings are unchanged; no report session and no temporary file is left; if available memory on
the target falls below the guard level, or swap-out goes above its bound, during the hold, the
reports are ended early and the summary says so; no crash restart is seen in the engine's log.

**Acceptance Scenarios**:

1. **Given** the run ends (normally or early), **When** the role's settings are read again,
   **Then** they equal the ones read at init.
2. **Given** available memory on the target falls below the guard level, or swap-out goes above
   its bound, **When** the guard triggers, **Then** the reports end within a few seconds, the reason and the lowest memory seen
   are recorded, and the run is still judged on the rules that must hold.
3. **Given** the run, **When** it is judged, **Then** lost commits, damaged data, partial commits,
   consistency violations and rolled-back markers kept are all 0, as in every scenario.
4. **Given** a server process is killed during the hold (for example by the system for lack of
   memory) and the server restarts every session, **When** the run is judged, **Then** the crash
   check fails (document check 12).

### User Story 4 - NL-N-I01 becomes NL-N-I01a, unchanged (Priority: P2)

**Why this priority**: two runs need two names; the existing run must keep working exactly as now.

**Independent Test**: A live run of NL-N-I01a gives the same checks and the same kind of results
as NL-N-I01 run 803dfa.

**Acceptance Scenarios**:

1. **Given** the renamed scenario, **When** it runs, **Then** its fault, rules and summary are the
   same as before the rename; only the ID differs.

### Edge Cases

- A report session's override is refused by the database: recorded as "enforced" with the error
  (a finding). An override accepted but read back with another value: recorded as "enforced" with
  the value read back. No answer at all: the overrides check is not measured (never a pass).
- The planner gives a report fewer than 8 helpers, or the server's total helper cap (8) is shared
  between sessions: recorded as found; it is a finding, not an error.
- Limit test 2 is still running when the fault ends (expected): cancelled by the harness, its run
  time recorded.
- Limit test 1 is not stopped with 53400: its check fails.
- The temp_file_limit override is accepted: its check fails, and the summary says whether the
  login was granted the right to set it (a grant by the operator) or the limit is not a hard cap
  on this target -- two different findings.
- Available memory cannot be read, or the guard stops receiving readings: before the fault the
  run is refused; during the hold the reports are ended (never run unguarded).
- The guard ends the reports early: the hold is shorter than planned; the findings say so and give
  the hold's actual length.
- The database restarts or a report process is killed by the system: the crash check fails;
  the customers' outage figures and the restart lines are recorded.
- The harness crashes during the hold: the kill switch ends every report session by its tag, as
  in 1a; nothing on the role needs reverting.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The scenario `NL-N-I01` is renamed `NL-N-I01a`; its behaviour, rules and summary do
  not change.
- **FR-002**: A new scenario `NL-N-I01b` uses the same workload, layout, report sessions, reports,
  limit tests, baseline and 5-minute hold as `NL-N-I01a`.
- **FR-003**: The scenario file declares the session overrides (the three settings and their
  values) and whether one session tries to lift the temporary-file limit; nothing about 1b is
  decided from the scenario ID.
- **FR-004**: Each report session in 1b runs the declared overrides right after it logs in, before
  its first report, reads each setting back, and the run records the values per session.
- **FR-005**: Per report session and setting, the run records the database's answer: "overridden"
  (accepted and read back as asked), or "enforced" (refused, with the error; or accepted but read
  back with another value). `overrides_recorded` is true when every session has an answer for
  every declared setting; any missing answer makes it not measured. A refusal never fails the run.
- **FR-006**: One report session (not the one running limit test 1) tries to lift its
  temporary-file limit, as one statement on its own outside a transaction, before its reports; it
  is not counted as a report and a refusal does not end the session. `temp_limit_override_refused`
  is true only when the database refuses with 42501; not tried or no answer is not measured. If
  accepted, the run records whether the login holds the right to set that parameter.
- **FR-007**: Limit test 1 runs as in 1a: one process (no helpers for that one transaction, even
  though the session raised its helpers), and must end with 53400 (`temp_limit_enforced`).
- **FR-008**: Limit test 2 runs as in 1a; in 1b the run is not refused because the session's time
  limit is off, the harness cancels it at the end of the fault, and its run time is recorded
  (not judged).
- **FR-009**: The role's settings are read at init and again after the reports have ended (in
  validate); `role_settings_unchanged` is true only when both readings exist and are equal. The
  harness writes no role or server setting.
- **FR-010**: Memory guard. The harness reads the target's available memory (counting
  reclaimable cache) and swap-out at least every 0.5 s, from before the first override until the
  reports end. If available memory falls below 2 GB, or swap-out since the guard was armed goes
  above 512 MB (both NEEDS SIGN-OFF), or readings stop for more than 2 s, the reports are ended at
  once; the run records the reason, the time, the lowest available memory and the swap-out seen.
  If memory cannot be read before the fault, the run is refused.
- **FR-011**: 1b's pass rules: `fault_confirmed`, `overrides_recorded`, `temp_limit_enforced`,
  `temp_limit_override_refused`, `customer_limit_errors == 0`, `role_settings_unchanged`,
  `server_crash_restarts == 0`, the
  data-safety rules (`rpo_txn`, `corruption_count`, `structural_integrity_errors`,
  `partial_commits`, `tpcc_consistency_violations`, `aborted_markers_present`) and the cleanup rules
  (`analytics_sessions_left`, `temp_files_left`, `settings_unchanged`).
- **FR-012**: Recorded in 1b without a pass rule: helpers per report (most and total), temporary
  files written, limit test 2's run time, the customers' TPS ratio, p99 ratio, worst p90 and cache
  hit ratio, the lowest available memory and the swap-out, whether the guard ended the reports
  early, the hold's actual length, and the write prober's longest gap between successful writes.
- **FR-012a**: 1a's validity rules apply to 1b unchanged: the customers' baseline cache hit ratio at
  least 99%, the planned rate kept in the baseline, and the reports seen running at minute 1.
- **FR-012b**: Before the first real 1b run on a target, a pilot runs Q2, Q3 and limit test 1 with
  the 1b settings one at a time while memory is watched, and confirms limit test 1 still ends with
  53400 with work_mem 1 GB.
- **FR-013**: The summary and report of 1b show a "1b findings" block: the settings the login was
  allowed to change (with the values read back), the settings enforced (with the database's error
  or the value read back), the temp_file_limit attempt's answer, and the recorded figures of FR-012.
- **FR-014**: The shared verdict, report and gate code and the other scenarios are not changed.
- **FR-015**: Every check without its evidence is not measured, never 0 or true.

### Key Entities

- **Session override**: a setting name, the value asked for, the value read back, and the
  database's answer (accepted or the error), per report session.
- **Override attempt on a hard limit**: the setting, the value asked for, and the database's answer.
- **Memory guard record**: guard levels, lowest available memory and swap-out seen, readings
  received, whether, when and why it ended the reports.
- **Role settings snapshot**: the report login's settings at init and at cleanup.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: A live run of NL-N-I01b on the lab target ends with every pass rule measured, and a
  "1b findings" block that names each overridden setting with its value read back from the database.
- **SC-002**: The run shows from the database's own answers which limits held (temporary-file
  limit: refused and enforced) and which did not (helpers, memory per sort, time limit).
- **SC-003**: The target is left as found: the role's and the server's settings are the same before
  and after, with no session or temporary file left.
- **SC-004**: The shared machine is protected: available memory never stays below the guard level
  for more than 1 s during the hold, and no server crash restart happens.
- **SC-005**: A live run of NL-N-I01a gives the same checks as NL-N-I01 did (run 803dfa).
- **SC-006**: Every new check has a unit test that fails for the right reason; unit tests and
  catalog checks pass; the teammate's scenarios are unchanged.

## Assumptions

- The operator's report login is the one 1a uses (CONNECTION LIMIT 4, 2 helpers, work_mem 16MB,
  temp_file_limit 4GB, statement_timeout 2min), and it is not a superuser.
- The server allows 8 helpers in total (`max_parallel_workers = 8`), so "8 helpers per report" is
  asked for but the server's total cap applies; this is part of the finding.
- The memory estimate (peak about 9-11 GB against about 13 GB available at rest) is an estimate;
  the allowance can exceed the available memory, so the guard and the pilot cover it. The 1a runs
  swapped out up to about 220 MB with work_mem 16 MB, so swap-out is bounded, not forbidden.
- 1b runs in the same pytest session as 1a or on its own, attached to the common workload, with the
  cooldown and health check between scenarios.
- Earlier run folders keep the old ID `NL-N-I01`.
- The guard levels (2 GB available, 512 MB swap-out, 2 s without readings) and every threshold
  not from the Framework or the document are NEEDS SIGN-OFF.
- Not in scope: the median of 3 runs for 1a; the crash check and the write prober for 1a.
