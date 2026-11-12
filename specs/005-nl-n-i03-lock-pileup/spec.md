# Feature Specification: NL-N-I03 Lock Pile-up Behind a Schema Change

**Feature Branch**: `scenario3-Lock-pile-up-behind-a-schema-change` (spec directory `005-nl-n-i03-lock-pileup`)

**Created**: 2026-11-05

**Status**: Draft

**Input**: User description: "Implement scenario 3, the lock pile-up behind a schema change, by
extension: scenario id NL-N-I03; runs 3a (lock timeout, judged), 3b (no timeout, finding) and
3c (rewriting schema change, finding); use the harness table until the TPC-C workload exists;
session-level settings only."

**Framework position**: Framework §10.6 (NL-N, internal noisy neighbour) declares 8 rows and
none of them is a lock pile-up. This scenario is therefore **internal**: it carries an `I` in its
id (`NL-N-I03`), belongs to category NL-N, and is left out of the Framework reconciliation
checks (catalog checks 2 and 6). Its three runs are lettered variants: `NL-N-I03a`, `NL-N-I03b`,
`NL-N-I03c`.

## Clarifications

### Session 2026-11-05

- Q: Which table is contended? → A: A harness-owned table, `resilience.lock_target` (2,000,000 rows),
  kept between runs. A teammate is building a TPC-C workload for this scenario; until then half
  the marker workload reads and updates a row of this table.
- Q: Which runs are judged? → A: Only 3a. 3b and 3c are findings: their measures are reported,
  and only the proof that the fault landed and the standing safety gates are gated.
- Q: Server or session settings? → A: Session settings only, on the pile-up's own sessions. The
  server's configuration is observed and never written.
- Q: One id or three? → A: The id is NL-N-I03; each run is a lettered variant of it, because a
  catalog file is one run.

## User Scenarios & Testing *(mandatory)*

The "user" is the database reliability engineer certifying that schema changes cannot freeze an
application table for longer than the protection they rely on.

### User Story 1 - The lock timeout protects the table (Priority: P1)

A long read holds the table; a schema change with a 5 s lock timeout queues behind it, and every
new query on the table queues behind the schema change. The engineer needs proof that the
schema change gives up when its timeout fires, the queue clears within the timeout, and the
server neither crashes nor loses a committed transaction.

**Independent Test**: Run NL-N-I03a against the lab target; the verdict is pass only when the
schema change ended with the lock-timeout error, the freeze was at most 6 s, the server named
the schema change and its blocker, and nothing was lost or corrupted.

**Acceptance Scenarios**:

1. **Given** the workload at steady state, **When** the read holds the table and the schema
   change is issued with a 5 s lock timeout, **Then** the server shows the schema change waiting
   for the read and at least one workload session waiting for the schema change (the fault landed).
2. **Given** the pile-up, **When** 5 s pass, **Then** the schema change ends with SQLSTATE 55P03
   and the table users' queue clears.
3. **Given** the run, **When** it is validated, **Then** rpo_txn == 0, corruption_count == 0,
   structural_integrity_errors == 0 and no crash-restart line is in the server log.

### User Story 2 - What happens without a lock timeout (Priority: P2)

The same pile-up with no lock timeout: the freeze lasts as long as the blocker does (until the
hold ends). Reported, not judged: freeze length, longest table-user wait, throughput the other
work lost, write-prober stall.

**Independent Test**: Run NL-N-I03b; it passes when the pile-up formed and the safety gates hold;
the measures appear in the report.

### User Story 3 - A rewriting schema change (Priority: P2)

A schema change that rewrites the table holds its exclusive lock throughout, so its lock timeout
never fires. Reported, not judged: the freeze and its spread.

**Independent Test**: Run NL-N-I03c; it passes when the schema change was seen holding its lock
with sessions queued behind it and the safety gates hold.

### Edge Cases

- The schema change is never issued, or the chain is never seen within 3 s: `fault_confirmed`
  is false or NOT_MEASURED, and every pile-up measure is NOT_MEASURED -- never a pass.
- A schema change still running when the hold ends is cancelled (SQLSTATE 57014).
- A harness crash mid-run: the kill switch ends the tagged sessions and drops the probe column.
- Dropped columns keep their slot: the table is rebuilt once its slots pass 1,000.
- The workload driver abandons a transaction after 10 s: counted as indeterminate, never lost.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The catalog MUST accept internal scenario ids (`-I`, optionally lettered) and keep
  them out of the Framework reconciliation checks; a lettered variant MUST be refused on a
  Framework id, and an internal scenario MUST NOT be environment-sensitive.
- **FR-002**: The scenario MUST declare the pile-up (`fault.lock_pileup`: blocker, schema change,
  its lock timeout, its delay) and the workload share that uses the contended table
  (`workload.contended_share`); malformed combinations MUST be refused at load.
- **FR-003**: The harness MUST prove the pile-up formed from the server itself before any verdict.
- **FR-004**: The hold MUST end when the queue clears or at `fault.duration`, whichever is first;
  T1 is the moment the schema change ended.
- **FR-005**: The harness MUST measure: whether the lock timeout fired, whether the schema change
  completed, freeze length, largest queue, blocker visibility, longest table-user wait, the other
  work's throughput lost over the minute after the schema change, the write prober's longest
  stall, and crash-restart lines in the server log.
- **FR-006**: The harness MUST write only session settings on its own sessions and MUST leave
  the target as found: tagged sessions ended, probe columns dropped, verified.
- **FR-007**: Existing scenarios MUST behave exactly as before.

### Key Entities

- **Contended table**: harness-owned, kept between runs, rebuilt when its column slots run low.
- **Pile-up sessions**: the read (`resilience-lock-reader`) and the schema change (`resilience-lock-ddl`).
- **Groups**: table users and others, each sampled every second.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: NL-N-I03a passes on the lab target with the lock timeout fired and freeze <= 6 s.
- **SC-002**: NL-N-I03b and NL-N-I03c complete with fault_confirmed true and all measures reported.
- **SC-003**: After every run no tagged session and no probe column remains on the target.
- **SC-004**: The full unit suite and catalog checks pass with no change to existing results.

## Assumptions

- The freeze bound of 6 s (the 5 s timeout plus 1 s margin) is NEEDS SIGN-OFF.
- oltp_tps_ratio / oltp_p99_ratio (Framework §10.6 contention acceptance) are reported, not
  gated: the hold contains the queue by design -- NEEDS SIGN-OFF.
