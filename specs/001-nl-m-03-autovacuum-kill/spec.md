# Feature Specification: NL-M-03 Autovacuum Worker Killed

**Feature Branch**: `001-nl-m-03-autovacuum-kill`

**Created**: 2026-10-06

**Status**: Draft

**Input**: User description: "NL-M-03 Autovacuum worker killed (Framework §10.7, Tier 1, category
NL-M, priority P1). Framework row: fault injection 'kill -9 an autovacuum worker repeatedly';
acceptance 'A new worker spawns; no relation is left permanently unvacuumed'. Each kill must hit
an autovacuum worker confirmed running at that moment; SIGKILL of any postmaster child causes a
crash-restart of the whole instance; cumulative statistics are discarded by crash recovery."

**Framework row (Framework §10.7, NL-M-03)**

| Fault injection | Acceptance criterion | Priority |
|---|---|---|
| kill -9 an autovacuum worker repeatedly | A new worker spawns; no relation is left permanently unvacuumed | P1 |

## User Scenarios & Testing *(mandatory)*

The "user" is the database reliability engineer certifying a ShaktiDB / DistDB node, and the
reader of the resulting report.

### User Story 1 - Autovacuum survives repeated worker kills (Priority: P1)

Under a write load that creates dead rows, the harness waits until an autovacuum worker is
actually running, kills exactly that worker, waits for the instance to recover, and repeats.
After the last kill it verifies that autovacuum is still working: a new worker appears, and
every relation that needed vacuuming is vacuumed again within a bounded time.

**Why this priority**: it is the Framework's acceptance criterion for this row. A database
whose autovacuum silently stops after a worker dies accumulates bloat until transaction-ID
wraparound forces a protective shutdown (NL-M-01) -- an outage with no immediate symptom.

**Independent Test**: run NL-M-03 against the lab target; the report states, per kill, which
worker was hit, and after the run whether a new worker spawned and whether every eligible
relation was vacuumed again. Delivers the P1 verdict on its own.

**Acceptance Scenarios**:

1. **Given** a steady write load that leaves dead rows and an autovacuum worker confirmed
   running, **When** that worker is killed, **Then** the kill is recorded as landed only if the
   killed process was that autovacuum worker and it is gone afterwards.
2. **Given** all kills have landed and the instance has recovered, **When** the harness
   watches the instance, **Then** a new autovacuum worker is observed running.
3. **Given** the instance has recovered from the last kill, **When** the observation window
   ends, **Then** every relation that was eligible for autovacuum has been vacuumed after the
   last kill; any relation that was not is named in the report and the run fails.

---

### User Story 2 - The crash each kill causes is measured honestly (Priority: P2)

Killing any server child process makes the instance terminate every session and run crash
recovery. The run therefore measures each kill the way a crash is measured: committed
transactions survive, the instance restarts without intervention, and nothing is corrupted.

**Why this priority**: the Framework row is about vacuum, but each kill is a real crash. A
run that reports "vacuum resumed" while losing committed data, or while needing a human to
restart the instance, would be a false pass.

**Independent Test**: from the same run, the report shows transactions lost, whether recovery
was unattended, and the structural-integrity result.

**Acceptance Scenarios**:

1. **Given** marker-instrumented load during the kills, **When** the run is evaluated,
   **Then** acknowledged transactions lost and the structural-integrity error count are
   reported, and the structural-integrity count must be zero.
2. **Given** each kill, **When** the instance restarts, **Then** it restarts with no harness
   or operator action on the database.

---

### User Story 3 - A kill that missed can never pass (Priority: P3)

If no autovacuum worker appears within the allowed time, or the worker exits before the kill
lands, that kill did not test anything. The run must say so and must not count it.

**Why this priority**: required by the constitution (Principle III). Autovacuum workers are
short-lived; a naive kill frequently hits nothing.

**Independent Test**: force a situation with no autovacuum activity (e.g. no dead rows);
the run aborts with the reason instead of producing a verdict.

**Acceptance Scenarios**:

1. **Given** no autovacuum worker appears within the wait bound, **When** the harness would
   inject, **Then** the run aborts before the kill, naming the reason.
2. **Given** the targeted worker exited between being observed and being killed, **When** the
   kill is confirmed, **Then** the cycle is reported as not landed and does not count toward
   the required number of kills.

### Edge Cases

- The worker found is vacuuming a system catalog rather than the workload's tables: it still
  counts as an autovacuum worker; the relation it was working on is recorded.
- Two or more workers are running at once: exactly one is killed per cycle; the others are
  recorded.
- An anti-wraparound ("to prevent wraparound") autovacuum is the one killed: recorded as such;
  it must also resume.
- Autovacuum is disabled in the deployment's configuration: no worker can ever appear; the run
  refuses with that reason rather than reporting a vacuum failure (configuration is observed,
  never changed).
- The instance does not come back after a kill within the recovery bound: the run fails on
  unattended recovery; remaining kills are not attempted.
- Crash recovery discards the activity statistics: "vacuumed after the last kill" must be
  proven from evidence that survives a crash or is collected after it, never from counters a
  kill resets.
- The repeated restarts trip the service manager's own restart limit: refused before the first
  kill (as for NL-C-05), not discovered mid-run.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The scenario MUST be defined as catalog data (fault, workload, measures, accept
  predicates) with no host, tool or driver named, and MUST cite Framework §10.7 for its fault
  and acceptance (Constitution I, IV).
- **FR-002**: Before each kill, the harness MUST observe, from the database itself, an
  autovacuum worker that is running, and record its identity, the relation it is working on,
  and whether it is an anti-wraparound vacuum.
- **FR-003**: Each kill MUST target exactly that worker process (not the postmaster, not a
  client backend, not another worker) with an immediate, uncatchable kill, and MUST be
  confirmed landed only if that process was gone afterwards.
- **FR-004**: The run MUST perform 5 landed kills, waiting for unattended recovery between
  them; a kill that did not land MUST NOT count. The Framework says "repeatedly" without a
  count, so 5 is marked NEEDS SIGN-OFF in the scenario file.
- **FR-005**: If no autovacuum worker is observed within a bounded wait, the run MUST abort
  before the kill with the reason (Constitution II, III).
- **FR-006**: The workload MUST create dead rows continuously, so that autovacuum has real
  work before, between and after the kills.
- **FR-007**: After the last kill, the run MUST report whether a new autovacuum worker was
  observed running (Framework acceptance: "a new worker spawns").
- **FR-008**: After the last kill, the run MUST determine, for every relation eligible for
  autovacuum, whether it was vacuumed after the last kill, within an observation bound derived from
  the deployment's own observed autovacuum settings -- three autovacuum cycles (3 x the observed
  naptime) plus the time the vacuums themselves take -- never a harness-chosen number. It MUST
  use evidence that is not erased by crash recovery and MUST report each relation that was not
  vacuumed within the bound.
- **FR-009**: The run MUST measure each kill as a crash: acknowledged transactions lost,
  unattended restart, time to first write, structural-integrity errors. It MUST gate
  structural-integrity errors at zero (Framework §16.3) and MUST gate zero lost acknowledged
  transactions and unattended restart, as for every crash in Framework §10.2 (each kill is a
  crash-restart of the whole instance).
- **FR-010**: The harness MUST NOT change the deployment's configuration (autovacuum settings
  included); the settings that decide the outcome MUST be observed and disclosed in the report.
- **FR-011**: The harness MUST refuse before the first kill if the service manager's restart
  limit would be exceeded by the planned kill cadence.
- **FR-012**: Each new measure MUST have a fail path proven by a unit test that fails for the
  reason under test (Constitution III).
- **FR-013**: The scenario MUST leave the target as found: no harness-held session or object
  remains, and all faults are reverted or confirmed recovered (Constitution VI).

### Key Entities

- **Kill cycle**: one attempt -- the worker observed (identity, relation, anti-wraparound
  flag, time), whether the kill landed, recovery time, and whether it counts.
- **Eligible relation**: a relation whose dead rows exceed the deployment's own autovacuum
  threshold during the run; the set autovacuum is expected to process.
- **Vacuum evidence**: for each eligible relation, whether and when it was vacuumed after the
  last kill, from evidence that survives or post-dates crash recovery.
- **Observed configuration**: autovacuum settings read (never written) and disclosed.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100% of counted kills are proven to have hit a live autovacuum worker (zero
  kills of the wrong process, zero unproven kills counted).
- **SC-002**: After the last kill, a new autovacuum worker is observed (Framework §10.7).
- **SC-003**: After the last kill, 0 eligible relations remain unvacuumed at the end of the
  observation bound (Framework §10.7), and every one that does is named in the report.
- **SC-004**: 0 structural-integrity errors after the run (Framework §16.3).
- **SC-007**: 0 acknowledged transactions lost across all kills, and the instance restarts
  without intervention after every kill (Framework §10.2 crash criteria).
- **SC-005**: A run in which no worker could be caught, or a kill missed, produces an abort or
  a failure with its reason -- never a pass (verified by unit tests for each path).
- **SC-006**: Every threshold in the scenario cites Framework §10.7 / §16.3 or is marked
  NEEDS SIGN-OFF in the scenario file.

## Assumptions

- Target: the existing lab environment (standalone ShaktiDB 17 on the E2 lab VM) via the
  existing harness; DistDB roles are covered by the same scenario later, through the adapter.
- PostgreSQL behaviour assumed: killing any server child with an uncatchable signal makes the
  postmaster end all sessions and run crash recovery; activity statistics (including last
  vacuum times and dead-row counts) are discarded by that recovery.
- Autovacuum is enabled in the deployment with its own settings; the harness does not tune
  them (the earlier practice of setting a 5 s naptime is no longer permitted). The observation
  bound is therefore derived from the observed settings, not from a harness-chosen value.
- The churn workload already used by NL-C-05 provides a bounded table whose dead rows cross
  the autovacuum threshold repeatedly.
- The marker workload and RPO arithmetic are reused unchanged.
- Clarified 2026-10-06: 5 landed kills (NEEDS SIGN-OFF); observation bound = 3 x observed
  naptime + vacuum time; crash outcomes (RPO, unattended restart) are gated.
- Out of scope: killing the autovacuum launcher (a different process), and autovacuum
  starvation under sustained overload (NL-M-04).
