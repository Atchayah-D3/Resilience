# Feature Specification: NL-N-I01 Noisy Analytics Against Priority Customer Work

**Feature Branch**: `origin/resilience-scenario_3` (spec directory `006-nl-n-i01-noisy-analytics`)

**Created**: 2026-11-15

**Status**: Draft

**Input**: User description: "Build noisy-neighbour scenario 1 as in the 3 November document
(`docs/Noisy Neighbour/Resilience Scenario 1 Noisy analytics vs priority OLTP.md`): big reports
from an analytics role run for 5 minutes on the same TPC-C tables the customer workload uses; the
customers must keep most of their speed and cached data, the role's own limits must stop its
oversized reports, and service must be back to normal within 60 s. One run, judged pass or fail.
Sizes are calculated from the target at every run, never fixed."

**Framework position**: the scenario follows the Framework's NL-N-07 row (noisy analytics: TPS
at least 70% and p99 at most 2x of the baseline, cache hit ratio at least 80% of the baseline,
resource limits enforced if configured; Framework §10.6). The team numbers the new noisy-neighbour
scenarios by their own list, so this one is **internal**: id `NL-N-I01` (category NL-N, left out of
the Framework reconciliation checks), one run, no lettered variants.

## Clarifications

### Session 2026-11-15

- Q: One run or three (1a scan, 1b cache, 1c limits)? → A: One run, as the 3 November document:
  full-table reports, one index report, and the two limit tests as the first query of two
  full-table sessions. No 5th-login test (scenario 2 tests it).
- Q: Which target? → A: The new lab VM (`e2-u22-3`); `shared_buffers` raised to 1 GB by the operator.
- Q: Fixed sizes or calculated? → A: Calculated at every run from the target (memory cache, CPU
  cores, the role's limits), recorded in the results, and checked by measurement (baseline hit
  ratio at least 99%).
- Q: The size of `order_line` against the memory cache? → A: 8x (the harder of the document's two).
- Q: The customers' rate (about 60% of the measured maximum)? → A: Taken at run time from the
  maximum stored by the capacity test for the same layout; a fixed `rate_tps` remains possible.
  NEEDS SIGN-OFF (Framework §7.5 asks for a declared rate).
- Q: The customers' login? → A: The harness role (it has no limits; only the report role is limited).
- Q: Back to normal? → A: As the document: TPS at least 95%, p99 at most 1.5x, 60 s straight,
  within 60 s of the reports stopping.
- Q: Warning band? → A: None; pass or fail only.
- Q: Resource groups? → A: ShaktiDB has none (settings, extensions, SQL keywords and system tables
  checked on 2026-11-15); the role limits are tested.

## User Scenarios & Testing *(mandatory)*

The "user" is the database reliability engineer certifying that reports run next to the customer
workload cannot starve it, and that the report role's limits stop only the reports.

### User Story 1 - Customers keep their service while big reports run (Priority: P1)

Report sessions of the analytics role read the whole of `order_line` with parallel helpers, sort
large results to temporary files, and one session pulls cold pages of the extra warehouses into
the memory cache. The customers keep running at their planned rate.

**Independent Test**: Run NL-N-I01 on the lab target. Pass only when the reports were seen
running with their helpers at minute 1, the customers kept at least 70% of their TPS, their p99
stayed at most 2x, New-Order, Payment and Order-Status kept 90% of their transactions within 5 s,
and the customers' cache hit ratio stayed at least 80% of the baseline.

**Acceptance Scenarios**:

1. **Given** the customers at steady state with a baseline cache hit ratio of at least 99%,
   **When** the report sessions start, **Then** at minute 1 every report session is active and
   at least one helper process works for a report (the fault landed).
2. **Given** the reports running for 5 minutes, **When** the hold is measured, **Then** the
   customers' TPS ratio, p99 ratio, worst p90 per type and cache hit ratio are within the limits.
3. **Given** a report, **When** it runs, **Then** it never has more helpers than the role allows.

### User Story 2 - The report role's limits stop oversized reports, and only them (Priority: P1)

**Independent Test**: In the same run, limit test 1 (a sort that needs more temporary files than
the role's `temp_file_limit`) ends with SQLSTATE 53400; limit test 2 (a query that runs longer than
the role's `statement_timeout`) ends with SQLSTATE 57014 within the timeout plus 1 s; no customer
transaction fails with 53300, 53400 or 57014.

### User Story 3 - Back to normal, nothing lost, nothing left (Priority: P1)

**Independent Test**: After the reports stop, TPS at least 95% and p99 at most 1.5x of the
baseline for 60 s straight within 60 s; rpo_txn, corruption, structural errors, partial commits,
consistency violations, rolled-back markers kept: all 0; no report session and no temporary file
left; the recorded settings unchanged.

### Edge Cases

- The memory cache is too small for even one active warehouse: the measured baseline hit ratio
  stays below 99% and the run stops as invalid before the fault (never a pass).
- The role's connection limit is below the number of report sessions the target needs, or the
  sessions with their helpers cannot occupy every core: invalid, stopped at init or before the fault.
- `statement_timeout` is not shorter than the hold minus a margin: limit test 2 cannot finish
  inside the fault; the run is refused at init.
- No maximum was measured for the current layout: the run is refused at init with the command to
  measure it.
- A role limit is not configured: the check that needs it is not measured, which fails the run.
- A limit test is not stopped: it is cancelled at the end of the hold and its check fails.
- The harness crashes during the hold: the kill switch ends every report session by its tag.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The scenario file states rules, not lab numbers: warehouses and active warehouses
  "by cache", the customers' rate as a share of the stored maximum, the report sessions "by cores".
- **FR-002**: At every run the harness reads the memory cache and CPU cores, calculates
  warehouses (`order_line` at least 8x the cache), active warehouses (their data at most half the
  cache, at least 1), report sessions (ceil(cores / 3) full-table sessions + 1 index session), and
  records them in the results and the report.
- **FR-003**: The customers use only the active warehouses (home and remote choices); the reports
  read every warehouse; the index report reads only the extra ones.
- **FR-004**: The customers' rate is the stated share of the maximum stored by the capacity test
  for the same clerks, warehouses and active warehouses; without one the run is refused. The
  capacity test measures with the same layout.
- **FR-005**: The report sessions log in as the environment's analytics login, tagged
  `application_name = 'resilience-analytics'`, and run their queries back to back for the hold;
  full-table reports run as `EXPLAIN (ANALYZE, BUFFERS, TIMING OFF)` and their plan figures are kept.
- **FR-006**: Limit test 1 and limit test 2 are the first query of the first two full-table sessions.
- **FR-007**: The harness reads the role's limits (connection limit, helpers per query,
  `work_mem`, `temp_file_limit`, `statement_timeout`) at init and again in validate; it never
  writes them.
- **FR-008**: Every second during the hold the harness samples the report sessions and their
  helpers (by `leader_pid`).
- **FR-009**: The customers' cache hit ratio is measured on the TPC-C tables the reports never
  read, for the baseline window and the hold window.
- **FR-010**: At the end of the hold, every report query still running is cancelled and the
  sessions are closed; the backstop ends every tagged session on the server. Cleanup and the kill
  switch use the same tag.
- **FR-011**: Every check without its evidence is not measured, never 0 or true.

### Key Entities

- **Layout**: warehouses, active warehouses, memory cache, cores, report sessions; calculated per run.
- **Report session**: one connection of the analytics login, its queries and their outcomes.
- **Report query outcome**: kind, start, end, completed or SQLSTATE, plan figures.
- **Capacity record**: the maximum TPS measured for clerks, warehouses and active warehouses.

## Success Criteria *(mandatory)*

- **SC-001**: A live run on the lab target produces a verdict with every check measured.
- **SC-002**: Every new check has a unit test that fails for the right reason.
- **SC-003**: Unit tests and catalog checks pass; existing scenarios are unchanged.

## Assumptions

- The operator created the analytics login with its limits and access, its `pg_hba.conf` line and
  its `~/.pgpass` entry, and granted `pg_monitor` to the harness role (done 2026-11-15).
- All thresholds not from the Framework are NEEDS SIGN-OFF until the 3-run pilot.
