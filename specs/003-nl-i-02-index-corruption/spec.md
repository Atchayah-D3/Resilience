# Feature Specification: NL-I-02 Index Corruption

**Feature Branch**: `atchayah` (spec directory `003-nl-i-02-index-corruption`)

**Created**: 2026-10-12

**Status**: Draft

**Input**: User description: "Proceed to implement NL-I-02" -- Framework §10.4 row NL-I-02,
built on the NL-I-01 data-corruption fault (Arch §5 "targeted byte writes ... must be
precise"; Arch §10.1 names pg_amcheck for NL-I-01/02/07).

**Framework row (Framework §10.4, NL-I-02)**

| Fault injection | Acceptance criterion | Priority |
|---|---|---|
| Corrupt a btree file | amcheck detects it; REINDEX restores; no wrong results returned first | P0 |

## Clarifications

### Session 2026-10-12

- Q: How is the btree file corrupted -- a byte flip the page checksum exposes, or a
  checksum-valid logical change (keys out of order, checksum recomputed)? → A: Byte flip, the
  same precise fault as NL-I-01, aimed at a leaf page of a harness-owned btree index. With
  `data_checksums=on` (required, Framework §16.2) the damage is exposed on read; amcheck,
  REINDEX and the "no wrong results" rule are then measured on it.

## User Scenarios & Testing *(mandatory)*

The "user" is the database reliability engineer certifying a ShaktiDB / DistDB node, and the
reader of the resulting report.

### User Story 1 - A damaged index is detected and never answers wrongly (Priority: P1)

One byte in one leaf page of a harness-owned btree index is corrupted while the database is
cleanly shut down; the damage is proven at exactly that block before the database starts again.
The harness then queries the table *through that index*. Each query must either fail with the
engine's data-corruption error or return exactly the rows that were written -- never a
different set. The engine's structural checker, run on the index, must report it damaged.

**Why this priority**: it is the Framework's acceptance criterion. An index that silently
returns the wrong rows is undetectable from the application and corrupts every decision made
on its results.

**Independent Test**: run NL-I-02 on the lab target; the report shows the injected block, what
each index read returned (error or rows, and whether they matched), and the checker's finding.

**Acceptance Scenarios**:

1. **Given** a leaf page of the index damaged on disk, **When** the table is read through the
   index, **Then** the read fails with the data-corruption error or returns exactly the rows
   written; any other row set counts as a wrong result and fails the run.
2. **Given** a read that the engine did not actually serve from the index (plan used no index
   scan), **When** results are judged, **Then** "no wrong results" is NOT_MEASURED, never 0.
3. **Given** the damaged index, **When** the structural checker runs on it, **Then** it reports
   the damage; a clean report fails, an inconclusive one is NOT_MEASURED.

### User Story 2 - REINDEX restores the index (Priority: P1)

After detection, the harness runs the documented recovery path, REINDEX, on the damaged
index, and proves the rebuilt index is sound: the rebuild succeeds, a read through the index
returns exactly the rows written, and the structural checker -- including the heap-to-index
cross-check -- finds nothing on the table and its index.

**Why this priority**: the second clause of the Framework criterion.

**Independent Test**: same run; the report shows the REINDEX outcome and duration, the
post-rebuild read, and the post-rebuild check.

**Acceptance Scenarios**:

1. **Given** the detected damage, **When** REINDEX runs, **Then** `reindex_restores` is true only
   if the rebuild succeeded AND an index read returns exactly the rows written AND the checker
   is clean on the table with its index.
2. **Given** a REINDEX that errors, or a post-rebuild read that errors or returns other rows, or
   a post-rebuild check that still finds damage, **Then** `reindex_restores` is false.

### User Story 3 - The fault is real, precise, and nothing else is harmed (Priority: P2)

The fault must be shown to have landed in a leaf page of that index before T0, and the rest of
the database must stay intact.

**Acceptance Scenarios**:

1. **Given** the chosen block, **When** the database is cleanly stopped, **Then** the page is
   confirmed to be a btree leaf page before any byte is written; otherwise the run is aborted.
2. **Given** the flip, **When** offline checksum verification runs, **Then** it reports exactly
   one failure, at that block of that index file; otherwise the run is aborted.
3. **Given** the run, **Then** the server log names no other damaged relation, the
   whole-database check (excluding only the harness's corruption table and its index) is clean,
   and no acknowledged commit was lost across the clean stop and start.

### Edge Cases

- The planner chooses a sequential scan: the read does not count as an index read
  (NOT_MEASURED), it is not accepted as "no wrong results".
- A read fails with a connection or other non-corruption error: neither a detection nor a
  correct answer (NOT_MEASURED).
- An unclean stop: aborted before any write (crash recovery could restore the page).
- The page is not a leaf page (layout or byte order differs): aborted before any write.
- The harness dies after the flip: the kill switch drops the corruption table (DROP never
  reads its pages), taking the index with it.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The catalog MUST choose what is corrupted with a scenario field
  (`fault.relation_kind`: `heap` | `btree_index`), never by scenario ID; NL-I-01 behaviour is
  unchanged (`heap`, the default).
- **FR-002**: For `btree_index` the harness MUST create its own table and btree index (schema
  `resilience`), choose a populated leaf page in the index file, and flip one byte in it with
  the database cleanly shut down.
- **FR-003**: Before writing, the driver MUST confirm the page is a btree leaf page from its
  on-disk bytes; after writing, offline checksum verification MUST report exactly one failure
  at that block of that file. Otherwise FaultNotLanded (run aborted, still reverted).
- **FR-004**: Validate MUST read the table through the index (index scan forced, and the plan
  confirmed to use that index) on fresh connections, recording per attempt the error code and
  location, or the rows returned and whether they equal the rows written.
- **FR-005**: `wrong_results_returned` = number of index reads that returned a row set other
  than the one written; NOT_MEASURED when any read neither used the index nor failed with the
  corruption error.
- **FR-006**: `amcheck_detects_target` MUST come from the structural checker run on the index.
- **FR-007**: The harness MUST run REINDEX on the damaged index and set `reindex_restores` true
  only when the rebuild succeeded, a forced index read returned exactly the rows written, and
  the checker (heap-to-index cross-check on) ran clean on the table and index; NOT_MEASURED
  when a step could not say either way.
- **FR-008**: `corruption_outside_target` (server log), `structural_integrity_errors`
  (Framework §16.3) and `rpo_txn` MUST be measured exactly as for NL-I-01.
- **FR-009**: Every new measure MUST have a unit test proving its fail path, failing for the
  reason under test (Constitution III).
- **FR-010**: Revert and the kill switch MUST drop the harness table (and with it the index);
  nothing else.

### Key Entities

- **Corruption target**: relation (the index), owning table, file path, filenode, block, byte,
  leaf-page check (byte offset, mask, expected value), rows written.
- **Index read**: per attempt -- index used (plan), error code, block and file named, rows
  returned, whether they matched.
- **Rebuild**: REINDEX outcome and duration, post-rebuild read, post-rebuild check.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: On the lab target with checksums on, NL-I-02 passes with
  `wrong_results_returned == 0`, `amcheck_detects_target == true`, `reindex_restores == true`,
  `corruption_outside_target == 0`, `structural_integrity_errors == 0`, `rpo_txn == 0`.
- **SC-002**: Each accept rule has a unit test in which a fake engine violates only that rule
  and the run fails (or is NOT_MEASURED) on that rule.
- **SC-003**: NL-I-01 unit tests and its live run still pass (shared driver and adapter code).

## Assumptions

- Steady-state and workload numbers are not given in the Framework row; NL-I-01's values are
  reused. NEEDS SIGN-OFF.
- The target is little-endian (x86-64); the leaf-page check fails closed (abort) otherwise.
- The `amcheck` extension is installed by the operator (as for every scenario, Framework §16.3).
- `corruption_detected_on_read`, `detection_identifies_block`, `checksum_failure_reported`,
  `reindex_duration_s` and `corruption_count` are reported, not gated: the Framework row does
  not require them for NL-I-02.
