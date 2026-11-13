---
description: "Task list for NL-I-02 Index Corruption"
---

# Tasks: NL-I-02 Index Corruption

**Input**: Design documents from `specs/003-nl-i-02-index-corruption/`

**Tests**: REQUIRED -- spec FR-009 and Constitution III.

## Format: `[ID] [P?] [Story] Description`

## Phase 1: Setup

- [ ] T001 Starting point: on the harness VM, `pytest tests -q` and `python -m catalog.schema --partial` pass on the merged main before any change (record counts)

## Phase 2: Foundational

- [X] T002 Add `Fault.relation_kind: Literal["heap","btree_index"] | None` with a validator (only for `data_corruption`) in catalog/schema.py
- [X] T003 Add `FaultInjector.corruption_kind = "heap"` in resilience_tests/execution/injectors/base.py; orchestrator sets it from `fault.relation_kind` in resilience_tests/control/orchestrator.py
- [X] T004 Extend the adapter contract in resilience_tests/adapters/base.py: `prepare_corruption_target(kind)`, `read_index_target`, `rebuild_corruption_target`, `amcheck_relation(..., heapallindexed=False)`

## Phase 3: User Story 3 - the fault is real and precise (P2, prerequisite of US1/US2)

- [X] T005 [US3] PostgreSQL index target in resilience_tests/adapters/postgresql/adapter.py: table + `corruption_target_idx`, leaf block, byte, `page_check`, `table`, `integrity_exclusions`
- [X] T006 [US3] Driver in resilience_tests/execution/injectors/corruption.py: pass `corruption_kind`; verify `page_check` after the clean stop and before the write (FaultNotLanded otherwise); exclusions from the target; revert drops `table`
- [X] T007 [P] [US3] Tests: leaf check passes/aborts, unknown kind refused, revert drops the table, schema rejects `relation_kind` on other fault types (tests/test_index_corruption.py)

## Phase 4: User Story 1 - detected, never wrong (P1)

- [X] T008 [US1] `read_index_target` in the PostgreSQL adapter: fresh connection, scans forced, EXPLAIN must use the index, rows compared with 1..N
- [X] T009 [US1] Orchestrator: index read before the integrity check; `wrong_results_returned` in `_measure_corruption`
- [X] T010 [P] [US1] Tests: error naming the block; correct rows; wrong rows -> fail; plan without index -> NOT_MEASURED; connection error -> NOT_MEASURED; amcheck clean -> fail

## Phase 5: User Story 2 - REINDEX restores (P1)

- [X] T011 [US2] `rebuild_corruption_target` (REINDEX INDEX) in the PostgreSQL adapter; `amcheck_relation(heapallindexed=True)`
- [X] T012 [US2] Orchestrator: REINDEX, read-after, amcheck-after on the table -> `reindex_restores`, `reindex_duration_s`
- [X] T013 [P] [US2] Tests: REINDEX error, wrong read-after, amcheck-after finds damage -> fail; inconclusive -> NOT_MEASURED

## Phase 6: Catalog, report, polish

- [X] T014 catalog/NL/NL-I-02.yaml (Framework §10.4 row, accept rules, NEEDS SIGN-OFF steady state)
- [X] T015 [P] Report lines for index reads and REINDEX in resilience_tests/analysis/report.py
- [X] T016 Local (2026-10-12): tests/test_index_corruption.py 37 passed, tests/test_heap_corruption.py 36 passed; catalog checks pass; full suite 372 passed, 3 failed -- the same 3 fail on clean HEAD (local Elle/Java and a timing test), tests/test_reporting.py not collected locally (no Jinja2 in the local .venv). Mutation check: disabling each new rule makes its own tests fail
- [X] T017 Harness VM: full `pytest tests -q` and catalog checks (2026-10-13, ~/Resilience on the harness VM: 412 passed, 2 aborted on the slow driver journal flush (CLAUDE.md Gotchas) -- test_nl_m_05_fails_closed_on_data_loss and test_nlc03_partial_rows_after_recovery_fail -- both passed when re-run singly; catalog checks all PASS)
- [X] T018 Live run NL-I-02 on e2-dedicated-vm passes; evidence dir referenced in the PR (NL-I-02-20261013T062642Z-60450a, NL-I-02-20261013T063046Z-a0755d, NL-I-02-20261013T063634Z-93776f)
- [X] T019 Live re-run NL-I-01 (shared driver/adapter code) passes (NL-I-01-20261013T064224Z-077115, NL-I-01-20261013T065941Z-cf0791, NL-I-01-20261013T072054Z-d72293)
- [X] T020 CLAUDE.md owner table: NL-I-01 and NL-I-02 added to Owner B (2026-10-13)

## Phase 7: Review fixes (2026-10-13)

- [X] T021 Gate `detection_identifies_block == true` in catalog/NL/NL-I-02.yaml (Framework §16.2; NEEDS SIGN-OFF): a read that walked the damaged leaf and returned the right rows unverified passed before
- [X] T022 Locate the flipped byte from the page: `locate_corruption_byte` in resilience_tests/adapters/postgresql/adapter.py (header bounds, special size, leaf/level 0, middle data entry, plain entry, key written; low byte of the heap row pointer's offset number); driver reads the page with `od -v` after the leaf check and aborts unless the single-byte read agrees (resilience_tests/execution/injectors/corruption.py); report line for the entry
- [X] T023 Tests (tests/test_index_corruption.py): locator happy path and rightmost page, refusals, driver abort before any write, page/byte disagreement, the undetected-read false pass, a failure at another block. Local: 50 passed; full suite 386 passed, 2 failed (the real-Elle tests, local Java -- as before), catalog checks pass; mutation check: each protection removed makes its tests fail
- [X] T024 Index read is a plain Index Scan (2026-10-13, after lab run NL-I-02-20261013T051043Z-ac5f31 chose an Index Only Scan, which is blind to a redirected row pointer): `SELECT id, payload`, `enable_indexonlyscan=off`, only "Index Scan" counts as reading through the index; tests for an Index Only Scan plan (not an index read) and for the lab's redirected pointer (row 6410 for key 6405 -> wrong result)
- [X] T025 Live re-run NL-I-02 (plan must read `Index Scan`) and NL-I-01: 3 consecutive passes each (2026-10-13); every NL-I-02 read and read-after used a plain Index Scan (results.json `plan`)
- [X] T026 Review hardening (2026-10-13): `amcheck_detects_target` requires the checker to name the injected block and file (`amcheck_relation` returns `corrupt_pages`; NL-I-01 and NL-I-02); `reindex_restores` requires the index in a new file (`new_storage`); summary shows each index read's plan, the file change and whether amcheck named the block. Tests in tests/test_heap_corruption.py and tests/test_index_corruption.py
- [ ] T027 Live re-run NL-I-01 and NL-I-02 after T026 (shared adapter/orchestrator code)
