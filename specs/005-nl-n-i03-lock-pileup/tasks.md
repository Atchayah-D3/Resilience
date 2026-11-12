# Tasks: NL-N-I03 Lock Pile-up Behind a Schema Change

**Input**: [spec.md](spec.md), [plan.md](plan.md)

## Phase 1: Setup

- [X] T001 Copy the branch to the harness VM in a separate checkout with its own `.venv`; record the baseline unit-test result (485 passed)

## Phase 2: Foundational

- [X] T002 Internal scenario ids with lettered variants, kept out of checks 2 and 6, in catalog/schema.py
- [X] T003 `lock_pileup` fault type, `LockPileup` model and `Workload.contended_share` with validators in catalog/schema.py
- [X] T004 `Capability.CONTENDED_TABLE`, session/core/SessionFaultSupport hooks in resilience_tests/adapters/base.py
- [X] T005 Report case ids for internal scenarios in resilience_tests/reporting/cases.py

## Phase 3: User Story 1 - lock timeout (P1)

- [X] T006 [US1] Contended table, contended transaction, crash-restart log patterns in resilience_tests/adapters/postgresql/adapter.py
- [X] T007 [US1] start / state / end / revert of the pile-up and the chain watcher in resilience_tests/adapters/postgresql/sessions.py
- [X] T008 [US1] OsSshLockPileupDriver in resilience_tests/execution/injectors/lock_pileup.py (+ import in __init__.py)
- [X] T009 [US1] Contended share and group_sample events in resilience_tests/execution/workload/driver.py
- [X] T010 [US1] LockPileupFault(HeldFault) and its measures in resilience_tests/control/faults/lock_pileup.py (+ import)
- [X] T011 [US1] catalog/NL/NL-N-I03a.yaml (judged)
- [X] T012 [US1] Unit tests in tests/test_lock_pileup.py; extend the registry pins in tests/test_fault_behaviours.py

## Phase 4: User Stories 2 and 3 - findings (P2)

- [X] T013 [P] [US2] catalog/NL/NL-N-I03b.yaml (no lock timeout)
- [X] T014 [P] [US3] catalog/NL/NL-N-I03c.yaml (rewriting schema change)

## Phase 5: Polish

- [X] T015 CLAUDE.md notes; spec, plan, research, data model, contract, quickstart
- [X] T016 Full unit suite and catalog checks on the harness VM
- [X] T017 Adapter mechanics against the lab target for 3a/3b/3c and a harness crash reverted from a fresh adapter
- [X] T018 Live runs on the lab target, target left as found (2026-11-06, 64 workers, 2M-row table): NL-N-I03a-20261106T124432Z-66f522, NL-N-I03b-20261106T132159Z-bcf486, NL-N-I03c-20261106T140112Z-d3f862 all passed; earlier attempts aborted on steady state before any fault
