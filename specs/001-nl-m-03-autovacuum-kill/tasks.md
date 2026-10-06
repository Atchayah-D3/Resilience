---
description: "Task list for NL-M-03 Autovacuum Worker Killed"
---

# Tasks: NL-M-03 Autovacuum Worker Killed

**Input**: Design documents from `specs/001-nl-m-03-autovacuum-kill/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/, quickstart.md

**Tests**: REQUIRED -- spec FR-012 and Constitution III: every new measure and refusal has a
unit test proving its fail path, failing for the reason under test.

**Organization**: grouped by user story (spec.md): US1 autovacuum resumes after repeated
worker kills (P1), US2 each kill measured as a crash (P2), US3 a kill that missed never passes
(P3).

## Format: `[ID] [P?] [Story] Description`

- **[P]**: parallelizable (different files, no dependency on an incomplete task)
- Paths are repository-relative. Run tests on the harness VM in `~/resilience` (CLAUDE.md).

---

## Phase 1: Setup

- [X] T001 Confirm the starting point (merged main: 264 passed + catalog checks on the VM, 2026-10-05): on the harness VM copy of the working tree, `.venv/bin/python -m pytest tests -q` and `.venv/bin/python -m catalog.schema --partial` pass before any change (record the counts in the PR description)

---

## Phase 2: Foundational (blocks all stories)

- [X] T002 Add `"autovacuum_worker"` to `FaultDuring` in catalog/schema.py (keep the existing values `"checkpoint", "large_transaction", "concurrent_index_build"`; update the comment to cite Framework §10.7 NL-M-03)
- [X] T003 [P] Add `kill_target: Mapping[str, Any] | None = None` to `FaultInjector.__init__` in resilience_tests/execution/injectors/base.py, documented as "generic: {pid, parent_pid, title_marker}; set per attempt by the orchestrator, cleared after"
- [X] T004 [P] In resilience_tests/adapters/base.py: change `observe_fault_settings(self, fault_type)` to `observe_fault_settings(self, fault_type, during=None)`; add defaults `start_autovacuum_worker()` -> `{"in_progress": False, "note": "not implemented by this engine"}` and `verify_autovacuum_resumed()` -> `{}`
- [X] T005 Update the call in resilience_tests/control/orchestrator.py `_p_init` to `observe_fault_settings(self.scenario.fault.type, self.scenario.fault.during)` and update every override/fake in resilience_tests/adapters/postgresql/adapter.py, tests/test_orchestrator.py, tests/test_repeated_cycles.py, tests/test_idle_transaction_vacuum.py to accept `during=None` (no behaviour change for existing scenarios)

**Checkpoint**: `pytest tests -q` still passes unchanged.

---

## Phase 3: User Story 1 - Autovacuum survives repeated worker kills (P1) 🎯 MVP

**Goal**: five landed kills of a confirmed autovacuum worker, then proof that a new worker
spawned and every eligible relation was vacuumed within the bound.

**Independent Test**: unit tests below with the fake engine; then the live run in
quickstart.md §3 reports `kills_landed=5`, `autovacuum_worker_respawned=true`,
`relations_left_unvacuumed=0`.

### Tests for User Story 1 (write first; they must fail before the implementation)

- [X] T006 [P] [US1] Create tests/test_autovacuum_worker_kill.py: injector test -- with `kill_target={"pid": 4242, "parent_pid": 100, "title_marker": "autovacuum worker"}`, `OsSshProcessDriver("process_kill")._kill` sends ONE root command that kills 4242 only if its parent is 100 and its cmdline contains the marker, never `systemctl kill`; death is confirmed from /proc; detail has `target_pid`, `landed: True`
- [X] T007 [P] [US1] In tests/test_autovacuum_worker_kill.py: adapter verification tests with mocked samples -- (a) all eligible relations show `last_autovacuum >= t_recovered` -> `relations_left_unvacuumed == 0`; (b) one eligible relation without a post-recovery autovacuum -> count 1 and its name listed; (c) no relation ever eligible -> `relations_left_unvacuumed` is None; (d) a `last_autovacuum` earlier than `t_recovered` does NOT count as vacuumed; (e) a worker seen after `t_recovered` -> `autovacuum_worker_respawned` True, none -> False
- [X] T008 [P] [US1] In tests/test_orchestrator.py: extend `OutageAdapter` with `start_autovacuum_worker` (returns a kill target) and `verify_autovacuum_resumed` (driven by `Engine.after_during`); add `test_autovacuum_kill_nlm03_passes_verdict` (5 cycles, kills_landed 5, verdict passed) and `test_nlm03_unvacuumed_relation_fails` / `test_nlm03_no_respawn_fails` / `test_nlm03_zero_eligible_is_not_measured` asserting the exact failing predicate

### Implementation for User Story 1

- [X] T009 [US1] In resilience_tests/adapters/postgresql/adapter.py add constants and DDL for the harness table `resilience.avac_target` (id bigint, v int; ~1,000,000 rows; table storage options that make its vacuum last seconds: `autovacuum_vacuum_cost_delay` and a per-table threshold low enough that one batch of updates makes it eligible) -- harness-owned object, never global configuration
- [X] T010 [US1] In resilience_tests/adapters/postgresql/adapter.py extend `prepare_scenario_objects(during)` for `"autovacuum_worker"`: create the table, reseed only when the row count differs, `VACUUM (ANALYZE)` after seeding; extend `cleanup_scenario_objects()` to `TRUNCATE resilience.avac_target`
- [X] T011 [US1] In resilience_tests/adapters/postgresql/adapter.py implement `observe_fault_settings(fault_type, during)`: for `during == "autovacuum_worker"` return (SHOW only) `autovacuum, track_counts, autovacuum_naptime, autovacuum_max_workers, autovacuum_vacuum_cost_delay, autovacuum_vacuum_cost_limit, autovacuum_vacuum_threshold, autovacuum_vacuum_scale_factor, restart_after_crash, update_process_title`; keep the existing idle_in_transaction behaviour
- [X] T012 [US1] In resilience_tests/adapters/postgresql/adapter.py implement `start_autovacuum_worker()`: update ~25% of `avac_target` rows (creates dead tuples after the latest recovery), then poll every 50 ms for up to `2 x naptime + 30 s` for a row in the activity view with backend type autovacuum worker on `resilience.avac_target`; return `{"in_progress": True, "kill_target": {pid, parent_pid (postmaster pid), title_marker: "autovacuum worker", relation, wraparound, observed_after_s}}` or `{"in_progress": False, "note": ...}`
- [X] T013 [US1] In resilience_tests/execution/injectors/process.py implement the targeted path of `_kill` when `self.kill_target` is set: one `as_root` command checking `/proc/<pid>` exists, parent pid == `parent_pid`, cmdline contains `title_marker`, then `kill -9 <pid>`; exit with a distinct status when the check fails -> raise `FaultNotLanded(..., {"changed_nothing": True, ...})`; otherwise confirm death with the existing `process_gone` loop and record `target_pid`, `postmaster_pid`, `death_confirmed_s`, `landed: True`; reuse an `arm()`-prepared session when present; leave the no-target path unchanged
- [X] T014 [US1] In resilience_tests/control/orchestrator.py `_establish_fault_state`: for `during == "autovacuum_worker"` call `start_autovacuum_worker()`, record `facts["during"]`, abort with the note when not in progress, otherwise set `self.injector.kill_target` (cleared after the attempt); refuse in `_p_init` with PhaseAbort when the observed `autovacuum` or `track_counts` is `off`
- [X] T015 [US1] In resilience_tests/control/orchestrator.py: count landed kills per cycle into measure `kills_landed`; record per-cycle kill targets in `facts["cycles"]`
- [X] T016 [US1] In resilience_tests/adapters/postgresql/adapter.py implement `verify_autovacuum_resumed()` per research R6: read `t_recovered` from the database clock; regenerate dead tuples on `avac_target`; sample every 2 s until `3 x naptime`, extended while an autovacuum worker is processing an eligible relation, capped at half the validate phase bound; eligible = dead tuples above its own threshold (per-table options or `threshold + scale_factor x reltuples`) in any sample; vacuumed = `last_autovacuum IS NOT NULL AND last_autovacuum >= t_recovered`; return the contract dict (`relations_left_unvacuumed` None when 0 eligible)
- [X] T017 [US1] In resilience_tests/control/orchestrator.py register `"autovacuum_worker"` in `_DURING_MEASURES` / `_verify_during_operation` mapping `autovacuum_worker_respawned`, `relations_eligible`, `relations_left_unvacuumed`; None -> NOT_MEASURED with reason "no relation became eligible for autovacuum during verification"; include it in the `during` cleanup branch of `_p_cleanup`; add disclosures: observed settings, the harness table's storage options, "each kill is a crash-restart of the whole instance"
- [X] T018 [US1] Create catalog/NL/NL-M-03.yaml per contracts/catalog-nl-m-03.md (header cites Framework §10.7/§10.2/§16.3; `repeat: {cycles: 5, interval_s: 10}` with 5 marked NEEDS SIGN-OFF; workload `mixed`; measures and accept exactly as the contract)

**Checkpoint**: T006-T008 pass; `pytest tests -q` and `python -m catalog.schema --partial` pass.

---

## Phase 4: User Story 2 - The crash each kill causes is measured honestly (P2)

**Goal**: each kill is scored as a crash: RPO 0, unattended restart, amcheck clean.

**Independent Test**: unit tests below; live run shows `rpo_txn=0`, `starts_unattended=true`,
`cycles_recovered=5`, `structural_integrity_errors=0`.

- [X] T019 [P] [US2] In tests/test_orchestrator.py add `test_nlm03_lost_commit_fails` (fake engine drops one acknowledged marker -> `rpo_txn == 0` fails) and `test_nlm03_corruption_fails` (`IntegrityResult(structural_errors=1, checksum_failures=0)` -> both corruption predicates fail)
- [X] T020 [US2] Verify in resilience_tests/control/orchestrator.py that per-cycle recovery (`_await_cycle_recovery`) treats the postmaster's own crash-restart (restart_after_crash, postmaster pid unchanged) as recovery from the write probe, with no service restart issued; record `postmaster_survived` per cycle from the injector detail
- [X] T021 [US2] Confirm `OsSshProcessDriver.preflight` still runs `_check_restart_cadence` for this scenario (`repeat_plan` set) in resilience_tests/execution/injectors/process.py and add a unit test in tests/test_autovacuum_worker_kill.py that a cadence beyond `StartLimitBurst` refuses before the first kill

---

## Phase 5: User Story 3 - A kill that missed can never pass (P3)

**Goal**: misses never count; bounded retry; abort with the reason.

**Independent Test**: unit tests below.

- [X] T022 [P] [US3] In tests/test_autovacuum_worker_kill.py: wrong parent pid or missing title marker -> the command does not kill and `FaultNotLanded` with `changed_nothing` is raised
- [X] T023 [P] [US3] In tests/test_orchestrator.py: `test_nlm03_no_worker_aborts` (start returns not in progress -> status aborted, error names the reason, no T0); `test_nlm03_miss_is_retried_not_counted` (first attempt changed_nothing, second lands -> kills_landed counts 1 for that cycle); `test_nlm03_three_misses_abort`
- [X] T024 [US3] In resilience_tests/control/orchestrator.py `_inject_once`: on `FaultNotLanded` whose detail has `changed_nothing: True`, re-run `_establish_fault_state` and retry, at most 3 attempts per cycle (constant `MAX_LANDING_ATTEMPTS = 3`), recording each attempt; after the last, abort with the reason; any other `FaultNotLanded` keeps the existing abort behaviour

---

## Phase 6: Polish & Cross-Cutting

- [X] T025 [P] Update tests/test_safety_ledger_profile.py so the fault-type loop also exercises `process_kill` with a `kill_target` (targeted path) and asserts no `systemctl kill` is sent for it
- [X] T026 [P] Update CLAUDE.md: add NL-M-03 to the owners table (Gowri) and gotchas (two clusters on the target -> ppid-bound kill; `avac_target` table kept between runs; run length ~15-20 min)
- [X] T027 (2026-10-06: unit 290 pass + 4 VM-env-file-only failures; catalog 6/6; dry run ok; live run NL-M-03-20261006T075615Z-5bd741 PASSED -- 5/5 kills landed, 0 unvacuumed of 2, RPO 0, amcheck 0; second cluster untouched) Run the full gate on the harness VM: `pytest tests -q`, `python -m catalog.schema --partial`, dry run (`--stop-before-fault`), then the live NL-M-03 run per quickstart.md §2-§4; confirm the second cluster's processes were untouched and the target is clean
- [X] T028 (2026-10-06: NL-C-01, NL-C-05, NL-C-06 PASSED; NL-C-03 PASSED on re-run (first attempt aborted pre-fault on harness storage stall); NL-C-02 aborted twice without a verdict -- once 'checkpoint completed before the kill' (its own fail-closed check; timing race in that scenario), once pre-fault steady state (target p99 935 ms) -- neither touches NL-M-03 code; reported to the owner) Re-run the live scenarios that share the changed code (NL-C-01, NL-C-02, NL-C-03, NL-C-05, NL-C-06) on the VM, per the constitution's merge gate

---

## Dependencies & Execution Order

- Phase 1 -> Phase 2 -> US1 (MVP) -> US2 and US3 -> Polish.
- US2 is mostly verification of existing crash handling; it can proceed right after T013-T015.
- US3's retry (T024) is needed for reliable LIVE runs of US1 but not for US1's unit tests; do it
  before T027.
- Within US1: T009-T012 (adapter) and T013 (injector) are independent files; T014-T017
  (orchestrator) depend on them; T018 last.

## Parallel Opportunities

- T003 and T004 (different files).
- Tests T006, T007, T008 can be written in parallel; T019, T022, T023 likewise.
- Adapter tasks (T009-T012, T016 - same file, sequential) in parallel with the injector task T013.

## Implementation Strategy

MVP = Phases 1-3 (US1) with unit tests green, then US3's retry, then the live run (T027).
US2 adds tests over behaviour the harness already has. Stop and validate at each checkpoint.
