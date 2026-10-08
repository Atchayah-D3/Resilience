---
description: "Task list for pgbench as the Harness Workload"
---

# Tasks: pgbench as the Harness Workload

**Input**: Design documents from `specs/002-pgbench-workload-driver/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/, quickstart.md

**Tests**: REQUIRED. Spec FR-019 and constitution III: every refusal and fail-closed path gets a unit test that fails for the reason under test.

**Organization**: grouped by user story (spec.md):
- US1: existing scenarios unchanged, selection and refusals (P1)
- US2: data-loss evidence (P1). **Gated on research R1, team-owned.**
- US3: load through faults, and per-second samples (P2)
- US4: transaction shapes (P2)
- US5: throughput capacity and disclosure (P3)

**R1 gate**: tasks marked **[GATED-R1]** cannot start until the team has recorded the R1 decision in research.md. Everything else is built and tested against a fake record channel (T008), so it doesn't wait for R1.

**Superseded detail (2026-10-08)**: R1 was decided as option B (research.md R1). The `RecordChannel` / `InMemoryRecordChannel` interface of T008, and the per-command overhead and scheduling-lag facts of T046, were replaced by the record service (`shell_records.py`) and its journal-flush figures. Task texts below are kept as written; their done notes say what was built.

## Format: `[ID] [P?] [Story] Description`

- **[P]**: parallelizable (different files, no dependency on an incomplete task)
- Paths are repository-relative. Work on the `pgbench` branch.

---

## Phase 1: Setup

- [x] T001 Confirm the starting point on the `pgbench` branch: `.venv/bin/pytest -q -n 4` and `.venv/bin/python -m catalog.schema --partial` pass before any change. Record the counts in specs/002-pgbench-workload-driver/research.md under a "Baseline" note (at plan time: 336 passed; 2 Elle tests need Java 21).
- [x] T002 [P] Create the fake pgbench executable tests/fakes/fake_pgbench.py (with tests/fakes/__init__.py). It accepts `-c 1 -j 1 -T <s> -D name=value -f <script> -P 1 --report-per-command --failures-detailed --max-tries=1 --version` and prints, in ShaktiDB 17 format (and honours `\sleep` in scripts by sleeping, so spacing can be tested): `progress: <t> s, <tps> tps, lat <ms> ms stddev <ms>, <n> failed`, a per-launch summary with per-command latencies, and on demand (environment variables) a `client 0 aborted in command <n> …` line, an unexpected exit, or unparseable output. No database access.

---

## Phase 2: Foundational (blocks all stories)

- [x] T003 Add the profile section from contracts/profile-workload.md to resilience_tests/control/profile.py: `workload: {generator: Literal["pgbench", "builtin"] = "pgbench", pgbench_bin: str = "pgbench"}`, optional, with `extra="forbid"` inside the section. Profiles without the section must keep validating.
- [x] T004 [P] Set `workload: {generator: builtin}` **explicitly** in envs/e2-dedicated-vm.yaml and envs/local-lab.yaml, with a comment citing plan.md "Build-out rule" (switched to pgbench in T052)
- [x] T005 Create resilience_tests/execution/workload/interface.py with `make_workload_driver(profile, adapter, workload, journals, stream, history)`. It returns the existing `WorkloadDriver` for `builtin` and `PgbenchWorkloadDriver` for `pgbench`, and raises `UnsupportedWorkload` with a named cause otherwise; it **never returns the other driver instead** (contracts/workload-driver.md "Construction")
- [x] T006 Change resilience_tests/control/orchestrator.py `_p_init` to build the driver through `make_workload_driver`. Record `facts["workload_generator"]` (and the pgbench version, when used). No other orchestrator change.
- [x] T007 [P] In resilience_tests/adapters/base.py add `Capability.PGBENCH_WORKLOAD`, `pgbench_launch(shape, launch, client)` (default raises `NotImplementedError`), and `sessions_with_application_name(name)` (default returns `None` = cannot tell). Per contracts/adapter-pgbench.md.
- [x] T008 [P] Create resilience_tests/execution/workload/record_channel.py: the **mechanism-neutral** interface the pgbench driver consumes. It yields `TransactionRecord` events `(identity, kind ∈ {pre, ack}, t_wall)` per data-model.md, and reports `complete: bool` with a reason when records may be missing. Include `InMemoryRecordChannel` for tests. It decides nothing about how records are produced (that's R1).
- [x] T009 [P] Create resilience_tests/execution/workload/identity.py: `identity_text(launch, client, seq) -> "L<launch>-C<client>-S<seq>"` and `identity_uuid(text) -> str(uuid.UUID(hashlib.md5(text.encode()).hexdigest()))` (data-model.md, MarkerRow), with tests in tests/test_pgbench_identity.py checking that the Python value equals PostgreSQL's `md5(text)::uuid` for fixed examples
- [x] T010 In resilience_tests/analysis/report.py: add `workload generator: <name>` (plus the pgbench version and launch count when pgbench) to the summary header block

**Checkpoint**: `pytest -q` passes unchanged. Every scenario still runs on `builtin`.

---

## Phase 3: User Story 1 - Existing scenarios run unchanged; selection and refusals (P1) 🎯 MVP

**Goal**: choosing the generator is a profile setting. `builtin` behaves exactly as today. `pgbench` refuses, with a named cause, whenever it cannot meet the guarantees, including until R1 exists.

**Independent Test**: tests/test_workload_selection.py, plus the existing scenario tests parametrised over `builtin`.

### Tests for User Story 1 (write first)

- [x] T011 [P] [US1] Create tests/test_workload_selection.py:
  - (a) no `workload` section → generator `pgbench`;
  - (b) `generator: builtin` → the existing `WorkloadDriver`, with `facts["workload_generator"] == "builtin"`;
  - (c) `pgbench_bin` not executable → `UnsupportedWorkload` naming the path, raised before the baseline;
  - (d) pgbench's major version ≠ the target server's major version (from `adapter.server_version()`) → refusal naming both versions (FR-004);
  - (e) adapter without `Capability.PGBENCH_WORKLOAD` → refusal;
  - (f) a scenario with `transaction_markers: true` while no record channel is registered → refusal citing research R1;
  - (g) in none of these cases is the built-in driver returned instead.
- [x] T012 [P] [US1] Parametrise the end-to-end orchestrator fixture in tests/test_orchestrator.py over `generator ∈ {builtin}` (and later `pgbench`, T048), so every existing scenario test runs through the factory. All must pass unchanged (spec SC-008).

### Implementation for User Story 1

- [x] T013 [US1] Implement the pgbench availability and version probe used by the factory in resilience_tests/execution/workload/pgbench_driver.py (`probe_pgbench(path, server_major) -> version | raises UnsupportedWorkload`), run on the driver host before the baseline; supported = same major version as the target server (FR-004)
- [x] T014 [US1] Implement the refusal rules of T011 in resilience_tests/execution/workload/interface.py, with messages naming the cause and the profile field to change
- [x] T015 [P] [US1] Optional: add a `--workload {pgbench,builtin}` override to resilience_tests/conftest.py that replaces `profile.workload.generator` for one run (contracts/profile-workload.md example)

**Checkpoint**: US1 is complete for `builtin`. `pgbench` refuses cleanly with the R1 reason.

---

## Phase 4: User Story 2 - Data-loss evidence as strong as today (P1) [GATED-R1]

**Goal**: the R1 mechanism implemented against contracts/transaction-record.md, proven by its contract tests. The factory lifts the pgbench refusal only when they pass.

**Independent Test**: tests/test_transaction_record.py (CT-1 to CT-6) with the fake pgbench and a fake database; then quickstart V4's independent journal recount.

- [x] T016 [US2] **Team decision**: record the R1 decision (decision / rationale / alternatives / measured cost) in specs/002-pgbench-workload-driver/research.md, judged against R1's criteria. Owner: the team. Nothing below starts before this.
- [x] T017 [US2] Run quickstart V2 on the lab (per-client variable persistence; client abort on database restart) and record both results in research.md R7 and R4. If either differs from the plan, update plan.md before continuing.


### Contract tests for User Story 2 (write first; must fail before T024)

- [x] T018 [P] [US2] tests/test_transaction_record.py **CT-1**: every acknowledged identity's `pre` record precedes the commit-send time observed by the fake database
- [x] T019 [P] [US2] tests/test_transaction_record.py **CT-2**: database killed mid-load. Acknowledged ⊆ pre; unknown outcomes reported; `rpo_txn == 0` for an honest database.
- [x] T020 [P] [US2] tests/test_transaction_record.py **CT-3**: a fake database that acknowledges and then loses a commit → `rpo_txn >= 1`, failing for that reason (constitution III negative case)
- [x] T021 [P] [US2] tests/test_transaction_record.py **CT-4**: the `pre` record fails → the transaction never commits, or the run aborts. Never acknowledged-without-pre.
- [x] T022 [P] [US2] tests/test_transaction_record.py **CT-5**: the `ack` record fails after a commit → the run aborts and no `rpo_txn` is issued
- [x] T023 [P] [US2] tests/test_transaction_record.py **CT-6**: repeated relaunches → no identity repeats; `diff_from_journals` reports 0 torn and 0 unjournalled

### Implementation for User Story 2

- [x] T024 [US2] Implement the R1 mechanism as decided in T016, as a `RecordChannel` (T008) registered with the factory. Files as R1 specifies, including the record steps in the adapter's scripts (contracts/adapter-pgbench.md `script`). It must make T018–T023 pass.
  - **Done 2026-10-08 (option B):** `resilience_tests/execution/workload/shell_records.py`; CT-1 to CT-6 in tests/test_transaction_record.py drive real record steps.
- [x] T025 [US2] Write records into `marker.jrnl` / `acked.jrnl` in today's format, using `identity_uuid` (T009), so resilience_tests/execution/workload/markers.py `diff_from_journals` runs unchanged
- [x] T026 [US2] In resilience_tests/execution/workload/interface.py, lift refusal (f) only when a registered channel reports that its contract tests passed (a constant set by T024's module), and keep the refusal otherwise
  - **Done 2026-10-08:** the refusal and the channel registry are removed; the pgbench driver always carries the record steps (tests/test_workload_selection.py `test_marker_scenarios_run_on_pgbench`).

**Checkpoint**: CT-1 to CT-6 pass. The pgbench driver may now measure RPO.

---

## Phase 5: User Story 3 - Load through faults; per-second samples (P2)

**Goal**: one pgbench process per client; drops detected, counted and relaunched; samples identical in form to today's; no process ever left behind.

**Independent Test**: tests/test_pgbench_driver.py with the fake pgbench and `InMemoryRecordChannel`.

### Tests for User Story 3 (write first)

- [x] T027 [P] [US3] Create tests/test_pgbench_output.py: parse recorded ShaktiDB 17 pgbench output lines: progress, `client N aborted in command …`, failures-detailed counts, the per-command latency summary. Unparseable input raises (fail closed), never returns zeros.
- [x] T028 [P] [US3] tests/test_pgbench_driver.py: `start()` launches `concurrency` processes with `-c 1 -j 1` and **no `-R`**, each with `-D interval_us=<1e6×concurrency/rate>`, each with a unique launch number and `PGAPPNAME=resilience-pgbench-<run_id>`. Emits `workload:start` with `generator=pgbench` and `workload:launch` per process.
- [x] T029 [P] [US3] tests/test_pgbench_driver.py: even spacing (FR-021, research R11). With the fake pgbench honouring `\sleep`, each client's transaction starts fall on evenly spaced slots `interval_us` apart (within a stated tolerance); an overrunning transaction delays only the next start, with no catch-up burst; the per-launch scheduling lag p50/p99 is recorded; argv never contains `-R`.
  - **Done 2026-10-08:** paced by the record service's shared `RateLimiter` (research R11 revised); `test_the_declared_rate_is_offered_evenly` checks the rate and the median spacing.
- [x] T030 [P] [US3] tests/test_pgbench_driver.py: all clients aborted (fault) → each counted as one drop, its in-flight transaction marked unknown, and relaunched with a new launch number once the adapter reports the database accepting connections. The relaunch time is recorded.
- [x] T031 [P] [US3] tests/test_pgbench_driver.py: partial loss (2 of 8 abort) → 2 drops, only those 2 relaunched, the other 6 processes untouched (same PID, same launch)
- [x] T032 [P] [US3] tests/test_pgbench_driver.py: unexpected exit with the database healthy, or unparseable output → `failure` set, `workload:fatal` emitted (FR-014). Relaunch attempts are bounded by the deadline passed in; never two live processes for one client.
- [x] T033 [P] [US3] tests/test_pgbench_driver.py: samples built from channel records:
  - p50/p95/p99 from `t_ack − t_pre`;
  - `commits` = acknowledgements in the interval;
  - `errors` = definitely aborted (R5);
  - `indeterminate` and `drops` from aborts;
  - a value that cannot be computed is `None`, never 0;
  - wall→monotonic conversion per launch (R3);
  - no gap > 1.5 s when records flow.
- [x] T034 [P] [US3] tests/test_pgbench_driver.py: `stop()` and interrupt terminate every process group and confirm none remains. A remaining process, or `sessions_with_application_name() > 0`, makes cleanup fail, and the run cannot report `passed` (constitution VI).

### Implementation for User Story 3

- [x] T035 [US3] Implement resilience_tests/execution/workload/pgbench_output.py: pure parsers for the formats in T027
- [x] T036 [US3] Implement the supervisor in resilience_tests/execution/workload/pgbench_driver.py:
  - per-client process management (own process group, `PGAPPNAME`);
  - stderr reader;
  - the launch state machine `running → stopped | client_aborted | failed` (data-model.md WorkloadLaunch);
  - relaunch once the adapter's existing session check succeeds, retrying every 0.2 s, bounded;
  - `connected_workers`, `wait_until_ready()`.
- [x] T037 [US3] Implement the sample builder in resilience_tests/execution/workload/pgbench_driver.py: consume `RecordChannel` events, emit `workload:sample` each second with every field of contracts/workload-driver.md, and implement `begin_window()` / `end_window()` returning `MeasuredWindow`
- [x] T038 [US3] Implement `stop()` and cleanup in resilience_tests/execution/workload/pgbench_driver.py, plus `sessions_with_application_name` in resilience_tests/adapters/postgresql/adapter.py (`SELECT count(*) FROM pg_stat_activity WHERE application_name = $1`)
- [x] T039 [US3] Write per-launch evidence files `<run_dir>/pgbench/launch-<n>.txt` (argv without secrets, stderr, parsed summary) and `workload:launch_end` events (FR-018)

**Checkpoint**: the supervisor works end to end on the fake pgbench with an in-memory channel.

---

## Phase 6: User Story 4 - Transaction shapes reproduced (P2)

**Goal**: the adapter supplies marker, churn and list-append scripts with today's semantics.

**Independent Test**: tests/test_pgbench_scripts.py, plus NL-C-05 / NL-C-03 runs (quickstart V4/V5).

### Tests for User Story 4 (write first)

- [x] T040 [P] [US4] Create tests/test_pgbench_scripts.py:
  - the `marker` script inserts `md5('L'||:launch||'-C'||:client||'-S'||:seq)::uuid` with `seq` incremented once per transaction;
  - the `churn` script updates `resilience.churn` on `random(1, :churn_keys)`, with `:churn_keys` equal to `PostgreSQLAdapter.churn_key_space`, and replaces the row one in `CHURN_REPLACE_EVERY` via `\if`;
  - the `list_append` script reads and appends `resilience.lists` with `\gset` capture of each read;
  - no script contains a password or a host literal.
- [x] T041 [P] [US4] In tests/test_pgbench_scripts.py: a scenario with `profile: mixed` gets the churn shape, and `history: list_append` gets the list-append shape, derived exactly as resilience_tests/execution/workload/driver.py derives them today. A combination the built-in driver refuses is refused too.

### Implementation for User Story 4

- [x] T042 [US4] Implement `pgbench_launch(shape, launch, client)` in resilience_tests/adapters/postgresql/adapter.py, returning `PgbenchLaunchSpec(script, variables, connection, application_name)` per contracts/adapter-pgbench.md. Connection from the node's **client** endpoint; never a password.
- [x] T043 [US4] Declare `Capability.PGBENCH_WORKLOAD` on `PostgreSQLAdapter` in resilience_tests/adapters/postgresql/adapter.py
- [x] T044 [US4] [GATED-R1] Deliver list-append read values into history.edn through the R1 channel, in today's format (`:ok` with real reads, `:info` for unknown, never `:fail` unless definitely aborted). Verify with the existing Elle tests in tests/test_elle.py (needs Java 21 for the real-checker tests).
  - **Done 2026-10-08:** reads travel as chunks through the record steps; `test_list_append_history_records_what_the_database_returned`, `test_a_read_that_does_not_reassemble_fails_closed`.

**Checkpoint**: shapes are correct on the fake pgbench. List-append is complete once R1 lands.

---

## Phase 7: User Story 5 - Throughput capacity known and disclosed (P3)

**Goal**: the recording overhead is reported; the limiting side is named on a steady-state abort; the first lab figures are recorded.

**Independent Test**: tests/test_pgbench_driver.py overhead test; quickstart V4.

- [x] T045 [P] [US5] tests/test_pgbench_driver.py: from a fake per-command summary, the report states the record steps' share of transaction latency (R8). Without a summary, it's `None` with a reason.
- [x] T046 [US5] Compute and record `facts["pgbench_recording_overhead"]` in resilience_tests/execution/workload/pgbench_driver.py, and render it, together with the scheduling lag p50/p99 (FR-021), in resilience_tests/analysis/report.py (FR-011, SC-006)
- [x] T047 [US5] In resilience_tests/control/orchestrator.py `_p_pre_fault`: for pgbench, name the limiting side (the limits themselves stay the scenario's own: no allowance, no subtraction of overhead, spec FR-010) using the record steps' latency vs the database statement latency, as `journal_p99_ms` vs `p99_ms` does today
- [x] T048 [US5] [GATED-R1] Extend T012's parametrisation to `pgbench` in tests/test_orchestrator.py, using the fake pgbench and the R1 channel's test double. All existing scenario tests must pass on both generators (SC-001).
- [ ] T049 [US5] [GATED-R1] Lab: run quickstart V4 for NL-C-01 (200 TPS) and NL-M-07 (1000 TPS). Record the achieved rates, the recording overhead and any limiting side in research.md R1 ("measured cost"). If NL-M-07 < 750 TPS, note that NL-M-07 uses `generator: builtin` (FR-020).
  - **Open:** needs the lab (no lab access from the workstation where option B was built). Local figures are in research.md R1.

---

## Phase 8: Polish & Cross-Cutting

- [x] T050 [P] Document the `workload` profile section and the two generators in README.md (usage, selection, refusals, evidence files)
- [ ] T051 [GATED-R1] Lab: quickstart V5. Every scenario once per generator; compare verdicts and key measurements in a short table in research.md (SC-001, SC-008).
  - **Open:** needs the lab. Every scenario's orchestrator test already runs on both generators against the fakes.
- [x] T052 [GATED-R1] After T049/T051 pass, remove the explicit `generator: builtin` from envs/e2-dedicated-vm.yaml and envs/local-lab.yaml so the default (`pgbench`) applies. Keep `builtin` where a scenario's rate needs it (FR-020).
  - **Done on the branch 2026-10-08, ahead of T049/T051:** both profiles set `generator: pgbench` with a full `pgbench_bin` path. Before merge, T049/T051 must pass on the lab; `--workload builtin` is the fallback.
- [x] T053 Final check, recorded in specs/002-pgbench-workload-driver/research.md: `.venv/bin/pytest -q -n 4`, `python -m catalog.schema --partial`, and quickstart V3. No pgbench process left on the driver host after the suite (SC-007).


---

## Dependencies & Execution Order

- **Phase 1 → Phase 2** before any story.
- **US1 (Phase 3)** needs only Phase 2. **This is the MVP**: selection works, `builtin` is unchanged, and `pgbench` refuses honestly.
- **US3 (Phase 5)** and **US4 (Phase 6, except T044)** need Phase 2 only. They are built against the fake pgbench and `InMemoryRecordChannel`, so they can proceed **before R1**.
- **US2 (Phase 4)** starts only after **T016** (the team's R1 decision) and T017.
- **T044, T048, T049, T051, T052** need US2 complete.
- **US5** T045–T047 can proceed after US3; T048/T049 wait for US2.

```text
Phase 1 → Phase 2 → US1 (MVP)
                 ├→ US3 ─┐
                 ├→ US4 ─┤ (T044 waits for US2)
                 └→ [T016 team R1 decision] → US2 → T044, T048, T049 → Phase 8
```

## Parallel Execution Examples

- **Phase 2**: T004, T007, T008 and T009 together (different files); then T005 → T006 → T010.
- **US1**: T011 and T012 in parallel, then T013 → T014 (T015 any time).
- **US3**: tests T027–T034 in parallel (two files), then T035 → T036 → T037 → T038 → T039.
- **US4**: T040 and T041 in parallel, then T042 → T043.
- **US2** (after T016): T018–T023 in parallel, then T024 → T025 → T026.

## Implementation Strategy

1. **MVP = Phase 1 + Phase 2 + US1.** The branch stays fully usable on `builtin`, with selection, refusals and reporting in place. Mergeable on its own.
2. **Build ahead of R1**: US3 and US4 (except T044) against fakes. The supervisor, relaunch, samples and scripts are ready when R1 lands.
3. **R1** (team): decide (T016), verify the lab behaviours (T017), implement against the contract tests (T018–T026).
4. **Prove on the lab**: T048, T049, T051, then switch the default (T052).

Each checkpoint leaves the whole suite green, and every scenario runnable on `builtin`.
