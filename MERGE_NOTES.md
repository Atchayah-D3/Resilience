# Merge notes: Atchayah's TPC-C workload + Gowri's NL-N-I03 lock pile-up

Merged on 2026-11-12.

- Base: `Atchayah-resilience-tpcc-workload.zip` (her updated tree, the one she verified).
- Added from: `GowriSelva_Resilience-scenario3-NL-N-I03.zip`.
- Neither original zip was changed.

## What the merged tree contains

Everything in Atchayah's tree is unchanged, plus Gowri's NL-N-I03 (lock pile-up behind a schema change):

| Added from Gowri | Where |
|---|---|
| Scenarios 3a / 3b / 3c | `catalog/NL/NL-N-I03a.yaml`, `-I03b.yaml`, `-I03c.yaml` |
| Fault behaviour and measures | `resilience_tests/control/faults/lock_pileup.py` |
| Fault driver | `resilience_tests/execution/injectors/lock_pileup.py` |
| Unit tests | `tests/test_lock_pileup.py`, `tests/test_lock_pileup_run.py` |
| His spec, plan, quickstart | `specs/005-nl-n-i03-lock-pileup/` |

Not brought over: his `.specify/`, `.claude/`, `CLAUDE.md` and specs 001-003 (tooling and notes, no code the scenario needs).

## Files where the two sides were combined by hand

| File | What was combined |
|---|---|
| `catalog/schema.py` | Gowri's `lock_pileup` fault type and `LockPileup` model, `workload.contended_share`, the `contended_table` capability, internal scenario IDs (`NL-N-I03`, lettered variants) and their exclusion from catalog checks 2 and 6. Atchayah's `tpcc` profile, `groups`, `session` and `tpcc` options are kept. |
| `adapters/base.py` | Gowri's contended-table and lock pile-up methods, `crash_restart_log_patterns`, `Capability.CONTENDED_TABLE`. Atchayah's `TpccSupport` and `session(settings=...)` are kept. |
| `adapters/postgresql/adapter.py` | Gowri's contended table (DDL, seeding, `commit_marker_with_contended_row`, `prepare_contended_table`). Atchayah's `TpccMixin` and the `privileged` sudo forms are kept. |
| `execution/workload/driver.py` | Gowri's table-user / other groups and their `group_sample` events. They sit beside Atchayah's `by_type` / `by_group` samples; the two do not share state. |
| `tests/test_fault_behaviours.py` | Registry pins now include `lock_pileup`. |

Copied from Gowri unchanged because Atchayah's side had no changes there: `adapters/postgresql/sessions.py`, `control/faults/__init__.py`, `execution/injectors/__init__.py`, `reporting/cases.py`.

## How it was checked

- `python -m catalog.schema --partial`: all six checks pass.
- Full unit suite (`pytest tests`, no live database): **774 passed, 1 failed, 23 skipped**.
- The same suite on Atchayah's tree alone: 736 passed, 1 failed, 23 skipped. So the merge adds 38 passing tests and no new failure.
- The 1 failure is `tests/test_tpcc_speedtest.py::test_output_folder_that_cannot_be_written_is_refused_at_once`. It fails identically without the merge. It was run as `root`, which can write to any folder, so the cause is probably the test environment. Please run it as a normal user.
- The 23 skipped tests need a throw-away PostgreSQL (`RESILIENCE_TEST_PG`).
- **Nothing was run against a live target.** The merged scenarios have not been exercised on a lab machine.

## What is NOT done: Scenario 3 on the TPC-C workload

The merge makes both features coexist. NL-N-I03 still runs on the old marker workload and its own table, `resilience.lock_target`. To run it on TPC-C (as the Resilience Testing document describes), the following remain, and they need a live target to verify:

1. Point the fault at the TPC-C `stock` table (`resilience_tpcc.stock`) instead of `resilience.lock_target`. The table name is currently hard-coded in `adapters/postgresql/sessions.py`.
2. Rebuild `longest_table_wait_s` and `other_tables_tps_lost_pct` from `by_type` samples. Table users are `new_order` and `stock_level`; the others are `payment`, `order_status` and `delivery`. Atchayah's samples now include `max_ms` per type, which the first measure needs.
3. Add a TPC-C version of the 3a scenario file (`workload.profile: tpcc`). `lock_pileup` currently requires `workload.contended_share`, which only the marker workload has.
4. Check that adding and dropping a column on `stock` leaves the TPC-C data consistent. A dropped column keeps its slot in PostgreSQL; Atchayah's `tpcc_columns` (reset step) is the place to look.
5. Align the 3a pass rules with the document: gate the longest table-user wait (6 s), the other tables' loss (10%), recovery within 60 s and the probe column being absent. Add "ALTER completed" to 3b. Decide on the 3c variant, which is not in the document.
6. Load the TPC-C data on the lab target first (`tpcc.load --warehouses N`); the live runs need it.

## Update 2026-11-12: Scenario 3 brought in line with the Resilience Testing document

Scenario 3's pass rules (3a/3b) now follow the document, and Scenario 3 runs on TPC-C (items 1-5 above are done). The metrics the document asks for are now collected.

**Scenario 3 (`catalog/NL/NL-N-I03a.yaml`, `-I03b.yaml`):**
- Workload `profile: tpcc` (320 warehouses: must equal the loaded data; `rate_tps: 200` NEEDS SIGN-OFF, set it to ~60% of `tpcc.capacity`), `fault.lock_pileup.table: tpcc_stock`: the pile-up forms on `resilience_tpcc.stock`.
- Table users = New-Order + Stock-Level, others = Payment + Order-Status + Delivery, from the per-type samples.
- 5-minute unmeasured warm-up (`steady_state.warmup_s: 300`), latency timed from the intended start (`workload.latency_from: intended_start`).
- Gated as well: `longest_table_wait_s <= 6`, `other_tables_tps_lost_pct <= 10`, `rto_to_slo_s <= 60` (95% TPS definition), `ddl_completed` (3b), `probe_column_absent`, `harness_sessions_left == 0`, `settings_unchanged`, `partial_commits == 0`, `tpcc_consistency_violations == 0`.
- 3c is unchanged (still the harness's contended table).

**Shared code (default behaviour unchanged unless a scenario opts in):**
- `catalog/schema.py`: optional `steady_state.warmup_s`, `workload.latency_from`, `lock_pileup.table`.
- `execution/workload/driver.py`, `tpcc/engine.py`: `RateLimiter.acquire()` returns the intended start; `keep_schedule` only with `latency_from: intended_start`. Marker samples also carry `p90_ms` and `max_ms`; TPC-C totals carry `p90_ms` and `max_ms`.
- **Every scenario now records** (evidence only, never gated unless a scenario lists it): database activity every 1 s (`execution/probes/activity.py` DbActivitySampler: connections, wait events, lock waiters, longest transaction, autovacuum workers, WAL, checkpoints, temp files, deadlocks, rollbacks, cache hits, dead rows), host `vmstat 1` over SSH (CPU, iowait, memory, swap, block I/O), the read probe (it existed but was never started), `settings_unchanged` (settings re-read in validate), and a per-phase summary (`facts.activity`, `summary.txt`).
- `tests/test_orchestrator.py`: its `env` fixture also stubs `HostStatsSampler` (no SSH from a unit test).

**Checked:** catalog checks pass; `tests/test_lock_pileup.py` + `tests/test_lock_pileup_run.py`: 49 passed. The rest of the suite was not run this time: run it before merging, since the orchestrator, driver and TPC-C engine changed. Nothing was run against a live target.

**Still needs the operator:** `log_lock_waits = on` on the target (the harness never writes server settings); until then `lock_waits_logged` is NOT_MEASURED.
