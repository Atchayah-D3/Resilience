# NL-N-I03: Lock pile-up behind a schema change

A short guide to what the scenario tests, how it is built in the harness, and where each
piece lives. Details: [spec.md](spec.md), [plan.md](plan.md), [research.md](research.md).

## 1. What the scenario tests

A schema change (`ALTER TABLE ... ADD COLUMN`) needs the strongest lock on its table. While it
**waits** for that lock, or **holds** it during a rewrite, every new query on the table queues
behind it, and the table is frozen.

```
long read holds the table  →  schema change waits for it  →  every new query waits behind the schema change
```

The usual protection is a **lock timeout on the schema-change session**: it gives up after N
seconds and the queue clears. The scenario tests that protection, what happens without it, and
the case it cannot cover.

It is an **internal** scenario: Framework §10.6 (NL-N, internal noisy neighbour) has no
lock-pile-up row, so the id carries an `I` and is kept out of the Framework reconciliation
checks (catalog checks 2 and 6). Each run is a lettered variant:

| Run | Blocker | Schema change | Its lock timeout | Judged? |
|---|---|---|---|---|
| **NL-N-I03a** | a long read; the schema change follows 5 s later | add a column (instant once locked) | **5 s** | **yes** |
| **NL-N-I03b** | the same long read | add a column | **none** | no, a finding |
| **NL-N-I03c** | nothing | add a column whose default **rewrites every row** | 5 s | no, a finding |

- **3a:** the protection works. The schema change gives up (SQLSTATE 55P03) and the queue clears.
- **3b:** without it, the freeze lasts as long as the blocker (here: until the 60 s hold limit,
  when the harness ends the read).
- **3c:** the protection cannot help. The lock is granted at once, so the timeout never fires,
  and the table is frozen for the whole rewrite.

## 2. Setup

| Item | Value |
|---|---|
| Contended table | `resilience.lock_target`: harness-owned, 2,000,000 rows (~160 MB), kept between runs. It stands in for an application table until the TPC-C workload exists. |
| Workload | 64 workers, 200 TPS. Every transaction writes a marker row (`resilience.markers`). Half also read and update one random row of the contended table (the **table users**); the other half never touch it (the **others**). |
| Hold limit | `fault.duration` = 60 s: the longest the harness keeps the pile-up before committing the read itself. |
| Steady state | ≥ 150 TPS and p99 ≤ 300 ms in the 120 s baseline, or the run stops before any fault. |
| Settings | **Session settings only**, on the harness's own sessions: the schema change's lock timeout, no timeouts on the read, and none inside table-user transactions. The server's configuration is read, never written. |
| Machines | Harness: VM1 (10.11.21.13), checkout `~/resilience-nl-n-i03`. Target: VM2 (10.11.21.112), PostgreSQL on port 5433. Env profile `e2-dedicated-vm-u22-2`. |

## 3. How one run goes

1. **Reset / init:** refuse if the ledger shows a fault left by an earlier run; create or check
   the contended table, remove any leftover probe column, read the server's settings.
2. **Baseline (120 s):** undisturbed workload; steady state must hold.
3. **Fault inject:**
   - a tagged session (`resilience-lock-reader`) opens a read on the table;
   - a second tagged session (`resilience-lock-ddl`) issues the schema change with its own lock timeout;
   - a watcher checks the server every 0.1 s (`pg_stat_activity`, `pg_blocking_pids`). The fault is
     **confirmed** only if, within 3 s, the server shows the schema change held up and at least one
     workload session queued behind it.
4. **Hold:** until the schema change ends by itself (timeout fired, or rewrite done) or the
   60 s limit is reached; then the read is committed. T1 = the moment the queue cleared.
5. **Recovery:** watch the workload return to normal.
6. **Validate:**
   - compute the measures;
   - check every acknowledged commit is in the database (marker journals);
   - run `pg_amcheck`;
   - judge the accept lines.
7. **Report:** `results.json`, `summary.txt`, `report.html`.
8. **Cleanup:** stop the workload, end every tagged session on the server, drop the probe
   column, verify nothing is left. A failed cleanup means the run cannot pass, and the next run
   is refused until the kill switch has cleaned up.

## 4. Measures and acceptance

| Measure | Meaning |
|---|---|
| `fault_confirmed` | the server showed the blocking chain (the pile-up really formed) |
| `ddl_lock_timeout_fired` / `ddl_completed` | how the schema change ended |
| `queue_freeze_s` | how long the table was frozen (schema change issued → ended) |
| `queue_max_sessions` | largest queue behind the schema change |
| `blocker_visible` | the server named the schema change and what held it up |
| `longest_table_wait_s` | longest table-user transaction (capped by the workload's 10 s per-transaction limit) |
| `other_tables_tps_lost_pct` | throughput lost by the others in the minute after the schema change, against the baseline |
| `writer_max_stall_s` | longest gap in the write probe (own table, own connection) |
| `server_crash_restarts` | crash-restart lines in the server log after the fault |
| `rpo_txn`, `corruption_count`, `structural_integrity_errors` | lost commits, corruption and integrity errors |

**Accept lines:**
- **3a:**
  - `fault_confirmed == true`
  - `ddl_lock_timeout_fired == true`
  - `queue_freeze_s <= 6.0` (NEEDS SIGN-OFF)
  - `blocker_visible == true`
  - `server_crash_restarts == 0`
  - `rpo_txn == 0`
  - `corruption_count == 0`
  - `structural_integrity_errors == 0` (Framework §16.3)
- **3b / 3c (findings):** `fault_confirmed == true`, `rpo_txn == 0`, `corruption_count == 0`,
  `structural_integrity_errors == 0`. Everything else is reported, not judged.

A measure that cannot be taken is `NOT_MEASURED` and fails its accept line. It is never written as 0.

## 5. Results (lab target, 6 Nov 2026, all passed)

| | 3a | 3b | 3c |
|---|---|---|---|
| Schema change ended | timed out (55P03) | completed when the read was released | completed |
| Table frozen | **5.0 s** | **55.3 s** | **13.9 s** |
| Sessions queued | 64 | 64 | 64 |
| Longest table-user wait | 5.0 s | 10 s* | 10.9 s* |
| Others' throughput lost | 11% | 89% | 26% |
| Lost commits / crashes / corruption / integrity errors | 0 | 0 | 0 |

\* capped by the workload's 10 s per-transaction limit; the freeze column is the true length.

**What the results show:**
- A lock timeout bounds the freeze.
- Without one, a single long query plus a schema change freezes the table for as long as the
  query runs. Through the shared worker pool, even work that never touches the table nearly stops.
- A rewriting schema change freezes the table for the whole rewrite, with or without a timeout.

Run evidence on VM1:
- `~/resilience-runs/NL-N-I03a-20261106T124432Z-66f522`
- `~/resilience-runs/NL-N-I03b-20261106T132159Z-bcf486`
- `~/resilience-runs/NL-N-I03c-20261106T140112Z-d3f862`

## 6. Which file does what

Added beside the existing code. No existing scenario changed behaviour.

| File | Role |
|---|---|
| `catalog/NL/NL-N-I03a.yaml`, `NL-N-I03b.yaml`, `NL-N-I03c.yaml` | The three runs as data: fault, plan, workload, measures, accept lines. No host or tool named. |
| `catalog/schema.py` | Accepts internal ids (`-I`, lettered variants) and keeps them out of checks 2/6. Adds the `lock_pileup` fault type, the `fault.lock_pileup` block (blocker, schema change, lock timeout, delay), `workload.contended_share` and the `contended_table` capability, with validation. |
| `resilience_tests/control/faults/lock_pileup.py` | **Fault behaviour** (`LockPileupFault`, a `HeldFault`). Holds until the queue clears or the limit is reached, sets T1, computes the measures, writes the summary section. |
| `resilience_tests/execution/injectors/lock_pileup.py` | **Driver** (`OsSshLockPileupDriver`). Checks preconditions, starts the pile-up through the run's adapter, confirms it from the server's chain, reverts it. Also used by the kill switch. |
| `resilience_tests/adapters/postgresql/sessions.py` | **All pile-up SQL**: the tagged read and schema-change sessions, the chain watcher, state, release, revert (end tagged sessions, drop probe columns, verify). |
| `resilience_tests/adapters/postgresql/adapter.py` | Contended table (create, seed once, rebuild when column slots run low), the table-user transaction, crash-restart log patterns. |
| `resilience_tests/adapters/base.py` | Engine-neutral interface for the above. Engines without it refuse rather than fake it. |
| `resilience_tests/execution/workload/driver.py` | Contended mode: the share of table-user transactions, per-group 1 s samples (`group_sample` events). Off unless `contended_share` is set. |
| `resilience_tests/control/faults/__init__.py`, `execution/injectors/__init__.py` | Register the new behaviour and driver. |
| `resilience_tests/reporting/cases.py` | Report/JUnit case ids recognise internal ids. |
| `tests/test_lock_pileup.py` | Unit tests: catalog validation, workload groups, every measure including `NOT_MEASURED` paths, the driver. |
| `tests/test_lock_pileup_run.py` | All nine phases through the real orchestrator against a fake engine: pass and fail cases. |
| `tests/test_fault_behaviours.py` | Existing registry pins, extended with the new fault. |
| `specs/005-nl-n-i03-lock-pileup/` | Spec Kit feature: spec, plan, research, data model, contract, quickstart, tasks, this overview. |
| `envs/e2-dedicated-vm-u22-2.yaml` (VM1 checkout) | Environment profile: VM1 as harness, VM2 as target, ports, paths, phase limits. |

**Each run folder** (`~/resilience-runs/<run id>/`):

| File | Contents |
|---|---|
| `marker.jrnl` / `acked.jrnl` | workload journals on the harness host: written before each commit / after it was acknowledged |
| `results.json` | complete result: status, verdict, measures, baseline, phases, facts, disclosures |
| `events.jsonl` | timeline: workload and per-group samples, probes, server log lines, phases, T0/T1 |
| `integrity.txt` | `pg_amcheck` output |
| `summary.txt` / `report.html` | human-readable summary / web page with charts |

## 7. How to run (on VM1)

```bash
cd ~/resilience-nl-n-i03
.venv/bin/python -m catalog.schema --partial
.venv/bin/python -m pytest tests/test_lock_pileup.py tests/test_lock_pileup_run.py -q
.venv/bin/pytest resilience_tests/test_scenarios.py --env e2-dedicated-vm-u22-2 --scenario NL-N-I03a -q -rA
#   likewise NL-N-I03b, NL-N-I03c; add --stop-before-fault for a dry run
.venv/bin/python -m resilience_tests.control.killswitch --env e2-dedicated-vm-u22-2   # after a crashed run
```

Outside the checkout, a run uses only these:
- `~/.pgpass` (database password);
- `~/.ssh/id_ed25519` (SSH to VM2);
- `~/resilience-runs/` (output).