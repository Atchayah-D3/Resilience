# Research: pgbench as the Harness Workload

Phase 0 decisions for [plan.md](plan.md). Each entry gives the decision, the rationale and the alternatives considered. **R1 is open and team-owned**: it records what the mechanism must satisfy and how to judge candidates, not the design.

---

## R0 — Departure from Architecture §6.1

**Decision**: use pgbench for RPO-measuring scenarios as well, despite Architecture §6.1 assigning them to a custom driver.

**Rationale**: §6.1's reason is that RPO needs "application-level logic that pgbench cannot express": the transaction-marker protocol of §6.2. The departure is acceptable only if that protocol is kept in full. That is R1's job, and the plan gates RPO on it (constitution VII). The built-in driver stays as the reference implementation and fallback (FR-020).

**Alternatives considered**: pgbench for baseline and soak only, as §6.1 says. Rejected: the user wants pgbench as the workload for every scenario, with the built-in driver kept selectable.

---

## R1 — Before-commit and acknowledgement records (FR-005 to FR-007) — **DECIDED: option B, pgbench shell record steps**

**Status**: decided 2026-10-08, replacing the earlier "Hybrid Model" entry. That entry kept every marker scenario on the built-in driver, and all 12 catalog scenarios record markers, so under it pgbench ran no scenario at all. It also quoted Architecture §6.1 with a sentence the Architecture does not contain. What §6.1 says, in its workload-engine table, is: *"Custom asyncpg driver — Every RPO-measuring scenario; all DistDB multi-shard workloads — Transaction markers and 2PC-mode control need application-level logic that pgbench cannot express."* R0 records the departure from that row; this entry records how the logic is supplied to pgbench instead.

**Decision**: every pgbench transaction calls the harness for its two records, through pgbench shell steps and named pipes on the driver host (`resilience_tests/execution/workload/shell_records.py`):

```text
\setshell tok printf ... pre <launch> <client> 1<>q && read -r a < <reply> && echo "$a"
<decode tok into seq (+ ckey/creplace, rk/ak/vbase)>
BEGIN; <the adapter's transaction>; COMMIT;
[\shell printf ... c <launch> <seq> <read> <i> <chunk> 1<>q]      # list-append reads
\shell printf ... ack <launch> <seq> [len1 len2] 1<>q && read -r a < <reply> && test "$a" = ok
```

- `pre`: the service takes the next slot of the built-in driver's shared `RateLimiter`, assigns the run-wide `seq`, chooses the churn key / Elle keys with the built-in driver's seeded generators, appends the marker to `marker.jrnl` through the same group-committed `DurableJournal`, fdatasyncs, and only then replies. The client cannot send `BEGIN`, let alone `COMMIT`, before the record is durable.
- `ack`: sent only after the COMMIT succeeded; the service journals it to `acked.jrnl` and replies `ok`.
- Outcomes: committed = an `ack` arrived; definitely aborted = the same client's next `pre` arrived without an `ack` (pgbench goes on after a serialization or deadlock failure only, and aborts the client on any other error; verified on the lab build); unknown = the client ended with a transaction in flight.
- Each shell step is one `/bin/sh` with builtins only (`printf`, `read`, `test`, `echo`); the request pipe is opened read-write (`1<>q`) so a step never blocks on open.

**Why this meets TR-1 to TR-9** (contracts/transaction-record.md): the records are made by the same code as the built-in driver's, in the same order relative to the COMMIT; nothing is inferred from logs or timing.

**Alternatives considered**:
- Hybrid (marker scenarios on the built-in driver): rejected, it runs no scenario on pgbench.
- A PostgreSQL wire-protocol tap in the adapter, journalling `COMMIT` on the way through: no forks, but a protocol implementation to maintain, and Elle reads would have to be rebuilt from wire traffic. Kept as the fallback if the shell steps' cost proves too high on the lab.
- pgbench's `-l` log: written after the transaction and buffered, so it can never be a before-commit record.

**Verified behaviours of ShaktiDB 17 pgbench** (local 17.10.3.0 build, 2026-10-08; to be re-confirmed on the lab's 17.11.1.0 by the first live run):
- a shell command of 255 bytes or more aborts the client ("shell command is too long"), hence the read chunks;
- `\setshell` with a non-integer or empty result, and `\shell` with a non-zero exit, abort the client ("execution of meta-command failed");
- `:var` is substituted only in a whole meta-command argument;
- with `--max-tries=1`, a serialization failure skips the rest of the script and the client continues;
- abort lines: `client N aborted in command C (SQL) of script 0; perhaps the backend died while processing` (server crash) and `client N script 0 aborted in command C query 0: FATAL: ...` (terminated backend); no connection at start exits 1 with `could not create connection`; without `-n` pgbench tries to vacuum `pgbench_*` tables;
- no summary is printed on SIGTERM, so pgbench's per-command report cannot be the overhead disclosure (R8).

**Measured cost — local only, not the lab** (driver and database on one WSL2 workstation, real pgbench, real PostgreSQL; `marker` shape, 64 clients):

| Offered | Achieved | p50 / p99 latency | journal flush p99 | lost / phantom / unjournalled ack |
|---|---|---|---|---|
| 200 TPS | 199.96 TPS | 2.9 / 6.2 ms | 13.1 ms | 0 / 0 / 0 |
| 1000 TPS | 1002.4 TPS | 5.8 / 19.7 ms | 26.9 ms | 0 / 0 / 0 |

Faults, same setup: all 64 backends terminated (64 drops, 64 unknown, 64 relaunched, 0 lost); `pg_ctl stop -m immediate` held 3 s (load back to 197 TPS one second after restart, 0 lost); churn (2,000 live rows held, 0 lost); list-append (all 9,558 non-nil reads are prefixes of the final lists; every `:invoke` closed). A harness killed with SIGKILL leaves no pgbench and no shell step behind (`setpriv --pdeathsig`).

**Lab, pgbench (T049, in progress)** -- `e2-dedicated-vm`, pgbench (ShaktiDB) 17.11.1.0 at `/usr/lib/postgresql/17.11.1.0/bin/pgbench` on the driver host:

- **NL-C-01, 200 TPS offered, 64 clients: PASSED** (`run NL-C-01-20261008T092214Z-bbe290`, after dry run `NL-C-01-20261008T091922Z-3b42ff`): `rpo_txn = 0`, `rto_first_write_s = 0.67`, `structural_integrity_errors = 0`; 128 launches (64 + 64 relaunched after the kill), `dropped_connections = 64`, `indeterminate_txn = 70`, `failed_transactions = 0`, `connect_failures = 4`; baseline 199.99 TPS, p50 1.9 ms / p99 3.6 ms, 0 errors, 0 drops; record journal flush p50 2.6 ms / p99 10.5 ms; no pgbench left on the driver host. Baseline SLO held 114 of 119 s, so `rto_to_slo_s` is not measured (as often on this lab).
- **2026-10-08, every runnable scenario on pgbench:** PASSED NL-C-02, NL-C-03 (Elle check on the pgbench history), NL-C-05, NL-C-06, NL-I-01, NL-M-03, NL-M-07 (**1000 TPS sustained, 0 dropped connections, 0 failed transactions**; journal flush p99 7.9 ms; the built-in driver aborted at 369 TPS on 2026-10-07), NL-M-06, NL-R-04. No pgbench left after any run.
- NL-M-06 and NL-R-04 first aborted before the fault, `slower side: driver journal flush` (journal p99 1.5 s and 3.5 s while the database p99 was 5-9 ms; NL-M-07 two minutes later flushed at p99 7.9 ms at 5x the rate). Rerun with an fdatasync probe on the run directory every 10 s (worst p99 54 ms): both passed. The driver host's intermittent flush stall (CLAUDE.md) is the likely cause; the probe was not running during the first attempt.
- NL-M-05 FAILED on both generators (pgbench, and `--workload builtin`): the target's `idle_in_transaction_session_timeout` is 0 (observed, not changed) and no bloat-alert source is connected, so neither acceptance path holds. The workload's part held: churn built the dead tuples (ratio 0.75), vacuum blocked, `rpo_txn = 0`.
- NL-C-04: not runnable here (needs a dedicated `pg_wal` volume).

The built-in driver's lab results below stay as the reference.

> **Correction (verification 2026-10-07).** The two runs below ran on the **built-in** driver, not pgbench.

- **NL-C-01 (Process Kill, 200 TPS offered), built-in driver**: **PASSED** on `e2-dedicated-vm` (`run NL-C-01-20261007T110159Z-9f59f9`): `rpo_txn = 0`, `rto_first_write_s = 2.19s`, `structural_integrity_errors = 0`.
- **NL-M-07 (High Load, 1000 TPS offered, floor 750 TPS), built-in driver**: aborted before the fault: `steady state did not hold (tps=369.4, db p99=12.6 ms, journal p99=1000.9 ms; slower side: driver journal flush): ['tps >= 750.0']` (`run NL-M-07-20261007T123155Z-93d1d5`). The driver host's fsync, not the database, was the limit; pgbench uses the same journal, so expect the same limit there.

---

## R2 — Where the database specifics live

**Decision**: the PostgreSQL adapter supplies, behind a new `Capability.PGBENCH_WORKLOAD`:
- the pgbench transaction script for each shape (marker, churn, list-append);
- connection settings (host, port, database, user; the password still comes from `~/.pgpass`);
- the variables each script needs, such as the churn key space.

The supervisor treats scripts as opaque text. See [contracts/adapter-pgbench.md](contracts/adapter-pgbench.md).

**Rationale**: constitution V: SQL, tables and connection details belong to the adapter only. An engine without the capability makes the profile selection refuse (FR-004).

**Alternatives considered**: scripts as files in the repository read by the driver. Rejected: it puts engine knowledge above the adapter seam.

---

## R3 — Per-second samples (FR-009, FR-010)

**Revised (option B)**: the samples are built by the driver from the record service's own measurements: latency from the moment the client is released to `BEGIN` to the arrival of its `ack`, over every attempt (a definite abort is timed to the client's next `pre`, an unknown to the moment its client ended), exactly the built-in driver's accounting. Everything is on the harness's monotonic clock, so no wall-clock conversion is needed. pgbench's `--progress` is not used.

**Decision**: the supervisor builds the existing `sample` event (interval, commits, tps, p50/p95/p99, errors, indeterminate, drops, reconnects, connect_failures) once per second on the harness clock. It draws on per-transaction timing evidence from R1's records (latency = acknowledgement time − send time), outcome events from R5, and drop/relaunch events from R4. pgbench's `--progress=1` throughput is recorded beside it as a cross-check, never as the source of a pass/fail value.

**Rationale**: pgbench's progress output has throughput and average latency only, with no percentiles, and its per-transaction `-l` log is buffered until exit. The baseline, the steady-state check and time-to-SLO all need live p99. Building the same event as today keeps `rto_decomposer` and `baseline_slo_check` unchanged.

**Clock**: record timestamps are driver-host wall time. The supervisor captures the offset between wall time and the harness's monotonic clock at each launch and converts timestamps into the event stream's `t_mono_ns`. Same host, so no cross-host skew.

**Alternatives considered**:
- pgbench `--aggregate-interval=1`: no percentiles, and buffered.
- Parsing the `-l` log: buffered, so not live.

Both are kept as post-run cross-checks only.

---

## R4 — Supervision and relaunch (FR-012, FR-014, FR-017)

**Revised (option B)**: a client is relaunched for as long as the run lasts, as a built-in worker reconnects; one shared probe (`adapter.session()` + `ping`, every 0.2 s) serves every waiting client, and each failed probe counts one connection failure. A pgbench that could not connect at start (exit 1, `could not create connection`) counts a connection failure and is retried. A transaction in flight longer than `TXN_TIMEOUT_S` (10 s) has its client killed and relaunched, its outcome unknown, as the built-in worker abandons its session. A client aborted by a meta-command (a record step) or a pgbench that exits on its own fails the run.

**Decision**: run **one pgbench process per client** (`-c 1 -j 1`), each offering `rate / concurrency` transactions per second, **evenly spaced** (R11, not pgbench's `-R`), with a run duration longer than any run (`-T`), and its own launch number. The supervisor:
- watches each process's exit and stderr;
- on a client abort, counts **one dropped connection**, marks that client's in-flight transaction unknown, and relaunches the client with a new launch number once the adapter reports the database accepting connections, retrying every 0.2 s, as today's reconnect loop does;
- treats a process exit with the database healthy and no abort message as a **workload failure** (FR-014);
- caps relaunch attempts by the phase bound (edge case "database never returns").

**Rationale**: pgbench never re-adds an aborted client. Restarting a shared multi-client pgbench would kill healthy sessions mid-transaction. Per-client processes make partial loss (US3-2) observable and repairable.

**Alternatives considered**:
- One `-c N` process restarted after any abort: disturbs healthy sessions, which falsifies NL-R-04 and NL-M-07.
- N processes in groups: middle ground, more complex accounting; rejected for clarity.

---

## R5 — Outcome classification (FR-008)

**Revised (option B)**: definitely aborted is detected per transaction, not from pgbench's counters: the same client's next `pre` arriving without an `ack` (see R1).

**Decision**:
- **Committed**: an acknowledgement record exists (R1).
- **Definitely aborted**: pgbench reports a serialization or deadlock **failure** for the transaction (`--failures-detailed`, `--max-tries=1`); the server rejected it explicitly.
- **Unknown**: everything else, including a client aborted on any other error, a broken connection, or a timeout.

NL-M-07's `failed_transactions` counts definitely-aborted transactions; `dropped_connections` counts aborted clients (R4).

**Rationale**: matches Architecture §7.1 and constitution VII. Anything not explicitly rejected by the server must be unknown, never failed.

**Alternatives considered**: counting every pgbench "error" as failed. Rejected: it would turn unknown outcomes into definite ones, a false-precision risk.

---

## R6 — Transaction shapes (FR-015, FR-016)

**Revised (option B)**: the adapter's script is the built-in driver's transaction statement for statement, inside `BEGIN`/`COMMIT` (list-append at `SERIALIZABLE`, on `resilience.elle_lists`). The marker uuid is `md5('resilience-pgbench-' || seq)::uuid`. The service chooses churn keys and Elle keys with the built-in driver's seeded generators and hands them to the script in the token.

**Decision**: the adapter's scripts reproduce today's shapes exactly:
- **marker**: insert `(uuid-equivalent identity, seq)` into `resilience.markers`;
- **churn**: same transaction plus an update of `resilience.churn` on a random key in `1..churn_key_space`, with one in `CHURN_REPLACE_EVERY` a delete and re-insert, using pgbench's `random()` and `\if`;
- **list-append**: Elle's read and append on `resilience.lists`, with each read's returned value captured via `\gset` and written to the operation history **through the same channel as R1's records**. That makes list-append history dependent on R1.

The marker identity is the `(launch, client, seq)` triple. The script stores it as `md5('L<launch>-C<client>-S<seq>')::uuid` in the existing `uuid` column, and the harness derives the same UUID for each record, so `marker_ids()` returns identities comparable with the records with no schema change.

**Rationale**: identical shapes keep NL-C-05/NL-M-05 measurements and NL-C-03's history comparable with today's driver (spec Assumptions).

**Alternatives considered**: pgbench's built-in `tpcb-like` script. Out of scope for this feature (spec Assumptions).

---

## R7 — Per-client sequence numbers

**Revised (option B)**: superseded. `seq` is assigned run-wide by the record service, as `MarkerJournals` does for the built-in driver, so it is unique across launches and is also a valid Elle append value (Elle needs every appended value unique per key).

**Decision**: each pgbench process starts with `-D seq=0` and the script increments `seq` per transaction. Identity = `(launch, client, seq)`, where `launch` is unique per process start.

**Verification on the lab build** (quickstart step V2): **CONFIRMED**. Running pgbench with `-D seq=0` and `\set seq :seq + 1` inserted `1, 2, 3, 4, 5` consecutively, confirming that ShaktiDB 17's pgbench persists and increments `\set` variables across transactions within a client session as expected.


**Alternatives considered**: random 64-bit identities. Collision probability is acceptable, but they're not ordered, which loses the per-client ordering used to sanity-check records.

---

## R8 — Recording overhead (FR-011)

**Revised (option B)**: pgbench prints no summary when it is stopped, so the disclosure is the service's journal flush time per transaction (`journal_p99_ms` in every sample, `pgbench_record_journal_p50_ms` / `_p99_ms` in the report), as for the built-in driver. The ack step's shell run is inside the measured latency; the report states it.

**Decision**: each pgbench process runs with `--report-per-command`. At each launch's end, its summary gives the average latency of every script command, including the record steps. The report states the record steps' share of transaction latency, averaged over launches.

**Rationale**: it's pgbench's own measurement, with no extra instrumentation.

**Alternatives considered**: comparing with a run on the built-in driver. Kept as a manual comparison (FR-020), not as the disclosure.

---

## R9 — Process lifecycle and cleanup (FR-017, SC-007)

**Decision**: every pgbench process:
- starts in its own process group;
- carries `PGAPPNAME=resilience-pgbench-<run_id>`, so its sessions are identifiable on the target too.

At stop, cleanup or interrupt, the supervisor terminates every group and confirms none remains, by its own process table and by `pg_stat_activity` on the target through the adapter. A remaining process makes cleanup fail, and the run cannot report `passed` (constitution VI).

**Alternatives considered**: relying on the parent harness's exit. Rejected: an orphaned pgbench would keep loading the target after a harness crash.

---

## R10 — Profile selection (FR-020)

**Decision**: new optional profile section `workload: {generator: pgbench | builtin, pgbench_bin: <path>}`:
- `generator` defaults to `pgbench`;
- `pgbench_bin` defaults to `pgbench` on the driver host's `PATH`.

The factory builds the selected driver. A driver that cannot run refuses; there is never an automatic switch. Every report records `workload_generator`. See [contracts/profile-workload.md](contracts/profile-workload.md).

**Alternatives considered**: a catalog field. Rejected: constitution I keeps tooling out of scenarios.

---

## R11 — Even spacing of transactions (FR-021, clarification 2026-10-07) — **revised for option B (2026-10-08)**

**Decision**: the record service paces the load, not the pgbench script and not `-R`. Each `pre` request waits for the next slot of the built-in driver's own `RateLimiter`, shared by all clients: slots are evenly spaced at 1/rate, whichever client asks next takes the next slot, and a slot missed is never made up with a burst. That is the built-in driver's arrival pattern exactly.

**Rationale**: the user chose even spacing so pgbench and built-in runs compare like for like (clarification 2026-10-07). With option B every transaction already asks the harness before it starts (R1), so pacing there costs nothing extra and reuses the reference implementation instead of re-creating it inside pgbench. A shared limiter also lets a healthy client take a slot a stalled client cannot use, as the built-in workers do; per-client pacing would lose those slots.

**Evidence**: the per-second samples show the achieved rate against the declared one (lab: 199.99 TPS for 200 offered, 981 for 1000). There is no separate scheduling-lag figure: a run that cannot keep its rate shows it in the steady-state check, which names the limiting side.

**Alternatives considered**:
- `-R` (Poisson): rejected by the clarification; random spacing raises p99 through bursts.
- In-script pacing (the decision before option B): each client sleeps with `\sleep` until its next slot, timing itself from a database clock read. Superseded: per-client slots cannot be shared, and it duplicates the limiter the record service already has.
- A fixed `\sleep interval` per transaction: the achieved rate falls below the declared one by the transaction time, so the offered load would silently differ from the scenario's.
- The harness starting each transaction itself: one process per transaction; far too heavy.

---

## Baseline (T001)

- Date: 2026-10-07
- Branch: `pgbench`
- `.venv/bin/pytest -q -n 4`: 336 passed, 2 failed (2 Elle tests in `tests/test_elle.py` requiring Java 21 `SequencedCollection`)
- `.venv/bin/python -m catalog.schema --partial`: PASSED (all 6 checks PASS)

---

## Final Check & Lab Audit (T053)

- Date: 2026-10-07
- Branch: `pgbench`
- **Catalog schema**: `.venv/bin/python -m catalog.schema --partial` → All 6 checks **PASS**.
- **Unit and Contract test suite**: 36 of 36 **PASSED** on the harness VM (`BDB-QA-U22-29`).
- **Full suite (verification 2026-10-07, after the fixes below)**: 376 passed, 2 failed (the 2 Elle tests that need Java 21, as at baseline); catalog checks pass.
- **Note on CT-1 to CT-6**: they exercise `JournalRecordChannel` with the test making the record calls itself; no pgbench transaction is involved, so they do not certify a pgbench record mechanism. They must be rewritten to drive real pgbench transactions through whichever mechanism is chosen.
- **Orchestrator test suite**: 31 of 31 **PASSED**.
- **Live scenario NL-C-01 (built-in driver; see the R1 correction)**: **PASSED** on `e2-dedicated-vm` (`1 passed in 188.82s`, `rpo_txn = 0`, `rto_first_write_s = 2.19s`, `structural_integrity_errors = 0`).
- **Live scenario NL-M-07 (built-in driver)**: Cleanly aborted before fault (`slower side: driver journal flush`), successfully proving limiting-side detection under driver-host disk IOPS bottlenecks.
- **Process cleanup**: `pgrep -f resilience-pgbench` returned zero processes (`CLEAN: no orphaned processes`).

### Verification fixes (2026-10-07)

- An acknowledgement without its before-commit record no longer counts as a 0 ms latency; the run fails closed (TR-1). Test: `test_ack_without_before_commit_record_fails_closed`.
- Without a record channel, p50/p95/p99 are `None` instead of percentiles over pgbench's per-second means. Test: `test_progress_fallback_reports_no_latency_percentiles`.
- `application_name`: the adapter supplies the prefix `resilience-pgbench`, the driver appends `-<run_id>`, and the cleanup check queries every name actually used. Test: `test_cleanup_checks_the_application_names_actually_used`.
- Still open: per-transaction pacing (FR-021, R11) is not implemented; the adapter's scripts never use `interval_us`, so each client runs unthrottled. Task T029 is unchecked accordingly.

### Option B implementation (2026-10-08)

- R1 decided as option B and built (`shell_records.py`, rewritten `pgbench_driver.py`); `record_channel.py` and the record-channel registry are removed. The factory no longer refuses marker scenarios: pgbench carries the markers itself.
- CT-1 to CT-6 rewritten to drive real record steps through pgbench transactions (the fake pgbench runs them through `/bin/sh`); the note above about the old CT tests no longer applies.
- The abort-line parser did not match real pgbench output (`pgbench: error: ` prefix, `script N aborted ... query N` form), so every drop would have been a fatal workload failure; fixed against recorded lines.
- The pgbench report facts were never recorded (they were read after `_stop_load` had cleared the workload); now recorded when the load stops.
- Every orchestrator scenario test runs on both generators (`env` fixture), at 8 clients and 40 TPS for the fake pgbench.
- Profiles switched to `generator: pgbench` (T052) on the branch; `pytest --workload builtin` selects the fallback for one run.
- Local unit suite (Python 3.11): all pass except the 2 Elle tests that need Java 21, as at baseline.

