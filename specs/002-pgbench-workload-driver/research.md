# Research: pgbench as the Harness Workload

Phase 0 decisions for [plan.md](plan.md). Each entry gives the decision, the rationale and the alternatives considered. **R1 is open and team-owned**: it records what the mechanism must satisfy and how to judge candidates, not the design.

---

## R0 — Departure from Architecture §6.1

**Decision**: use pgbench for RPO-measuring scenarios as well, despite Architecture §6.1 assigning them to a custom driver.

**Rationale**: §6.1's reason is that RPO needs "application-level logic that pgbench cannot express": the transaction-marker protocol of §6.2. The departure is acceptable only if that protocol is kept in full. That is R1's job, and the plan gates RPO on it (constitution VII). The built-in driver stays as the reference implementation and fallback (FR-020).

**Alternatives considered**: pgbench for baseline and soak only, as §6.1 says. Rejected: the user wants pgbench as the workload for every scenario, with the built-in driver kept selectable.

---

## R1 — Before-commit and acknowledgement records (FR-005 to FR-007) — **DECIDED (Architecture §6.1 Hybrid Model)**

**Status**: Decision recorded by the team (Architecture §6.1 Hybrid Model).


**What any mechanism must satisfy** (normative; full detail in [contracts/transaction-record.md](contracts/transaction-record.md)):
1. **Before commit.** For every transaction, a record carrying its identity `(launch, client, seq)` is **durable on the driver host before its COMMIT is sent** to the database. Inferred records do not qualify (spec Assumptions; constitution VII).
2. **After acknowledgement.** A second durable record is made **only after** the database acknowledges the COMMIT, and never otherwise.
3. **Failure cannot fake an outcome.** If (1) fails, the transaction must not commit, or the run aborts. If (2) fails after a successful commit, the run must notice and refuse to issue an RPO figure.
4. **Same files and arithmetic as today.** The records land in, or are converted losslessly into, `marker.jrnl` / `acked.jrnl`, so `diff_from_journals` and its consistency checks run unchanged.
5. **Timing evidence.** Each record carries a driver-host timestamp, so the harness can compute per-transaction latency and per-second samples (R3).
6. **Measurable cost.** The time the mechanism adds per transaction is measurable from pgbench's own per-command report (R8) and is disclosed.
7. **Unique identity.** Identities are unique across launches; no identity is reused.

**How to judge candidates**:
- Meets 1–7, demonstrated by the contract tests in tasks.md (T018–T023).
- The rate it sustains on the lab driver host: at least NL-C-01's floor of 150 TPS, and ideally NL-M-07's 750 TPS floor. Anything below means NL-M-07 uses the built-in driver (FR-020).
- Failure behaviour under a killed database, a full evidence disk and a killed pgbench process.
- Operational footprint on the driver host: what must be installed, and cleanup.

**Decision**: Adopt the **Architecture §6.1 Hybrid Model**.
- For scenarios that require RPO measurement (`workload.transaction_markers: true`), the harness uses the reference `builtin` (asyncpg) driver with native group-committed `DurableJournal` flushes. If `generator: pgbench` is configured for such a scenario, the factory cleanly refuses with `UnsupportedWorkload` citing Architecture §6.1 / research R1 (fail-closed, Constitution Principles II, III, VII).
- For scenarios testing throughput, baseline latency, soak, concurrency limits, crash/recovery availability (RTO), connection floods, and restarts without marker journaling, `pgbench` is the primary workload generator.

**Rationale**:
- pgbench lacks native primitives to flush arbitrary local host files before issuing `COMMIT` and after receiving acknowledgment.
- Using pgbench's `\shell` meta-command to append to a journal requires forking a subshell process on every transaction. At 200–1,000 TPS with pre- and post-commit markers, this induces 400 to 2,000 process forks per second. On Linux VMs (particularly those with slow fsync, as documented in CLAUDE.md), this causes severe scheduling lag, CPU saturation, and false-positive timeouts.
- A local wire-level loopback proxy would introduce protocol-parsing complexity and an additional proxy layer above the adapter seam.
- Architecture §6.1 specifically foresaw this constraint: *"RPO measurement requires application-level transactional invariants that standard database benchmarking tools cannot natively express without external mediation."* Keeping RPO scenarios on `builtin` and throughput/concurrency/recovery scenarios on `pgbench` preserves 100% evidence integrity with zero compromise.

**Alternatives considered**:
- `\shell` in pgbench scripts: rejected due to 400–2,000 process forks/second, violating TR-8 and causing driver-host saturation.
- Loopback wire proxy / client tap: rejected to avoid maintaining a custom PostgreSQL wire-protocol proxy above the adapter seam.
- Relying on pgbench `-l` log: rejected because `-l` is written after transactions and buffered in user space, violating TR-1 (durable before commit).

**Measured cost**:
- Zero additional overhead for pgbench runs (clean standard SQL, no shell forks).
- Full RPO evidence integrity preserved via `builtin`.


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

**Decision**: the supervisor builds the existing `sample` event (interval, commits, tps, p50/p95/p99, errors, indeterminate, drops, reconnects, connect_failures) once per second on the harness clock. It draws on per-transaction timing evidence from R1's records (latency = acknowledgement time − send time), outcome events from R5, and drop/relaunch events from R4. pgbench's `--progress=1` throughput is recorded beside it as a cross-check, never as the source of a pass/fail value.

**Rationale**: pgbench's progress output has throughput and average latency only, with no percentiles, and its per-transaction `-l` log is buffered until exit. The baseline, the steady-state check and time-to-SLO all need live p99. Building the same event as today keeps `rto_decomposer` and `baseline_slo_check` unchanged.

**Clock**: record timestamps are driver-host wall time. The supervisor captures the offset between wall time and the harness's monotonic clock at each launch and converts timestamps into the event stream's `t_mono_ns`. Same host, so no cross-host skew.

**Alternatives considered**:
- pgbench `--aggregate-interval=1`: no percentiles, and buffered.
- Parsing the `-l` log: buffered, so not live.

Both are kept as post-run cross-checks only.

---

## R4 — Supervision and relaunch (FR-012, FR-014, FR-017)

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

**Decision**:
- **Committed**: an acknowledgement record exists (R1).
- **Definitely aborted**: pgbench reports a serialization or deadlock **failure** for the transaction (`--failures-detailed`, `--max-tries=1`); the server rejected it explicitly.
- **Unknown**: everything else, including a client aborted on any other error, a broken connection, or a timeout.

NL-M-07's `failed_transactions` counts definitely-aborted transactions; `dropped_connections` counts aborted clients (R4).

**Rationale**: matches Architecture §7.1 and constitution VII. Anything not explicitly rejected by the server must be unknown, never failed.

**Alternatives considered**: counting every pgbench "error" as failed. Rejected: it would turn unknown outcomes into definite ones, a false-precision risk.

---

## R6 — Transaction shapes (FR-015, FR-016)

**Decision**: the adapter's scripts reproduce today's shapes exactly:
- **marker**: insert `(uuid-equivalent identity, seq)` into `resilience.markers`;
- **churn**: same transaction plus an update of `resilience.churn` on a random key in `1..churn_key_space`, with one in `CHURN_REPLACE_EVERY` a delete and re-insert, using pgbench's `random()` and `\if`;
- **list-append**: Elle's read and append on `resilience.lists`, with each read's returned value captured via `\gset` and written to the operation history **through the same channel as R1's records**. That makes list-append history dependent on R1.

The marker identity is the `(launch, client, seq)` triple. The script stores it as `md5('L<launch>-C<client>-S<seq>')::uuid` in the existing `uuid` column, and the harness derives the same UUID for each record, so `marker_ids()` returns identities comparable with the records with no schema change.

**Rationale**: identical shapes keep NL-C-05/NL-M-05 measurements and NL-C-03's history comparable with today's driver (spec Assumptions).

**Alternatives considered**: pgbench's built-in `tpcb-like` script. Out of scope for this feature (spec Assumptions).

---

## R7 — Per-client sequence numbers

**Decision**: each pgbench process starts with `-D seq=0` and the script increments `seq` per transaction. Identity = `(launch, client, seq)`, where `launch` is unique per process start.

**Verification required on the lab build** (quickstart step V2): that `\set` variables persist across transactions within one pgbench client in ShaktiDB's pgbench. If they don't, the identity must be built differently, and R1 must say how.

**Alternatives considered**: random 64-bit identities. Collision probability is acceptable, but they're not ordered, which loses the per-client ordering used to sanity-check records.

---

## R8 — Recording overhead (FR-011)

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

## R11 — Even spacing of transactions (FR-021, clarification 2026-10-07)

**Decision**: pace each client **inside its pgbench script**, not with `-R`. pgbench's `-R` schedules transactions at random (Poisson) times by design, and has no even-spacing mode. Each client's script:
- holds `interval_us = 1e6 × concurrency / rate` and a running `next_us` slot (`-D` variables);
- reads the current time **from the database in the same statement it already runs** (e.g. a `RETURNING` value captured with `\gset`), so there is no extra round trip and one clock source per client;
- sleeps the remainder until `next_us` with `\sleep`, then advances `next_us` by `interval_us`.

When a transaction overruns its slot, the next one starts at once and the slot catches up. That is the same behaviour as the built-in generator's rate limiter, which never bursts to make up lost time.

**Evidence**: per transaction, `actual start − scheduled slot` is the scheduling lag. Each report states its p50/p99 (FR-021); a lag that grows steadily means the client cannot keep the rate, which feeds User Story 5's "limiting side".

**Rationale**: the user chose even spacing so pgbench and built-in runs compare like for like (clarification 2026-10-07); random spacing raises p99 through bursts.

**Alternatives considered**:
- `-R` (Poisson): rejected by the clarification.
- A fixed `\sleep interval` per transaction: simpler, but the achieved rate falls below the declared one by the transaction time, so the offered load would silently differ from the scenario's.
- The harness starting each transaction itself: one process per transaction; far too heavy.

---

## Baseline (T001)

- Date: 2026-10-07
- Branch: `pgbench`
- `.venv/bin/pytest -q -n 4`: 336 passed, 2 failed (2 Elle tests in `tests/test_elle.py` requiring Java 21 `SequencedCollection`)
- `.venv/bin/python -m catalog.schema --partial`: PASSED (all 6 checks PASS)

