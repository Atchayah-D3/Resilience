# Contract: Before-Commit and Acknowledgement Records (research R1)

**Status**: the mechanism is a **team-owned decision**, not yet made. This contract is normative: whatever mechanism is chosen must pass every test below before the pgbench driver may measure RPO. Until then, the factory refuses `generator: pgbench` for any scenario with `workload.transaction_markers: true`, which is every current scenario. The built-in driver remains available (FR-020).

## Requirements

| # | Requirement | Source |
|---|---|---|
| TR-1 | For every transaction, a record with its identity is durable on the driver host **before its COMMIT is sent** | Arch §6.2, constitution VII, FR-005 |
| TR-2 | A second durable record is made **only after** the database acknowledges the COMMIT | FR-005 |
| TR-3 | If TR-1 fails, the transaction does not commit, or the run aborts | spec US2-2 |
| TR-4 | If TR-2 fails after a successful commit, the run detects it and issues no RPO figure | spec US2-3 |
| TR-5 | Records land in, or convert losslessly into, `marker.jrnl` / `acked.jrnl` (format: data-model.md) | FR-007 |
| TR-6 | Identities are unique across the run, including relaunches | FR-006 |
| TR-7 | Each record carries a driver-host timestamp, used for latency and samples | FR-009, research R3 |
| TR-8 | The added per-transaction time is measurable from pgbench's per-command report | FR-011, research R8 |
| TR-9 | For list-append scripts, the values read are recorded to the operation history through the same guarantees | FR-016, research R6 |

## Contract tests (must pass, with the fake pgbench and on the lab)

- **CT-1, order.** For a run's records, every acknowledged identity's before-commit record has `t_pre <` the commit-send time observed by the fake database, and the database-side insert time.
- **CT-2, kill mid-load.** Kill the database during load. Acknowledged ⊆ before-commit records, unknown outcomes are reported, and the RPO arithmetic gives 0 lost for an honest database.
- **CT-3, a lying database.** A fake database that acknowledges and then drops a commit: the run reports `rpo_txn ≥ 1`. This is the negative test proving the criterion can fail (constitution III).
- **CT-4, record failure before commit.** Make the before-commit record fail: that transaction never commits, or the run aborts. Never "acknowledged without a record".
- **CT-5, acknowledgement failure.** Make the acknowledgement record fail after a commit: the run issues no RPO figure.
- **CT-6, relaunch.** Relaunch clients repeatedly: no identity repeats, and the journals stay consistent.
- **CT-7, throughput.** On the lab driver host, report the sustained rate at NL-C-01's 200 TPS and NL-M-07's 1000 TPS offered, with the added per-transaction time (spec US5).
