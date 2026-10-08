# Contract: Before-Commit and Acknowledgement Records (research R1)

**Status**: decided, research R1 option B. Implemented by `resilience_tests/execution/workload/shell_records.py`. Every pgbench transaction calls the harness's journal service through two pgbench shell steps; the service writes the same `marker.jrnl` / `acked.jrnl`, with the same `DurableJournal`, as the built-in driver.

## Requirements and how they are met

| # | Requirement | Source | Met by |
|---|---|---|---|
| TR-1 | For every transaction, a record with its identity is durable on the driver host **before its COMMIT is sent** | Arch §6.2, constitution VII, FR-005 | the script's first step (`\setshell`) blocks until the service has fdatasync'd the marker and replied; `BEGIN` comes after it |
| TR-2 | A second durable record is made **only after** the database acknowledges the COMMIT | FR-005 | the `ack` step follows `COMMIT` in the script; pgbench runs it only if the COMMIT succeeded |
| TR-3 | If TR-1 fails, the transaction does not commit, or the run aborts | spec US2-2 | the service replies `err`, `\setshell` gets no integer, pgbench aborts the client before `BEGIN`; the run fails |
| TR-4 | If TR-2 fails after a successful commit, the run detects it and issues no RPO figure | spec US2-3 | the service fails the run (`workload driver failed`), which aborts it without a verdict |
| TR-5 | Records land in `marker.jrnl` / `acked.jrnl` (format: data-model.md) | FR-007 | the same `MarkerJournals` object as the built-in driver |
| TR-6 | Identities are unique across the run, including relaunches | FR-006 | `seq` is assigned by the one service for the whole run |
| TR-7 | Each record carries a driver-host timestamp, used for latency and samples | FR-009, research R3 | `t_pre` / `t_ack`; latency is measured by the service, from release to acknowledgement |
| TR-8 | The added per-transaction time is measurable | FR-011, research R8 | journal flush time per transaction (`journal_p99_ms`, `pgbench_record_journal_*`); the ack step's shell run is inside the latency, disclosed |
| TR-9 | For list-append scripts, the values read are recorded to the operation history through the same guarantees | FR-016, research R6 | reads travel as chunks before the `ack`; a read that does not reassemble to its stated length fails the run |

## Contract tests

With the fake pgbench, whose record steps run through `/bin/sh` for real (`tests/test_transaction_record.py`):

- **CT-1, order.** Every committed marker has a before-commit record with `t_pre` earlier than the fake database's commit time.
- **CT-2, kill mid-load.** The database goes down during load and returns: acknowledged ⊆ written, `rpo_txn == 0`, no torn line, no unjournalled acknowledgement, no phantom.
- **CT-3, a lying database.** Commits acknowledged and not kept: `rpo_txn` counts them (the negative test, constitution III).
- **CT-4, record failure before commit.** The marker journal fails: the run fails, and no transaction commits without a before-commit record.
- **CT-5, acknowledgement failure.** The acknowledgement journal fails after a commit: the run fails; no acknowledgement claims it.
- **CT-6, relaunch.** Two outages and relaunches: no `uuid` or `seq` repeats, journals consistent.
- **CT-7, throughput.** On the lab: NL-C-01 at 200 TPS and NL-M-07 at 1000 TPS offered (tasks T049).

Race and ordering cases are covered in `tests/test_shell_records.py` (an acknowledgement left in the pipe by a client that died; a client stopped while its marker was being flushed).
