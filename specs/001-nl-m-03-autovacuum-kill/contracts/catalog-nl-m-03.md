# Contract: NL-M-03 catalog entry and schema delta

## Schema delta (`catalog/schema.py`)

```text
FaultDuring = Literal["checkpoint", "large_transaction", "concurrent_index_build",
                      "autovacuum_worker"]          # + new value
```
No other schema change: `repeat`, `requires`, measures and accept predicates already exist.

## Scenario file (`catalog/NL/NL-M-03.yaml`) -- required content

| Field | Value | Source |
|---|---|---|
| id / category / tier | NL-M-03 / NL-M / node_local | Framework §10.7 |
| priority | P1 | Framework §10.7 |
| applies_to | standalone, distdb.pc, distdb.qc, distdb.worker | Framework §10 (all instances) |
| environment | classes E1-E4, env_sensitive: false | Framework §7.3 (NL-M is engine-internal) |
| workload | profile mixed, markers true, rate 200, concurrency 64 | as NL-C-05 (NEEDS SIGN-OFF) |
| fault | type process_kill, driver os_ssh, target primary, during autovacuum_worker | Framework §10.7 "kill -9 an autovacuum worker" |
| repeat | cycles 5, interval_s 10 | "repeatedly"; 5 = NEEDS SIGN-OFF (clarified 2026-10-06) |
| requires | transactional_markers, workload_churn, structural_integrity_check | |

### measure (all must be produced or reported NOT_MEASURED with a reason)

rpo_txn, indeterminate_txn, rto_first_write_s, starts_unattended, cycles_run,
cycles_recovered, kills_landed, autovacuum_worker_respawned, relations_eligible,
relations_left_unvacuumed, corruption_count, structural_integrity_errors

### accept

```text
kills_landed == 5                    # each counted kill hit our live autovacuum worker
cycles_recovered == 5
autovacuum_worker_respawned == true  # Framework §10.7: a new worker spawns
relations_left_unvacuumed == 0       # Framework §10.7: none left permanently unvacuumed
rpo_txn == 0                         # Framework §10.2: each kill is a crash
starts_unattended == true            # Framework §10.2
corruption_count == 0
structural_integrity_errors == 0     # Framework §16.3
```

The header comment states: the Framework row, that each kill is a crash-restart of the whole
instance, the derived bounds (worker wait 2 x naptime + 30 s; verification 3 x naptime +
vacuum time), the harness-owned table and its storage options, and NEEDS SIGN-OFF items.
