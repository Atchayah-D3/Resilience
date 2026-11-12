# Data Model: NL-N-I03

- **fault.lock_pileup**: `blocker` (long_read | none), `ddl` (add_column | add_column_rewrite),
  `ddl_lock_timeout_s` (>= 0; 0 = none), `ddl_after_s` (> 0 with long_read, 0 with none,
  < fault.duration). Required iff `fault.type == lock_pileup`.
- **workload.contended_share**: (0, 1); only with profile oltp_write_heavy and history none;
  needs `requires: contended_table`; required by lock_pileup.
- **resilience.lock_target**: id bigint PK, val bigint, updated_at timestamptz; 2,000,000 rows (~150-200 MB);
  rebuilt when pg_attribute slots > 1,000.
- **Pile-up state** (adapter): reader_pid, ddl_pid, ddl_issued/ended_mono_ns, ddl_outcome
  (lock_timeout | completed | cancelled | error | None), ddl_sqlstate, chain_confirmed, chain,
  queue_max, longest_wait_s.
- **group_sample** event: group (table_users | others), interval_s, commits, tps, p99_ms, max_ms,
  errors, indeterminate.
