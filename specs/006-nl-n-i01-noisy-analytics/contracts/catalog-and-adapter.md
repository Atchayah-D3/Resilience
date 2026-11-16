# Contracts: catalog fields, measures, adapter methods

## Catalog

```yaml
needs_infra: [analytics_login]           # the env file names the report login (node.analytics)
requires: [transactional_markers, tpcc_workload, analytics_sessions]
workload:
  profile: tpcc
  concurrency: 64
  transaction_markers: true
  rate_fraction_of_capacity: 0.6         # exactly one of rate_tps / rate_fraction_of_capacity
  latency_from: intended_start
  tpcc:
    warehouses: by_cache                 # or a number
    active_warehouses: by_cache          # or a number; absent: every warehouse is active
    order_line_cache_multiple: 8         # with warehouses: by_cache
    active_cache_share: 0.5              # with active_warehouses: by_cache
fault:
  type: analytics_load
  duration: 300                          # seconds; required
  analytics_load:
    full_table_sessions: by_cores        # ceil(cores / 3), or a number
    index_sessions: 1
    limit_tests: true                    # limit test 1 and 2 as first query of full-table sessions 1, 2
```

## Measures

| Measure | Meaning | Gate |
|---|---|---|
| fault_confirmed | at minute 1 every report session active and at least one helper seen | == true |
| analytics_active_share | share of hold samples with every report session open | reported |
| oltp_tps_ratio, oltp_p99_ratio | customers in the hold over the baseline (HeldFault) | >= 0.70, <= 2.0 (Framework NL-N-07) |
| customer_p90_max_s | worst 1-second p90 of New-Order, Payment, Order-Status in the hold | <= 5.0 (TPC-C 5.2.5.3) |
| customer_cache_hit_ratio_baseline / _hold | customers' own tables | reported |
| customer_cache_hit_ratio_vs_baseline | hold over baseline | >= 0.80 (Framework NL-N-07) |
| report_helpers_max | most helpers of one report in any sample | reported |
| helpers_over_limit | samples where a report had more helpers than the role allows | == 0 |
| temp_limit_enforced | limit test 1 ended with SQLSTATE 53400 | == true |
| statement_timeout_enforced | limit test 2 ended with 57014 within the timeout + 1 s | == true |
| statement_timeout_overrun_s | limit test 2's run time minus the timeout | reported |
| customer_limit_errors | customer transactions failed with 53300, 53400, 57014 | == 0 |
| rto_to_slo_s | back to normal after the reports stopped (95% TPS, 1.5x p99, 60 s) | <= 60 |
| analytics_sessions_left, temp_files_left | after the run | == 0 |

## Adapter (AnalyticsSupport; PostgreSQL in adapters/postgresql/analytics.py)

- `observe_analytics_settings() -> dict[str, str | None]`: shared_buffers, max_parallel_workers,
  max_worker_processes, max_connections, cpu_cores, role_exists, role_can_login,
  role_connection_limit, role_max_parallel_workers_per_gather, role_work_mem,
  role_temp_file_limit, role_statement_timeout
- `start_analytics(*, full_table_sessions, index_sessions, limit_tests, extra_warehouses) -> dict`:
  returns once every session is open and has issued its first query; `supported` false with
  `error` otherwise
- `analytics_sample() -> dict`; `analytics_state() -> dict`; `stop_analytics() -> dict`
- `revert_analytics() -> dict` (idempotent; usable from a fresh adapter by the kill switch)
- `analytics_leftovers() -> dict`; `customer_cache_counters() -> dict`
- TpccSupport additions: `tpcc_cache_bytes()`, `set_tpcc_layout(layout)`, `tpcc_layout()`,
  `tpcc_capacity_records()`, `tpcc_record_capacity(tps, clerks=None, active_warehouses=None)`
