# Data Model: NL-N-I01

| Entity | Fields | Where |
|---|---|---|
| Layout | warehouses, active_warehouses, shared_buffers_bytes, rules (order_line multiple, active share) | calculated in init by the TPC-C profile; `facts.workload_reset.layout`; handed to the adapter (`set_tpcc_layout`) |
| Session plan | full_table_sessions, index_sessions, limit_tests, cores, helpers_per_report | calculated from the target (cores, role settings) by the driver and the behaviour with the same function |
| Report query outcome | session, kind (q1, q2, q3, q4, limit_temp, limit_timeout), started/ended (monotonic ns), outcome (completed / error / cancelled), sqlstate, plan figures (workers planned/launched, shared hit/read, temp written) | adapter state; `facts.analytics.queries` (capped) and per-kind counts |
| Activity sample | t_mono_ns, sessions_active, sessions_open, helpers per leader pid, temp_files, temp_bytes | `analytics_sample` events every 1 s during the hold |
| Cache counters | hit, read (customers' own tables) at baseline start/end and hold start/end | `facts.analytics_cache` |
| Role settings | connection limit, helpers per query, work_mem, temp_file_limit, statement_timeout (null when unset) | `facts.scenario_observed` (init) and `facts.settings_after` (validate) |
| Capacity record | clerks, warehouses, active_warehouses, tps, measured_at | table `resilience_tpcc.capacity` (dropped with the TPC-C data; kept across a same-size reload) |

States of a report session: opened -> running queries (limit test first, for sessions 1 and 2)
-> stopping (end of hold: running query cancelled) -> closed. Backstop: ended on the server by tag.
