# Hand-over: what the workload service offers, and what attach replaces in the 9 phases

Agreed approach (2026-11-18): the workload service does every workload job. The 9 phases keep
their own copy for now, so every scenario keeps working. The attach story removes each copy at the
moment it replaces it with the running workload. Both copies call the same functions, so they
cannot drift apart. Line numbers: `resilience_tests/control/orchestrator.py` on branch
`common-oltp-workload`.

## What the service offers (built)

| Need | Where |
|---|---|
| start / wait / stop from a runner | `service/client.py`: `start(env, config) -> folder`, `wait_until_running(folder, timeout_s)` (past the warm-up), `status(folder)`, `info(folder)`, `stop(folder) -> summary` |
| the running workload on a node | `client.running_folder(env, node)`; from any harness host: `state.running_workload(env, node)` (lock `/var/tmp/resilience-workload-<port>.lock` on the target) |
| settings used, times, TPC-C facts | `<folder>/service.json`: `settings.workload_used` (sizes and rate as numbers), `clerks_started_wall`, `measure_start_planned_wall`, `planned_end_wall`, `tpcc.orders_at_start`, `tpcc.history_since`, `tpcc.run_constants`, file names |
| live state | `<folder>/status.json` (state `warming_up` / `running` / `stopping` / `checking` / `finished` / `stopped` / `failed`, failure, outage now) |
| 1-second samples | `<folder>/metrics-1s.jsonl`: the same fields as a scenario's `workload/sample` events, plus `t_wall`, `elapsed_s`, `part` |
| all workload events | `<folder>/events.jsonl` (`source: workload`: sample, driver_load, fatal; `source: service`: outages, start, stop) |
| journals | `<folder>/marker.N.jrnl`, `acked.N.jrnl`, `aborted.N.jrnl` (one set per worker process; from the workload's start) |
| the scenario's own harness tables | `adapter.prepare_scenario_state()`: grants, objects, probe writes, bulk tables, leftovers of earlier faults -- never the marker or churn table |
| the workload's tables | `adapter.prepare_workload_state()`: called by the service at its start only |

## What attach replaces in the 9 phases

| Phase | Line(s) today | Replace with |
|---|---|---|
| init | 436-443 `refuse_if_workload_running(...)` | find the running workload and connect to it; refuse if none, or if it does not match the scenario's `workload:` section |
| init | 465 `prepare_harness_state()` | `prepare_scenario_state()` |
| init | 501-512 `hooks_for`, `reset_data`, `prepare` | nothing (the service did them at its start); keep the service's disclosures from `summary.json`/`service.json` |
| init | 517-521 `MarkerJournals`, `HistoryWriter`, `make_workload_driver` | the attach handle (windows, failure, events) |
| baseline | 545 `workload.start()`, 550 `wait_until_ready()`, 552-557 warm-up | wait until the service state is `running` (past its warm-up) |
| baseline | 559, 562 `begin_window()` / `end_window()` | a window on the running clerks through the harness-workload channel (the service owns the engine; one window at a time, as today) |
| fault behaviours | `control/faults/context.py:89, 94` `begin_window` / `end_window` | the same channel |
| abort check | 1265 `workload.failure` | the service's status (`failed`) or its process gone |
| recovery / analysis | reads `workload/sample` events from the scenario's stream | copy the workload's events into the scenario's stream (or read `metrics-1s.jsonl`) for the scenario's time span |
| validate | 931-933 `marker_ids()` + `diff_from_journals(run_dir)` | the cut-off check: note T; read the markers (and, TPC-C, `sum(d_next_o_id - 1)`) in ONE snapshot; wait ~10 s; copy the journal lines stamped before T into the run folder; run the existing check on that copy. `rpo_txn` = losses found now minus those already lost at the scenario's init |
| validate | 1007 `consistency_checks(adapter, run_dir)` | the same, on the copied journals, with `orders_before` = `service.json` `tpcc.orders_at_start` and `orders_after` from the same snapshot as the markers (the order count must not include orders after T) |
| cleanup | 1225 `workload.stop()`, 1244 `journals.close()` | nothing: the workload keeps running |

## Points to agree

- Scenarios whose `workload:` differs from the running workload (rate, layout, session settings):
  refused with the differences, or run with a disclosure.
- NL-C-03 (Elle history): the service does not record Elle history yet (`service/config.py` refuses it).
- NL-C-05 / NL-M-05: the churn table carries bloat from earlier scenarios; their measures start
  from the table as it is at their init.
- Once attach works: remove the refusal at 436-443 and in `tpcc/load.py` `check_target` only where
  a command must still not run under the workload (load, trial and capacity must stay refused).
