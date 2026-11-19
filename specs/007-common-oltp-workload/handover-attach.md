# Hand-over: what the workload service offers, and what attach replaces in the 9 phases

Agreed approach (2026-11-18): the workload service does every workload job. The 9 phases keep
their own copy for now, so every scenario keeps working. The attach story removes each copy at the
moment it replaces it with the running workload. Both copies call the same functions, so they
cannot drift apart. Line numbers: `resilience_tests/control/orchestrator.py` on branch
`common-oltp-workload` (they move as the file changes; search for the code shown).

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
| any window's figures (baseline, hold windows) | `client.window(folder, from_wall, to_wall)` (`service/windows.py`): the fields of `driver.MeasuredWindow` from the 1-second samples, whose latency histograms (`latency_hist`, `corrected_hist`, `journal_hist`) are summed. A sample belongs to the window when it ended inside it (windows move in whole seconds). Percentiles are the upper bound of a 2% bucket: never below the exact value, at most 2% above, so a rule can fail early but never pass falsely. A window in which no sample ended raises `windows.NoSamples` (never TPS 0). No request/answer channel is needed. |
| scenario marks | `client.mark(folder, "run_start" / "t0" / "t1" / "run_end", run_id=..., scenario=...)` appends one line to `<folder>/scenario-marks.jsonl`. The service judges its own baseline only outside these windows (plus `scenario_settle_s`) and reports each scenario's window in `summary.json` -> `scenarios` (TPS, errors, unknown, drops, outages, lost commits acknowledged inside). A scenario without `run_end` counts to the end of the workload. A mark is refused (`client.ServiceFailed`) unless the workload is `warming_up` or `running` and its process is alive. |
| run until stopped | `duration_s: null` in the settings file: the workload runs until `client.stop(folder)`; `service.json` `planned_end_wall` is then null. With a fixed duration, refuse a scenario when `planned_end_wall` is too close. |
| the clerks' sessions | `application_name = 'resilience-workload'` on every clerk connection (`contract.WORKLOAD_APPLICATION_NAME`, also in `service.json`). No harness cleanup ends it: every session-ending query matches its own exact name (`resilience-flood`, `resilience-analytics`, the lock pile-up names). Count it out of "harness sessions left". |
| liveness | `pid` in both `status.json` and `service.json`; `status.json` rewritten every `status_interval_s` (5 s). |
| the scenario's own harness tables | `adapter.prepare_scenario_state()`: grants, objects, probe writes, bulk tables, leftovers of earlier faults -- never the marker or churn table |
| the workload's tables | `adapter.prepare_workload_state()`: called by the service at its start only |

## Rules for reading files while the workload writes them

- Every line of `metrics-1s.jsonl`, `events.jsonl`, `scenario-marks.jsonl` and the journals is one
  JSON object ending in a newline. A reader can meet a last line that is still being written:
  read with `state.read_complete_lines(path)`, which leaves out a last line without its newline.
  (`markers.read_journal` counts such a line as torn, which makes the lost-commit count untrusted:
  do not use it on a journal that is still being written.)
- Times: `metrics-1s.jsonl` and `events.jsonl` lines carry `t_wall`; journal lines carry `t_pre`
  (marker), `t_ack` (acknowledged), `t_abort` (rolled back). All are the harness host's wall clock.
- The journals and the marker table are never emptied or rotated while the workload runs (the
  cut-off check depends on it). Journal splitting by time, if added, must keep every line.
- The workload lock (`/var/tmp/resilience-workload-<port>.lock`) is not the scenarios' lock: a
  scenario taking its own lock never touches the workload's. The kill switch stops the workload
  only with `--stop-workload`.

## What attach replaces in the 9 phases

| Phase | Line(s) today | Replace with |
|---|---|---|
| init | 436-443 `refuse_if_workload_running(...)` | find the running workload and connect to it; refuse if none, or if it does not match the scenario's `workload:` section |
| init | 465 `prepare_harness_state()` | `prepare_scenario_state()` |
| init | 501-512 `hooks_for`, `reset_data`, `prepare` | nothing (the service did them at its start); keep the service's disclosures from `summary.json`/`service.json` |
| init | 517-521 `MarkerJournals`, `HistoryWriter`, `make_workload_driver` | the attach handle (windows, failure, events) |
| baseline | 545 `workload.start()`, 550 `wait_until_ready()`, 552-557 warm-up | wait until the service state is `running` (past its warm-up) |
| baseline | `begin_window()` / `end_window()` | note the wall time at begin and end; `client.window(folder, begin, end)`; `MeasuredWindow(**{k: w[k] for k in windows.MEASURED_WINDOW_FIELDS})` |
| fault behaviours | `control/faults/context.py` `begin_window` / `end_window` | the same |
| every phase | (none) | `client.mark(...)`: `run_start` at init, `t0` at the fault, `t1` at the end of recovery, `run_end` after cleanup |
| abort check | 1265 `workload.failure` | the service's status (`failed`) or its process gone |
| recovery / analysis | reads `workload/sample` events from the scenario's stream | copy the workload's events into the scenario's stream (or read `metrics-1s.jsonl`) for the scenario's time span |
| validate | 931-933 `marker_ids()` + `diff_from_journals(run_dir)` | the cut-off check: note T; read the markers (and, TPC-C, `sum(d_next_o_id - 1)`) in ONE snapshot; wait ~10 s; copy the journal lines stamped before T into the run folder; run the existing check on that copy. `rpo_txn` = losses found now minus those already lost at the scenario's init |
| validate | 1007 `consistency_checks(adapter, run_dir)` | the same, on the copied journals, with `orders_before` = `service.json` `tpcc.orders_at_start` and `orders_after` from the same snapshot as the markers (the order count must not include orders after T) |
| cleanup | 1225 `workload.stop()`, 1244 `journals.close()` | nothing: the workload keeps running |

## Points to agree

- Scenarios whose `workload:` differs from the running workload (rate, layout, session settings):
  refused with the differences, or the scenario states only what it needs (profile, markers,
  session settings) and takes sizes and rate from `service.json`, recorded in its report. Scenario
  3a/3b were validated at 64 warehouses and 60 TPS; on other sizes and rates their results must be
  validated again (which the attach story does anyway). A decision for the team and the lead.
- NL-C-03 (Elle history): the service does not record Elle history yet (`service/config.py` refuses it).
- NL-C-05 / NL-M-05: the churn table carries bloat from earlier scenarios; their measures start
  from the table as it is at their init.
- Once attach works: remove the refusal at 436-443 and in `tpcc/load.py` `check_target` only where
  a command must still not run under the workload (load, trial and capacity must stay refused).
