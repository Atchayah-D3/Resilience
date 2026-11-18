# Spec 007: Common long-running TPC-C workload

**Story:** Build Common Long-Running TPCC-Like Workload for Resilience Framework.
**Next story (teammate):** Scenario Integration with Common Workload and Sequential Orchestrator.

## Goal
Today each scenario starts its own customers (TPC-C clerks) in its baseline phase and stops them
in validate. This story moves the workload **out of the 9 phases**: one TPC-C workload runs for
the whole resilience run (about 2 hours, configurable), like a production OLTP system's traffic.
It is started and stopped only by its own commands. No scenario starts, stops or changes it.

## Acceptance criteria (from the story)
- A configurable TPC-C-like OLTP workload that runs 2 hours continuously as one shared workload.
- It emits per-interval throughput, latency and error metrics.
- It survives faults by reconnecting automatically.
- A validated baseline.

## Decisions (agreed with the user, 2026-11-18)
| # | Decision |
|---|---|
| D1 | The workload is a service of its own (`service/cli.py start | status | stop`), running in the background. It does not hold the scenarios' lock: scenarios must be able to run while it runs (they attach in the next story). |
| D2 | The TPC-C data is loaded or reset **only at the start** of the workload run. During the run nothing reloads it; a reload by any other command is refused while the workload runs. |
| D3 | The reconnect pause stays the fixed 0.2 s everywhere (scenario runs and the service): one rule, no existing figure changes. |
| D4 | The settings actually used go into `summary.json` (no separate copy of the settings file). |
| D5 | Journal splitting by time is NOT in this story (the teammate builds it this sprint); `markers.py` is not changed here. |
| D6 | Rate: `rate_fraction_of_capacity: 0.6` (needs the capacity record for the layout); a fixed `rate_tps` later. NEEDS SIGN-OFF. |
| D7 | Baseline thresholds live in the settings file; NEEDS SIGN-OFF by the lead. |
| D8 | The settings file lives in `workloads/`. |
| D9 | The service does every workload job (data step, the workload's tables, clerks, warm-up, journals, metrics, stop) for any workload profile a scenario uses. The 9 phases keep their own copy until the attach story removes it while replacing it (both call the same functions). Hand-over: `handover-attach.md`. |

## What a scenario may not do while the workload runs (until the next story)
A scenario's init empties `resilience.markers` (the workload's lost-commit evidence), may reload
the TPC-C data, and its baseline starts a second set of clerks. So, until attach exists, a
scenario run is **refused** with a clear message while the workload runs, and so are the TPC-C
commands that write (load, trial, capacity). The workload refuses to start while a scenario run
holds the target, because its start empties the marker table.

## Requirements
- FR-1 The settings file is checked before anything starts (any profile a scenario uses except idle; no Elle history yet; markers on, warm-up
  shorter than the duration, worker processes not more than the clerks).
- FR-2 Start: target checks; refused under a running scenario; the workload lock on the target;
  marker table emptied once; data loaded/reset only if needed; data checked; rate fixed; clerks
  started; warm-up (not measured).
- FR-3 While running: 1-second samples (`metrics-1s.jsonl`), interval lines
  (`intervals-<n>s.jsonl`) with exact window figures plus errors by kind, business rollbacks
  and commits per type, `events.jsonl`, `status.json` every few seconds.
- FR-4 Errors by kind in every TPC-C sample: `server_<SQLSTATE>`, `timeout`, `connection_lost`,
  `client_error` (business rollbacks are not errors; failed connects stay in `connect_failures`).
- FR-5 A database outage does not stop the workload: clerks reconnect; seconds without a commit
  are reported as an outage (start, end, length), with an alert after `no_commit_alert_s`.
- FR-6 The workload ends at `duration_s`, on `stop` (or the kill switch with `--stop-workload`),
  or when it fails: a journal line that cannot be saved, a worker process that dies, the harness
  being the limit, the workload lock taken over.
- FR-7 After the stop: lost commits and phantoms (journals against the marker table), the TPC-C
  checks, then the baseline rules; `summary.json` (with the settings used) and `summary.txt`.
- FR-8 The baseline is valid only when every rule passes and the run finished its duration;
  missing evidence is NOT_MEASURED and fails (never a pass without evidence).

## Out of scope (next story)
Attaching a scenario to the running workload (its init/baseline/validate reading the workload's
outputs instead of owning a workload), the harness-workload channel, the sequential orchestrator
with baseline checks and cooldowns, the lost-commit check by time slice, journal splitting.
