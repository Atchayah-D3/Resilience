# Common long-running workload

One TPC-C workload that runs for hours (2 h by default), outside any scenario's 9 phases, like a
production OLTP system's traffic. Only its own commands start and stop it. Spec:
`specs/007-common-oltp-workload/spec.md`.

Any workload profile a scenario uses can run as the common workload (`oltp_write_heavy`, `mixed`,
`tpcc`); not yet Elle history (`list_append`, NL-C-03). The service does every workload job: the
data step, the workload's tables (`adapter.prepare_workload_state`: markers, churn), the clerks, the
warm-up, the journals, the metrics, the stop. A runner drives it from Python with `client.py`
(`start`, `wait_until_running`, `info`, `status`, `stop`). What the attach story replaces in the 9
phases: `specs/007-common-oltp-workload/handover-attach.md`.

## Commands (on the harness VM, in ~/resilience)

```bash
# start in the background (keeps running after you log out)
.venv/bin/python -m resilience_tests.execution.workload.service.cli start  --env e2-u22-3 --config workloads/oltp-common.yaml
# what it is doing now
.venv/bin/python -m resilience_tests.execution.workload.service.cli status --env e2-u22-3
# stop before the end of its duration (waits for the end checks and prints the summary)
.venv/bin/python -m resilience_tests.execution.workload.service.cli stop   --env e2-u22-3
# a finished run's last status
.venv/bin/python -m resilience_tests.execution.workload.service.cli status --env e2-u22-3 --folder <run folder>
# kill switch: undo injections AND stop the workload (the workload is never stopped without the flag)
.venv/bin/python -m resilience_tests.control.killswitch --env e2-u22-3 --stop-workload
```

First time on a target with `rate_fraction_of_capacity`: measure the maximum for the layout once:
`.venv/bin/python -m resilience_tests.execution.workload.tpcc.capacity --env e2-u22-3 --clerks 64`.

## What happens

| When | What |
|---|---|
| start (once) | target checks; refused while a scenario run holds the target; the workload lock on the target (`/var/tmp/resilience-workload-<port>.lock`); the workload's tables emptied (markers, churn; churn seeded); TPC-C data loaded or reset **only if needed** (missing, other size, grown past the limit, inconsistent, or `fresh_load`); data checked; rate fixed; clerks started; warm-up (not measured) |
| while running | the clerks send transactions at the rate; every writing transaction writes its journal lines before and after (lost-commit evidence); the files below are written live. Nothing is reloaded or emptied. A database that is down does not stop it: clerks reconnect, the gap is reported as an outage |
| end | at `duration_s`, on `stop`, or when the workload itself fails (a journal line cannot be saved, a worker process dies, the harness is the limit, the lock is taken over) |
| after the stop | lost commits and phantoms (journals against the marker table), TPC-C checks, baseline rules, `summary.json` and `summary.txt` |

## Files in the run folder (`<driver_host.run_dir>/workload-<id>-<time>/`)

| File | Contents |
|---|---|
| `service.json` | written once the clerks run: the settings used (sizes and rate as numbers), start and warm-up end times, the TPC-C order count and payment start time at the workload's start, the file names |
| `status.json` | live state: state, step, time left, last interval's figures, errors so far, outage now, alerts |
| `metrics-1s.jsonl` | every 1-second sample (same fields as a scenario's `sample` events) + `t_wall`, `elapsed_s`, `part` (warmup/measured) |
| `intervals-<n>s.jsonl` | one line per interval: exact TPS, p50/p95/p99, errors, unknown, drops, reconnects, plus errors by kind, business rollbacks, commits per type |
| `events.jsonl` | start, warm-up end, outages (start/long/end), failures, stop; also the engine's samples |
| `marker.N.jrnl`, `acked.N.jrnl`, `aborted.N.jrnl` | the journals, one set per worker process |
| `markers_db.txt.gz` | the markers the database held at the end (the lost-commit check can be redone) |
| `summary.json` | state, baseline checks, the **settings used** (sizes and rate as numbers), totals, 10-minute periods, outages, harness CPU and memory, end checks, disclosures |
| `summary.txt` | the same, short, for people |
| `service.log` | the background process's output |

## While it runs, scenarios are refused (until the next story)

A scenario's init empties the marker table, may reload the data, and its baseline starts its own
clerks -- each would damage the running workload. So a scenario run, and the TPC-C commands that
write (load, trial, capacity), are refused with a message while the workload runs. The next story
lets scenarios attach to the running workload instead.

## Baseline rules (settings file, NEEDS SIGN-OFF)

Measured part only (after the warm-up). Valid only if every rule passes and the run finished its duration:
every 60 s window at least 95% of the rate; the median of the 1-second p99 values in every 10 s
window at or under the limit; errors (server rollbacks except business rollbacks, unknown outcomes)
at most 0.01%; no dropped connection; the harness was not the limit; harness memory grows at most
10% from minute 10; no lost commit, no phantom; TPC-C checks clean. Missing evidence fails.
