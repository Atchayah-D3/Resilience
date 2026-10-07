# Contract: Workload Driver Interface

Both drivers implement exactly this: the existing built-in `WorkloadDriver` (unchanged) and the new `PgbenchWorkloadDriver`. The orchestrator, `rto_decomposer`, `baseline_slo_check` and the report depend only on what is listed here, so they don't change.

## Construction (through the factory)

```text
make_workload_driver(profile, adapter, workload, journals, stream, history) -> driver
```

- `profile.workload.generator` selects `pgbench` (default) or `builtin`.
- Raises `UnsupportedWorkload` with a named cause when the selected driver cannot run: pgbench missing or unsupported, the adapter lacking `Capability.PGBENCH_WORKLOAD`, a profile/shape it doesn't support, or **RPO evidence requested while R1 is not implemented**. It never returns the other driver instead.

## Methods and attributes

| Member | Contract |
|---|---|
| `await start()` | begin offering the declared load (`workload.concurrency`, `workload.rate_tps`, shape from `workload.profile` / `workload.history`). Emits `workload:start` with `generator`, `profile`, `concurrency` and `rate_tps`. |
| `await wait_until_ready()` | returns when every declared client has an open session, or at once if `failure` is set |
| `begin_window()` / `end_window() -> MeasuredWindow` | exact measurement window; `MeasuredWindow` fields as today (`duration_s, commits, tps, p50_ms, p95_ms, p99_ms, journal_p99_ms, errors, indeterminate, drops, reconnects, connect_failures`) |
| `window_t0_ns: int` | harness-monotonic start of the last window |
| `connected_workers: int` | clients that have opened their first session |
| `failure: str or None` | set (once) when the driver can no longer keep its evidence. The orchestrator aborts the run on it. |
| `await stop()` | stop offering load. Every in-flight transaction ends as committed or unknown, the journals are complete, and, for pgbench, **no process remains**. Emits `workload:stop`. Idempotent. |

## Events (to `events.jsonl`, harness clock)

| Event | When | Required fields |
|---|---|---|
| `workload:start` | at `start()` | `generator`, `profile`, `concurrency`, `rate_tps` |
| `workload:sample` | every ~1 s | `interval_s, commits, tps, p50_ms, p95_ms, p99_ms, journal_p99_ms, errors, indeterminate, drops, reconnects, connect_failures`. A value that cannot be computed is `None`, never 0. |
| `workload:fatal` | when `failure` is set | `error` |
| `workload:stop` | at `stop()` | none |
| `workload:launch` (pgbench only) | each process start | `launch`, `client`, `command` |
| `workload:launch_end` (pgbench only) | each process end | `launch`, `client`, `ended_as`, `abort_message` |

## Evidence files (run directory)

- `marker.jrnl`, `acked.jrnl`: same format as today (data-model.md, TransactionRecord).
- `history.edn`: same format as today, when `workload.history == list_append`.
- `pgbench/` (pgbench only): one `launch-<n>.txt` per process, holding argv, stderr and the parsed summary (FR-018).

## Invariants tested for both drivers

1. Every acknowledged identity has a before-commit record.
2. Samples never invent values: a missing value is `None`.
3. After `stop()`: no worker or process remains, and the journals are closed.
4. Swapping drivers changes no catalog field and no scenario test outcome (spec SC-001, SC-008).
