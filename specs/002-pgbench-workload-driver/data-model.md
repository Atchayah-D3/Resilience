# Data Model: pgbench as the Harness Workload

Entities this feature adds or reuses. Today's entities (marker journals, `sample` events, the operation history, `MeasuredWindow`) keep their exact shape, so analysis code does not change.

## WorkloadLaunch (new)

One pgbench process start, for one client.

| Field | Type | Rule |
|---|---|---|
| `launch` | int | unique within the run; increases with every start, including relaunches |
| `client` | int | `0 .. concurrency-1`; stable across that client's relaunches |
| `started_mono_ns` / `ended_mono_ns` | int | harness monotonic clock |
| `wall_to_mono_offset_ns` | int | captured at start; converts record timestamps (R3) |
| `command` | list[str] | the exact argv; kept as evidence (FR-018). Never contains a password. |
| `rate_tps` | float | `scenario rate / concurrency`, offered **evenly spaced** (research R11); `interval_us = 1e6 / rate_tps` |
| `ended_as` | enum | `running` → `stopped` (by the harness) / `client_aborted` (fault) / `failed` (unexpected) |
| `abort_message` | str or None | pgbench's own stderr line when `client_aborted` |
| `summary` | dict or None | parsed per-launch summary, including per-command latencies (R8) |
| `scheduling_lag_us` | p50/p99 | actual start − scheduled slot, per transaction, aggregated per launch (FR-021) |

**State transitions**:
- `running → stopped`: the harness stopped it at validate or cleanup.
- `running → client_aborted`: a recognised client-abort line on stderr. The supervisor then relaunches the client with a new `launch`.
- `running → failed`: any other exit, or unparseable output. This is a workload failure, and the run aborts (FR-014).

## TransactionRecord (new view over existing journals)

| Field | Type | Rule |
|---|---|---|
| `identity` | `(launch, client, seq)` | unique across the run (FR-006). Its text form is e.g. `"L12-C3-S4567"`; the journals' `uuid` field holds `md5(text)` formatted as a UUID, the same value the script stores in `resilience.markers.uuid` |
| `t_pre` | float (wall s) | from the before-commit record; must precede the commit being sent |
| `t_ack` | float (wall s) or absent | from the acknowledgement record; absent means not acknowledged |
| `outcome` | enum | `committed` (ack exists) / `definitely_aborted` (pgbench failure, R5) / `unknown` |

Stored as today's `marker.jrnl` (one line per `t_pre`) and `acked.jrnl` (one line per `t_ack`), so `diff_from_journals` reads them unchanged. **How** the lines get there is research R1 (open).

**Validation**, enforced by existing code:
- an acknowledgement without a before-commit record means the journals are inconsistent, and the run aborts;
- torn lines are counted, and the run aborts.

## MarkerRow (target table, unchanged schema)

`resilience.markers(uuid, seq, ts)`, unchanged: `uuid` remains a `uuid` column. The pgbench script stores `md5('L<launch>-C<client>-S<seq>')::uuid`, a deterministic UUID derived from the identity. The harness computes the same value for each record (Python `uuid.UUID(hashlib.md5(text).hexdigest())`), so `marker_ids()` and the journals compare as today, with no schema change and no change to the built-in driver.

## Sample event (unchanged)

The `sample` event on the `workload` source: `interval_s, commits, tps, p50_ms, p95_ms, p99_ms, journal_p99_ms, errors, indeterminate, drops, reconnects, connect_failures`, built by the pgbench supervisor:

| Field | pgbench source |
|---|---|
| `commits`, latencies | transaction records acknowledged in the interval (latency = `t_ack − t_pre`) |
| `errors` | definitely-aborted transactions (R5) |
| `indeterminate` | transactions in flight on an aborted client |
| `drops` | client aborts (R4) |
| `reconnects` | successful relaunches |
| `connect_failures` | relaunch attempts refused by the database |
| `journal_p99_ms` | the record steps' latency, where R1's mechanism exposes it; otherwise `None`, never 0 |

## OperationHistory (unchanged format)

`history.edn` as today: `:invoke`, `:ok` with real read values, `:info` for unknown outcomes, never `:fail` unless definitely aborted. The pgbench path writes the same entries; the read values come from the script's `\gset` capture, delivered through R1's channel (research R6).

## Profile: workload section (new)

```yaml
workload:
  generator: pgbench        # pgbench (default) | builtin
  pgbench_bin: pgbench      # path on the driver host; default: found on PATH
```

See [contracts/profile-workload.md](contracts/profile-workload.md).
