# Contract: What the Adapter Supplies to the pgbench Driver

Constitution V: only the adapter knows the database. The pgbench driver adds the record steps (research R1, option B) around the adapter's transaction and never reads its SQL.

## Capability

`Capability.PGBENCH_WORKLOAD`, declared by the PostgreSQL adapter. An engine without it makes `generator: pgbench` refuse (FR-004). The built-in driver's refusals apply too: `TRANSACTIONAL_MARKERS`, `WORKLOAD_CHURN` for `mixed`, `LIST_APPEND_HISTORY` for `history: list_append`.

## Hooks

```text
adapter.pgbench_launch(shape: str, run_id: str, read_chunks: int = 0, chunk_chars: int = 0) -> PgbenchLaunchSpec
adapter.pgbench_marker_uuid(seq: int, run_id: str) -> str
```

- `shape`: `marker` | `churn` | `list_append`, derived exactly as the built-in driver derives it.
- `pgbench_marker_uuid(seq, run_id)`: the uuid the transaction with `seq` inserts. PostgreSQL: `md5('resilience-pgbench-<run_id>-' || seq)::uuid`. The record service journals this uuid, so `marker_ids()` and the journals compare as today. The run id is part of it because `seq` restarts at 1 every run: a marker an earlier run left behind must never carry the id of one this run lost (the built-in driver's random uuids have the same property). A run id that is not a safe SQL literal (`[A-Za-z0-9_.-]+`) is refused.

`PgbenchLaunchSpec`:

| Field | Meaning |
|---|---|
| `script` | the transaction only, `BEGIN` to `COMMIT`: the built-in driver's statements (`commit_marker`, `commit_marker_with_churn`, `commit_marker_list_append`) in the same order, at the same isolation level |
| `variables` | extra `-D` values (none for PostgreSQL) |
| `connection` | host, port, dbname, user from the node's **client** endpoint. No password: libpq reads `~/.pgpass` on the driver host, as asyncpg does. |
| `application_name` | the prefix `resilience-pgbench`; the driver appends `-<run_id>` and sets it as `PGAPPNAME` |

Variables the driver sets before the transaction, which the script uses:

| Variable | Shapes | Meaning |
|---|---|---|
| `seq` | all | the run-wide marker sequence number (also the Elle append value) |
| `ckey`, `creplace` | churn | the churn row, and 1 when the row is replaced instead of updated |
| `rk`, `ak`, `vbase` | list_append | the read key, the append key, and the key window's first value |

A list-append transaction leaves each read in `r1len`, `r1c1..r1c<read_chunks>` (read of `rk`) and `r2len`, `r2c1..` (read of `ak` after the append): the list as offsets from `vbase` joined by `.`, or `nil` when there is no row, cut into chunks of `chunk_chars` characters. pgbench refuses a shell command of 255 bytes or more, so the record steps carry the reads in chunks.

## Hooks reused unchanged

- `marker_ids()`, `churn_key_space`, `prepare_harness_state()`, `session()` (the driver's reconnect probe).

## Post-run check

```text
adapter.sessions_with_application_name(name) -> int
```

Must return 0 after the driver's `stop()`; otherwise cleanup fails (constitution VI).
