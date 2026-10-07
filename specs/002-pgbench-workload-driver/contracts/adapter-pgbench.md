# Contract: What the Adapter Supplies to the pgbench Driver

Constitution V: only the adapter knows the database. The pgbench driver treats everything below as opaque.

## Capability

`Capability.PGBENCH_WORKLOAD`, declared by the PostgreSQL adapter. An engine without it makes `generator: pgbench` refuse (FR-004).

## Hook

```text
adapter.pgbench_launch(shape: str, launch: int, client: int) -> PgbenchLaunchSpec
```

- `shape`: `marker` | `churn` | `list_append`, derived by the driver from `workload.profile` / `workload.history` exactly as the built-in driver derives it today.

`PgbenchLaunchSpec`:

| Field | Meaning |
|---|---|
| `script` | the pgbench script text for this shape (marker insert; churn update or replace; list-append read and append with `\gset` reads), including the record steps defined by research R1 |
| `variables` | `-D` values: `launch`, `client`, `seq=0`, and shape parameters such as the churn key space and the replace-every-Nth value |
| `connection` | host, port, dbname, user from the node's **client** endpoint. No password: libpq reads `~/.pgpass` on the driver host, as today. |
| `application_name` | `resilience-pgbench-<run_id>`; used to confirm no session remains at cleanup (research R9) |

## Hooks reused unchanged

- `marker_ids()`: must return the same UUID form the records use (data-model.md, MarkerRow).
- `churn_key_space`: the same number the churn script uses; never a second copy.
- `prepare_harness_state()`: the harness tables (`markers`, `churn`, `lists`) as today.

## Post-run check

```text
adapter.sessions_with_application_name(name) -> int
```

Must return 0 after the driver's `stop()`; otherwise cleanup fails (constitution VI).
