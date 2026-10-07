# Contract: Environment Profile — `workload` Section

```yaml
workload:                  # optional; omitted = all defaults
  generator: pgbench       # pgbench (default) | builtin
  pgbench_bin: pgbench     # executable on the driver host; default: found on PATH
```

| Field | Type | Default | Validation |
|---|---|---|---|
| `generator` | `pgbench` \| `builtin` | `pgbench` | any other value fails profile validation |
| `pgbench_bin` | str | `pgbench` | checked at run start: executable, and its `--version` major version **equals the target server's major version** (from the adapter); otherwise the run refuses before the baseline, naming both versions (FR-004) |

## Rules

- Selection is only ever explicit. A run never switches generator on its own (FR-020).
- Every run report records `workload_generator`, plus the pgbench version when it is used (FR-020, FR-018).
- Profiles without the section keep validating. `extra="forbid"` stays in force for unknown keys inside the section.
- Scenarios never name a generator (constitution I). The catalog schema does not change.

## Example: run NL-M-07 on the built-in driver as a fallback

Use a profile copy with `generator: builtin`, or the planned `--workload builtin` command-line override (tasks.md, optional task T015).
