# Quickstart: Validating the pgbench Workload

How to prove the feature works end to end. Links point to the contracts; nothing here is implementation.

## Prerequisites

- The `pgbench` branch checked out on the driver host (`BDB-QA-U22-29`), with `.venv` installed.
- `pgbench --version` on the driver host reports the **same major version as the target server** (ShaktiDB 17 on the lab), or `workload.pgbench_bin` points to one (spec FR-004).
- The lab target set up as for every other scenario (sentinel row, `amcheck`, `~/.pgpass`).

## V1 — Unit tests (no lab)

```bash
.venv/bin/pytest -q tests/test_workload_selection.py tests/test_pgbench_output.py tests/test_pgbench_driver.py
.venv/bin/pytest -q -n 4           # the whole suite: existing scenario tests still pass on both drivers
```

Expected: all pass. The scenario tests parametrised over `generator` (tasks T012; `pgbench` added in T048) pass for every generator enabled.

## V2 — Two pgbench behaviours on the lab build (once, before Phase 4)

On the driver host, against the lab target. Run these by hand; they are read-only apart from a temporary table.

1. **Per-client variables persist across transactions (research R7).** Run a 2-second, 1-client pgbench whose script increments a `-D` variable and writes it to a temporary table. The values written must be 1, 2, 3 and so on, not 1, 1, 1.
2. **Clients are aborted, not reconnected, when the database dies (research R4).** Start a 1-client pgbench, restart the database service, and read pgbench's stderr. Expect a "client 0 aborted…" line and no further transactions from that client.

Record both outcomes in research.md (R7, R4). If either differs, update the plan before Phase 4.

## V3 — Selection and refusals

| Profile | Expect |
|---|---|
| `workload.generator: builtin` | every scenario runs exactly as today; the report says `workload_generator: builtin` |
| `workload.generator: pgbench`, `pgbench_bin` wrong | refusal before the baseline naming the missing executable |
| `workload.generator: pgbench`, before R1 is implemented | refusal before the baseline: "RPO evidence requires the transaction-record mechanism (research R1)" |

## V4 — First lab runs with pgbench (after R1 is implemented)

```bash
.venv/bin/pytest resilience_tests/test_scenarios.py --env e2-dedicated-vm --scenario NL-C-01 -q -rA
.venv/bin/pytest resilience_tests/test_scenarios.py --env e2-dedicated-vm --scenario NL-M-07 -q -rA
```

Check in each run's `summary.txt` and `results.json`:

| Check | Spec |
|---|---|
| `workload_generator: pgbench`, pgbench version, launches listed | FR-018, FR-020 |
| baseline TPS ≥ floor (NL-C-01 150, NL-M-07 750), or an abort before the fault naming the limiting side | SC-005, US5 |
| recording overhead stated | FR-011, SC-006 |
| scheduling lag p50/p99 stated and small (transactions evenly spaced) | FR-021 |
| NL-C-01: relaunches recorded after the kill; load restored within 5 s of first write | SC-003 |
| an independent journal recount matches `rpo_txn` (as done for NL-I-01 / NL-C-02) | SC-002 |
| no sample gap > 1.5 s outside the outage | SC-004 |
| after the run: `pgrep -f resilience-pgbench` empty on the driver host; no `resilience-pgbench-*` sessions on the target | SC-007 |

If NL-M-07 cannot reach 750 TPS, re-run it with `generator: builtin` (FR-020), and record the measured pgbench ceiling in research.md (R1, throughput criterion).

## V5 — Whole catalog on both drivers

Run every scenario once with each generator. Compare verdicts and key measurements side by side (spec SC-001, SC-008).
