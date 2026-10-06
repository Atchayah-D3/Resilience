# Quickstart: validate NL-M-03

## Prerequisites

- Harness VM `~/resilience` with its `.venv` (see CLAUDE.md); target reachable; ledger clean
  (`.venv/bin/python -m resilience_tests.control.killswitch --env e2-dedicated-vm` reports
  nothing outstanding).
- Target observed settings: `autovacuum = on`, `track_counts = on` (the run refuses otherwise).

## 1. Unit tests and catalog checks

```bash
.venv/bin/python -m pytest tests/test_autovacuum_worker_kill.py tests/test_orchestrator.py -q
.venv/bin/python -m pytest tests -q
.venv/bin/python -m catalog.schema --partial
```

Expected: all pass. The NL-M-03 tests must include failing paths (see contracts):
no worker appears -> aborted; worker exits before kill -> retried, never counted; 3 misses ->
aborted; wrong parent pid / title -> not killed; relation not vacuumed -> fail naming it;
0 eligible relations -> NOT_MEASURED (fail); no respawn -> fail.

## 2. Dry run (no fault)

```bash
.venv/bin/pytest resilience_tests/test_scenarios.py --env e2-dedicated-vm \
  --scenario NL-M-03 --stop-before-fault -q -rA
```

Expected: steady state holds; `avac_target` seeded; observed autovacuum settings disclosed.

## 3. Live run (kills 5 autovacuum workers -> 5 crash-restarts on the target)

```bash
.venv/bin/pytest resilience_tests/test_scenarios.py --env e2-dedicated-vm --scenario NL-M-03 -q -rA
```

Expected (`results.json`): `kills_landed = 5`, `cycles_recovered = 5`,
`autovacuum_worker_respawned = true`, `relations_eligible >= 1`,
`relations_left_unvacuumed = 0`, `rpo_txn = 0`, `starts_unattended = true`,
`structural_integrity_errors = 0`. Facts show, per cycle, the killed pid, its relation, its
parent (our postmaster) and death confirmation; the second cluster on the host is untouched.

## 4. After the run

The target's `pg_stat_activity` shows no harness sessions; `resilience.avac_target` is empty;
the ledger has no outstanding entry.
