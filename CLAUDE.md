# CLAUDE.md

Guidance for Claude Code (and anyone new) working in this repository. The rules that are not
negotiable live in `.specify/memory/constitution.md` -- read it first. This file is the
practical how-to.

## What this is

A resilience test harness for ShaktiDB (standalone PostgreSQL 17) and DistDB. It deliberately
breaks a running database -- kills it, restarts it, floods it, crashes it mid-operation -- and
measures what the failure cost: transactions lost (RPO), time to recover (RTO), corruption.

- Specs: `docs/DistDB-050826-050505.pdf` is the **Framework** (155 scenarios; cite as
  `Framework §n`), `docs/BP-Resilience Test Harness Architecture-050826-050547.pdf` is the
  **Architecture** (cite as `Arch §n`). Their section numbers differ -- never cite a bare `§n`.
- `docs/infra-requirements.md` lists what the lab still lacks (data disk, power control,
  packages, co-tenant, standby). `docs/NL_Blocked_By_Infra.xlsx` maps scenarios to those gaps.
- `README.md` explains the layers, phases and every file in depth.

## Layout in one screen

| Path | Holds |
|---|---|
| `catalog/NL/<ID>.yaml` | One scenario: fault, workload, measures, `accept` predicates. Never a host or tool. |
| `catalog/schema.py` | Scenario schema + catalog checks (`FaultType`, `FaultDuring`, `InfraFeature`, ...) |
| `envs/*.yaml` | Machines, ports, paths, drivers, phase bounds, safety allowlist, disclosures, `infra` |
| `resilience_tests/control/orchestrator.py` | The 9 phases: reset, init, baseline, pre_fault, fault_inject, recovery, validate, report, cleanup |
| `resilience_tests/adapters/postgresql/adapter.py` | The ONLY place that knows PostgreSQL |
| `resilience_tests/execution/injectors/process.py` | os_ssh driver: kill, restart, reload, idle txn, connection flood |
| `resilience_tests/execution/workload/driver.py` | asyncpg marker workload (+ Elle list-append mode) |
| `resilience_tests/analysis/` | RTO decomposer, predicates/verdict, report, Elle checker |
| `tests/` | Unit tests with fake engines -- plumbing, not scenarios |
| `vendor/elle/` | `setup_elle.sh` installs the pinned elle-cli jar (Java 21+) |
| `specs/` | Spec Kit features (one per scenario or shared change) |

## How a scenario is wired (no scenario-ID branches)

Behaviour comes only from catalog fields:
- `fault.type` -> the profile resolves it to a driver (`os_ssh`, power, storage, ...).
- `fault.during` -> the orchestrator first establishes the state the fault must land inside
  (`checkpoint` NL-C-02, `large_transaction` NL-C-03, `concurrent_index_build` NL-C-06) and
  confirms it from the engine before injecting.
- `fault.duration` -> held faults (NL-R-04 flood) hold for this many seconds.
- `workload.history: list_append` -> real Elle-checkable history (NL-C-03).
- `requires` (engine capabilities) / `needs_infra` (environment) -> skipped with a reason.
- Measures such as `fault_confirmed`, `operation_in_progress_at_fault` prove the fault landed.

## Environments

- **Harness / driver host** (runs the harness): `10.11.21.111`, SSH port 5015, user
  `distdbsdb17user`, hostname `BDB-QA-U22-29`. Key-based SSH. Ubuntu 22.04, Java 21 default.
- **Target** (the database under test): `10.11.21.112`, SSH port 5015, same user, hostname
  `BDB-QA-U22-30`; PostgreSQL on port 5433, database `resilience`, role `harness`,
  binaries `/usr/lib/postgresql/17.11.1.0/bin` (use full paths -- `/usr/bin/pgbench` etc. hit
  Debian's pg_wrapper and fail).
- Credentials never go into this repo (DB password from `~/.pgpass` on the driver host).
- On the harness VM, work in **`~/resilience`** (it has its own `.venv`). **Never modify
  `~/Resilience-A`.** Back up `~/resilience` before replacing code in it.
- Run evidence lands in `~/resilience-runs/<RUN_ID>/` (`results.json`, `summary.txt`,
  `events.jsonl`, `history.edn`, `elle/`); the ledger is `~/resilience-runs/injection-ledger.jsonl`.

## Commands

```bash
# on the harness VM, in ~/resilience
.venv/bin/python -m pytest tests -q            # unit tests (~7 min; avoid -n 4 there, see Gotchas)
.venv/bin/python -m catalog.schema --partial   # catalog checks while the catalog is incomplete

# live scenario run against the target (really kills/restarts PostgreSQL on .112)
.venv/bin/pytest resilience_tests/test_scenarios.py --env e2-dedicated-vm --scenario NL-C-01 -q -rA
#   --scenario is repeatable; --stop-before-fault = dry run with no fault and no verdict

# undo anything a crashed run left applied
.venv/bin/python -m resilience_tests.control.killswitch --env e2-dedicated-vm

# Elle (once per machine): needs Java 21+; graphviz optional (only for anomaly plots)
bash vendor/elle/setup_elle.sh
```

Reports (spec 002): every scenario run writes `<run_dir>/report.html`. Add `--report-dir reports`
to either suite for `scenarios-junit.xml` + `index.html` + `gate.txt` (scenarios) or
`unit-summary.html` (unit tests; JUnit via `--junitxml`). P0 gate:
`.venv/bin/python -m resilience_tests.reporting.gate reports/scenarios-junit.xml` (exit 1 = a P0
failed or aborted). Regenerate pages: `python -m resilience_tests.reporting.html <run_dir>...`.
Jinja2 is required (installed in both VM venvs).

The local workstation has no project venv; run tests on the harness VM (or create `.venv`
with `pip install -e '.[test]'` on Python 3.11+).

## Scenario status and owners

| Owner | Scenarios |
|---|---|
| Owner A | NL-C-01, NL-C-03, NL-C-04 (blocked: needs a dedicated pg_wal volume), NL-C-06, NL-M-03, NL-M-06, NL-M-07, NL-R-04 |
| Owner B | NL-C-02, NL-C-05, NL-M-05 |

Change another owner's scenario only with that owner (constitution, workflow section).
Next candidates: NL-N-02 (cgroup CPU throttling, P0) with NL-R-01/02 (shared resource_limit driver).

## Working rules (summary -- the constitution is authoritative)

- **Never a false pass.** A measure that cannot be taken is `NOT_MEASURED` (fails), never 0.
  Prove every fault landed. Every new criterion gets a unit test that fails for the right reason.
- **Cite the Framework** for every threshold or mark it `NEEDS SIGN-OFF`; always gate
  `structural_integrity_errors == 0` (Framework §16.3).
- **Leave the target as found.** Harness objects are found by schema-qualified name or
  `application_name` (`resilience-flood`, `resilience-harness-idle`); configuration is observed,
  never written.
- **Before merge:** unit tests + catalog checks pass, and a live run on the lab target passes
  for every scenario the change touches (including via shared code).
- **Merging main:** re-apply changes on top of main's version; do not trust auto-merged files
  in `orchestrator.py`, `adapter.py`, `process.py`, `driver.py` -- check for duplicate or
  missing methods.

## Spec Kit workflow

One feature per scenario: `/speckit-specify` -> `/speckit-clarify` -> `/speckit-plan` ->
`/speckit-tasks` -> `/speckit-analyze` -> `/speckit-implement`, then the live run. The spec
quotes the Framework row (fault, acceptance, priority); the plan's Constitution Check covers
Principles I-VII. Feature creation does not create git branches; it writes `specs/NNN-name/`.

## Gotchas

- **Slow fsync on the harness VM**: marker-journal flushes stall up to ~600 ms. Unit tests
  with 1 s baselines occasionally abort ("slower side: driver journal flush") under load
  (`-n 4` or the full suite); re-run the single test. Not a code defect.
- The VM env file sets `recovery: 120`, so `rto_to_slo_s` is often `None` -- not gated anywhere.
- elle-cli 0.1.11 **hangs on a G1a history** (read of an aborted value); the harness times out
  to `NOT_MEASURED` (fail-closed). Cycles (G1c, G2, ...) are detected in about a second.
- Without graphviz, Elle is run without `--directory` (verdict only, no explanations).
- NL-C-06 keeps a 2M-row `resilience.cic_target` table (~150 MB) between runs on purpose;
  NL-C-03 truncates its bulk tables at cleanup.
- The harness grants `pg_checkpoint, pg_read_all_stats` to its role; disclosed, not revoked.
- `vendor/elle/elle-cli.jar` (40 MB) is tracked in git on main.
- **The target host runs a second PostgreSQL cluster.** Never kill by PID without binding it to
  our postmaster: NL-M-03's guarded kill checks parent pid (our `postmaster.pid`) and process
  title in the same command.
- NL-M-03 keeps `resilience.avac_target` (1M rows, ~40 MB) between runs, seeded once; a run
  takes ~15-20 min at a 60 s `autovacuum_naptime` (worker wait per cycle up to 2 x naptime + 30 s).
- `observe_fault_settings` gets `during=` only when the scenario sets one (keeps older fakes valid).
