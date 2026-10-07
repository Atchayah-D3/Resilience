# Implementation Plan: pgbench as the Harness Workload

**Branch**: `pgbench` (spec directory `specs/002-pgbench-workload-driver`) | **Date**: 2026-10-07 |
**Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/002-pgbench-workload-driver/spec.md`

## Summary

Add a second implementation of the harness's workload driver that offers load with pgbench, and select between it and today's built-in driver in the environment profile (pgbench by default, spec FR-020). The orchestrator, analysis, catalog and every scenario stay unchanged: both drivers implement the same interface ([contracts/workload-driver.md](contracts/workload-driver.md)) and emit the same per-second `sample` events and evidence files.

Approach:
- **Supervisor.** A pgbench supervisor runs **one pgbench process per client** (`-c 1`, rate split evenly), so a client dropped by a fault is detected and relaunched on its own, without disturbing healthy sessions (research R4).
- **Even spacing.** Each client paces itself inside its script to evenly spaced slots, not with pgbench's random `-R` schedule (spec FR-021, research R11). The scheduling lag is reported.
- **Same limits.** The steady-state limits are the scenario's own, identical for both generators: no allowance, and no subtraction of recording overhead (spec FR-010).
- **Adapter supplies the database specifics.** The PostgreSQL adapter supplies the transaction scripts (marker, churn, list-append) and connection settings, so the supervisor stays engine-agnostic (constitution V; research R2).
- **Outcome classification.** Only pgbench-reported serialization/deadlock failures are "definitely aborted"; every other end of a transaction is "unknown" (research R5).
- **Per-second samples.** Built from per-transaction timing evidence on the harness clock, with pgbench's own progress output as a throughput cross-check (research R3).
- **The before-commit / acknowledgement record (FR-005 to FR-007) is a team-owned design decision** (research R1, status OPEN). This plan states exactly what that mechanism must satisfy ([contracts/transaction-record.md](contracts/transaction-record.md)); the tasks that implement it are gated on the team recording the decision.

## Technical Context

**Language/Version**: Python 3.11 (harness); pgbench of the **same major version as the target server** on the driver host (ShaktiDB 17 on the lab; spec FR-004)

**Primary Dependencies**: existing harness stack (asyncpg, asyncssh, pydantic v2, PyYAML, pytest + xdist); pgbench as an external executable, started with `asyncio` subprocesses

**Storage**: evidence on the driver host (`<run_dir>/<run_id>/`): the same marker journals, `events.jsonl` and `history.edn` as today, plus `pgbench/` (per-launch command lines, stderr, summaries)

**Testing**: unit tests with a **fake pgbench** executable (a small script honouring the subset of options and output formats used); catalog checks; lab runs on `envs/e2-dedicated-vm.yaml`

**Target Platform**: Linux driver host (`BDB-QA-U22-29`) and target (`BDB-QA-U22-30`)

**Project Type**: CLI test harness (pytest-driven), catalog-as-data

**Performance Goals**: sustain each scenario's offered rate. NL-C-01: 200 TPS, steady-state floor 150. NL-M-07: 1000 TPS, floor 750. The achievable rate depends on R1's per-transaction cost and is measured in the first lab runs (spec SC-005).

**Constraints**: the evidence requirements of Architecture §6.2 / constitution VII without exception; no scenario or catalog change; no pgbench process outliving a run; the driver host's slow flush is already known to limit baselines (see CLAUDE.md)

**Scale/Scope**: one new driver module, one profile field, adapter additions (scripts, connection settings, capability), report lines, a fake pgbench, ~20 unit tests; the built-in driver stays as is

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Check | Status |
|---|---|---|
| I. A Test Is Data | The generator is chosen in the environment profile, never in a scenario. The catalog schema does not change. | PASS |
| II. Anything Unbuilt Refuses | pgbench missing, unsupported, or the adapter lacking the pgbench capability → refusal before the baseline, naming the cause. No silent fallback to the built-in driver (FR-004, FR-020). | PASS |
| III. Never a False Pass | Every fail-closed path is specified: unparseable pgbench output, unexpected exit, a missing record, a missing acknowledgement, relaunch races. Each gets a negative test (FR-019). | PASS (design); tests in tasks |
| IV. Framework Is the Source of Truth | No acceptance criterion changes. Architecture §6.1's assignment of RPO scenarios to a custom driver is departed from, and recorded in research R0 with the reason. | PASS (deviation recorded) |
| V. The Adapter Seam | The supervisor never sees SQL. The adapter supplies scripts and connection settings, behind a new capability. No scenario-ID branch. | PASS |
| VI. Leave the Target as Found | No new objects on the target beyond today's harness tables. Every pgbench process is tracked and confirmed gone at cleanup and by the kill switch's process check. | PASS |
| VII. Evidence Integrity | Depends on R1 meeting [contracts/transaction-record.md](contracts/transaction-record.md). Until R1 is decided and its contract tests pass, the pgbench driver must refuse to measure RPO. | **GATED on R1** |

**Gate result**: proceed with design. Tasks touching RPO evidence are blocked until R1 is decided and recorded. Everything else (selection, supervisor, samples from timing evidence, relaunch, shapes, cleanup, reporting) can be built and tested first.

**Build-out rule**: every current scenario measures RPO, so until R1 is implemented the pgbench driver refuses all of them. To keep the branch usable meanwhile, the repository's environment profiles set `workload.generator: builtin` **explicitly**. The schema default stays `pgbench` (FR-020), and the profiles switch to it in the final task, once R1's contract tests pass.

**Post-design re-check (Phase 1)**: unchanged. I–VI pass on the design artifacts; VII remains gated on R1. The contracts make the gate mechanical: the factory refuses pgbench for `transaction_markers: true` until the transaction-record contract tests (CT-1 to CT-6) pass.

## Project Structure

### Documentation (this feature)

```text
specs/002-pgbench-workload-driver/
├── plan.md                         # this file
├── research.md                     # Phase 0: decisions R0-R10 (R1 open, team-owned)
├── data-model.md                   # Phase 1: launches, transaction records, samples
├── quickstart.md                   # Phase 1: how to validate on the lab
├── contracts/
│   ├── workload-driver.md          # interface both drivers implement (unchanged callers)
│   ├── adapter-pgbench.md          # what the adapter supplies to the pgbench driver
│   ├── transaction-record.md       # what ANY before-commit/ack record mechanism must satisfy (R1)
│   └── profile-workload.md         # the new environment-profile field
└── tasks.md                        # Phase 2 (/speckit-tasks)
```

### Source Code (repository root)

```text
resilience_tests/
├── execution/workload/
│   ├── driver.py                   # existing built-in driver (unchanged behaviour)
│   ├── interface.py                # NEW: the shared driver interface + factory (profile -> driver)
│   ├── pgbench_driver.py           # NEW: pgbench supervisor (per-client processes, relaunch, samples)
│   ├── pgbench_output.py           # NEW: parsers for pgbench progress/abort/summary lines (pure)
│   ├── markers.py                  # existing journals + RPO arithmetic (reused unchanged)
│   └── history_writer.py           # existing (reused unchanged)
├── adapters/
│   ├── base.py                     # + Capability.PGBENCH_WORKLOAD, + pgbench_launch() hook
│   └── postgresql/adapter.py       # + transaction scripts per shape, connection settings
├── control/
│   ├── profile.py                  # + workload.generator (pgbench | builtin), pgbench path
│   └── orchestrator.py             # constructs the driver through the factory (one-line change)
└── analysis/report.py              # + generator used, launches, recording overhead

envs/*.yaml                         # + workload: {generator: pgbench}
tests/
├── fakes/fake_pgbench.py           # NEW: fake executable for unit tests
├── test_pgbench_output.py          # NEW: parser tests on recorded pgbench output
├── test_pgbench_driver.py          # NEW: supervisor, relaunch, partial loss, cleanup
└── test_workload_selection.py      # NEW: profile selection, refusals, no silent fallback
```

**Structure Decision**: a single project, following the harness's existing layout. The new driver sits beside the built-in one under `execution/workload/`. Database specifics go into the PostgreSQL adapter, per constitution V.

## Complexity Tracking

| Item | Why needed | Simpler alternative rejected because |
|---|---|---|
| One pgbench process per client (`-c 1` × concurrency) | A fault that drops some clients must be detected and repaired without touching healthy sessions (spec US3-2; NL-R-04 "existing sessions unaffected", NL-M-07 "zero dropped connections") | One pgbench with `-c N` cannot add clients back. Restarting it would kill healthy sessions mid-transaction and manufacture the very disturbance those scenarios measure. |
| Two drivers kept side by side | Spec FR-020: comparison, and a fallback for scenarios pgbench cannot sustain | Removing the built-in driver loses both; rejected by the user's decision. |
