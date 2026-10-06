# Implementation Plan: NL-M-03 Autovacuum Worker Killed

**Branch**: `001-nl-m-03-autovacuum-kill` (spec directory; work happens on the team branch) |
**Date**: 2026-10-06 | **Spec**: [spec.md](spec.md)

**Input**: Feature specification from `specs/001-nl-m-03-autovacuum-kill/spec.md`

## Summary

Kill a confirmed-running autovacuum worker five times (Framework §10.7), each kill a
crash-restart of the instance, then prove autovacuum resumed: a new worker appears and every
eligible relation is vacuumed within 3 x the observed `autovacuum_naptime` (+ vacuum time).

Approach, reusing the harness's existing mechanisms:
- `fault.type: process_kill` with a new `fault.during: autovacuum_worker` value. The
  orchestrator's existing `_establish_fault_state` asks the adapter to make a worker appear on
  a harness-owned table and to identify it; the adapter returns a generic **kill target**
  (pid, parent pid, expected process title). The `os_ssh` driver kills exactly that pid after
  re-checking parent and title in the same command, instead of the whole unit cgroup.
- `repeat: {cycles: 5}` drives the five kills through the existing repeat loop (NL-C-05).
  A kill attempt that changed nothing (worker already exited) is retried within the cycle.
- After the last recovery, `_verify_during_operation` (existing) calls a new adapter check
  that samples the instance until the bound and reports `autovacuum_worker_respawned`,
  `relations_eligible` and `relations_left_unvacuumed`.
- Settings are read with `observe_fault_settings` (extended with `during`); nothing is written.

## Technical Context

**Language/Version**: Python 3.11 (harness), PostgreSQL 17 / ShaktiDB 17.11.1.0 (target)

**Primary Dependencies**: asyncpg, asyncssh, pydantic v2, PyYAML; pytest (+xdist) for tests

**Storage**: target database `resilience` (harness-owned schema `resilience`); evidence files
on the driver host (`~/resilience-runs/<run>/`)

**Testing**: `pytest tests/` with fake engines (unit); `python -m catalog.schema --partial`;
live run against the lab target (`envs/e2-dedicated-vm.yaml`)

**Target Platform**: Linux (Ubuntu 22.04) lab VMs: harness `.111`, target `.112`

**Project Type**: CLI test harness (pytest-driven), catalog-as-data

**Performance Goals**: a run completes within the profile's phase bounds: five cycles of
(worker wait <= 2 x naptime + 30 s, recovery, 10 s settle) plus a verification window of
3 x naptime + vacuum time. About 15-20 min at the target's 60 s naptime.

**Constraints**: the target hosts two PostgreSQL clusters (two autovacuum launchers seen), so a
pid kill must be bound to our postmaster; no configuration writes; driver-host fsync is slow
(baseline flakiness, see CLAUDE.md); one node blast radius.

**Scale/Scope**: one scenario file, one new `during` value, ~4 adapter methods, injector
targeted-kill path, orchestrator retry + observe-settings call, ~10 unit tests.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

| Principle | Check | Status |
|---|---|---|
| I. A Test Is Data | NL-M-03 is a catalog file; worker selection, table, title marker live in the adapter; driver chosen by the profile | PASS |
| II. Anything Unbuilt Refuses | Run refuses if autovacuum or track_counts is off (observed), if no worker appears within the bound (abort, reason), or if the restart cadence exceeds the unit limit | PASS |
| III. Never a False Pass | Each kill counts only if the pid was our autovacuum worker and is gone; `kills_landed == 5` gated; `relations_left_unvacuumed` is NOT_MEASURED when 0 relations were eligible (no vacuous pass); evidence post-dates the last recovery; negative unit test per new measure | PASS |
| IV. Framework Source of Truth | Accept cites Framework §10.7 (worker spawns, none unvacuumed), §10.2 (crash: RPO 0, unattended), §16.3 (amcheck); 5 kills and the 3 x naptime bound marked NEEDS SIGN-OFF / derived from observed config | PASS |
| V. Adapter Seam | Orchestrator sees only `during`, a generic kill target and a verify dict; no scenario-ID branch; PostgreSQL views stay in the adapter | PASS |
| VI. Leave Target as Found | Settings observed only; the harness-owned table's own storage options are harness objects (disclosed); table truncated/dropped at cleanup; ppid check prevents touching the other cluster | PASS |
| VII. Evidence Integrity | RPO from marker journals (mixed workload); "vacuumed" read from statistics that crash recovery reset, compared with the database clock at the final recovery, so they can only post-date the last kill; disclosures added | PASS |

No violations; Complexity Tracking not needed.

## Project Structure

### Documentation (this feature)

```text
specs/001-nl-m-03-autovacuum-kill/
├── spec.md
├── plan.md              # this file
├── research.md          # Phase 0 decisions
├── data-model.md        # kill cycle, kill target, vacuum evidence, observed settings
├── quickstart.md        # how to validate (unit + live)
├── contracts/
│   ├── catalog-nl-m-03.md     # scenario file contract + schema delta
│   └── adapter-injector.md    # new adapter/injector interface contract
├── checklists/requirements.md
└── tasks.md             # /speckit-tasks (not created here)
```

### Source Code (repository root)

```text
catalog/
├── schema.py                         # FaultDuring += "autovacuum_worker"
└── NL/NL-M-03.yaml                   # NEW scenario
resilience_tests/
├── adapters/base.py                  # defaults: start_autovacuum_worker, verify_autovacuum_resumed;
│                                     #   observe_fault_settings(fault_type, during)
├── adapters/postgresql/adapter.py    # avac_target table, dead-tuple generator, worker finder,
│                                     #   verification sampler, observed settings, cleanup
├── execution/injectors/base.py       # FaultInjector.kill_target (generic)
├── execution/injectors/process.py    # targeted pid kill with ppid + title re-check, confirm
└── control/orchestrator.py           # during=autovacuum_worker in _establish_fault_state /
                                      #   _verify_during_operation; not-landed retry; kills_landed
tests/
├── test_orchestrator.py              # fake adapter/injector support + NL-M-03 cases
├── test_autovacuum_worker_kill.py    # NEW: injector targeting, adapter verification logic
└── test_safety_ledger_profile.py     # fault-type loop covers targeted kill
CLAUDE.md                             # owners table + NL-M-03 gotchas
```

**Structure Decision**: existing single-project layout; NL-M-03 is a catalog row plus the
adapter/injector capabilities it needs. No new top-level modules.

## Complexity Tracking

Not applicable -- no constitution violations.
