# Implementation Plan: NL-N-I03 Lock Pile-up Behind a Schema Change

**Branch**: `scenario3-Lock-pile-up-behind-a-schema-change` | **Date**: 2026-11-05 | **Spec**: [spec.md](spec.md)

## Summary

A new fault type `lock_pileup`, held like the other NL-N/NL-R faults (`HeldFault`). The adapter
opens a tagged read that holds the contended table and, after a delay, a tagged schema change with
its own session lock timeout; a watcher reads the blocking chain from `pg_stat_activity` /
`pg_blocking_pids`. A new os_ssh-section driver applies and reverts it through the run's adapter.
The workload gains an opt-in contended share and per-group samples. Everything is added beside
the existing code; no existing scenario, driver set or event changes.

## Technical Context

**Language/Version**: Python 3.11+ | **Primary Dependencies**: asyncpg, asyncssh (RemoteHost),
pydantic v2 | **Storage**: PostgreSQL 17 target | **Testing**: pytest with fake engines |
**Target Platform**: harness VM 10.11.21.13 -> target 10.11.21.112 (`envs/e2-dedicated-vm-u22-2.yaml`) |
**Constraints**: phase bounds from the env profile; never a false pass.

## Constitution Check

| Principle | Check | Status |
|---|---|---|
| I. A Test Is Data | Three catalog files; the pile-up is the catalog field `fault.lock_pileup`, the contended share `workload.contended_share`; no host or tool named | PASS |
| II. Anything Unbuilt Refuses | Schema refuses malformed plans; engines without `contended_table` refuse the workload; an unknown schema change or a pile-up that did not start raises FaultNotLanded | PASS |
| III. Never a False Pass | `fault_confirmed` from the server's blocking chain; every measure NOT_MEASURED with a reason when its evidence is missing; negative unit test per measure | PASS |
| IV. Framework Source of Truth | Internal scenario, cited to Framework §10.6 category; thresholds marked NEEDS SIGN-OFF; `structural_integrity_errors == 0` gated in all runs (Framework §16.3) | PASS |
| V. Adapter Seam | All SQL in `adapters/postgresql/`; the driver, behaviour and workload call named adapter methods; no fault-type or scenario-ID branches | PASS |
| VI. Leave Target as Found | Session settings only; sessions tagged by application_name; revert ends them on the server and drops probe columns, verified; kill switch reaches them from any process | PASS |
| VII. Evidence Integrity | RPO from marker journals; server state, chain and log lines kept in facts; harness-table stand-in disclosed | PASS |

No violations.

## Project Structure

```text
catalog/schema.py                                   internal ids, LockPileup, Workload.contended_share, lock_pileup fault type
catalog/NL/NL-N-I03{a,b,c}.yaml                     the three runs
resilience_tests/adapters/base.py                   Capability.CONTENDED_TABLE; session, core and SessionFaultSupport hooks
resilience_tests/adapters/postgresql/adapter.py     contended table, contended transaction, crash-restart log patterns
resilience_tests/adapters/postgresql/sessions.py    start / state / end / revert of the pile-up
resilience_tests/execution/injectors/lock_pileup.py OsSshLockPileupDriver (new)
resilience_tests/execution/workload/driver.py       contended share, group_sample events
resilience_tests/control/faults/lock_pileup.py      LockPileupFault(HeldFault) (new)
resilience_tests/reporting/cases.py                 report case ids for internal scenarios
tests/test_lock_pileup.py                           unit tests
```

## Phases

- Phase 0: [research.md](research.md). Phase 1: [data-model.md](data-model.md),
  [contracts/](contracts/), [quickstart.md](quickstart.md).
