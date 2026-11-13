# Implementation Plan: NL-I-02 Index Corruption

**Branch**: `atchayah` | **Date**: 2026-10-12 | **Spec**: [spec.md](spec.md)

## Summary

Extend the NL-I-01 `data_corruption` fault so the catalog can aim it at a btree index
(`fault.relation_kind: btree_index`). The adapter builds a harness-owned table + index and names
a leaf page; the driver proves the page is a leaf, flips one byte with the database cleanly
down, and proves the flip with `pg_checksums`. Validate reads through the index (plan-checked),
runs amcheck on the index, then REINDEX and proves restoration.

## Technical Context

**Language/Version**: Python 3.11+ | **Primary Dependencies**: asyncpg, asyncssh (RemoteHost),
pydantic v2 | **Storage**: PostgreSQL 17 target | **Testing**: pytest with fake engines/hosts |
**Target Platform**: harness VM 10.11.21.111 -> target 10.11.21.112 |
**Constraints**: phase bounds from `envs/e2-dedicated-vm.yaml`; never a false pass.

## Constitution Check

| Principle | Check | Status |
|---|---|---|
| I. A Test Is Data | NL-I-02 is a catalog file; the object to corrupt is the catalog field `fault.relation_kind`; no host/tool named | PASS |
| II. Anything Unbuilt Refuses | `relation_kind` only valid for `data_corruption` (schema); unknown kind -> adapter refuses; non-leaf page / unclean stop / checksum mismatch -> abort | PASS |
| III. Never a False Pass | Read must be proven to use the index, else NOT_MEASURED; any non-corruption error NOT_MEASURED; `reindex_restores` needs three engine observations; negative test per accept rule | PASS |
| IV. Framework Source of Truth | Accept rules trace to Framework §10.4 (amcheck detects, REINDEX restores, no wrong results), §16.3 (amcheck clean); steady state NEEDS SIGN-OFF | PASS |
| V. Adapter Seam | Page layout, SQL, REINDEX, EXPLAIN live in the adapter; the driver gets a generic `page_check`; the orchestrator branches on the catalog field, never the ID | PASS |
| VI. Leave Target as Found | Only harness objects (`resilience.corruption_target` + `_idx`) are created, damaged, rebuilt and dropped; revert/kill switch drop the table; no configuration written | PASS |
| VII. Evidence Integrity | RPO from marker journals; every read, plan, REINDEX and check output kept in facts; exclusions disclosed | PASS |

No violations.

## Project Structure

```text
catalog/schema.py                                   Fault.relation_kind (+ validator)
catalog/NL/NL-I-02.yaml                             new scenario
resilience_tests/adapters/base.py                   read_index_target, rebuild_corruption_target, amcheck_relation(heapallindexed)
resilience_tests/adapters/postgresql/adapter.py     index target, forced index read, REINDEX
resilience_tests/execution/injectors/base.py        FaultInjector.corruption_kind
resilience_tests/execution/injectors/corruption.py  page_check, table-aware exclusions and revert
resilience_tests/control/orchestrator.py            pass relation_kind; index read; _measure_corruption index path
resilience_tests/analysis/report.py                 summary lines for index reads and REINDEX
tests/test_index_corruption.py                      unit tests (driver, adapter, run, false-pass paths)
```

## Phases

- Phase 0: [research.md](research.md) -- R1..R6.
- Phase 1: [data-model.md](data-model.md), [contracts/](contracts/), [quickstart.md](quickstart.md).
