# Implementation Plan: NL-N-I01 Noisy Analytics Against Priority Customer Work

**Branch**: `origin/resilience-scenario_3` | **Date**: 2026-11-15 | **Spec**: [spec.md](spec.md)

## Summary

A new fault type `analytics_load`, held like the other NL-N faults (`HeldFault`). A new os_ssh
driver opens the report sessions through the run's adapter as the environment's analytics login;
each runs its queries back to back until the hold ends. The TPC-C workload gains a layout
calculated from the target (warehouses and active warehouses "by cache") and a rate taken from the
stored maximum of the same layout. Measurements: 1-second samples of report sessions and helpers,
cache counters of the customers' own tables at the window edges, the reports' outcomes and plans.

## Technical Context

**Language/Version**: Python 3.11+ | **Primary Dependencies**: asyncpg, asyncssh, pydantic v2 |
**Storage**: ShaktiDB 17.11.1.0 target | **Testing**: pytest with fake engines |
**Target Platform**: harness VM 10.11.21.13 -> target 10.11.21.14:5434 (`envs/e2-u22-3.yaml`) |
**Constraints**: phase bounds from the env profile; never a false pass; configuration observed only.

## Constitution Check

| Principle | Check | Status |
|---|---|---|
| I. A Test Is Data | One catalog file; the reports are `fault.analytics_load`, the sizing rules `workload.tpcc` fields, the rate share `workload.rate_fraction_of_capacity`; the login comes from the env file | PASS |
| II. Anything Unbuilt Refuses | Schema refuses malformed plans; a missing analytics login skips the scenario (`needs_infra`); a missing maximum, too low a connection limit or a too long timeout refuse at init | PASS |
| III. Never a False Pass | Fault confirmed from server samples at minute 1; validity rules stop the run before the fault; every measure NOT_MEASURED with a reason when its evidence is missing; the p90 check is an upper bound | PASS |
| IV. Framework Source of Truth | NL-N-07 limits (70%, 2x, 80%, limits enforced) cited; others NEEDS SIGN-OFF; `structural_integrity_errors == 0` gated (Framework §16.3) | PASS |
| V. Adapter Seam | All SQL in `adapters/postgresql/analytics.py` and `tpcc.py`; behaviour and driver call named adapter methods | PASS |
| VI. Leave Target as Found | Role and server settings read only; sessions tagged and ended by tag; temporary files checked gone; TPC-C data kept | PASS |
| VII. Evidence Integrity | RPO from marker journals; layout, role settings, samples, report outcomes and plans kept in facts | PASS |

## Project Structure

```text
catalog/schema.py                                    analytics_load fault, AnalyticsLoad, TpccOptions sizing, rate_fraction_of_capacity, InfraFeature analytics_login
catalog/NL/NL-N-I01.yaml                             the scenario
resilience_tests/control/profile.py                  Node.analytics (the report login endpoint)
envs/e2-u22-3.yaml                                   analytics endpoint, infra analytics_login
resilience_tests/adapters/base.py                    AnalyticsSupport interface; TpccSupport layout and capacity records
resilience_tests/adapters/postgresql/analytics.py    report sessions, queries, samples, cache counters, role settings, revert (new)
resilience_tests/adapters/postgresql/tpcc.py         memory cache, capacity records
resilience_tests/execution/workload/tpcc/sizing.py   the layout rules (new, pure)
resilience_tests/execution/workload/tpcc/profile.py  layout and rate resolved at init; layout handed to the adapter
resilience_tests/execution/workload/tpcc/terminals.py, inputs  customers on the active warehouses only
resilience_tests/execution/workload/tpcc/capacity.py capacity measured and stored per layout
resilience_tests/execution/injectors/analytics.py    OsSshAnalyticsDriver and the session plan (new)
resilience_tests/control/faults/analytics.py         AnalyticsLoadFault(HeldFault) (new)
tests/test_analytics_*.py, tests/test_tpcc_sizing.py unit tests
```

## Phases

- Phase 0: [research.md](research.md). Phase 1: [data-model.md](data-model.md),
  [contracts/](contracts/), [quickstart.md](quickstart.md). Phase 2: [tasks.md](tasks.md).
