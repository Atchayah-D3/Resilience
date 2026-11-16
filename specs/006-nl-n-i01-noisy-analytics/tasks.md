# Tasks: NL-N-I01 Noisy Analytics Against Priority Customer Work

**Input**: [spec.md](spec.md), [plan.md](plan.md)

## Phase 1: Setup

- [X] T001 Environment: `baseline: 720`, `validate: 3600`, shared_buffers note in envs/e2-u22-3.yaml

## Phase 2: Foundational

- [X] T002 `analytics_load` fault type, `AnalyticsLoad`, `TpccOptions` sizing fields, `Workload.rate_fraction_of_capacity`, capability `analytics_sessions`, InfraFeature `analytics_login` with validators in catalog/schema.py
- [X] T003 `Node.analytics` endpoint (required by infra `analytics_login`) in resilience_tests/control/profile.py; analytics endpoint and infra in envs/e2-u22-3.yaml
- [X] T004 [P] Layout rules in resilience_tests/execution/workload/tpcc/sizing.py (pure) with tests/test_tpcc_sizing.py

## Phase 3: User Story 1 - customers keep their service (P1)

- [X] T005 [US1] TpccSupport additions (cache bytes, layout, capacity records) in resilience_tests/adapters/base.py and resilience_tests/adapters/postgresql/tpcc.py
- [X] T006 [US1] Layout and rate resolved in init, customers on the active warehouses only, in resilience_tests/execution/workload/tpcc/profile.py, terminals.py
- [X] T007 [US1] Capacity test per layout (`--active-warehouses N|by_cache`, `--active-cache-share`) in resilience_tests/execution/workload/tpcc/capacity.py
- [X] T008 [US1] AnalyticsSupport interface in resilience_tests/adapters/base.py; PostgreSQL report sessions, queries, samples, cache counters, role settings, revert in resilience_tests/adapters/postgresql/analytics.py (mixed into the adapter)
- [X] T009 [US1] OsSshAnalyticsDriver and the session plan in resilience_tests/execution/injectors/analytics.py (+ import)
- [X] T010 [US1] AnalyticsLoadFault(HeldFault): settings, validity rules, sampler, cache snapshots, measures, summary in resilience_tests/control/faults/analytics.py (+ import)

## Phase 4: User Story 2 - limits stop only the reports (P1)

- [X] T011 [US2] Limit tests as first queries; temp_limit_enforced, statement_timeout_enforced, customer_limit_errors

## Phase 5: User Story 3 - back to normal, nothing left (P1)

- [X] T012 [US3] Recovery definition (95%), recovery watch extension, leftovers (sessions, temporary files)

## Phase 6: Scenario and tests

- [X] T013 catalog/NL/NL-N-I01.yaml
- [X] T014 Unit tests: tests/test_tpcc_sizing.py, tests/test_analytics_fault.py, tests/test_analytics_run.py (orchestrator, fake engine), tests/test_analytics_db.py (real database)
- [X] T015 Full unit suite and catalog checks
- [ ] T016 Live pilot on e2-u22-3 (user): capacity per layout, then NL-N-I01
