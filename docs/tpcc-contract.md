# Workload contract: the names the scenarios and the TPC-C workload agree on

Date: 6 November 2026
Status: **draft for review** (TPC-C build step 1). Contract version 1.
Code: [`resilience_tests/execution/workload/contract.py`](../resilience_tests/execution/workload/contract.py)
Tests: [`tests/test_workload_contract.py`](../tests/test_workload_contract.py)

## Why this document exists

Two developers work at the same time:
- the **scenario developer** builds the noisy-neighbour scenarios
- the **TPC-C developer** builds the TPC-C workload

The scenarios will use things only the workload can give: groups of clerks, results per group and
per transaction type, the clerks' timeouts and the TPC-C table names. This document fixes **the
names** of those things now, so both developers can build in parallel and the parts fit at the end.

**Nothing that exists changes.** Every existing scenario keeps working without a change to its
file. The new scenario fields are optional. The new sample fields are extra. The custom workload
(`marker`, `churn`, `list_append`) stays as it is.

**Rule:** after both developers sign this document, a name changes only by agreement of both,
and the contract version goes up.

---

## 1. The scenario file: new optional fields

```yaml
workload:
  profile: tpcc                      # new value; until TPC-C is built: refused as "not built yet"
  concurrency: 64                    # as today: the number of clerks
  transaction_markers: true          # as today
  rate_tps: 600                      # as today: the declared rate (Framework §7.5)
  session:                           # NEW, optional: set by each clerk on its own connection
    lock_timeout: 5s
    statement_timeout: 30s
  groups:                            # NEW, optional: named groups of clerks
    - {name: colliding, clerks: 6}
    - {name: non_colliding, clerks: 58}
  tpcc:                              # NEW, only with profile: tpcc
    warehouses: 320
    home_warehouse: {colliding: 7}   # every clerk of group "colliding" works on warehouse 7
```

| Field | Meaning | Rules |
|---|---|---|
| `session` | settings each clerk sets on its own connection when it connects | only `lock_timeout`, `statement_timeout`, `idle_in_transaction_session_timeout`. Never `ALTER ROLE`, never a server setting, so nothing leaks into the next scenario |
| `groups` | names for groups of clerks; every sample reports each group separately | names: small letters, digits, `_`; unique; the name `all` is reserved. **The clerks of all groups must add up to `concurrency`**, so every clerk is in exactly one named group. Without `groups`, all clerks are in the group `all` |
| `tpcc.warehouses` | the TPC-C data size the scenario expects | init refuses the run if the loaded data has a different size |
| `tpcc.home_warehouse` | which warehouse the clerks of a group work on | clerks not named here are spread over all warehouses, 5 warehouses per clerk (TPC-C standard size) |

**Not a scenario field: direct connection or connection pooler (sdbPool).** The environment file
already decides this: each node's `client` address is "the pooler if the deployment has one"
(`resilience_tests/control/profile.py`). The proposal's "connection path" field is therefore not
needed.

**Open decision (needs sign-off): the rate as a share of capacity.** The proposal suggested
`rate: {fraction_of_capacity: 0.6}`. The Framework (§7.5) requires a **declared** rate; today
`rate_tps` is required. The contract keeps `rate_tps` and proposes an optional alternative,
`rate_fraction_of_capacity: 0.6`, allowed only with `profile: tpcc`. Init would turn it into a TPS
number from the target's stored capacity result (refused if none is stored), and the report would
show the resulting TPS. Exactly one of the two may be given. **NEEDS SIGN-OFF** against Framework
§7.5 before step 2 builds it.

## 2. The 1-second sample in `events.jsonl`

Today's fields stay exactly as they are, and they cover **all** transaction types and groups
together, so the existing analysis works for every workload:

`interval_s`, `commits`, `tps`, `p50_ms`, `p95_ms`, `p99_ms`, `journal_p99_ms`, `errors`,
`indeterminate`, `drops`, `reconnects`, `connect_failures`

**Two new, extra fields:**

```json
"by_type": {
  "new_order": {"commits": 270, "tps": 270.0, "p90_ms": 6.1, "p99_ms": 41.0, "errors": 0, "business_rollbacks": 3},
  "payment":   {"commits": 258, "tps": 258.0, "p90_ms": 3.2, "p99_ms": 22.0, "errors": 0, "business_rollbacks": 0}
},
"by_group": {
  "colliding":     {"commits": 30,  "tps": 30.0,  "p50_ms": 900.0, "p95_ms": 4800.0, "p99_ms": 5000.0, "errors": 2, "indeterminate": 0, "drops": 0},
  "non_colliding": {"commits": 570, "tps": 570.0, "p50_ms": 4.0,   "p95_ms": 9.0,    "p99_ms": 15.0,   "errors": 0, "indeterminate": 0, "drops": 0}
}
```

| Field | Keys | Inside |
|---|---|---|
| `by_type` | `new_order`, `payment`, `order_status`, `delivery`, `stock_level` (the custom workload: `marker`) | `commits`, `tps`, `p90_ms`, `p99_ms`, `errors`, `business_rollbacks` |
| `by_group` | the group names of the scenario file (no groups: `all`) | `commits`, `tps`, `p50_ms`, `p95_ms`, `p99_ms`, `errors`, `indeterminate`, `drops` |

The names inside are the same as the top-level names. `p90_ms` is added per type because TPC-C's
response-time rule is on the 90th percentile (Clause 5.2.5.3).

**Before TPC-C exists**, the custom workload fills `by_type` with one entry (`marker`) and
`by_group` with one entry (`all`), or one per declared group. A measurement written against
`by_group` therefore works today.

## 3. Outcomes of one transaction

| Name | Meaning | Counted as |
|---|---|---|
| `committed` | the database confirmed the commit | a commit |
| `definitely_aborted` | the database answered with an error: the transaction did not happen | an error |
| `business_rollback` | a `definitely_aborted` transaction the workload **expected** (TPC-C: 1% of New-Orders) | **not** an error; counted apart |
| `unknown` | no answer (timeout, connection lost): it may or may not have committed | `indeterminate`, never "lost" and never "failed" |

The first three names besides `business_rollback` are the adapter's existing values
(`TransactionOutcome` in `adapters/base.py`).

## 4. Measure names

**Existing, unchanged:** `rpo_txn`, `indeterminate_txn`, `rto_first_write_s`, `rto_to_slo_s`,
`corruption_count`, `failed_transactions`, `dropped_connections`.

**New** (produced once the `tpcc` profile is built; until then a scenario asking for them is
refused as not built, never passed):

| Measure | Meaning |
|---|---|
| `nopm` | committed New-Orders per minute |
| `tpcc_response_time_ok` | 90% within 5 s (Stock-Level: 20 s), Clause 5.2.5.3 |
| `mix_ok` | the measured mix meets Clause 5.2.3's minimums |
| `business_rollbacks` | about 1% of New-Orders, Clause 2.4.1.4 |
| `partial_commits` | transactions found half-saved after the fault |
| `aborted_markers_present` | rolled-back transactions found in the database, Clause 3.5.4 |
| `tpcc_consistency_violations` | TPC-C consistency rules failing after recovery, Clause 3.3.2 |
| `driver_limited` | the harness could not keep the declared rate: the run is invalid |
| `capacity_tps` | the target's measured maximum |

**Per group and per type.** A measure for one group or one transaction type is named with
`__group_` or `__type_`:

| Example | Name |
|---|---|
| TPS ratio of group `non_colliding` | `tps_ratio__group_non_colliding` |
| p90 of New-Order | `p90_ms__type_new_order` |

```yaml
accept:
  - tps_ratio__group_non_colliding >= 0.95     # threshold: cite the Framework or NEEDS SIGN-OFF
```

The proposal wrote `tps_ratio_by_group.non_colliding`. **That form does not work:** the catalog
accepts only names of letters, digits and `_` (`catalog/schema.py`), and `accept:` rules are read
as expressions, where a dot means something else. A test proves both
(`tests/test_workload_contract.py`).

## 5. TPC-C table names

All TPC-C objects live in schema **`resilience_tpcc`**, owned by the harness, separate from the
harness's own schema `resilience`. Faults and checks use the constants, never a name written by
hand:

| Constant | Table |
|---|---|
| `TPCC_WAREHOUSE` | `resilience_tpcc.warehouse` |
| `TPCC_DISTRICT` | `resilience_tpcc.district` |
| `TPCC_CUSTOMER` | `resilience_tpcc.customer` |
| `TPCC_HISTORY` | `resilience_tpcc.history` |
| `TPCC_ORDERS` | `resilience_tpcc.orders` |
| `TPCC_NEW_ORDER` | `resilience_tpcc.new_order` |
| `TPCC_ORDER_LINE` | `resilience_tpcc.order_line` |
| `TPCC_ITEM` | `resilience_tpcc.item` |
| `TPCC_STOCK` | `resilience_tpcc.stock` |
| `TPCC_META` | `resilience_tpcc.meta` (warehouses, loader version, capacity result) |

Example: scenario 4's batch job updates `TPCC_STOCK` rows of warehouse 7.

## 6. The driver and the profile hooks

**The driver** (exists today; written down, not changed): `start`, `wait_until_ready`,
`begin_window`, `end_window`, `stop`, and the attributes `failure` and `window_t0_ns`.

**The profile hooks** (added in build step 2; they do nothing for the custom workload):

| Hook | Called in | Does |
|---|---|---|
| `prepare(adapter)` | init | checks the profile's data is present, the right size and consistent; otherwise the run is refused with the reason |
| `consistency_checks(adapter, run_dir)` | validate | profile-specific checks after recovery; returns measure name → value |

## 7. What the scenario developer can do now

| Now (contract agreed) | After build step 2 | After build steps 6 and 8 |
|---|---|---|
| write scenario files with these field names; write measurements and `accept:` rules with these measure names | run them on the custom workload: `by_type` = `marker`, `by_group` = `all` or the declared groups | switch to `profile: tpcc`; the TPC-C types, groups and new measures are filled with real values |

## 8. Sign-off

| | Name | Date | Agreed |
|---|---|---|---|
| Scenario developer | | | ☐ |
| TPC-C developer | | | ☐ |
| Open decision: `rate_fraction_of_capacity` vs Framework §7.5 | lead | | ☐ |
