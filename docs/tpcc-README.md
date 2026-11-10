# TPC-C-like workload: what is built so far

Date: 10 November 2026
Branch: `tpcc-workload`
Status: **steps 0 to 5 of 11 built and tested; about 35% of the work is done.**

This document explains, in plain words, what has been built for the TPC-C-like workload, how
it was checked, and what is left. The design and the plan are in:

| Document | Contents |
|---|---|
| [tpcc-workload-proposal.md](tpcc-workload-proposal.md) | what TPC-C is, why we build it this way, the design |
| [tpcc-build-plan.md](tpcc-build-plan.md) | the 12 build steps (0 to 11) with their checkpoints |
| [tpcc-contract.md](tpcc-contract.md) | the names the scenarios and the workload agree on |
| [tpcc-architecture.svg](tpcc-architecture.svg) | the architecture diagram |
| [../tools/tpcc_speedtest/README.md](../tools/tpcc_speedtest/README.md) | the speed test (step 0) |

---

## 1. What we are building, in short

The noisy-neighbour scenarios need a realistic database load: many clerks entering orders,
payments and deliveries, like a real business application. TPC-C is the industry-standard
model of such a load. We build a **TPC-C-like workload inside the existing harness**, so that:

- the harness keeps all its guarantees: exact RPO (lost transactions) by markers written to the
  driver host's disk before and after each commit, unknown outcomes never counted as lost,
  measurements every second, reconnecting through a fault
- a scenario switches to TPC-C with one line in its scenario file: `profile: tpcc`
- **today's workload (`marker`, `churn`, `list_append`) and every existing scenario stay
  unchanged** (the "add-only" design)

---

## 2. Progress

| Step | Work | Status |
|---|---|---|
| 0 | Speed test and language decision | ✔ done. Decision: **Python** |
| 1 | Agreed names (the contract) | ✔ built and tested. Waiting for sign-off |
| 2 | Small additions to shared files | ✔ built, tested, **live-verified** |
| 3 | Profile registry and TPC-C skeleton | ✔ built, tested, **live-verified** |
| 4 | TPC-C tables, loader, consistency rules, check before every run | ✔ built, tested, **loaded on the lab** (1 warehouse) |
| 5 | TPC-C inputs (NURand, the deck of 23 cards, the inputs of each transaction, clerk placement) | ✔ built and tested (pure code, nothing to run on the lab) |
| 6 | The 5 TPC-C transactions as database functions, and the engine that sends them | next |
| 7 | RPO and atomicity checks after a fault | to do |
| 8 | Heavy load: worker processes, capacity test | to do |
| 9 | Data reset between scenarios, sdbPool | to do |
| 10 | HammerDB cross-check, switching the noisy-neighbour scenarios | to do |
| 11 | Optional clean-up: one engine for all workloads | later, optional |

**How much is done:**

| Measure | Done |
|---|---|
| steps | 6 of 11 (steps 0 to 5) |
| effort | **about 35%**: the steps still ahead are the biggest (step 6, the transactions and the engine; step 7, the checks after a fault; step 8, heavy load) |
| risk | the biggest risk, whether Python can create enough load, is removed (step 0) |

**What exists now:** the harness knows TPC-C (step 3), has the TPC-C data (step 4), knows which
transaction each clerk sends next and with which values (step 5), and has the fields and names
the scenarios need (steps 1 and 2).
**What is still missing to run TPC-C:** the transactions in the database and the engine that sends
them (step 6), the checks after a fault (step 7) and the heavy load (step 8). Until then the profile `tpcc` is marked "not built", and any scenario that
asks for it is refused before anything touches the database.

---

## 3. What each step built

### Step 0: Speed test (can Python create enough load?)

**Why:** Python is slower than Java or Go. Before building the engine, we measured whether
Python is fast enough, so that nothing would have to be rebuilt in another language later.

**What:** a separate test tool, `tools/tpcc_speedtest/`. It sends TPC-C New-Orders (the biggest
TPC-C write transaction) with exactly the work per transaction the real engine will do: create
the inputs, write the marker line to disk, send one call to a database function, write the
"committed" line. After every run it checks itself: counts agree, nothing lost, nothing left
behind by rolled-back calls, every order complete.

**Result (two lab runs, about 64,000 New-Orders, all checks passed):**

| Measure | Value |
|---|---|
| Python CPU per transaction | **0.21 ms** (measured) |
| one Python process at full use of one core | **about 4,800 transactions per second** (calculated) |
| planned per Python process, with a safety margin | **2,000 transactions per second** |
| planned per harness machine | 4 cores: 6,000; 8 cores: 14,000; 16 cores: 30,000 transactions per second |

**Decision: Python.** To be agreed by the lead.

### Step 1: The agreed names (the contract)

**Why:** another developer builds the noisy-neighbour scenarios at the same time. Both sides must
use the same names, or the parts will not fit at the end.

**What:** `resilience_tests/execution/workload/contract.py` (names only, no behaviour) and
[tpcc-contract.md](tpcc-contract.md). It fixes the names of:

- the new scenario-file fields: `session` (timeouts per clerk), `groups` (named groups of clerks),
  `tpcc` (TPC-C options)
- the two new fields of the 1-second sample: `by_type` (per transaction type) and `by_group`
  (per group of clerks)
- transaction types, outcomes (including the expected 1% "business rollback", never an error) and
  measures; per-group and per-type measures are written like `tps_ratio__group_non_colliding`
- the TPC-C table names (`resilience_tpcc.stock` and the others), as constants

**Found while writing it:** the measure name in the first proposal
(`tps_ratio_by_group.non_colliding`) would not have worked, because the catalog and the pass
rules do not accept a dot in a name. The contract uses `__group_` instead; a test proves both.

**Tests:** 19, comparing the contract with the real harness, so they cannot drift apart.

### Step 2: Small additions to shared files

**Why:** TPC-C and the noisy-neighbour scenarios need a few new abilities in shared code, added
without changing anything for today's scenarios.

**What was added:**

| Addition | Effect on today's scenarios |
|---|---|
| optional scenario-file fields `session`, `groups`, `tpcc` and the profile name `tpcc`; wrong values refused with the reason | none |
| `profile: tpcc` refused as "not built yet" before anything touches the database | none |
| two profile hooks: "prepare" before a run, "consistency checks" after a fault (they do nothing for today's workload) | none |
| `by_type` and `by_group` in every 1-second sample (today: one type `marker`, one group `all`) | two extra fields; the existing analysis ignores them |
| session settings set by each clerk on its own connection (for example `lock_timeout`), never on the role or the server; listed in the report | none without the field |
| the RPO comparison also reads one journal file per worker process (for the later TPC-C engine); a missing journal is still an error, never a perfect RPO | identical result |

**A safety problem found and fixed:** the first version would have read a missing "committed"
journal as "nothing committed", which could hide lost data. It now refuses, and a test proves it.

**Tests:** 22 new; session settings also checked on a real database (set on the clerk's
connection only, nothing stored on the role or the database).

### Step 3: The profile registry and the TPC-C skeleton

**Why:** one place where workloads plug in by name, the same way the harness already handles
faults. A new workload is a new class; the orchestrator and the driver do not change.

**What:**

- the workload-profile interface and `@register_profile` (a second registration with the same name,
  or one without a name, is refused)
- the `tpcc` profile, registered but marked **not built**; each of its parts refuses with the build
  step that will fill it in
- a new engine capability `tpcc_workload` and an adapter part `TpccSupport`, where all TPC-C SQL
  lives (project rule: SQL only in the adapter); a TPC-C scenario on an engine without it is skipped
  with the reason

**Tests:** 15 new.

### Step 4: The TPC-C data

**Why:** TPC-C needs its real data, and a run must be able to trust that the data is complete,
the right size and correct before the test starts (TPC-C Clause 3.3.3.1).

**What:**

| Part | Contents |
|---|---|
| the 9 TPC-C tables | warehouse, district, customer, history, orders, new_order, order_line, item, stock, in their own schema `resilience_tpcc`, columns as in TPC-C 5.11 (BenchBase as the reference) |
| the loader | fills the tables inside the database by the TPC-C population rules (Clause 4.3): 10 districts per warehouse, 3,000 customers and 3,000 orders per district (the last 900 not yet delivered), last names made of syllables, 10% "ORIGINAL" items, and the rest; warehouses load in parallel |
| the 12 consistency rules (Clause 3.3.2) | for example "a warehouse's total equals the sum of its districts" and "an order has exactly as many lines as it says"; rules 1 to 3 check everything, the heavier rules can check a random sample of districts |
| safe loading | after loading, every table is counted and all 12 rules are checked; **only if everything passes** is the `meta` row (the "finished and correct" stamp) written. A load that stopped part-way has no stamp, and is never used |
| the load command | `python -m resilience_tests.execution.workload.tpcc.load --env <env> --warehouses N` (also `--status`, `--check`, `--drop`); first the same safety checks as the harness (allowlisted hostname, sentinel row, inventory tag, never production) |
| the check before every run | refuses data that is missing, has a different number of warehouses than the scenario, was loaded by another loader version, has grown past 1.5 times its loaded size, or breaks rules 1 to 4. It never reloads silently |

**Size:** about 105 MB per warehouse plus 13 MB of items. The standard size of 5 warehouses per
clerk gives 320 warehouses for 64 clerks: about **34 GB**.

**Tests:** 15 without a database, and 9 against a real ShaktiDB:

- a real load is exact, and all 12 rules hold
- the population rules are followed (customer 372 is named `PRICALLYOUGHT`, as in the TPC-C
  specification's own example)
- deliberately broken data is caught by the rule that owns it; a half-saved order makes the run
  refuse
- growth over the limit, an unfinished load and a second load without `--replace` are all refused
- the load command refuses every wrong target

### Step 5: The TPC-C inputs

**Why:** TPC-C defines exactly which transaction a clerk sends next and with which values: which
customer, which items, how many, which warehouse. These rules make the load realistic and
comparable with other TPC-C-like tools.

**What:** pure Python, no database (`tpcc/inputs.py`, `tpcc/terminals.py`):

| Part | Rule (TPC-C 5.11) |
|---|---|
| NURand | some customers and items are chosen far more often than others, like real popular products and regular customers (Clause 2.1.6) |
| the run's NURand constants | chosen once per run; the last-name constant must differ from the load's by 65 to 119 (not 96 or 112), so the run does not look up exactly the load's popular names (Clause 2.1.6.1); the load's constant is read from `meta` |
| last names | three syllables from a number 0 to 999; identical to the loader's SQL for all 1,000 numbers (Clause 4.3.2.3) |
| the mix | each clerk has a deck of 23 cards (10 New-Order, 10 Payment, 1 Order-Status, 1 Delivery, 1 Stock-Level), shuffled; one card per transaction (Clause 5.2.4.2) |
| New-Order | district 1 to 10, customer by NURand, 5 to 15 lines, items by NURand, quantity 1 to 10; 1% of lines from another warehouse; 1% of orders end with an item that does not exist and must roll back (Clause 2.4.1) |
| Payment | 85% by a customer of the home warehouse, 15% of another warehouse; amount 1.00 to 5,000.00 (Clause 2.5.1) |
| Order-Status, Payment | 60% of customers found by last name, 40% by number (Clauses 2.5.1.2, 2.6.1.2) |
| Delivery | carrier 1 to 10 (Clause 2.7.1); run as a normal transaction, as the common TPC-C-like tools do |
| Stock-Level | the clerk's own fixed district, threshold 10 to 20 (Clause 2.8.1) |
| clerk placement | a pinned group (for example 6 clerks on warehouse 7) works on its warehouse; every other clerk gets its own home warehouse, spread over the rest and avoiding the pinned ones |
| repeatable | fixed random seeds: the same scenario sends the same sequence of transactions every time |

Choices within the rules: items are distinct within one order, and order lines are sorted so
that two New-Orders always lock stock rows in the same order and cannot deadlock each other.

**Tests:** 42, each naming its clause; plus the check that Python and SQL last names are identical.
Full suite: 654 passed.

---

## 4. Live verification on the lab

Every run below was checked from its raw evidence (journals, events, database log, results), not
only from its summary.

| Run | Result | What was verified |
|---|---|---|
| speed test, 2 runs | ✔ all checks passed | the measurement is true: counts agree, nothing lost, every order complete |
| TPC-C load, 1 warehouse | ✔ 21 s, 118 MB | exact row counts, all 12 rules hold, `--status`, `--check` and `--drop` work, target left as found |
| **NL-C-01** (kill -9) | ✔ passed | a real kill and real crash recovery; RPO 0 recomputed from the journals (36,455 acknowledged, all present); 64 in-flight transactions reported as unknown, not lost; `pg_amcheck` clean; all samples carry correct `by_type` and `by_group` |
| **NL-C-03** (crash during a 10-million-row transaction) | ✔ passed | the big transaction was confirmed running when the kill hit and left no rows behind; RPO 0 (34,943 acknowledged, all present); Elle: 34,943 transactions, 0 anomalies; the 1,703 failed transactions are serializable conflicts the workload causes on purpose, before and after the fault |
| **NL-C-05** (10 crash cycles) | ✔ passed | 10 real kills (10 different postmaster processes), 10 crash recoveries in the database log, every cycle recovered in 1.0 to 2.7 s; RPO 0 (46,714 acknowledged, all present); 640 unknown outcomes = 64 clerks × 10 kills, plus 2 at the stop; quick integrity check clean after every cycle; full `pg_amcheck` clean |

Steps 2 and 3 touch code that every scenario uses, so these three scenarios had to give the same
results as before. **They do.** This was the condition for merging `tpcc-workload` into `main`.

**Two observations, no action taken (the scenarios belong to their owners):**

- NL-C-05's rule "the last cycle's recovery at most 1.5 times the first" passed with 1.44. Single
  cycles vary between 1.0 and 2.7 s on this target, so this comparison of two single cycles is
  close to its limit.
- For today's workload, `indeterminate_txn` also counts transactions that were surely rolled back
  (the workload keeps no "rolled back" journal). It is reported, never a pass rule, so it cannot
  cause a false pass. The TPC-C engine adds that journal (step 6).

**The lab's disk problem (found during these checks):** for some days, many runs aborted before
the fault with "steady state did not hold, slower side: target database". The cause is the lab
target's storage: the database volume spans two virtual disks, and the second one (`vdb`) was at
times very slow (flushes up to 456 ms, against under 0.3 ms on the first). The harness correctly
refused to inject faults on a database that was freezing; no false result was produced. The disk
recovered (later baselines: typical transaction 1.6 ms). A dedicated fast data disk for the target
would prevent this.

---

## 5. Open decisions

| Decision | Who | Why it matters |
|---|---|---|
| Python as the engine language (step 0) | lead | recorded decision of step 0 |
| the contract (step 1) | scenario developer and TPC-C developer | both build against it |
| a rate given as a share of capacity (`rate_fraction_of_capacity`) against Framework §7.5 | lead | not built until decided |
| the growth limit of 1.5 times (step 4) | lead | marked NEEDS SIGN-OFF |
| the full 320-warehouse load on the lab | when the target's disk is reliable | about 34 GB, about 30 minutes |
| a short pause after init, before the baseline starts measuring | lead and scenario owners | would keep setup disk activity out of the baseline on slower storage (idea only) |

**Stated choices:** random texts in the TPC-C tables use hex characters; their lengths follow
TPC-C exactly, and no rule or measurement depends on the characters. Reports will say
"TPC-C-like" and NOPM, never the audited name "tpmC".

---

## 6. How to use what exists

All commands run on the harness machine, in the harness folder.

```bash
# unit tests (all, about 14 minutes)
.venv/bin/python -m pytest tests -q

# the TPC-C data on a target
.venv/bin/python -m resilience_tests.execution.workload.tpcc.load --env <env> --warehouses 320 --jobs 4
.venv/bin/python -m resilience_tests.execution.workload.tpcc.load --env <env> --status
.venv/bin/python -m resilience_tests.execution.workload.tpcc.load --env <env> --check
.venv/bin/python -m resilience_tests.execution.workload.tpcc.load --env <env> --drop

# the real-database tests (only against a throw-away database, never the lab target)
RESILIENCE_TEST_PG=<host>:<port>/<database>/<user> .venv/bin/python -m pytest tests/test_tpcc_load_db.py -q

# the speed test (step 0)
.venv/bin/python -m tools.tpcc_speedtest all --env <env> --target-max-tps <maximum TPS of the target>
```

---

## 7. Where the files are

| File | Step | Contents |
|---|---|---|
| `tools/tpcc_speedtest/` | 0 | the speed test |
| `resilience_tests/execution/workload/contract.py` | 1 | the agreed names |
| `catalog/schema.py` | 2, 3 | the optional scenario fields; capability `tpcc_workload` |
| `resilience_tests/execution/workload/driver.py` | 2 | `by_type`, `by_group`, session settings |
| `resilience_tests/execution/workload/markers.py` | 2 | journal reading per worker process |
| `resilience_tests/execution/workload/hooks.py` | 2, 3 | the profile hooks; refusing profiles that are not built |
| `resilience_tests/control/orchestrator.py`, `control/matrix.py` | 2, 3 | calling the hooks; checking the workload's capability |
| `resilience_tests/adapters/base.py` | 2, 3, 4 | session settings; the `TpccSupport` part |
| `resilience_tests/execution/workload/profiles/` | 3 | the profile interface and registry |
| `resilience_tests/execution/workload/tpcc/profile.py` | 3, 4, 5 | the `tpcc` profile (prepare, clerk placement and next transaction built; execute and the checks after a fault still refuse) |
| `resilience_tests/execution/workload/tpcc/inputs.py`, `terminals.py` | 5 | the TPC-C inputs and clerk placement |
| `resilience_tests/execution/workload/tpcc/data.py` | 4 | expected sizes and the checks of the loaded data |
| `resilience_tests/execution/workload/tpcc/load.py` | 4 | the load command |
| `resilience_tests/adapters/postgresql/tpcc.py` | 4 | loading, status, consistency, drop |
| `resilience_tests/adapters/postgresql/sql/tpcc_*.sql` | 4 | tables, indexes, loader, consistency rules |
| `tests/test_workload_contract.py`, `test_workload_additions.py`, `test_workload_profiles.py`, `test_tpcc_data.py`, `test_tpcc_load_db.py`, `test_tpcc_inputs.py`, `test_tpcc_speedtest.py` | 0 to 5 | the tests |

---

## 8. Next steps

| Step | What | Checkpoint |
|---|---|---|
| 6 | the 5 transactions as database functions (marker row inside, order number returned), the single-process engine, the "rolled back" journal | a dry run keeps the rate; the mix, the 1% rollback and the per-order check pass |
| 7 | the checks after a fault: RPO over all journals, the per-order check, the consistency rules, "partial commit" labels | a live kill with `profile: tpcc`: 0 lost, 0 partial commits |
| 8 | worker processes, merged statistics, the driver-limited guard, the capacity test | the capacity test reaches the database's maximum without the harness being the limit |
| 9 | data reset between scenarios, sdbPool | the reset gives back correct data |
| 10 | HammerDB cross-check; the noisy-neighbour scenarios switch to `profile: tpcc` | numbers within the agreed range; every switched scenario passes |
