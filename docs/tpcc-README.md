# TPC-C-like workload: what is built so far

Date: 11 November 2026
Branch: `tpcc-workload`
Status: **steps 0 to 9 of 11 built and tested (step 9 without sdbPool); about 80% of the work is done.** TPC-C runs, can be used in fault scenarios, creates heavy load across several processes, and resets its data before a run.

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
| 6 | The 5 TPC-C transactions as database functions, and the engine that sends them | ✔ built, tested, **trial passed on the lab** (new target BDB-QA-U22-3: 198 of 200 TPS, p99 10.5 ms) |
| 7 | RPO and atomicity checks after a fault | ✔ built and tested (including a real crash locally); live kill on the lab waits for sudo on the new target |
| 8 | Heavy load: worker processes, merged numbers, driver-limited guard, capacity test | ✔ built, tested, **verified on the lab** (capacity of the new target: 1,473 TPS at 64 clerks; the harness was not the limit) |
| 9 | Data reset before a run (reload, until a snapshot reset exists); p50 and slowest time per type | ✔ built and tested; lab dry run next. sdbPool: only if the lead decides it |
| 10 | HammerDB cross-check, switching the noisy-neighbour scenarios | to do |
| 11 | Optional clean-up: one engine for all workloads | later, optional |

**How much is done:**

| Measure | Done |
|---|---|
| steps | 10 of 11 (steps 0 to 9) |
| effort | **about 80%**: still ahead: `sudo_mode` for the new target with the step 7 live proof, the 320-warehouse load, and step 10 (cross-check, switching the scenarios) |
| risk | the biggest risk, whether Python can create enough load, is removed (step 0) |

**What exists now:** TPC-C runs: the five transactions in the database, the engine that sends
them with the harness's journals and samples (step 6), the inputs (step 5), the data (step 4), the
registry (step 3) and the fields and names the scenarios need (steps 1 and 2).
Heavy load across several worker processes and the capacity test are built (step 8), and the
data is reset before a run (step 9).
**What is still missing:** running the harness's sudo commands the way the new target allows
(`sudo_mode`), the step 7 live proof, the 320-warehouse load, and step 10 (HammerDB cross-check,
switching the noisy-neighbour scenarios to TPC-C). sdbPool waits for the lead's decision.

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

### Step 6: The five transactions and the engine

**Why:** this is where TPC-C really runs: the transactions inside the database, and the engine that
sends them while keeping every guarantee of the harness.

**What:**

| Part | What it does |
|---|---|
| the 5 database functions (`sql/tpcc_functions.sql`) | New-Order, Payment, Order-Status, Delivery, Stock-Level, following TPC-C Clauses 2.4.2 to 2.8.2. The three write transactions insert the harness's marker row **first, in the same transaction**: saved together or not at all. New-Order returns its order number; an unknown item raises its own error code (TP001), and the whole call, marker included, rolls back |
| one call per transaction (`adapters/postgresql/tpcc.py`) | sent on its own, not inside BEGIN ... COMMIT: "the call returned" means "committed". The outcome is classified: committed; business rollback (the expected 1%); sure rollback (the server said no: deadlock, lock timeout, statement timeout); unknown (no answer: never guessed) |
| the engine (`tpcc/engine.py`) | one process, many clerks: each with its home warehouse and session settings, the shared rate limiter, reconnecting, a 10 s limit per transaction. Write transactions: marker line on disk before the call; "committed" line after (New-Order with its order number and line count); a sure rollback gets a line in the new **"rolled back" journal**; an unknown outcome gets none. Read-only transactions write nothing |
| samples | every second: the totals (today's fields, read by the existing analysis), plus `by_type` with all 5 types (including business rollbacks, never counted as errors) and `by_group` |
| the per-order check (`tpcc_orders_incomplete`) | every acknowledged New-Order must exist with exactly its lines; used by the trial now and after a fault in step 7 |
| the trial command (`tpcc/trial.py`) | runs real TPC-C load on a target for a set time without a fault, then checks: the rate, the mix, errors, RPO, rolled-back transactions kept, the per-order check, the consistency rules, business rollbacks about 1% |
| the orchestrator | picks the TPC-C engine for `profile: tpcc`; refuses a real fault run with it until step 7 |

PostgreSQL now declares the capability `tpcc_workload`.

**Tests:** 11 for the engine and the trial checks (every outcome path with a scripted session; a
fault run with `tpcc` refused before anything is applied), and 9 against a real ShaktiDB:
- each function: New-Order visible from another connection the moment the call returns; a
  business rollback leaves no marker, no order and the order counter unchanged; Payment by number
  and by last name (the middle customer, Clause 2.5.2.2); read-only transactions write nothing;
  Delivery delivers the oldest order of every district
- a broken connection gives "unknown", never "committed"; a lock timeout gives a sure rollback
- 2,300 mixed transactions: exact mix, about 1% rollbacks, nothing lost, every order complete, all
  consistency rules hold
- the real engine for 6 s at 300 TPS: RPO 0, nothing rolled back kept, every order complete, rules
  hold; the per-order check catches a deliberately half-saved order

**Local trial (16 clerks, 400 TPS, 20 s):** 392 TPS, p50 3.1 ms, p99 9.3 ms; mix 43.5 / 43.4 /
4.4 / 4.3 / 4.4%; 0 errors, 0 unknown; RPO 0; 3,400 orders all complete; all rules hold;
business rollbacks 0.94%.

### Step 7: The checks after a fault

**Why:** this is what TPC-C is for in this project: after a crash, proving that no committed
transaction was lost **and none was half-saved**.

**What** (`tpcc/checks.py`, the profile's `consistency_checks`): from the journals and the database,
every write transaction gets one label:

| Marker in the database | Its TPC-C rows | Label |
|---|---|---|
| yes | complete | committed and kept |
| no | none | **lost** (the harness's `rpo_txn`) |
| yes | missing or incomplete | **partial commit** |
| no | present | **partial commit**, not only "lost" |
| a "rolled back" line, but the marker is in the database | | **rolled back but kept** |

How each case is found: the per-order check for acknowledged and lost New-Orders (now telling
"missing" from "incomplete"); for New-Orders with an unknown outcome, the number of orders created
during the run must equal the New-Orders whose marker survived; for Payment and Delivery, the
consistency rules (1 to 9 on all data, 10 and 12 on a sample of 20 districts). Unknown outcomes are
now exact: sure rollbacks are journaled and no longer counted as unknown. Two rare limits are stated
in the code (two opposite failures of unknown New-Orders cancel out in the count; one case can be
counted twice, which only over-counts).

New measures: `partial_commits`, `aborted_markers_present`, `tpcc_consistency_violations`, and an
exact `indeterminate_txn`. A check that cannot run makes its measure **not measured**, with the
reason; never 0. `tpcc` is now ready for real fault runs.

A run option tries any scenario with the TPC-C workload before the scenarios are switched for good:
`--tpcc-warehouses N` (disclosed in the report; the scenario file is not changed).

**Tests:** 27 without a database (every label, every "not measured" reason); 1 against a real
ShaktiDB in which four atomicity failures made by hand are each found with the right label; and a
**real crash** locally (immediate shutdown during TPC-C load): RPO 0, 7 unknown outcomes (the
transactions in flight), 0 partial commits, 0 rolled back but kept, 0 consistency violations, and
872 orders created = 872 New-Orders whose marker survived.

### Step 8: Heavy load (several worker processes) and the capacity test

**Why:** one Python process uses one CPU core, which is enough for about 2,000 TPC-C transactions
per second (the speed test measured about 4,800 at full use; we plan with a safety margin). The
noisy-neighbour scenarios need more, so the load is spread over several processes. And a scenario
must never measure the harness instead of the database.

**What:**

| Part | What it does |
|---|---|
| **worker processes** (`tpcc/coordinator.py`) | above 2,000 TPS asked, the harness starts one worker process per 2,000 TPS (at most one per clerk, and one core is left free). Each runs the step 6 engine for its share of the clerks and of the rate, and writes **its own journals** (`marker.0.jrnl`, `acked.0.jrnl`, `aborted.0.jrnl`, then `.1.`, ...). At 200 TPS (today's scenarios) nothing changes: one process, as before |
| **exact combined numbers** (`tpcc/samples.py`) | every second, each process sends its raw numbers: counts and **every single response time**. The coordinator merges them into one sample. Percentiles (p50, p95, p99) are taken over all response times together, never averaged: the average of two p99 values is not the p99 of all transactions |
| **the same seconds** | every process ends its 1-second intervals on the same whole second of the machine's clock, so one sample covers the same real second in every process (important around a crash, where RTO is measured) |
| **the driver-limited guard** | every second, each process's CPU use and how late its timer is are recorded (`driver_load` events). If a process uses 95% or more of a core, or its timer is 0.5 s or more late, for 5 seconds in a row, the run is **stopped as invalid**: the harness, not the database, is the limit. A slow database does not trigger it: clerks waiting for the database use no CPU |
| **a worker process that dies** | the run is stopped with the reason; its clerks' load silently missing would make a false baseline |
| **graceful stop** | at the end, each clerk finishes the transaction it is running (at most 5 s), and the last part-second is written as a final sample. So **the sample totals equal the journals exactly**, and fewer transactions end as "unknown" merely because the run ended |
| **the capacity test** (`tpcc/capacity.py`) | raises the rate in steps (200, ×1.5, ...), 30 s each. A step "kept up" when the database delivered at least 95% of the rate asked **and** TPC-C's response-time rule held (90% of transactions within 5 s; Stock-Level 20 s; Clause 5.2.5.3). The capacity is the best step that kept up; it is stored in the database (`resilience_tpcc.meta`). If the harness was the limit, only a lower bound is reported, never a capacity |
| the trial command | new option `--processes N`; it now reads every journal file, and fails if fewer orders were checked than New-Orders committed |

**Not built (waiting for a decision):** a scenario rate given as a share of the capacity
(`rate_fraction_of_capacity`, Framework §7.5). The capacity is measured and stored, ready for it.

**Tests:** 9 for merging and the guard (percentiles from all response times, totals, each process's
own interval, groups, how many processes, fair split of clerks, the guard firing only after 5 busy
seconds, a slow database not firing it); 6 for the capacity test's decisions; 1 for the journal
names; 2 against a real database: **3 worker processes** (RPO 0, nothing rolled back kept, every
acknowledged order complete, all rules hold, every second has all 3 processes, sample totals =
journal lines exactly) and **a worker process killed** (the run stops with the reason). Local
command checks: a 3-process trial passed every check; the capacity test found the laptop database's
maximum (509 TPS) and stored it, with the worker processes at 26 to 47% of a core.

**Two fixes found by the new tests** (in step 6 code): the last part-second of transactions was not
in any sample (nothing lost; the journals were complete, only the sample totals were short), and the
stop could cut a transaction between its "confirmed" journal line and its count. Both fixed by the
graceful stop.

### Step 9: The reset before a run, and p50 and slowest time per type

**Why:** every run adds orders, and a scenario may change a table (noisy-neighbour scenario 3 adds
a column to `history` and removes it). Runs are comparable only if each starts from the same data.
Option A was chosen: **reload the data**, until a disk snapshot reset is possible.

**What** (`tpcc/profile.py` `reset_data`, `tpcc/data.py` `reset_reasons`):

| | |
|---|---|
| **when** | in the init phase, **after** the harness has checked it is connected to the right target (hostname and sentinel row). Not in phase 0 "reset": that phase runs before the target check, and a reload writes gigabytes |
| **reload when** | the orders have grown past **1.2 times** the loaded size (NEEDS SIGN-OFF); a consistency rule (1 to 4) fails or cannot be checked; the data was loaded by another loader version; a table's columns differ from the loader's (a column left behind); a table has **1,000 dropped columns** (scenario 3's rule: PostgreSQL allows 1,600 columns per table, dropped ones included) |
| **never** | creates or resizes the data: missing data or another number of warehouses is refused with the load command, because loading a new size is a deliberate setup step (disk space, time) |
| **time** | init has a 180 s limit; a reload takes longer (16 warehouses: about 215 s). Before reloading, the harness extends init's limit by **twice the load time this target measured last time** plus 120 s, and records `phase_extended` in the events. The phase stays bounded |
| **after the reload** | the stored capacity is written back (same target, same size); a `CHECKPOINT` runs so the database's writing of the new data is done before the baseline (if the role may not run it, that is disclosed); a lock timeout of 30 s on the drop, so a session left holding a lock gives a clear error instead of a long wait |
| **evidence** | `workload_reset` in the results (reasons, time, growth before and after, checkpoint) and a line in the report's disclosures |

**Per transaction type**, each sample now also has `p50_ms` (the typical time) and `max_ms` (the
slowest response in that second), as the noisy-neighbour plans ask ("p50/p99 per transaction
type", "the worst Payment response"). Added to the contract; the custom workload reports them too.

**Not built:** sdbPool (waits for the lead's decision; the environment file's `client` entry is
where it would go). A snapshot reset can later replace the reload without changing the scenarios.

**Tests:** 21 without a database (each reason to reset, the limits, the reload bound, what is
recorded, a checkpoint that cannot run, another size refused; and in the orchestrator: a reset
longer than init's limit completes when it asks for time, and is stopped when it does not); 1
against a real database (a column left in `history` is found, the reload restores the columns and
dropped-column count, keeps the capacity, runs the checkpoint, and prepare accepts the data).

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

# a trial of the TPC-C engine on a target (step 6): real load, no fault, then every check
.venv/bin/python -m resilience_tests.execution.workload.tpcc.trial --env <env> --clerks 16 --rate 200 --seconds 60
#   --processes 3 runs the same trial in 3 worker processes (step 8)

# the capacity test (step 8): the target's maximum TPS, stored in resilience_tpcc.meta
.venv/bin/python -m resilience_tests.execution.workload.tpcc.capacity --env <env> --clerks 64

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
| `resilience_tests/execution/workload/tpcc/engine.py` | 6 | the TPC-C engine (one process) |
| `resilience_tests/execution/workload/tpcc/trial.py` | 6 | the trial command |
| `resilience_tests/execution/workload/tpcc/checks.py` | 7 | the checks after a fault |
| `resilience_tests/execution/workload/tpcc/coordinator.py` | 8 | several worker processes |
| `resilience_tests/execution/workload/tpcc/samples.py` | 8 | merging the numbers; the driver-limited guard |
| `resilience_tests/execution/workload/tpcc/capacity.py` | 8 | the capacity test |
| `tests/test_tpcc_reset.py` | 9 | the reset before a run |
| `envs/e2-u22-3.yaml` | lab | the new target BDB-QA-U22-3 |
| `resilience_tests/adapters/postgresql/sql/tpcc_functions.sql` | 6 | the five transactions |
| `resilience_tests/execution/workload/tpcc/data.py` | 4 | expected sizes and the checks of the loaded data |
| `resilience_tests/execution/workload/tpcc/load.py` | 4 | the load command |
| `resilience_tests/adapters/postgresql/tpcc.py` | 4 | loading, status, consistency, drop |
| `resilience_tests/adapters/postgresql/sql/tpcc_*.sql` | 4 | tables, indexes, loader, consistency rules |
| `tests/test_workload_contract.py`, `test_workload_additions.py`, `test_workload_profiles.py`, `test_tpcc_data.py`, `test_tpcc_load_db.py`, `test_tpcc_inputs.py`, `test_tpcc_engine.py`, `test_tpcc_functions_db.py`, `test_tpcc_checks.py`, `test_tpcc_speedtest.py`, `test_tpcc_samples.py`, `test_tpcc_capacity.py` | 0 to 8 | the tests |

---

## 8. Next steps

| Step | What | Checkpoint |
|---|---|---|
| `sudo_mode` | run the harness's sudo commands directly, without `-n` and `sh`, on the new target only | step 7 live proof: NL-C-01 with `--tpcc-warehouses 16` passes |
| 10 | HammerDB cross-check; the noisy-neighbour scenarios switch to `profile: tpcc` | numbers within the agreed range; every switched scenario passes |
