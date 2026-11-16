# TPC-C-like workload

Date: 13 November 2026
Branch: `resilience-scenario_3` (the TPC-C work of branch `tpcc-workload`, merged with
noisy-neighbour scenario 3)

This document explains, in plain words, the harness's TPC-C-like workload:

1. what it is
2. how its tables are built and its data is loaded
3. how the workload works
4. how it runs inside a scenario
5. how its terms compare with HammerDB's (warehouses, virtual users, duration)
6. how it differs from the official TPC-C
7. what is built, what is left, the commands and the files

Related documents:

| Document | Contents |
|---|---|
| [tpcc-workload-proposal.md](tpcc-workload-proposal.md) | what TPC-C is, why we build it this way, the design |
| [tpcc-build-plan.md](tpcc-build-plan.md) | the build steps (0 to 11) with their checkpoints |
| [tpcc-contract.md](tpcc-contract.md) | the names the scenarios and the workload agree on |
| [tpcc-architecture.svg](tpcc-architecture.svg) | the architecture diagram |
| [../tools/tpcc_speedtest/README.md](../tools/tpcc_speedtest/README.md) | the speed test (step 0) |

Every TPC-C rule below names its clause in the TPC-C standard specification, revision 5.11.

---

## 1. What it is

TPC-C is the industry-standard model of an order-processing business: a company with
**warehouses**, each with 10 **districts** and 30,000 **customers**, whose **clerks** enter orders,
take payments, deliver orders and check stock. It is the usual way to put a realistic,
write-heavy business load on a database.

The harness has its own implementation of this model, called **TPC-C-like**: it follows TPC-C,
but it is not an audited TPC-C benchmark (section 6). It is a **workload profile** named `tpcc`,
next to the harness's original workload. A scenario switches to it with one line in its scenario
file: `profile: tpcc`.

What it keeps from the harness:

- **exact RPO (lost transactions):** every write transaction also saves a small marker row in the
  same transaction, and the harness writes each marker to the driver host's disk **before** the
  transaction is sent and **after** the database confirms it
- **outcomes that are never guessed:** committed, rolled back, or unknown (no answer); unknown is
  never counted as lost
- **measurements every second:** throughput and response times
- **reconnecting** through a fault, as a real application does

What it adds:

- the TPC-C data: 9 tables, sized by the number of warehouses
- the five TPC-C transactions, in the TPC-C mix
- checks after a fault that TPC-C makes possible: no transaction half-saved, and the TPC-C
  consistency rules still hold
- numbers per transaction type and per group of clerks
- several worker processes for high load

The original workload (marker inserts, churn, the Elle list-append history) and every scenario that
does not choose `tpcc` are unchanged.

---

## 2. Building the tables and loading the data

### 2.1 What is created

Everything lives in its own schema, **`resilience_tpcc`**, in the harness database. Nothing outside
it is created; the write transactions only insert their marker into the harness's own table
`resilience.markers`.

| Table | Rows (W = number of warehouses) | What a row is |
|---|---|---|
| `warehouse` | W | a warehouse and its year-to-date payments |
| `district` | 10 × W | a district, with the next order number |
| `customer` | 30,000 × W | a customer: name, credit, balance |
| `history` | 30,000 × W at load, +1 per Payment | one payment |
| `orders` | 30,000 × W at load, +1 per New-Order | an order |
| `new_order` | 9,000 × W at load | an order not yet delivered |
| `order_line` | about 300,000 × W at load | one line of an order (5 to 15 per order) |
| `item` | 100,000 (fixed) | an item for sale |
| `stock` | 100,000 × W | the stock of one item in one warehouse |
| `meta` | 1 | the harness's record of the load (not a TPC-C table, see 2.3) |

Also created: the primary keys, two extra indexes (customer by last name, a customer's latest
order), and **the five transactions as database functions** (section 3.3).

**Size:** about 105 to 110 MB per warehouse, plus about 15 MB for the items. For example:
16 warehouses about 1.8 GB, 32 warehouses about 3.5 GB, 320 warehouses about 34 GB.

### 2.2 How the load works

The data is made **inside the database**, by SQL functions that follow TPC-C's population rules
(Clause 4.3): customer last names made of syllables, 10% bad credit, 3,000 orders per district of
which the last 900 are not yet delivered, 10% "ORIGINAL" items, and so on. Nothing is generated on
the harness machine and sent over the network.

The steps of every load:

1. create the schema, the tables and the functions
2. load the 100,000 items, then each warehouse in its own transaction, **4 warehouses in
   parallel** (changeable with `--jobs` when loading by hand)
3. add the primary keys and indexes; run `VACUUM (ANALYZE)`, so the database has statistics to plan
   the transactions
4. count every table's rows against TPC-C's numbers (Clause 4.2.2) and check **all 12 consistency
   rules** (Clause 3.3.2, for example "a warehouse's total equals the sum of its districts")
5. **only if everything is correct**, write the `meta` row

A load that stops part-way, or whose data is wrong, has no `meta` row, and its data is never used.
The same loader seed gives the same data, so a load can be repeated exactly.

### 2.3 The `meta` row: the "finished and correct" stamp

| Column | Meaning |
|---|---|
| `warehouses`, `items` | the size that was loaded |
| `loader_version` | the loader's version; data from another version is loaded again |
| `load_seed` | the loader's random seed |
| `c_last_load` | a random constant of the load, needed by the transactions (Clause 2.1.6.1) |
| `loaded_orders` | the number of orders right after the load: the base of the growth check |
| `loaded_at`, `load_seconds` | when, and how long the load took |
| `capacity_tps` | the target's measured maximum (section 7.3); empty until measured |

### 2.4 Two ways to load

**A. Automatically, inside a scenario run (the normal way).** In the init phase, **after** the
harness has checked that it is connected to the right target (hostname and sentinel row), the
data step decides:

| The data on the target | What the run does |
|---|---|
| no TPC-C data | **loads** it |
| another number of warehouses than the scenario's | **loads again** at the scenario's number |
| grown past 1.2 times its loaded size, a consistency rule (1 to 4) failing, another loader version, columns changed, or 1,000 dropped columns | **loads again**, back to the loaded state |
| the run option `--tpcc-fresh-load` was given | **loads again**, even if the data is fine |
| otherwise | **uses it as it is** (takes about 2 seconds) |

Around a load:

| | |
|---|---|
| free disk space | before loading, the free space on the database's filesystem plus what the old TPC-C data frees must be at least **1.5 ×** the expected size; otherwise the run is refused with the numbers, before anything is changed (NEEDS SIGN-OFF) |
| time | the init phase normally has 180 s. Before loading, the harness extends that limit from measurements: from this target's last load of the same size, or else first by 300 s and then, after the first warehouse, by 3 × the expected total measured from its real speed. Every extension is recorded (NEEDS SIGN-OFF) |
| after the load | a `CHECKPOINT`, so the database has written the new data before the baseline starts; the stored capacity is kept when the size is the same |
| evidence | `workload_reset` in `results.json` (loaded or not, the reasons, the time, the seed, the free space) and a line in the report's disclosures |

**B. By hand, with the load command (optional).** Used to prepare a large size ahead of time, and
for trials and the capacity test, which never load by themselves:

```bash
.venv/bin/python -m resilience_tests.execution.workload.tpcc.load --env e2-u22-3 --warehouses 16 --jobs 4
.venv/bin/python -m resilience_tests.execution.workload.tpcc.load --env e2-u22-3 --warehouses 16 --jobs 4 --replace
.venv/bin/python -m resilience_tests.execution.workload.tpcc.load --env e2-u22-3 --status
.venv/bin/python -m resilience_tests.execution.workload.tpcc.load --env e2-u22-3 --check
.venv/bin/python -m resilience_tests.execution.workload.tpcc.load --env e2-u22-3 --drop
```

`--replace` is needed when TPC-C data already exists; without it the load is refused. Before any
load the command makes the same safety checks as a scenario run (allowlisted hostname, sentinel
row, inventory tag, never production), and it only creates or drops the schema `resilience_tpcc`.
No server or role setting is changed.

### 2.5 Growth

Every New-Order adds an order and every Payment a history row, so the data grows with use. Growth
is **current orders ÷ loaded orders**. Above **1.2**, a scenario run loads the data again by itself
(2.4). Above **1.5**, a trial or capacity test refuses to start. Both limits are NEEDS SIGN-OFF.

---

## 3. How the workload works

### 3.1 Clerks

A **clerk** is one simulated user with **one database connection** of its own. The scenario's
`concurrency` is the number of clerks (the trial's `--clerks`). Each clerk has:

- a **home warehouse**: clerks are spread evenly over the warehouses, one warehouse each; only
  when there are more clerks than warehouses do clerks share one. A scenario can also pin a named
  group of clerks to one warehouse (`tpcc.home_warehouse`), for scenarios where clerks must collide
- a fixed district for Stock-Level
- its own **deck of 23 cards**: 10 New-Order, 10 Payment, 1 Order-Status, 1 Delivery, 1 Stock-Level,
  shuffled. Each transaction takes the next card, so the mix is exactly 43.5 / 43.5 / 4.3 / 4.3 /
  4.3% (Clause 5.2.4.2)

### 3.2 One transaction, step by step

```
1. wait for the next start slot (the rate, 3.4)
2. take the next card from the deck: which transaction
3. choose its inputs by the TPC-C rules: which district, which customer (some are popular),
   which items and how many (Clauses 2.4 to 2.8)
4. write transactions only: write the marker line to marker.jrnl and flush it to disk
5. send ONE call to the transaction's database function
6. record the outcome:
     committed           -> "confirmed" line in acked.jrnl, flushed to disk
     business rollback   -> line in aborted.jrnl (the planned 1% of New-Orders, never an error)
     sure rollback       -> line in aborted.jrnl (deadlock, lock timeout, ... : nothing was saved)
     unknown (no answer) -> no line; the connection is closed and opened again
7. count it in this second's numbers; back to 1
```

A transaction may take at most 10 seconds; after that its outcome is unknown. After a lost
connection the clerk reconnects (2 s limit per attempt, 0.2 s pause between attempts) and
continues. Read-only transactions (Order-Status, Stock-Level) write no marker and no journal line.

### 3.3 The five transactions in the database

Each transaction is **one database function**, called once, on its own (not inside
`BEGIN ... COMMIT`): when the call returns, the transaction is committed.

| Transaction | What it does | Writes a marker |
|---|---|---|
| New-Order | creates an order with 5 to 15 lines, takes the items from stock; 1% of orders name an item that does not exist and must roll back completely (Clause 2.4) | yes |
| Payment | a customer pays: updates warehouse, district and customer totals, adds a history row (Clause 2.5) | yes |
| Delivery | delivers the oldest undelivered order of each of the 10 districts (Clause 2.7) | yes |
| Order-Status | reads a customer's latest order and its lines (Clause 2.6) | no |
| Stock-Level | counts recently sold items with low stock (Clause 2.8) | no |

A write transaction inserts its marker row **first, in the same transaction**: the marker and the
TPC-C rows are saved together or not at all. That is what lets the harness tell "lost" from
"half-saved" after a crash.

The workload is **repeatable**: a fixed seed drives every random choice, so the same scenario sends
the same sequence of transactions every time.

### 3.4 The rate (pacing)

There is **no waiting between a clerk's transactions** (TPC-C's keying and thinking times are not
used, as in most TPC-C-like tools, section 6). The only pacing is the scenario's rate
(`rate_tps`): the starts of all clerks together are spread evenly, at most `rate_tps` per second.

- a rate the database can deliver: the database works at that steady load (scenarios use about
  60% of the target's maximum)
- a rate far above what the database can deliver (for example 100,000): each clerk sends its next
  transaction as soon as the previous one is answered, **as fast as the database answers**, as
  benchmark tools do

### 3.5 Journals (RPO)

| File (driver host, run folder) | Written | Content |
|---|---|---|
| `marker.jrnl` | before a write transaction is sent | marker id, transaction type |
| `acked.jrnl` | after the database confirmed it | marker id; New-Order also its order number and line count; Payment also the customer and amount |
| `aborted.jrnl` | after the database answered "rolled back" | marker id, type, error code |

With several worker processes, each writes its own set (`marker.0.jrnl`, `marker.1.jrnl`, ...).

### 3.6 Measurements every second

Every second the workload writes one `sample` event to `events.jsonl`:

| Fields | Meaning |
|---|---|
| `tps`, `commits` | committed transactions (all types together) |
| `p50_ms`, `p90_ms`, `p95_ms`, `p99_ms`, `max_ms` | response times over every attempt |
| `p99_corrected_ms`, `missed` | p99 corrected for stalls and outages, and the planned starts that a stall skipped (so a freeze or a crash cannot hide in the numbers) |
| `errors`, `indeterminate`, `drops`, `reconnects`, `connect_failures` | failures and connection events |
| `by_type` | per transaction type: TPS, p50, p90, p99, slowest, errors, business rollbacks |
| `by_group` | per named group of clerks |

Response times are measured from when the transaction was sent; a scenario can choose to measure
them from when it **should** have started instead (`latency_from: intended_start`), so a stall is
part of every delayed transaction's time.

### 3.7 Heavy load: several worker processes

One Python process uses one CPU core, which is enough for about 2,000 TPC-C transactions per second.
Above that, the workload starts **one worker process per 2,000 TPS** asked (at most one per clerk,
always leaving one core free). Each runs its share of the clerks and the rate; every second their
raw numbers are merged into one sample, with percentiles taken over all response times together.

**Driver-limited guard:** if a worker process uses 95% or more of a core, or its timer is 0.5 s or
more late, for 5 seconds in a row, the run is stopped as invalid, because the harness machine, not
the database, would be the limit.

### 3.8 Stopping

At the end, no new transaction starts; each clerk may finish the one it is running (at most 5 s),
and the last part-second is written as a final sample. So the sample totals equal the journals.

---

## 4. How it runs inside a scenario

### 4.1 In the scenario file

```yaml
requires: [transactional_markers]
workload:
  profile: tpcc
  concurrency: 64            # clerks
  rate_tps: 900              # all clerks together
  transaction_markers: true
  tpcc:
    warehouses: 32           # the data size the run needs (loaded by the run if missing)
steady_state:
  warmup_s: 300              # optional: load runs this long, unmeasured, before the baseline
  duration_s: 600            # the baseline window
accept:                      # these four are required with profile tpcc, plus the scenario's own
  - rpo_txn == 0
  - partial_commits == 0
  - aborted_markers_present == 0
  - tpcc_consistency_violations == 0
```

The scenario file names only the workload. Which machine it runs on comes from the environment
file (`--env`).

### 4.2 The run, phase by phase

| Phase | What happens with the TPC-C workload |
|---|---|
| reset | nothing TPC-C-specific |
| init | the target's identity is checked; then **the data step** (2.4): use, or load (again); the data is checked (rules 1 to 4) and the functions are installed |
| baseline | the clerks connect; when all are connected and sending, the optional **warm-up** (`warmup_s`, unmeasured) runs; then the **baseline window** (`duration_s`) is measured: TPS, p99, per type |
| pre-fault | the baseline must meet the scenario's steady-state limits (TPS, p99); otherwise the run stops before any fault, with the slower side named (the database or the harness machine) |
| fault inject | the fault is applied **while the clerks keep running**. Through a crash they reconnect by themselves; their failed and unknown transactions are counted |
| recovery | the clerks keep running; the harness measures how long until TPS and p99 are back near the baseline |
| validate | the clerks stop (3.8); then the checks after a fault (4.3), the database's integrity check, and the pass rules |
| report | `results.json`, `summary.txt`, `report.html`, with the TPC-C label and the data step's disclosure |
| cleanup | the harness's own leftovers removed; the TPC-C data is **kept** for the next run |

### 4.3 The checks after a fault

From the journals and the database, every write transaction gets one label:

| Marker in the database | Its TPC-C rows | Label | Measure |
|---|---|---|---|
| yes | complete | committed and kept | |
| no, but it was confirmed | none | **lost** | `rpo_txn` |
| yes | missing or incomplete | **partial commit** | `partial_commits` |
| no | present | **partial commit** | `partial_commits` |
| yes, but the database answered "rolled back" | | **rolled back but kept** | `aborted_markers_present` |

How they are found: every confirmed New-Order is checked for its order and exactly its lines; every
confirmed Payment for its history row; and the TPC-C consistency rules (1 to 9 on all data, 10 and
12 on a new random sample of districts every run) → `tpcc_consistency_violations`.
A check that cannot run makes its measure **not measured**, which fails the run; it is never
reported as 0.

### 4.4 Run options

| Option | Effect |
|---|---|
| `--tpcc-warehouses N` | run any scenario with the TPC-C workload instead of its own, at N warehouses, without changing its file (the report says so; the four TPC-C rules are added) |
| `--tpcc-fresh-load` | load the data again before the run, even when it is fine |
| `--stop-before-fault` | dry run: the data step and the baseline, no fault, no verdict |

### 4.5 Outside scenarios: trial and capacity test

- **Trial:** the workload on its own, no fault, for a set time, then every check (rate, mix, RPO,
  orders, payments, consistency rules, about 1% business rollbacks):
  `tpcc.trial --env <env> --clerks 16 --rate 200 --seconds 60`. It measures from the moment all
  clerks are connected (no warm-up).
- **Capacity test:** raises the rate in steps of 30 s (each with 5 s warm-up) until the database
  no longer keeps up (95% of the rate, and TPC-C's response-time rule: 90% within 5 s); stores the
  best step in `meta.capacity_tps` and prints it with an NOPM estimate.

Neither of them loads data; both refuse when the data is missing, of another size, or grown past
1.5 times.

---

## 5. HammerDB's terms in our workload

HammerDB's TPC-C-like test is called **TPROC-C**. Its settings map to ours as follows.

**HammerDB uses the name "virtual user" for two different things**, in two different steps:

| HammerDB setting | Used when | What each one does | Ours |
|---|---|---|---|
| **Virtual Users to Build Schema** | only while **loading** the data | loads warehouses; sends no transactions | **load jobs** (`--jobs`) |
| **Virtual Users** (in the driver) | while **running** the test | a simulated user sending transactions | **clerks** (`concurrency`) |

Example: HammerDB with "Virtual Users to Build Schema = 4" and "Virtual Users = 64" is like our
`load --jobs 4`, then a run with 64 clerks.

### 5.1 Building the schema

| HammerDB | Ours | Note |
|---|---|---|
| Number of Warehouses | `tpcc.warehouses` in the scenario file; `--warehouses` of the load command | the same meaning: TPC-C's size unit |
| **Virtual Users to Build Schema** (the build's virtual users, **not** the driver's) | **load jobs**: `--jobs` of the load command (4 inside a scenario run) | the number of warehouses loaded at the same time. They only load data and send no transactions, so they are not clerks |
| data made by the HammerDB client and sent to the database | data made **inside** the database by SQL functions | same TPC-C population rules |
| stored procedures / functions created by the build | the five database functions created by the load | each transaction is one call in both |

### 5.2 Running the driver

| HammerDB | Ours | Note |
|---|---|---|
| **Virtual Users** (the driver's virtual users) | **clerks** (`concurrency`; trial `--clerks`) | see 5.3 |
| the extra **monitor** virtual user of a timed test | none: the harness measures inside each process | HammerDB's monitor sends no transactions, so N virtual users plus 1 monitor = our N clerks |
| Keying and Thinking Time (off by default) | not used, ever | with it off, both send the next transaction as soon as the previous one is answered |
| (no rate setting: each virtual user runs flat out) | `rate_tps` (trial `--rate`) | to match HammerDB, give a rate far above the database's maximum (3.4) |
| **Minutes of Rampup Time** | `steady_state.warmup_s` in a scenario; the trial has no warm-up | unmeasured time before measuring |
| **Minutes for Test Duration** | trial `--seconds`; in a scenario, the baseline window `steady_state.duration_s` | the measured time. In a scenario the workload runs on through the fault and recovery, so the whole run is longer |
| Total Transactions per User | none: our runs are timed | |
| Use All Warehouses | always: clerks are spread evenly over the warehouses | HammerDB by default gives each virtual user a random home warehouse; with this option on, all warehouses are used |
| Asynchronous Scaling (many connections per virtual user) | **built in**: one worker process runs many clerks at once, each with its own connection | HammerDB needs it for runs **with** thinking time, where each simulated user is idle most of the time and thousands of them are needed. Thinking time is not used here, so each clerk is always busy and few connections reach the database's maximum |
| Time Profile | built in: a sample every second, with p50 to p99 per transaction type | |

### 5.3 Is a HammerDB virtual user the same as our clerk?

**Yes, for the run.** A HammerDB virtual user in the driver, with keying and thinking time off
(its default), is the same thing as our clerk:

| | HammerDB virtual user | Our clerk |
|---|---|---|
| database connections | one | one |
| home warehouse | one | one |
| what it does | sends one transaction after another, each a call to a database function | the same |
| waiting between transactions | none (keying and thinking off) | none |

The differences:

| Difference | HammerDB | Ours | Effect on a comparison |
|---|---|---|---|
| pacing | each virtual user runs as fast as it can | all clerks share one rate | none, when our rate is set far above the maximum |
| monitor | one extra virtual user that sends nothing | none | count only HammerDB's working virtual users |
| choosing the transaction | a random number from 1 to 23 for each transaction (the same 10/10/1/1/1 shares on average) | a shuffled deck of 23 cards (exact shares) | none over minutes |
| extra work per write transaction | none | a marker row in the same transaction, and two journal lines flushed to the harness machine's disk | ours does a little more work per transaction |

HammerDB's **build** virtual users ("Virtual Users to Build Schema") are something else: they
only load the data, like our load jobs (`--jobs`). They are not clerks.

With asynchronous scaling on, one HammerDB virtual user drives many connections; that is like one
of our worker processes, which always runs many clerks at once (section 3.7):

| HammerDB | Ours |
|---|---|
| 1 virtual user, no asynchronous scaling = 1 connection | 1 clerk = 1 connection |
| 1 virtual user **with** asynchronous scaling = many connections | 1 worker process = many clerks (always) |

In the official TPC-C, a terminal waits 5 to 25 seconds between transactions (keying and thinking),
so one terminal makes only about one transaction every 10 to 20 seconds. Without those waits, one
HammerDB virtual user, or one of our clerks, does the work of many TPC-C terminals.

### 5.4 The result numbers

| HammerDB | Ours | Note |
|---|---|---|
| **NOPM** (New-Orders per minute) | New-Orders committed ÷ minutes: in the trial's `summary.json`, `summary.commits.new_order` ÷ (`--seconds` ÷ 60); the capacity test prints an estimate | **the number to compare**: both count only New-Orders |
| **TPM** (transactions per minute) | TPS × 60 | not the same: HammerDB reads TPM from the database's own commit counters, which include every transaction in the database; ours counts only the workload's committed transactions |

### 5.5 A like-for-like comparison

Same target, same data size, same number of users, no fault:

| Setting | HammerDB | Ours |
|---|---|---|
| warehouses | Number of Warehouses = 32 | data loaded at 32 warehouses (`load --warehouses 32`) |
| users | Virtual Users = 64 (plus the monitor) | `--clerks 64` |
| pacing | keying and thinking off | `--rate 100000` (far above the maximum) |
| warm-up | Rampup = 1 minute | run one short trial first (the trial itself has no warm-up) |
| measured time | Test Duration = 5 minutes | `--seconds 300` |
| result | NOPM | New-Orders committed ÷ 5 |

```bash
.venv/bin/python -m resilience_tests.execution.workload.tpcc.trial --env e2-u22-3 --clerks 64 --rate 100000 --seconds 300
```

With `--rate 100000` the trial's "rate kept" check fails by design; every other check must pass.

---

## 6. Differences from the official TPC-C

| Official TPC-C | Ours | Why |
|---|---|---|
| keying and thinking times between transactions | none | as HammerDB, BenchBase and most TPC-C-like tools; a load near the database's limit needs far fewer connections |
| Delivery queued and run in the background (Clause 2.7.2) | run as a normal transaction | as the common TPC-C-like tools |
| random texts of any characters | hexadecimal characters, TPC-C lengths | no rule or measurement depends on the characters |
| audited result "tpmC" | "TPC-C-like" and NOPM | it is not an audited benchmark |
| (nothing) | a marker row in every write transaction, journals on the harness machine | the harness's exact RPO |

---

## 7. Status, commands and files

### 7.1 What is built

| Step | Work | Status |
|---|---|---|
| 0 | speed test; decision: Python | done |
| 1 | the agreed names (the contract) | done; waiting for sign-off |
| 2, 3 | shared additions; the workload-profile registry | done, live-verified |
| 4 | tables, loader, consistency rules, the check before every run | done, loaded on the lab |
| 5 | TPC-C inputs (popular customers and items, the deck, the clerks' warehouses) | done |
| 6 | the five transactions and the engine; the trial | done, trial passed on the lab |
| 7 | the checks after a fault (lost, partial commit, rolled back but kept, consistency); the per-payment check | done; NL-C-01 with a real kill passed on the lab |
| 8 | worker processes, merged numbers, the driver-limited guard, the capacity test | done, verified on the lab |
| 9 | data step before a run: **the run loads the data itself** (free-space check, measured time limit, `--tpcc-fresh-load`); p50, p90 and slowest time per type; the corrected p99 and missed starts | done, verified on the lab |
| 10 | HammerDB cross-check; switching the noisy-neighbour scenarios to `profile: tpcc` | cross-check run on the lab; scenario 3 (NL-N-I03a/b) already uses `tpcc`; the others when they are built |
| 11 | optional clean-up: one engine for all workloads | later, optional |

### 7.2 Open decisions

| Decision | Who |
|---|---|
| Python as the engine language | lead |
| the contract | scenario and TPC-C developers |
| growth limits 1.2 (reload) and 1.5 (refuse) | lead |
| free-space margin 1.5 × and the first-load allowance of 300 s | lead |
| a scenario rate given as a share of the measured capacity (Framework §7.5) | lead |
| a default warm-up for every scenario (harness change list H4, H20) | lead and scenario owners |

### 7.3 Commands

All on the harness machine, in the harness folder.

```bash
# unit tests
.venv/bin/python -m pytest tests -q

# a scenario with the TPC-C workload (the run loads the data if needed)
.venv/bin/pytest resilience_tests/test_scenarios.py --env e2-u22-3 --scenario NL-C-01 --tpcc-warehouses 32 -q -rA

# the data by hand
.venv/bin/python -m resilience_tests.execution.workload.tpcc.load --env e2-u22-3 --warehouses 32 --jobs 4
.venv/bin/python -m resilience_tests.execution.workload.tpcc.load --env e2-u22-3 --status

# a trial (no fault) and the capacity test
.venv/bin/python -m resilience_tests.execution.workload.tpcc.trial --env e2-u22-3 --clerks 16 --rate 200 --seconds 60
.venv/bin/python -m resilience_tests.execution.workload.tpcc.capacity --env e2-u22-3 --clerks 64

# the real-database tests (only against a throw-away database, never the lab target)
RESILIENCE_TEST_PG=<host>:<port>/<database>/<user> .venv/bin/python -m pytest tests/test_tpcc_load_db.py -q
```

### 7.4 Where the files are

| File | Contents |
|---|---|
| `resilience_tests/adapters/postgresql/sql/tpcc_schema.sql`, `tpcc_indexes.sql`, `tpcc_load.sql`, `tpcc_functions.sql`, `tpcc_consistency.sql` | tables, indexes, loader, the five transactions, consistency rules |
| `resilience_tests/adapters/postgresql/tpcc.py` | loading, status, checks, the transaction call (all TPC-C SQL lives in the adapter) |
| `resilience_tests/execution/workload/tpcc/profile.py` | the `tpcc` profile: the data step before a run, the checks after a fault |
| `resilience_tests/execution/workload/tpcc/data.py` | expected sizes, the data step's decisions, free space and time limits |
| `resilience_tests/execution/workload/tpcc/inputs.py`, `terminals.py` | TPC-C inputs and the clerks' warehouses |
| `resilience_tests/execution/workload/tpcc/engine.py` | the engine (one process): clerks, pacing, journals, samples |
| `resilience_tests/execution/workload/tpcc/coordinator.py`, `samples.py` | several worker processes, merged numbers, the driver-limited guard |
| `resilience_tests/execution/workload/tpcc/checks.py` | the checks after a fault |
| `resilience_tests/execution/workload/tpcc/load.py`, `trial.py`, `capacity.py` | the load command, the trial, the capacity test |
| `resilience_tests/execution/workload/contract.py` | the agreed names |
| `tools/tpcc_speedtest/` | the speed test |
| `tests/test_tpcc_*.py`, `tests/test_workload_*.py` | the tests |
