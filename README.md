# Resilience test harness — ShaktiDB / DistDB

A test harness that deliberately breaks a running database and measures what the failure cost:
how much data was lost, how long service was unavailable, and whether anything was corrupted.

It implements the *Resilience Test Harness Architecture v1.0* for the *Unified Resilience
Testing Framework v1.0*. Both specification documents are in `docs/`. Throughout this file and
the code, **`§n`** means the Framework document and **`arch §n`** means the Architecture
document. The two have separate numbering, so mixing them up is a real mistake — one of the
catalog checks exists to catch it.

**Diagrams** (open in any browser):

| File | Shows |
|---|---|
| `docs/architecture.svg` | The six layers, the two machines, and the two doors between them |
| `docs/nl-c-01-overview.svg` | One scenario in twelve boxes — the shortest explanation |
| `docs/phase-flow.svg` | The nine phases from start to end, every refusal point, and what runs in parallel |
| `docs/nl-c-01-workflow.svg` | Every command, file and artefact involved in one run of NL-C-01 |

---

## 1. Overview

“Is the database resilient?” cannot be tested. The specification turns that question into 162
specific questions, each with a number as the answer. For example:

> Kill the database process while the database is taking 200 writes a second. How many
> transactions that the application was **told** were committed are missing afterwards? How
> long until the first successful write? Was anything corrupted?

The harness runs one of those questions from start to finish with no human in the loop, and
produces a verdict that was **calculated from measurements** — never read from a log line,
never judged by eye.

## 2. Design principles

**A test is data.** `catalog/NL/NL-C-01.yaml` says *what* to break and what “good” means. That
file never names a machine, a login or a tool.

**A machine is separate data.** `envs/e2-dedicated-vm.yaml` says *where* the database is and
*how* faults can be caused there. Same test, different machine, no code edit.

**Anything unbuilt refuses.** No snapshot tool, no power driver, no cluster support? The run
stops and says so. A weaker fault is never substituted, and a missing measurement is never
reported as zero.

## 3. Terminology

| Word | Meaning |
|---|---|
| tier | How far the failure reaches. **NL** one machine, **CL** a primary with replicas, **DX** DistDB. |
| scenario | One numbered test, such as NL-C-01. |
| fault type | What is done: `process_kill`, `service_restart`, `config_reload`, `host_power_loss`. |
| driver | The tool that causes a fault on this machine. Chosen by the machine file, never by the scenario. |
| role / position | What kind of node (`standalone`, `distdb.pc`, …) and where the node sits (`primary`, `sync_standby`, …). |
| RPO | Transactions lost. Reported as a count, `rpo_txn`. |
| RTO | How long until service returned. |
| T0 / T1 | The instant the fault was caused / the instant any repair action finished. |
| marker | A row with a unique id, written by the load generator and recorded off the target, used to count losses exactly. |
| steady state | The load the database must already be carrying before the fault, or the run is abandoned. |
| disclosure | A sentence printed with every result saying what this environment could not prove. |

Two values are not numbers:

- **NOT_APPLICABLE** — the question is meaningless here. “How long did the election take?” on a
  single instance.
- **NOT_MEASURED** — the question mattered and the harness failed to answer it.

Both fail any rule that touches them. Neither is ever written as `0`. That single habit is what
stops a half-finished run from looking perfect.

## 4. Repository layout

### Inputs

| File | Holds |
|---|---|
| `catalog/NL/*.yaml` | One file per test: the fault type, the load, what to measure, the pass rules. |
| `catalog/schema.py` | The shape a test file must have, plus the six catalog checks. Run `python -m catalog.schema`. |
| `catalog/reference.yaml` | The specification’s published totals, kept separately so the catalog is checked against an independent source. |
| `envs/*.yaml` | Machines, ports, paths, service names, drivers, phase time limits, the safety allowlist, the disclosures. |

### Control layer

| File | Job |
|---|---|
| `control/orchestrator.py` | The manager. Runs the nine phases in order, sets a time limit on each, collects every measurement, decides whether the result may be trusted. |
| `control/faults/` | What each fault type (and each `fault.during` operation) adds to the phases: its recovery action, its own measures, the operation the fault must land inside. The manager calls these and never compares a fault type itself. |
| `control/safety.py` | The refusals: not production, the right machine, the operator’s approval, one machine only. |
| `control/ledger.py` | The record book of faults caused. Written and forced to disk *before* each fault. |
| `control/killswitch.py` | The repair tool. Undoes every fault still recorded as outstanding, including after a crash. |
| `control/profile.py` | Reads and validates the machine file. |
| `control/matrix.py` | Decides which tests can run here, and writes a reason for each one skipped. |
| `control/reset.py` | Puts the database into a known state. Today: nothing, plus a disclosure. |

### Execution layer

| File | Job |
|---|---|
| `execution/remote.py` | The SSH layer. Every command sent to the target passes through here. |
| `execution/injectors/base.py` | The shape every fault tool must have, and the list of tools available. |
| `execution/injectors/process.py` | Kill the process, restart the service, reload the configuration, hold an idle transaction, flood connections -- one small class per fault. Confirms the kill really landed. |
| `execution/injectors/corruption.py` | Flip one byte in one page of the harness's own relation, and prove it landed. |
| `execution/injectors/os_ssh.py` | What the OS/SSH tools share: the service manager's state and stop mode, and psql on the target. |
| `execution/injectors/power.py` | Cut and restore power, through libvirt. |
| `execution/workload/driver.py` | The load generator: 64 connections, paced transactions, one summary per second. |
| `execution/workload/markers.py` | The data-loss evidence: two journals on the harness machine, and the arithmetic over them. |
| `execution/probes/probers.py` | Write and read probers every 200 ms, the log follower, and the clock comparison. |

### Adapters and analysis

| File | Job |
|---|---|
| `adapters/base.py` | The interface an engine must implement, grouped by topic (the core, and one support interface per kind of fault), and the capabilities an engine can declare. |
| `adapters/postgresql/` | The only package that knows PostgreSQL. `adapter.py` holds the core (connections, SQL, error meanings, `pg_amcheck`, checksum counters); `corruption.py`, `operations.py` and `sessions.py` each implement one fault-support interface; `host.py` declares what they use from the core. |
| `adapters/target_resolver.py` | Turns a test’s allowed roles into the actual machines in the file. |
| `observability/event_stream.py` | The append-only record: one JSON line per event into `events.jsonl`. |
| `observability/alerts.py` | Asks the environment's monitoring whether an alert fired (`alerting.source` in the profile). Only `none` exists: "alerted" measures are then NOT_MEASURED with the reason. |
| `analysis/degradation.py` | For a fault held under load: TPS and p99 during the hold over the baseline (`oltp_tps_ratio`, `oltp_p99_ratio`). |
| `analysis/rto_decomposer.py` | Everything time-related: when service stopped, when writes returned, when throughput truly recovered, and the breakdown. |
| `analysis/cycles.py` | For a repeated fault: each cycle's recovery against the replay work it faced, the trend across cycles, errors per cycle, and on-disk growth. |
| `analysis/predicates.py` | Evaluates one rule, such as `rpo_txn == 0`, against the measured values. |
| `analysis/threshold_eval.py` | Applies every rule and produces the verdict, with a reason recorded for each rule. |
| `analysis/report.py` | Writes `results.json` and `summary.txt`. |
| `test_scenarios.py` | The entry point: turns the catalog and the machine file into one pytest case per run. |

**The rule that keeps the layers apart:** only the adapter knows what PostgreSQL is. The
manager, the load generator, the probes and the analysis never see SQL, a port or a connection.
That is why supporting DistDB later means writing one new file rather than editing twenty.

## 5. Startup and preconditions

Nothing touches the database yet.

1. Your options are recognised — `--env`, `--scenario` and three more. → `conftest.py`
2. Every test file is read and validated: fields present, values allowed, every pass rule a real
   comparison, no machine details hidden inside a test. A bad file stops everything here.
   → `catalog/schema.py`
3. The machine file is read and validated. A file where the harness machine is also the target
   is refused, as is any missing time limit. → `control/profile.py`
4. The harness works out which tests can run here, and writes a reason for each one skipped.
   → `control/matrix.py`
5. The machine to aim at is chosen: the role must be one the test allows, and the position in
   the group must match the test’s target. → `adapters/target_resolver.py`
6. The run is named — `NL-C-01-20260924T093015Z-a3f9c2` — a folder is chosen, and the PostgreSQL
   adapter is built. → `control/orchestrator.py`
7. **Am I on the harness machine?** The harness claims the address the file declares. Claiming
   only works on the machine that owns the address. → `control/orchestrator.py`
8. **Did an earlier run leave this machine broken?** The record book is read. Any fault not
   marked undone stops the run, and the repair command is printed. → `control/ledger.py`
9. **Is another run using this machine?** A lock is taken, one per machine.
   → `control/orchestrator.py`
10. The results folder is created and `events.jsonl` is opened. → `observability/event_stream.py`

## 6. Lifecycle phases

Each phase runs with a time limit taken from the machine file. An unlimited wait is treated as a
defect.

### Phase 1 — reset

The machine file chooses the method. Today both files say `none`, so nothing is sent to the
database and a sentence is recorded instead: *the target was not rolled back to a snapshot
before this run*. A method that is not built is refused — the harness never pretends to reset.

→ `control/reset.py`

### Phase 2 — init

| # | Step | What happens | File |
|---|---|---|---|
| 1 | Safety checks | Confirm this is a lab or staging machine, that only one machine will be affected, and that a data-destroying test was confirmed on the command line. | `control/safety.py` |
| 2 | Target identification | Ask the machine its own name over SSH. The machine file gives an address, and addresses can be repointed, so the machine is asked directly. | `execution/remote.py` |
| 3 | Sentinel lookup | Read the row a person created inside the target database to approve the machine as a test target. | `adapters/postgresql/adapter.py` |
| 4 | Fingerprint check | Compare the name the machine reported, the tag in that row, and the approval flag against the machine file. Any mismatch stops the run. | `control/safety.py` |
| 5 | Durability settings | Read and record the settings that decide whether committed data survives a crash, so the result says how the database was configured. | `adapters/postgresql/adapter.py` |
| 6 | Certification check | Stop the run if a setting makes the answer worthless — with page checksums switched off, corruption could never be detected. | `adapters/postgresql/adapter.py` |
| 7 | Clock comparison | Measure how far the target's clock differs from the harness clock, because timings from both machines are compared later. | `execution/probes/probers.py` |
| 8 | Fault tool chosen | Work out which tool causes this fault on this machine, and refuse if the machine file offers none. | `execution/injectors/base.py` |
| 9 | Corruption baseline | Note how many corrupted pages the database has already counted, so this run is judged only on the change. | `adapters/postgresql/adapter.py` |
| 10 | Old data cleared | Empty the harness's own tables, so the data-loss count covers only this run. | `adapters/postgresql/adapter.py` |
| 11 | Evidence journals opened | Create the two journal files on the harness machine, ready to record every transaction. | `execution/workload/markers.py` |
| 12 | Load generator built | Assemble the load generator and refuse a load type that is not built. Nothing starts yet. | `execution/workload/driver.py` |

Eight of the twelve can stop the run. Nothing has been broken and no load has started: init
exists purely to earn the right to continue.

### Phase 3 — baseline

1. The write prober starts: one small write every 200 ms, with the start and end of every
   attempt recorded. → `probes/probers.py`
2. The log follower starts, copying each new line of the database’s log into the event file.
   → `probes/probers.py`
3. **The load starts:** 64 connections open and begin sending transactions, paced to the rate
   the test asks for. → `workload/driver.py`
4. The abort monitor starts, checking once a second whether the run must stop early.
   → `control/orchestrator.py`
5. Warm-up, deliberately unmeasured, ends on evidence: every declared worker has connected, then
   one more 1 s sample settles. A low baseline would *lower the bar* for recovery.
6. The measuring window opens for 120 seconds, then closes. Throughput and latency (p50/p95/p99
   over every attempt, failed ones included) are frozen as the baseline. → `workload/driver.py`
7. The baseline is checked against the SLO definition it anchors: if the undisturbed service never
   holds ≥ 80 % TPS and p99 ≤ 1.5× for 60 s straight, time-to-SLO is reported as not measured.

### Phase 4 — pre_fault

- Did the baseline hold? Throughput at or above the floor, latency at or below the ceiling. The
  failure message names which side was slower — the database, or the harness’s own disk.
  → `control/orchestrator.py`
- Can the tool act right now? Service running, process id file present, service set to restart
  itself. → `execution/injectors/process.py`
  [Note:  --stop-before-fault skips the injector preflight (it records "not run").]

These checks are late on purpose: two minutes have passed since init, and a check is only
trustworthy immediately before the fault.

### Phase 5 — fault_inject

1. Write the intention into the record book and force the line onto the physical disk,
   **before** anything is broken. → `control/ledger.py`
2. Read the main process number from the file in the data directory, and note the exact moment
   that process started. → `injectors/process.py`
3. Send the kill. The instant the command returns is **T0**.
4. Confirm the death by looking again, repeatedly. A kill that did not land can never be
   reported as a fault.
5. Mark the record applied, then write T0 into the event file and force it to disk.
   → `control/ledger.py`, `observability/event_stream.py`

### Phase 6 — recovery

What the harness does here depends on the fault, and in every case the harness does as little as
possible.

**A process kill, a service restart, a configuration reload** — the harness does nothing at all.
Recovery must be unattended: on the target, the service manager notices the death, starts the
database again, and the database replays its log and reopens. **T1 equals T0**, because no
harness action was needed.

**A fault the harness holds and then removes** (contention and pressure, Framework §10.6) —
held for `fault.duration` and released while still in the fault_inject phase; the release is
**T1**. Recovery only watches what follows it, so the recovery phase's duration does not
include the hold.

**Power loss** — the harness switches the power back on, and that instant is **T1**, later than
T0 by however long the scenario keeps the machine dark. The harness restores power and nothing
else: the machine must boot, and the database must start by itself. All of that time counts
towards the recovery number.

The line the harness never crosses is starting the database by hand. `starts_unattended` is a
measured value — a successful write after T1 with no harness action against the database — so a
database that needed a push has not demonstrated what the scenario claims to measure.

Once a second the harness asks one question: is there a stretch of 60 consecutive seconds, after
T0, where throughput stayed at or above 80 % of baseline and latency at or below 1.5× baseline?
When the answer becomes yes, the phase ends. If the time limit arrives first, the phase ends
with a note.

Throughout, the load and the probes keep working. Every failed attempt and every retry is
evidence — without them nothing would record when service returned.

→ `control/orchestrator.py`, `analysis/rto_decomposer.py`

### Phase 7 — validate

1. Stop the load and the write prober. Measuring while still writing would count rows that
   arrived after measurement began.
2. Close both journals — workers first, journals second, or the last records would be lost.
   → `workload/markers.py`
3. Compute the final recovery numbers over the complete event file.
   → `analysis/rto_decomposer.py`
4. Count everything that went wrong after T0: failed transactions, dropped connections, failed
   connection attempts. → `control/orchestrator.py`
5. Ask the database for every marker id that survived, then do the set arithmetic that produces
   the data-loss count. → `adapters/postgresql/adapter.py`, `workload/markers.py`
6. Run the corruption check over every table and index, and subtract the counters noted in init.
   → `adapters/postgresql/adapter.py`
7. Grade every rule in the test’s `accept` list against the measured values.
   → `analysis/threshold_eval.py`, `analysis/predicates.py`

The run **aborts without a verdict** if the load generator recorded a failure of its own, or the
journals disagree with themselves. Evidence that contradicts itself produces no number.

### Phase 8 — report

`results.json` carries everything: status, each rule with its outcome and reason, every measured
value, every phase with its duration, the facts gathered in init, the disclosures, the
record-book entries. `summary.txt` is the readable version — start there.

→ `analysis/report.py`

### Phase 9 — cleanup

1. Stop the log follower last of all the instruments, because recovery lines arrive late.
2. Undo every fault this run caused. For a kill: make sure the service is active, waiting out a
   start already in progress. → `control/killswitch.py`, `injectors/process.py`
3. Record the repair, or record that the repair failed. A failed repair is never forgotten.
   → `control/ledger.py`
4. Re-read the record book independently, rather than trusting the cleanup step’s own report.
   → `control/orchestrator.py`
5. Write the result file once more, now including the cleanup record. → `analysis/report.py`

**A run that did not undo its own fault cannot report `passed`** — whatever the measurements
said. The status becomes `error` and carries the repair command.

## 7. Measurements

### Workload profiles (§7.1, §7.5)

The same fault has different effects on an idle database and on a busy one, so every scenario
declares its load in the `workload:` section of its file, and the harness never chooses it at
run time (§7.5). The catalog checks refuse a scenario that does not state
`rate_tps`, except for the `idle` profile.

| Field | Meaning | Example (NL-C-01) |
|---|---|---|
| `profile` | Which kind of load (table below). | `oltp_write_heavy` |
| `rate_tps` | Transactions started per second, across all connections together. | `200` |
| `concurrency` | Number of connections, each sending one transaction at a time. | `64` |
| `transaction_markers` | Every transaction carries a marker id, so lost commits can be counted (RPO). | `true` |
| `history` | `list_append` also records every read and write for the Elle checker (NL-C-03). | `none` |

How long the load runs is not part of `workload:`. The load runs for the whole test: the
baseline lasts `steady_state.duration_s` (120 s), and the load continues through the fault and
the recovery.

The Framework names six profiles (§7.1). Two are built:

| Profile | What each transaction does | Used by | Built |
|---|---|---|---|
| `oltp_write_heavy` | `BEGIN; INSERT marker; COMMIT` | NL-C-01..04, NL-C-06, NL-I-01, NL-I-02, NL-M-06, NL-M-07, NL-R-04 | yes |
| `mixed` | the marker insert, plus an UPDATE or DELETE of an existing row, so dead rows build up for the vacuum and bloat checks | NL-C-05, NL-M-03, NL-M-05 | yes |
| `idle` | no load | — | no |
| `oltp_read` | read-only queries | — | no |
| `bulk_load` | large COPY or INSERT batches | — | no |
| `long_analytic` | long-running analytic queries | — (NL-N-07 will need it) | no |

A scenario that asks for a profile that is not built is refused at the init phase ("workload
profile ... is not built yet"), never run with a different load.

**The load values are set for the lab VM, not taken from the Framework.** The Framework fixes
only one rate: NL-M-07 "at 1000 TPS" (§10.7). The other values (200 TPS offered,
`tps_min` 150, p99 at most 300 ms) were chosen so the current lab VM runs them comfortably, and
they need review when the lab hardware changes. Example from the 23 Oct runs: offered 200 TPS,
measured baseline 194–200 TPS, p99 3–103 ms.

### RPO — the marker protocol (§6.4, arch §6.2)

Every transaction follows four steps, in this order:

```
1  take a fresh unique id
2  write the id to marker.jrnl on the HARNESS machine, and force it to disk   -> "written"
3  INSERT the id and COMMIT on the target
4  when the server answers, write to acked.jrnl and force it to disk          -> "acked"
```

Step 2 happens before step 3 on purpose: the harness records the intention to commit before
asking for the commit, so a crash between the two leaves evidence. After recovery the surviving
ids are read back and three sets are compared:

| Result | Formula | Meaning |
|---|---|---|
| **lost** | `acked − in database` | **This is the RPO.** The server promised, and the row is gone. |
| indeterminate | `written − acked` | In flight when the fault struck. Reported, never counted as loss. |
| phantom | `in database − written` | A row nobody wrote. A serious integrity problem. |

Worked example:

```
written  24,000   every attempt reached the journal
acked    23,988   12 got no answer — the kill landed mid-flight
in db    23,985   after crash recovery

lost          = 23,988 − 23,985 =  3   -> rpo_txn = 3, so "rpo_txn == 0" FAILS
indeterminate = 24,000 − 23,988 = 12   -> reported, not counted
```

The verdict fails on 3, not on 15. Counting the 12 unknowable transactions as loss would be as
dishonest as ignoring the 3.

The journals live on the harness machine so a crash — or a power cut — cannot destroy the
evidence with the database.

### RTO (§6.3, §6.5)

- `rto_first_write_s` — T0 until the first successful write after the outage.
- `rto_to_slo_s` — T0 until the **start** of the 60-second stretch of normal service. This is
  what “recovered” means.
- The breakdown `RTO = t_detect + t_elect + t_promote + t_reconnect + t_warm` is reported too.
  On a single machine, election and promotion are **not applicable**, never zero.

The probes run every 200 ms and are the measurement of record. Prometheus, where present, is
context only: a 15-second scrape cannot resolve a 30-second budget (arch §7.1).

### Integrity

`pg_amcheck --heapallindexed` reads every table and index and reports structural damage.
Separately, the database’s own count of corrupted pages read is compared against the reading
taken in init. If the checking tool is missing, or any database was skipped, the check **fails
loudly**. A database that was not checked is never reported as clean.

### Verdict

Every measured value sits in one collection, and each rule in `accept` is evaluated against it.
A value that is missing, not applicable or not measured **does not pass** — the rule is recorded
as failed, with the reason attached. The verdict is `passed` only when every rule passed.

## 8. Safety controls (arch §15)

| Control | What the control does |
|---|---|
| Allowlist, default deny | The name the *machine reports over SSH* must match the pattern in the machine file. |
| Operator approval row | A row a person typed into the target database. The harness can never create it. |
| Two keys for destruction | Data-destroying tests need a command-line flag *and* an approval flag in that row. |
| One machine at a time | Fixed at one, enforced by the test file’s own validation. |
| Record book before the act | Forced to disk before every fault, so a crashed harness can still be cleaned up. |
| Repair tool | `python -m resilience_tests.control.killswitch --env <profile>`. |
| Standing abort | `safety.max_data_fs_used_pct` in the machine file: every run stops if the data filesystem fills past it, whatever the scenario's own `abort_if` (which may not apply to a standalone target). |
| Time limit on every phase | No step can hang forever. |
| One run per machine | A lock, released by the operating system if the harness dies. |
| No credentials in the repository | Database passwords come from the harness machine’s `~/.pgpass`; SSH uses keys with host keys verified. |

## 9. Usage

```bash
# install — never copy a .venv between machines, the paths inside are absolute
python3.11 -m venv .venv
.venv/bin/pip install -e '.[test]'

# the harness's own tests, and the catalog checks
.venv/bin/pytest -q -n 4
.venv/bin/python -m catalog.schema --partial   # while the catalog is being authored
.venv/bin/python -m catalog.schema             # release gate: the complete 162-test catalog

# dry run: reset .. pre_fault only. No fault, no verdict.
.venv/bin/pytest resilience_tests/test_scenarios.py --env e2-dedicated-vm \
    --scenario NL-C-01 --stop-before-fault -q -rA

# the real run
.venv/bin/pytest resilience_tests/test_scenarios.py --env e2-dedicated-vm \
    --scenario NL-C-01 -q -rA --junitxml=reports/junit.xml

# read the result
d=$(ls -td <run_dir>/NL-C-01-* | head -1); cat "$d/summary.txt"

# repair anything left behind
.venv/bin/python -m resilience_tests.control.killswitch --env e2-dedicated-vm
```

### Reports (Arch §10.4)

Every scenario run writes its own **`report.html`** into its run directory: verdict, every
acceptance rule with measured value, margin and reason, an annotated timeline (fault, outage,
first write, recovery, every cycle), throughput/latency charts, disclosures and links to the
evidence. One self-contained file; it opens offline. It shows the run's recorded status and
never re-decides it; NOT_MEASURED is always a labelled reason, never a number or a pass.

```bash
# scenario runs: JUnit XML for the CI gate + an index page + the P0 gate decision
.venv/bin/pytest resilience_tests/test_scenarios.py --env e2-dedicated-vm --report-dir reports
.venv/bin/python -m resilience_tests.reporting.gate reports/scenarios-junit.xml   # exit 1 = a P0 did not pass

# unit tests: JUnit XML (pytest's own) + an HTML summary
.venv/bin/python -m pytest tests --report-dir reports --junitxml=reports/unit-junit.xml

# regenerate pages from existing evidence (e.g. an older run)
.venv/bin/python -m resilience_tests.reporting.html <run_dir> [<run_dir> ...] --index reports/index.html
```

In `scenarios-junit.xml` a verdict failure is `<failure>` (listing every rule that did not
pass), a run without a verdict (aborted, error, no result) is `<error>`, and dry runs and
plan skips are `<skipped>`; each case carries `scenario_id`, `priority`, `status`, `run_id`,
`evidence_dir` and `report`. **Only P0 non-passes fail the build** (Arch §10.4); P1/P2
non-passes are reported. Jenkins marks a build UNSTABLE on any JUnit failure, so publish with
`skipMarkingBuildUnstable` and let the gate's exit code decide:

```groovy
sh '.venv/bin/pytest resilience_tests/test_scenarios.py --env e2-dedicated-vm --report-dir reports || true'
junit testResults: 'reports/scenarios-junit.xml', skipMarkingBuildUnstable: true
archiveArtifacts artifacts: 'reports/**'
sh '.venv/bin/python -m resilience_tests.reporting.gate reports/scenarios-junit.xml'
```

`--scenario` is repeatable; omit it to run the whole catalog. `--reference-class` (default `E2`)
picks the environment class used for tests whose behaviour does not depend on the environment.

### One-time target setup

```sql
CREATE SCHEMA IF NOT EXISTS resilience;
CREATE TABLE IF NOT EXISTS resilience.harness_target (
    hostname      text PRIMARY KEY,
    inventory_tag text NOT NULL,
    disposable    boolean NOT NULL,
    created_by    text NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now()
);
INSERT INTO resilience.harness_target (hostname, inventory_tag, disposable, created_by)
VALUES ('<hostname>', '<tag from the machine file>', true, '<operator>');

CREATE EXTENSION IF NOT EXISTS amcheck;   -- in every database the corruption check covers
```

The harness installs nothing in a target database, by design.
`infra/ansible/provision-target.yml` can do the rest (role, database, extension, service unit).

NL-M-06 restarts through the service manager and requires a **fast** shutdown: `pg_ctl stop`
(whose default is fast), `pg_ctl stop -m fast`, or no `ExecStop` with `KillSignal=SIGINT`.
Preflight refuses any other unit configuration rather than measuring a different shutdown.

## 10. Outputs

| Status | Meaning |
|---|---|
| `passed` | Every rule passed, *and* the harness left the machine as the harness found it. |
| `failed` | The database did not meet a rule. A real finding about the product. |
| `aborted` | The run could not proceed — the baseline did not hold, a safety check refused, the journals disagreed. Not a statement about the database. |
| `error` | Something about the run cannot be trusted, most often a fault that was not undone. The measurements are still in the file. |
| `stopped_before_fault` | A dry run. No fault, no verdict. |

Files a run leaves behind:

| File | Contents |
|---|---|
| `summary.txt` | Start here. Scenario, status, failing rules, disclosures. |
| `results.json` | Everything, including why each rule passed or failed. |
| `events.jsonl` | Every probe attempt, every one-second summary, every database log line. A run can be re-analysed from this file alone. |
| `marker.jrnl`, `acked.jrnl` | The data-loss evidence. |
| `integrity.txt` | Raw output of the corruption check. |

Plus `injection-ledger.jsonl` in the harness machine’s run directory, shared across runs.

## 11. Extending

**A new test** — one YAML file under `catalog/<TIER>/`, then `python -m catalog.schema
--partial`.

**A new fault type** — add it to `FaultType` and, if it needs a new profile section, to
`FAULT_DRIVER_SECTION` in `catalog/schema.py`; write a `FaultInjector` subclass with
`preflight` / `inject` / `revert`, call `register()` at the bottom of the file, add the file to
the import line in `injectors/__init__.py`, and add the section to the profile. If the run must
do anything different for it -- a recovery action, measures of its own -- add a `FaultBehaviour`
in `control/faults/` with `@register_fault`; the orchestrator itself is not edited. A behaviour
sees the run only through `self.ctx` (`control/faults/context.py`): the scenario, the adapter,
the run's facts / not-measured reasons / disclosures, T0, and named actions on the fault. When
a behaviour needs something new from the run, add it there rather than reaching into the
orchestrator.

Hooks a behaviour may override, in the order the phases call them (each defaults to nothing):
`observe_settings()` (engine settings that decide how the fault plays out, read in init through
a named adapter method, never written), `samplers()` (extra background measurements, started
and stopped with the run's probes),
`baseline_started()` / `baseline_ended()`, `before_inject()` / `after_inject(detail)`,
`hold_and_release()` (fault_inject: a fault the harness holds and removes itself),
`begin_recovery()`, `recovery_soak_s()` (a fixed time to observe instead of waiting for the SLO,
for a recovery that is the engine's own doing, e.g. NL-M-05's timeout), `measure_client_impact()`,
`before_integrity_check()`, `measure()`; `phase_extension_s(phase)` adds a hold to that phase's
profile bound, `slo_clock_from_t1()` measures time back to SLO from T1, and `cycle_evidence_keys`
names the injection-detail keys kept in each cycle's row of a repeated scenario. Its section of
`summary.txt` comes from the classmethod `summary_lines(results)`; the orchestrator hands every
registered section to `analysis/report.py` (`summary_sections()`), which names no fault.

A `fault.during` behaviour has the same `observe_settings()` and `summary_lines()`, plus
`create_objects()` for what the operation needs, `establish()` (bring the target into the state
the fault must land inside, just before each attempt; it returns evidence recorded with the
injection) and `after_inject(detail)`.

A fault the harness holds and then releases (contention, pressure: Framework §10.6) subclasses
`HeldFault`: its driver applies the fault in `inject`; still in the fault_inject phase,
`hold_and_release()` holds it for `fault.duration` with the workload measured in its own window
(`on_hold_start` / `on_hold_end` for snapshots) and releases it through the driver's `revert`
(T1). The recovery phase then only watches the return to SLO from T1, so each phase's duration
means what its name says; `oltp_tps_ratio` / `oltp_p99_ratio` are measured for it.

A fault type with no registered behaviour is not built: the run plan skips it and init refuses
it, with the reason, before anything is applied (`control/matrix.py: not_built_reason`).

**A driver** keeps its own settings: it overrides `configure()` to take what it needs from the
scenario (a hold duration, a repeat plan, a relation kind). The base class only aims a fault at
a single process if the driver overrides `aim_at()`; otherwise it refuses.

**A second database engine** — implement `BaseDatabaseAdapter`, declare its capabilities, and
`register_adapter()` it against the engine name in the profile. The interface is grouped by
topic in `adapters/base.py`: `CoreAdapter` (what every engine provides: sessions, markers,
settings, integrity, logs) and one support interface per kind of fault (`SessionFaultSupport`,
`OperationSupport`, `CorruptionSupport`). An engine overrides what it supports; every default it
leaves either refuses (`NotImplementedError`) or says it cannot tell (None, `supported: False`),
so an unbuilt feature can never pass on an invented answer. A capability the engine lacks makes
the affected measurement NOT_APPLICABLE rather than zero. The PostgreSQL adapter implements each
group in its own file (`sessions.py`, `operations.py`, `corruption.py`), mixed into the core
class in `adapter.py`; what those files use from the core is declared in `host.py`.
`tests/test_adapter_seam.py` shows the whole interface exercised by a fake engine, and
`tests/test_adapter_structure.py` keeps the grouping honest.

**A new environment** — one YAML in `envs/`. The model rejects a profile whose harness machine is
also a target, or that leaves any phase unbounded.

## 12. Target database footprint

### Objects created

At init the harness creates its own schema, and two tables if they are not already there:

| Object | Purpose |
|---|---|
| `resilience.markers` | One row per transaction — the data-loss evidence. |
| `resilience.probe_writes` | One row per write probe, every 200 ms — the availability evidence. |

Both are emptied at the **start** of a run, never at the end. The rows are the evidence the
verdict was computed from, so they stay readable until the next run needs the space. The
operator's approval row is deliberately excluded from that clearing: the harness never
truncates `resilience.harness_target`.

Nothing is ever dropped. Removing the harness's footprint after an engagement is a manual
step, and it takes the approval row with it:

```sql
DROP SCHEMA resilience CASCADE;   -- also removes resilience.harness_target
```

→ `adapters/postgresql/adapter.py`

### Schema visibility

Those tables live in the `resilience` schema, which is not in the default `search_path`, so a
plain `\dt` reports *“Did not find any relations”* against a database that is full of them:

```sql
\dt resilience.*                        -- the harness's tables
SELECT count(*) FROM resilience.markers;
SET search_path TO resilience, public;  -- or make them visible for this session
```

If they really are absent, the run stopped before init ever reached the database —
`summary.txt` names the phase that refused.

### Integrity check scope

`pg_amcheck` covers the harness database only, unless a node widens it:

```yaml
nodes:
  - name: shaktidb-standalone
    integrity_databases: [resilience, appdb]   # default: the harness database alone
```

Every database named must already carry the `amcheck` extension. The harness installs nothing
in a target database and refuses rather than reporting an unchecked one as clean. `--all` is
deliberately not used: on a customer cluster it would scan every database they own, and would
not finish inside the validate time limit.

→ `adapters/postgresql/adapter.py`, `control/profile.py`

## 13. Design note: tests on several machines (Tier 2 and 3)

**Status: not built.** This note records the plan for the cluster (CL, Tier 2) and distributed
(DX, Tier 3) scenarios, so the work can start from an agreed design. Until it is built:
- cluster scenarios cannot run: there is no cluster `Role` to declare yet, and on a node whose
  role is not `standalone` the orchestrator stops at the Tier-2 checks (`NotImplementedError`);
- a compound scenario (CL-X) is refused with a reason, in the run plan and again in init
  (`control/matrix.py` `not_built_reason`). Nothing injects its second fault yet, and a run that
  applied only the first could pass without testing the scenario.

### What is in place today

| Already supports several machines | Still assumes one machine per run |
|---|---|
| The environment file lists any number of `nodes`, each with `role` and `topology_role` (primary, sync_standby, async_standby) and a `client` endpoint (the pooler, if there is one) | `RunPlanItem.node`: a run gets one node |
| The scenario names the fault's target (`fault.target`: primary or sync_standby) | `TestOrchestrator.node`: used in 24 places (probes, injection, integrity check, safety) |
| `TargetResolver` chooses nodes through a replaceable `ResolverStrategy` | `RunContext.node`: fault classes see one node |
| The safety controller counts nodes affected (compound scenarios: at most 2) | RTO decomposition is called with `clustered=False`; Tier-2 probes raise `NotImplementedError` (`orchestrator.py`) |
| The injection ledger records the node of every injection | Only `standalone` and `distdb.*` exist as `Role` values (`catalog/schema.py`) |

Fault classes, drivers, the driver and fault registries, the database adapter interface, the
verdict and the report do not change. New cluster faults are added as new classes, as today.

### Proposed design

**1. Environment file: describe the cluster.** The node list stays. Add a `cluster` section
for what belongs to the whole group, and a role for members of a PostgreSQL HA group:

```yaml
cluster:
  manager: patroni                       # who elects the primary; read, never configured
  manager_api: {port: 8008}              # where the current primary and state are read
  consensus: [10.0.0.21:2379, 10.0.0.22:2379, 10.0.0.23:2379]   # etcd
nodes:
  - {name: pg-a, role: ha_member, topology_role: primary, ...}
  - {name: pg-b, role: ha_member, topology_role: sync_standby, ...}
  - {name: pg-c, role: ha_member, topology_role: async_standby, ...}
```

`topology_role` in the file is only the starting layout. After a failover the primary is a
different machine, so the current roles are read from the cluster manager when needed (a new
`ResolverStrategy`, e.g. `PatroniStrategy`).

**2. Run plan: one run gets the whole group.** `RunPlanItem` carries a `Topology`: every node of
the group, plus the node the fault targets. A standalone profile gives a topology of one node,
so today's scenarios run exactly as now.

**3. Orchestrator: work on the topology, not one node.**

| Today | Planned |
|---|---|
| `self.node` everywhere | `self.topology`; the fault node is `topology.target(fault.target)`, resolved at injection time |
| write probes on the node | write probes on the `client` endpoint (pooler or virtual IP), as an application would see it |
| integrity check on the node | integrity check on the current primary, plus a primary-to-replica data comparison |
| `clustered=False` | `clustered=topology.is_cluster`: detect / elect / promote times become measurable |
| Tier-2 probes raise `NotImplementedError` | replication lag, standby health and leader changes as samplers on each node |
| safety checks the one node | safety checks every node, and the blast radius against the topology |

**4. RunContext: give fault classes the group.** Add `ctx.topology` (all nodes, the current
primary) and `ctx.node_for(role)`. `ctx.node` stays, meaning the fault's target, so existing
fault classes need no change.

**5. Compound faults (CL-X).** A second driver is resolved for `compound.fault_b` and its target
node. A watcher waits for the trigger (`on_state`, `on_metric` or `after_phase`, within
`timeout_s`) and injects fault B, with its own ledger entry. If the trigger never comes, the run
is aborted. A compound run never reports a pass when only fault A was applied. The guard is
removed when this is built.

**6. Distributed tests (Tier 3).** A DistDB adapter in `adapters/distdb/`. Topology roles for the
primary coordinator, query coordinators and workers (`distdb.pc`, `distdb.qc`,
`distdb.worker`, already in the schema).

### Order of work

| Step | Proves | Existing scenarios |
|---|---|---|
| 1. `Topology` model + environment `cluster` section; standalone = topology of one | the model | must give identical results (before/after comparison of the orchestrator tests) |
| 2. Orchestrator and `RunContext` use the topology | the refactor | identical results again; then a live run of every scenario |
| 3. Cluster probes and clustered RTO decomposition | cluster measurements | unchanged (standalone skips them) |
| 4. First cluster scenario, CL-F-02 (kill the primary), on a lab cluster | the whole chain on real machines | — |
| 5. Remaining CL scenarios, a few at a time, with their drivers (network, pooler, backup) | each scenario | — |
| 6. Compound injection; remove the guard | CL-X | — |
| 7. DistDB adapter and roles | Tier 3 | — |

### Decisions needed before step 1

- The lab cluster: machines, cluster manager (Patroni assumed), pooler, consensus store.
- Checks that apply only to a cluster (e.g. `leader_changed == false`) on a standalone target:
  proposal is to mark them cluster-only and report them as not applicable there, with the
  reason.
- Whether a cluster test may change the cluster manager's state (e.g. pause automatic failover
  for a test). The current rule is that configuration is observed, never written.

→ `control/matrix.py`, `control/orchestrator.py`, `control/faults/context.py`,
`control/profile.py`, `adapters/target_resolver.py`
