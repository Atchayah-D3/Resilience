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

“Is the database resilient?” cannot be tested. The specification turns that question into 155
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
| `execution/injectors/process.py` | Kill the process, restart the service, reload the configuration. Confirms the kill really landed. |
| `execution/injectors/power.py` | Cut and restore power, through libvirt. |
| `execution/workload/driver.py` | The load generator: 64 connections, paced transactions, one summary per second. |
| `execution/workload/markers.py` | The data-loss evidence: two journals on the harness machine, and the arithmetic over them. |
| `execution/probes/probers.py` | Write and read probers every 200 ms, the log follower, and the clock comparison. |

### Adapters and analysis

| File | Job |
|---|---|
| `adapters/base.py` | The interface an engine must implement, and the capabilities an engine can declare. |
| `adapters/postgresql/adapter.py` | The only file that knows PostgreSQL: connections, SQL, error meanings, `pg_amcheck`, checksum counters. |
| `adapters/target_resolver.py` | Turns a test’s allowed roles into the actual machines in the file. |
| `observability/event_stream.py` | The append-only record: one JSON line per event into `events.jsonl`. |
| `analysis/rto_decomposer.py` | Everything time-related: when service stopped, when writes returned, when throughput truly recovered, and the breakdown. |
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
.venv/bin/python -m catalog.schema             # release gate: the complete 155-test catalog

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
the import line in `injectors/__init__.py`, and add the section to the profile.

**A second database engine** — implement `BaseDatabaseAdapter`, declare its capabilities, and
`register_adapter()` it against the engine name in the profile. A capability the engine lacks
makes the affected measurement NOT_APPLICABLE rather than zero. `tests/test_adapter_seam.py`
shows the whole interface exercised by a fake engine.

**A new environment** — one YAML in `envs/`. The model rejects a profile whose harness machine is
also a target, or that leaves any phase unbounded.

### Workload Generator Selection (`workload` profile section)

Environment profiles support an optional `workload` section configuring the workload driver:

```yaml
workload:
  generator: pgbench     # pgbench (default) | builtin
  pgbench_bin: pgbench   # executable on driver host PATH or absolute path
```

- **Generators**:
  - `pgbench`: Supervised pgbench execution (one process per client with `-c 1 -j 1`, evenly spaced transactions without `-R`, automatic drop detection, and per-client relaunch).
  - `builtin`: Built-in asyncpg workload driver.
- **Selection & CLI Override**:
  - Configured per environment in `envs/<profile>.yaml`.
  - Overridable via `pytest --workload {pgbench,builtin}`.
- **Refusals (Fail Closed)**:
  - `pgbench_bin` missing or not executable.
  - pgbench major version differing from target PostgreSQL major version (`FR-004`).
  - Adapter lacking `Capability.PGBENCH_WORKLOAD`.
  - Scenarios requiring `transaction_markers: true` when no verified `RecordChannel` is active (gated on `R1`).
- **Evidence Files**:
  - `<run_dir>/pgbench/launch-<n>.txt`: Contains exact argv without secrets, stderr, stdout, and parsed summary with per-command latencies.

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
