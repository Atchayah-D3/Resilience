# Feature Specification: pgbench as the Harness Workload

**Feature Branch**: `pgbench`

**Created**: 2026-10-07

**Status**: Draft

**Input**: User description: "Replace the harness's built-in workload generator with pgbench, launched and managed by the harness itself on the driver host (no second console), on the pgbench branch. Every existing scenario must keep working unchanged and keep every current guarantee: RPO by transaction markers recorded durably on the driver host before each commit is sent and after it is acknowledged (constitution VII, Arch §6.2); outcomes classified committed / definitely aborted / unknown; per-second throughput and p50/p95/p99 samples for baseline, steady-state check and time-to-SLO; load that continues after a crash or restart (pgbench drops clients whose connection breaks, so the harness must relaunch it); the churn workload for NL-C-05/NL-M-05 bloat; the Elle list-append history with real read values for NL-C-03; failed-transaction and dropped-connection counts for NL-M-07 and NL-R-04; never a false pass. Throughput figures to be taken from the first lab runs (NL-C-01 at 200 TPS, NL-M-07 at 1000 TPS)."

**Sources**: Architecture §6.1 (workload engines), §6.2 (transaction marker protocol), §6.3 (operation history), §7 (measurement); constitution principles III (never a false pass), V (adapter seam) and VII (evidence integrity).

## Context

Today every scenario is driven by the harness's own workload generator. It does more than offer load; the rest of the harness depends on it for:

1. **Data-loss evidence (RPO).** Each transaction is recorded durably on the driver host before its commit is sent, and again once the database acknowledges it. After recovery, acknowledged-but-missing transactions are the data loss; unacknowledged ones are reported as unknown, never counted.
2. **Outcome classification.** Every transaction ends committed, definitely aborted (the server said so), or unknown.
3. **Per-second measurement.** Throughput and p50/p95/p99 latency every second, for the baseline, the pre-fault steady-state check and time-to-SLO.
4. **Load through faults.** Load continues across crashes and restarts, reconnecting as the database returns.
5. **Scenario-specific transaction shapes.** Update/delete churn (NL-C-05, NL-M-05), and list-append reads and appends recorded as an operation history for the anomaly checker (NL-C-03).
6. **Client-visible disturbance.** Failed transactions and dropped connections after the fault (NL-M-07, NL-R-04).

This feature replaces the load generator with pgbench, the PostgreSQL project's standard benchmarking client, started and supervised by the harness itself. Every one of the six capabilities above must be preserved with the same strength of evidence. Architecture §6.1 assigns RPO-measuring scenarios to a custom driver; using pgbench for them is a recorded design decision of this feature, acceptable only because the evidence requirements of §6.2 and constitution VII are kept in full.

## Clarifications

### Session 2026-10-07

- Q: Should pgbench send transactions at evenly spaced intervals like the built-in generator, or at pgbench's normal randomly spaced times? → A: Evenly spaced, matching the built-in generator, so results compare like for like.
- Q: With pgbench, should the steady-state limits checked before the fault stay exactly the same as with the built-in generator, despite the recording overhead? → A: Yes, the same limits for both generators; a pgbench run that cannot meet them aborts before the fault, naming the cause.
- Q: Which pgbench versions should the harness accept on the driver host? → A: The same major version as the target database; any other version is refused before the baseline.

## User Scenarios & Testing *(mandatory)*

The "user" is the database reliability engineer running scenarios and certifying results, and the reader of each run report.

### User Story 1 - Every existing scenario runs on pgbench, unchanged, with the same guarantees (Priority: P1)

The engineer runs any existing catalog scenario exactly as today, with the same command, catalog entry and environment profile. The harness starts pgbench on the driver host, measures the baseline, injects the fault, follows recovery, and produces a verdict. Every acceptance rule is evaluated from evidence as strong as before. Nothing about the scenarios, the catalog or the run commands changes.

**Why this priority**: This is the feature. A pgbench workload that weakens any guarantee, or needs scenario edits, defeats the purpose of swapping the engine underneath.

**Independent Test**: Run NL-C-01 on the lab target with the pgbench workload. Confirm that the run completes with a verdict, that every measurement the scenario declares is produced, and that the run can be re-evaluated from its evidence alone.

**Acceptance Scenarios**:

1. **Given** an existing scenario and its unchanged catalog entry, **When** it runs with the pgbench workload, **Then** it produces every measurement it declares, and its verdict follows mechanically from them.
2. **Given** the harness's existing negative tests for a scenario, **When** they run against the pgbench workload, **Then** each still fails, for the reason under test.
3. **Given** pgbench is missing from the driver host, or is an unsupported version, **When** a run starts, **Then** the harness refuses before the baseline, naming what is missing. It never falls back silently to another load generator.
4. **Given** an environment profile that selects the built-in generator, **When** the same scenario runs, **Then** it runs exactly as today, and its report names the generator used, so the two can be compared run for run.

---

### User Story 2 - Data-loss evidence is as strong as today (Priority: P1)

For every transaction pgbench sends, the harness holds a durable record on the driver host, made **before** the commit is sent, and a second durable record made only **after** the database acknowledged it. After recovery the harness computes, exactly as today:
- lost = acknowledged − present;
- unknown outcome = recorded-before-commit − acknowledged (reported, not counted);
- rows present that were never recorded = integrity violation.

**Why this priority**: Every implemented scenario gates on `rpo_txn == 0`. Constitution VII is explicit that the before-commit record must exist and be flushed. An RPO computed from weaker evidence would be a false-pass risk.

**Independent Test**: Run a crash scenario with the pgbench workload, then independently recount the two records against the rows in the database. Every acknowledged transaction must have a before-commit record, and the computed loss must match.

**Acceptance Scenarios**:

1. **Given** the database is killed mid-load, **When** RPO is computed, **Then** transactions in flight at the kill are reported as unknown outcome and excluded, and any acknowledged transaction missing after recovery is counted as lost.
2. **Given** a before-commit record could not be made for a transaction, **When** that happens, **Then** the transaction is never committed, or the run is aborted as "evidence incomplete". An acknowledgement without a before-commit record never enters the RPO arithmetic.
3. **Given** the acknowledgement record could not be made after a commit succeeded, **When** the run is evaluated, **Then** no RPO figure is issued (evidence incomplete), because a missing acknowledgement would hide a loss.
4. **Given** pgbench is relaunched after an outage, **When** records are compared, **Then** every transaction from every launch is uniquely identifiable; no identifier repeats across launches.

---

### User Story 3 - Load continues through faults and is measured every second (Priority: P2)

When a fault breaks pgbench's connections (crash, restart, power loss), the harness notices and relaunches pgbench as soon as the database accepts connections again, so load after recovery resembles load before it. Throughout the run the harness records one sample per second (throughput, p50/p95/p99 latency, failures, dropped connections), in the same form today's analysis consumes. Partial loss, where only some pgbench clients drop, is detected and corrected too, not left as a silently reduced load.

**Why this priority**: The baseline, the steady-state check and time-to-SLO all depend on these samples. Without a relaunch, every crash scenario would measure an idle database after the fault.

**Independent Test**: Kill the database under pgbench load. Confirm that pgbench is relaunched once writes are accepted again, that per-second samples continue without gaps outside the outage, and that time-to-first-write still comes from the harness's write probe, which is independent of pgbench.

**Acceptance Scenarios**:

1. **Given** all pgbench clients dropped at a crash, **When** the database accepts connections again, **Then** pgbench is relaunched with the scenario's declared concurrency and rate, and the relaunch time is recorded.
2. **Given** only some clients dropped (for example, in a connection-exhaustion or reload scenario), **When** this is detected, **Then** the drops are counted as dropped connections and the declared concurrency is restored.
3. **Given** the per-second samples, **When** the baseline and time-to-SLO are computed, **Then** they use the same definitions as today (≥ 80 % of baseline throughput, p99 ≤ 1.5 × baseline, sustained 60 s).

---

### User Story 4 - Each scenario's transaction shape is reproduced (Priority: P2)

Scenarios that need a particular kind of transaction get the same shape from pgbench as today:
- **default:** the marker transaction;
- **churn (NL-C-05, NL-M-05):** update and delete/re-insert traffic on the harness's bounded churn table, with the same key space, so bloat and dead-tuple measurements stay comparable;
- **list-append history (NL-C-03):** list-append reads and appends whose **read values are exactly what the database returned**, recorded as an operation history with unknown outcomes marked as unknown, never as failed.

**Why this priority**: Without these, NL-C-05's bloat measurement and NL-M-05's vacuum-blocking evidence lose their input, and NL-C-03's anomaly check would run on an invented history, which constitution VII forbids.

**Independent Test**: Run NL-C-05 and NL-C-03 with the pgbench workload. Confirm churn traffic reaches the churn table at the declared rate, and that the operation history's read values match a direct read of the database.

**Acceptance Scenarios**:

1. **Given** a scenario declaring the churn workload, **When** it runs on pgbench, **Then** the churn table's live row count stays constant while dead rows accumulate, as today.
2. **Given** NL-C-03, **When** the history is produced, **Then** every read in it carries the value the database returned, and the checker's verdict enters the run's measurements as today.

---

### User Story 5 - Throughput capacity is known and disclosed (Priority: P3)

The first lab runs establish what the driver host and target sustain with pgbench and its evidence recording: NL-C-01 at its offered 200 TPS, and NL-M-07 at its offered 1000 TPS. Every report states the overhead the evidence recording adds to each transaction. If a scenario's steady-state floor cannot be reached, the run aborts before the fault, as today, naming whether the driver host or the database was the limit.

**Why this priority**: Durable per-transaction recording costs time on the driver host. NL-M-07's 1000 TPS may exceed what it sustains, and that must surface as an honest abort, never as a lower load measured silently.

**Independent Test**: Run NL-C-01 and NL-M-07 on the lab and read the achieved throughput, the recording overhead and, where relevant, the named limiting side from their reports.

**Acceptance Scenarios**:

1. **Given** the offered rate cannot be sustained, **When** the steady-state check runs, **Then** the run aborts before any fault, naming the driver host or the database as the limiting side.
2. **Given** any completed run, **When** its report is read, **Then** it states the recording overhead, as part of each transaction's measured latency.

---

### Edge Cases

- **pgbench exits unexpectedly (crash, killed, out of memory) while the database is healthy.** The workload is the measuring instrument, so the run aborts with "workload failed", as today. It is never treated as a fault outcome.
- **The driver host's evidence disk fills, or a record cannot be flushed.** The affected transaction is never counted as acknowledged; the run aborts with evidence incomplete.
- **The relaunch keeps failing because the database never returns.** Relaunch attempts are bounded by the phase time limit. The run reports recovery as not observed, never as recovered.
- **A relaunch races a still-running previous pgbench.** At most one pgbench instance offers load at a time; the previous one is confirmed gone before the next starts.
- **pgbench's output format differs in the ShaktiDB build.** Unparseable output is treated as a workload failure (fail closed), never as zero failures or full throughput.
- **The run is interrupted (Ctrl-C) or the harness crashes.** No pgbench process is left running on the driver host, and the kill switch's guarantees are unchanged.
- **Clock and timing.** Per-second samples are on the harness clock, as today, so samples from pgbench and from the probes are directly comparable.

## Requirements *(mandatory)*

### Functional Requirements

**Integration**

- **FR-001**: The harness MUST start, supervise and stop pgbench itself on the driver host as part of a run. No separate console or manual step is required.
- **FR-002**: Scenarios, the catalog schema, run commands and the run status vocabulary MUST remain unchanged. Workload selection belongs to the harness and environment configuration, never to a scenario (constitution I, V).
- **FR-003**: The workload MUST be engine-agnostic above the adapter. Anything database-specific (SQL, tables, connection details) is supplied through the adapter (constitution V).
- **FR-004**: The harness MUST refuse to start a run, before the baseline, if pgbench is missing, cannot reach the target, or is unsupported. **Supported** means the same major version as the target database's server (17 today). Any other version is refused. The refusal names the cause, the versions found and the version required.

**Data-loss evidence**

- **FR-005**: For every transaction, a record identifying it MUST be durable on the driver host before its commit is sent. A second record MUST be made durable only after the database acknowledges the commit (Arch §6.2, constitution VII).
- **FR-006**: Transaction identifiers MUST be unique across the whole run, including across pgbench relaunches.
- **FR-007**: RPO, unknown outcomes and never-recorded rows MUST be computed with today's arithmetic and its existing consistency checks (an acknowledgement without a before-commit record, or torn records, aborts the evaluation).
- **FR-008**: Every transaction outcome MUST be classified committed, definitely aborted or unknown. An outcome pgbench cannot attribute to an explicit server rejection MUST be classified unknown.

**Measurement**

- **FR-009**: The harness MUST emit one sample per second (throughput, p50/p95/p99 latency, failed transactions, dropped connections, failed connection attempts) on the harness clock, in the form today's analysis consumes.
- **FR-010**: The baseline window, the warm-up readiness rule, the steady-state check and the time-to-SLO definition MUST work unchanged on these samples. The steady-state limits are the scenario's own, identical for both generators: there is no generator-specific allowance, and the recording overhead is not subtracted before the check. A pgbench run that cannot meet them aborts before the fault, naming the limiting side (User Story 5).
- **FR-011**: Each report MUST state how much of each transaction's measured latency is the harness's own evidence recording.

**Load through faults**

- **FR-012**: When pgbench clients drop, all or some, the harness MUST restore the declared concurrency once the database accepts connections, count the drops, and record each relaunch time.
- **FR-013**: The harness's write probe MUST remain the source of time-to-first-write, independent of pgbench.
- **FR-014**: An unexpected pgbench failure while the database is healthy MUST abort the run as a workload failure.

**Transaction shapes**

- **FR-021**: pgbench MUST offer each scenario's declared rate as **evenly spaced** transactions, the same arrival pattern as the built-in generator. pgbench's default random spacing, with its short bursts, is not used. A run's achieved spacing is evidence: its report states the scheduling lag observed.
- **FR-015**: The default, churn and list-append transaction shapes MUST be reproduced with today's semantics: tables, key spaces, the replace-every-Nth rule, and list-append reads and appends.
- **FR-016**: The list-append history MUST record the values the database actually returned, and MUST mark unknown outcomes as unknown, never as failed (Arch §6.3, constitution VII).

**Safety and evidence**

- **FR-017**: No pgbench process may outlive the run, including after an interrupt or a harness crash. Cleanup MUST confirm none remains.
- **FR-018**: pgbench's own output and parameters (version, command line, per-launch summary) MUST be kept in the run's evidence.
- **FR-019**: Every existing scenario's negative tests MUST pass against the pgbench workload, and new tests MUST cover each fail-closed path above (constitution III).
- **FR-020**: Both workload generators MUST remain available: pgbench and today's built-in generator. The environment profile selects which one a run uses; pgbench is the default when the profile does not say. Every report MUST name the workload generator the run used. The selection is explicit only: a run never switches generator on its own, including when pgbench is missing or cannot keep up (that run refuses or aborts as in FR-004 and User Story 5, and the engineer may re-run it with the other generator).

### Key Entities

- **Workload launch**: one pgbench execution. Its launch number, parameters, start and end times, how it ended (completed, all clients dropped, failed), and its summary output.
- **Transaction record**: one transaction's identity (launch, client, sequence) with its before-commit and acknowledgement records and timestamps. The source of RPO and of per-transaction latency.
- **Per-second sample**: throughput, latency percentiles and failure counts for one second, on the harness clock. The same entity today's analysis consumes.
- **Operation history**: for list-append scenarios, each operation's invocation and outcome, with the read values the database returned.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100 % of existing catalog scenarios (12 at present) run with the pgbench workload with zero catalog changes, and 100 % of their existing negative tests still fail for the reason under test.
- **SC-002**: Across all lab runs, 0 acknowledged transactions lack a before-commit record, and an independent recount of each run's records reproduces its RPO exactly.
- **SC-003**: After a crash or restart, load resumes at the declared concurrency within 5 seconds of the database first accepting writes, in every lab run of NL-C-01, NL-C-02 and NL-M-06.
- **SC-004**: Outside outages, no gap between consecutive per-second samples exceeds 1.5 seconds.
- **SC-005**: NL-C-01 reaches at least its 150 TPS steady-state floor on the lab. NL-M-07 either reaches its 750 TPS floor or aborts before the fault naming the limiting side, and is never evaluated below its floor.
- **SC-006**: Every run report states the evidence-recording overhead and the pgbench launches it used.
- **SC-007**: After any run, interrupted or not, 0 pgbench processes remain on the driver host.
- **SC-008**: Every existing scenario still runs with the built-in generator when the profile selects it, with results unchanged from today, and 100 % of run reports name the generator they used.

## Assumptions

- **Same transaction shapes.** pgbench reproduces today's transaction shapes; pgbench's standard built-in workloads (for example `tpcb-like`) are out of scope for this feature.
- **The driver host provides pgbench** of the same major version as the target's server (ShaktiDB 17 on the lab; FR-004), with access to the target through the same credentials mechanism the harness uses today.
- **Throughput targets are discovered, not assumed.** The figures come from the first lab runs (NL-C-01 at 200 TPS, NL-M-07 at 1000 TPS). If NL-M-07 cannot be sustained, its rate is a separate decision after those runs.
- **Recording overhead is accepted and disclosed.** It is part of each transaction's measured latency, so baselines measured with pgbench are not directly comparable with baselines from the current workload generator.
- **The record mechanism is a planning decision.** It must meet FR-005 to FR-007. A mechanism that infers the before-commit record instead of recording it does not meet them, and would need a constitution amendment.
- **Fallback.** The built-in generator stays as a selectable fallback, for example for NL-M-07 if pgbench cannot sustain 1000 TPS, and for comparing results between generators. Removing it later would be a separate decision.
- **Branching.** All work stays on the `pgbench` branch, to be merged into `main` later.
