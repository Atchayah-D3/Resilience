# Resilience Test Harness Constitution

The harness implements the *Resilience Test Harness Architecture v1.0* ("Arch §n") for the
*Unified Resilience Testing Framework v1.0* ("Framework §n"), both in `docs/`, against
ShaktiDB (standalone PostgreSQL) and DistDB. It deliberately breaks a running database and
measures what the failure cost. A wrong "passed" is worse than no result: it certifies a
guarantee nobody measured.

## Core Principles

### I. A Test Is Data

- A scenario is a catalog file (`catalog/<tier>/<ID>.yaml`) that states *what* to break and
  *what good means*. It MUST NOT name a host, address, login, driver or tool; the catalog
  checks reject one that does.
- A machine is separate data: `envs/<profile>.yaml` holds hosts, ports, paths, services,
  drivers, phase bounds, the safety allowlist and the disclosures. The profile, never the
  scenario, resolves a fault type to its driver.
- The same scenario MUST run on another environment without a code edit.

*Rationale:* one catalog serves every environment class and both products; behaviour hidden in
code cannot be reviewed against the Framework row it claims to implement.

### II. Anything Unbuilt Refuses

- A weaker fault MUST NOT be substituted for the specified one (a process kill is not a power
  loss; a 120 s soak is not a 2 h hold unless disclosed as such).
- A scenario whose engine lacks a capability (`requires`) or whose environment lacks
  infrastructure (`needs_infra`) MUST be skipped with the reason -- by the run plan, and again
  by the orchestrator if run directly.
- A driver that is not configured or not built MUST refuse (`DriverNotAvailable`), never no-op.

*Rationale:* a scenario run against a substitute produces a number that answers a different
question, and it is indistinguishable from the real answer in a summary.

### III. Never a False Pass (NON-NEGOTIABLE)

- `NOT_MEASURED` and `NOT_APPLICABLE` fail every predicate that touches them and are never
  written as `0`, `true` or a default. A measure the harness could not take states why.
- Every fault MUST be shown to have landed before its result counts: confirmed from the
  target (`fault_confirmed`, process death from `/proc`, a `fault.during` operation reported in
  progress by the engine itself) and corroborated after recovery (e.g. an interrupted index
  left INVALID, no rows of a killed transaction visible). A fault that did not land aborts
  the run or fails its criterion; it never passes.
- A check that cannot run fails closed (missing checker, skipped database, unreadable
  journal, unparseable verdict).
- Every scenario's tests MUST include a negative case proving the criterion can fail, and it
  MUST fail for the reason under test -- not for an unrelated harness error.

*Rationale:* the review that preceded this constitution found P0 scenarios reporting `passed`
without their fault, their measurement or their acceptance criteria. This principle exists
so that cannot recur.

### IV. The Framework Is the Source of Truth

- Every `accept` predicate MUST trace to the Framework row it implements (cited as
  `Framework §n` / `Arch §n` -- never a bare `§n`) or be marked `NEEDS SIGN-OFF` with the
  reasoning in the scenario header. Unsourced thresholds are not permitted.
- Every scenario gates `structural_integrity_errors == 0` (pg_amcheck clean after every Tier-1
  and Tier-2 scenario, Framework §16.3).
- Where the Framework names a fault, a load or a duration, the scenario uses it or discloses
  the deviation.

*Rationale:* the harness certifies Framework compliance; a threshold with no source certifies
nothing.

### V. The Adapter Seam

- Only adapters (`resilience_tests/adapters/<engine>/`) know what the database is: SQL,
  connections, error taxonomy, catalog tables, engine tools.
- The orchestrator, workload driver, probes, analysis and injectors above the adapter MUST be
  engine-agnostic and MUST NOT branch on a scenario ID. Scenario-specific behaviour comes from
  catalog fields (`fault.type`, `fault.during`, `workload.history`, `repeat`, `requires`,
  `needs_infra`).
- Supporting a new engine (DistDB) means a new adapter, not edits across the harness.

*Rationale:* Arch §4.1/§5/§8; scenario-ID branches hide behaviour from the catalog and break
when the same behaviour is needed by a second scenario.

### VI. Leave the Target as Found

- The injection ledger is written and fsynced on the driver host BEFORE every fault. A run
  that cannot confirm its own revert MUST NOT report `passed`.
- The kill switch MUST be able to revert any outstanding fault from a fresh process, after a
  harness crash: faults inside the database are found by stable handles (application_name,
  schema-qualified object names), never by remembered connections or PIDs alone.
- The harness observes operator configuration and never writes it. Harness-owned objects
  (tables, indexes, sessions, flood connections) are removed by cleanup and again by the next
  run's init.
- Privileges or objects the harness leaves in place are stated in the run's disclosures.

*Rationale:* Arch §15 -- the harness must not strand a node in a faulted state, and one run
must not contaminate the next.

### VII. Evidence Integrity

- RPO is measured from transaction markers journalled on the separate driver host: written
  and flushed before the commit is sent, acknowledged only after the server confirms
  (Arch §6.2). Outcomes are classified committed / definitely aborted / unknown; an unknown
  is never counted as either (Arch §7.1).
- Elle runs only on a real list-append history whose reads are what the database returned
  (Arch §10.3). There is no substitute checker; without one the result is `NOT_MEASURED`.
- Every report carries disclosures for every evidence limitation of its environment and run
  (no snapshot reset, undeclared storage class, process kill instead of power loss, a
  shortened hold, missing tools). RPO = 0 under process kill is crash recovery, not
  durability; durability claims require NL-D.

*Rationale:* a verdict is only as trustworthy as the evidence it was calculated from, and a
reader must be able to see what the environment could not prove.

## Safety & Environment Constraints

- Lab and staging only; production is refused (Framework §7.1). The target must match the
  profile's allowlist AND carry the operator-created sentinel row; destructive categories
  additionally require `--target-is-disposable`.
- The harness runs on a driver host separate from every node under test (Arch §6.2). One run
  at a time per target (target lock). Every phase is bounded by the profile's
  `phase_timeouts_s`; an unbounded wait is a defect (Arch §15).
- The blast radius is one node (two for CL-X compound scenarios only, Arch §4.3).
- Infrastructure prerequisites (data disk, power control, packages, co-tenant, standby) are
  tracked in `docs/infra-requirements.md`; scenarios that need them declare `needs_infra`
  until a profile provides them.

## Development Workflow & Quality Gates

- One Spec Kit feature per scenario (or per shared harness change):
  `/speckit-specify` -> `/speckit-clarify` -> `/speckit-plan` -> `/speckit-tasks` ->
  `/speckit-analyze` -> `/speckit-implement`. The spec cites the Framework row; the plan's
  Constitution Check covers Principles I-VII.
- Before merge: `pytest tests/` passes; `python -m catalog.schema --partial` passes; a live
  run on the lab target (`envs/e2-dedicated-vm.yaml`) passes, with its evidence directory
  referenced in the PR, for every scenario the change touches -- including scenarios affected
  through shared code.
- Review checks every `accept` predicate against its Framework row and every new measure for
  a fail path (Principle III).
- A scenario has an owner. Changes to another owner's scenario are made with that owner.
- Work merges through `main` by pull request; conflicts are resolved by re-applying changes on
  top of the latest `main`, never by taking one side of an auto-merge on trust.

## Governance

- This constitution supersedes ad-hoc practice. Where code and constitution disagree, the code
  is the defect.
- Amendments are made by pull request stating the rationale and the version bump. Versioning
  is semantic: MAJOR for removing or redefining a principle, MINOR for a new principle or
  section or materially expanded guidance, PATCH for clarifications.
- Every plan's Constitution Check and every review verifies compliance; a justified exception
  is recorded in the plan's Complexity Tracking with the principle it departs from.

**Version**: 1.0.0 | **Ratified**: 2026-10-06 | **Last Amended**: 2026-10-06
