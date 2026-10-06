# Research: NL-M-03 Autovacuum Worker Killed

Facts observed on the lab target (read-only, 2026-10-06): `autovacuum=on`,
`autovacuum_naptime=60s`, `autovacuum_max_workers=3`, `autovacuum_vacuum_cost_delay=2ms`,
`autovacuum_vacuum_cost_limit=-1` (200 via vacuum_cost_limit), `scale_factor=0.2`,
`threshold=50`, `restart_after_crash=on`, `update_process_title=on`, `track_counts=on`;
unit `Restart=on-failure`, `StartLimitBurst=5` per 10 s. `ps` shows **two** autovacuum
launchers on the host: a second PostgreSQL cluster runs there.

## R1. How the fault is expressed in the catalog

- **Decision**: `fault.type: process_kill` + new `fault.during: autovacuum_worker`;
  `repeat: {cycles: 5, interval_s: 10}`.
- **Rationale**: the kill is a process kill; `during` already means "establish this state, then
  kill" (NL-C-02/03/06). Repeat already gives per-cycle recovery and the restart-cadence check.
- **Alternatives**: a new fault type (`child_process_kill`) -- rejected: duplicates process_kill
  recovery/revert/ledger handling for no gain.

## R2. Making a worker catchable

- **Decision**: a harness-owned table `resilience.avac_target` (~1M narrow rows, seeded in init
  like NL-C-06's table) with **table-level** storage options that slow its vacuum to several
  seconds (`autovacuum_vacuum_cost_delay` and a low per-table threshold). Each cycle, after the
  previous recovery, the adapter updates ~25% of its rows, then polls for an autovacuum worker
  vacuuming that table.
- **Rationale**: with the deployment's settings, a vacuum of the 2,000-row churn table finishes
  in milliseconds -- faster than an SSH round trip, so a kill would routinely miss. Dead tuples
  must be created *after* each recovery because crash recovery resets the statistics autovacuum
  uses to choose tables. Table storage options on a harness-owned table are harness objects,
  not operator configuration (Constitution VI); they are disclosed.
- **Alternatives**: tune global `autovacuum_naptime` / cost settings (forbidden, observe-only);
  kill any worker that happens to appear (too unreliable, too short-lived).

## R3. Identifying and killing exactly the worker

- **Decision**: the adapter finds the worker in the engine's activity view (backend type
  autovacuum worker, current relation = `avac_target`) and returns a generic kill target:
  `{pid, parent_pid (postmaster), title_marker: "autovacuum worker"}`. The driver runs ONE root
  command on the target: kill `pid` with SIGKILL only if `/proc/<pid>` still exists, its parent
  is `parent_pid`, and its command line contains the marker; otherwise exit with a distinct
  status meaning "nothing changed". Death is then confirmed from `/proc` as for NL-C-01, and
  the postmaster pid is confirmed unchanged (restart_after_crash) or the unit restarted.
- **Rationale**: the parent check binds the kill to our cluster on a host running two; the
  title check defends against pid reuse between observation and kill; one command minimises
  the window. SSH is pre-opened with the existing `arm()`.
- **Alternatives**: `pg_terminate_backend` (SIGTERM -- not the Framework's kill -9, and
  ignored semantics differ); `systemctl kill` (kills the whole cgroup -- that is NL-C-01).

## R4. A kill that missed

- **Decision**: "nothing changed" is raised as `FaultNotLanded` with `changed_nothing: true`;
  the orchestrator re-establishes and retries within the same cycle, up to 3 attempts, then
  aborts the run. Only confirmed kills count (`kills_landed`).
- **Rationale**: spec US3 -- a miss never counts and never passes; a bounded retry avoids
  aborting on the normal race of a worker finishing first.
- **Alternatives**: abort on first miss (wastes a 15-minute run on a benign race).

## R5. Worker wait bound

- **Decision**: per cycle, wait up to 2 x observed naptime + 30 s for the worker; abort with
  the reason if none appears.
- **Rationale**: after dead tuples are reported, the launcher visits the database within one
  naptime; two naptimes plus slack covers a launcher busy with other databases. Derived from
  observed configuration (Constitution IV).

## R6. "No relation left permanently unvacuumed" with statistics reset by every crash

- **Decision**: verification starts after the final recovery: record the database clock
  (`t_recovered`), regenerate dead tuples on `avac_target` (so at least one relation is
  eligible), then sample every 2 s until `3 x naptime` has passed, extended while an autovacuum
  worker is still processing an eligible relation, capped at half the validate phase bound.
  A relation is **eligible** if, in any sample, its dead tuples exceed its own threshold
  (per-table options or global `threshold + scale_factor x reltuples`). It is **vacuumed** if
  its last autovacuum time is set and is >= `t_recovered`. Report counts and names.
- **Rationale**: crash recovery resets activity statistics, so a last-autovacuum time present
  after the final recovery can only come from a vacuum after the last kill -- evidence that
  post-dates the fault (spec FR-008). Regenerating dead tuples prevents a vacuous "0 of 0".
  `relations_left_unvacuumed` is NOT_MEASURED if no relation became eligible.
- **Alternatives**: `relfrozenxid` advance (only proves freezing, not dead-tuple removal; slow
  to move); physical size (vacuum does not shrink files).

## R7. "A new worker spawns"

- **Decision**: during verification, any autovacuum worker of our instance observed after
  `t_recovered` sets `autovacuum_worker_respawned = true`.

## R8. Settings, refusals and disclosures

- **Decision**: extend `observe_fault_settings(fault_type, during=None)`; for
  `autovacuum_worker` return the settings above. The orchestrator refuses (PhaseAbort) when
  `autovacuum` or `track_counts` is off. Disclose: observed settings, the harness table's
  storage options, "each kill is a crash-restart of the whole instance".

## R9. Crash measurements

- **Decision**: workload `mixed` (markers + churn) as NL-C-05; gate `rpo_txn == 0`,
  `starts_unattended == true`, `structural_integrity_errors == 0`, `corruption_count == 0`,
  `cycles_recovered == 5` (Framework §10.2, §16.3).
