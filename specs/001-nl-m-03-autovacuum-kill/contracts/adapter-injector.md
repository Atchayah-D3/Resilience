# Contract: adapter and injector interfaces for `fault.during: autovacuum_worker`

All engine knowledge stays in the adapter (Constitution V). The orchestrator passes only
generic dictionaries.

## Adapter (`BaseDatabaseAdapter` defaults refuse; PostgreSQL implements)

| Method | Returns | Contract |
|---|---|---|
| `observe_fault_settings(fault_type, during=None)` | `dict[str,str]` | SHOW only. For `during == "autovacuum_worker"`: the settings in data-model E4. Never writes. |
| `prepare_scenario_objects(during)` | `dict` | For `autovacuum_worker`: create/seed `resilience.avac_target` with its table options, before the baseline. |
| `start_autovacuum_worker()` | `{"in_progress": bool, "kill_target": KillTarget, "note": str}` | Generate dead tuples on the harness table, then wait up to `2 x naptime + 30 s` for an autovacuum worker of THIS instance on that table. `in_progress` false + note if none. |
| `verify_autovacuum_resumed()` | `{"autovacuum_worker_respawned": bool, "relations_eligible": int, "relations_left_unvacuumed": int\|None, "unvacuumed": [names], "evidence": [...], "bound_s": float}` | Run after the final recovery as in research R6. `relations_left_unvacuumed` is None when no relation became eligible. |
| `cleanup_scenario_objects()` | `dict` | Also truncates `avac_target`. |

Base-class defaults: `start_autovacuum_worker` -> `{"in_progress": False, "note": "not implemented by this engine"}`; `verify_autovacuum_resumed` -> `{}` (every measure NOT_MEASURED).

## Injector (`FaultInjector`)

- New attribute `kill_target: Mapping | None` (generic; set by the orchestrator per attempt,
  cleared after).
- `OsSshProcessDriver._kill` when `kill_target` is set: one root command that kills
  `kill_target.pid` with SIGKILL only if the process exists, its parent pid equals
  `kill_target.parent_pid` and its command line contains `kill_target.title_marker`;
  otherwise raises `FaultNotLanded(..., {"changed_nothing": True, ...})`. After the kill, death
  is confirmed from `/proc` (existing `process_gone`); the detail records `target_pid`,
  `postmaster_pid`, `death_confirmed_s`, `landed: True`.
- Without `kill_target`, `_kill` is unchanged (whole-cgroup kill, NL-C-01..06).
- `confirm()` for process_kill already reports `fault_confirmed` from `death_confirmed_s`.

## Orchestrator

- `_establish_fault_state`: `during == "autovacuum_worker"` -> `start_autovacuum_worker()`;
  not in progress -> abort with the note; otherwise set `injector.kill_target`.
- `_inject_once`: on `FaultNotLanded` with `changed_nothing`, re-establish and retry, at most 3
  attempts per cycle; then abort. Count landed kills into `kills_landed`.
- `_verify_during_operation`: `autovacuum_worker` -> `verify_autovacuum_resumed()` mapped to
  the measures in data-model; None -> NOT_MEASURED with reason.
- Init: refuse (PhaseAbort) if observed `autovacuum` or `track_counts` is `off`.
