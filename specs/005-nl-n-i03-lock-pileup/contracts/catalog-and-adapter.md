# Contract: catalog fields and adapter hooks

Adapter (`SessionFaultSupport`): `observe_lock_settings()`, `start_lock_pileup(blocker, ddl,
ddl_lock_timeout_s, ddl_after_s)` -> `{"supported": bool, "t0_mono_ns", ...}`,
`lock_pileup_state()`, `end_lock_pileup()` (idempotent), `revert_lock_pileup()` (idempotent;
raises if a tagged session or probe column remains).
Core: `contended_key_space`, `prepare_contended_table()`, `crash_restart_log_patterns()`.
Session: `commit_marker_with_contended_row(seq, marker_id, key)`.
Engines that do not implement a hook refuse (NotImplementedError) or return {} / () as documented.
