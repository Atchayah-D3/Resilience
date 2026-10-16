"""NL-M-05: a session left idle inside an open transaction, blocking vacuum."""

from __future__ import annotations

import re
from typing import Any

from resilience_tests.analysis.predicates import NOT_MEASURED
from resilience_tests.control.faults.base import FaultBehaviour, register_fault

IDLE_TRANSACTION_GRACE_S = 2.0
IDLE_TRANSACTION_MIN_SOAK_S = 30.0


@register_fault
class IdleTransaction(FaultBehaviour):
    fault_type = "idle_in_transaction"
    fact_key = "idle_transaction"
    recovers_unattended = True

    def recovery_hold_s(self, available_bound_s: float) -> float | None:
        """Hold the session open for the configured timeout plus a grace when that fits the
        recovery bound; the return to SLO is polled only as telemetry, never as the exit."""
        ctx = self.ctx
        t_s = ctx.adapter.idle_session_timeout_s(ctx.facts.get("scenario_observed") or {})
        testable = (0.0 < t_s) and ((t_s + IDLE_TRANSACTION_GRACE_S) < available_bound_s)

        min_soak = min(IDLE_TRANSACTION_MIN_SOAK_S, available_bound_s)
        if testable:
            hold_s = min(max(min_soak, t_s + IDLE_TRANSACTION_GRACE_S), available_bound_s)
        else:
            hold_s = min_soak

        ctx.facts["idle_hold_s"] = hold_s
        ctx.facts["idle_timeout_testable"] = testable
        ctx.facts["idle_timeout_parsed_s"] = t_s
        ctx.disclosures.append(
            f"Idle transaction held for {hold_s:.0f} s; the Framework specifies 2 h "
            f"(Framework §10.7). The hold is the configured timeout plus a "
            f"{IDLE_TRANSACTION_GRACE_S:.0f} s grace when that fits the recovery bound, "
            f"otherwise {IDLE_TRANSACTION_MIN_SOAK_S:.0f} s of evidence collection.")
        return hold_s

    async def measure(self, events: list[Any]) -> dict[str, Any]:
        """Each acceptance path is accepted only on evidence the harness observed.

        Path A (timeout enforced): the session is gone, the harness's connection is closed,
        AND the server said why -- its idle-in-transaction timeout log line, or that error
        code on the closed connection. A session that merely disappeared proves nothing.

        Path B (vacuum blocked, and an alert fired): vacuum is run while the session is still
        open and must report dead tuples it was not allowed to remove, at a cutoff no newer
        than the session's transaction id. Whether MONITORING alerted is not something the
        harness can see -- no alert source is wired in -- so bloat_alert_fired is NOT_MEASURED;
        the harness's own dead-tuple ratio shows bloat exists, not that anyone was told."""
        ctx = self.ctx
        m: dict[str, Any] = {}
        idle_fact = ctx.facts.get("idle_transaction") or {}
        idle_pid = idle_fact.get("pid")
        try:
            idle_check = await ctx.adapter.check_idle_transaction(idle_pid)
        except Exception as exc:
            ctx.facts["idle_check_error"] = f"{type(exc).__name__}: {exc}"
            idle_check = {"terminated_by_timeout": False, "still_idle": False, "error": str(exc)}
        ctx.facts["idle_transaction_check"] = idle_check

        # --- path A
        log_patterns = [re.compile(p, re.IGNORECASE) for p in ctx.adapter.idle_timeout_log_patterns()]
        log_evidence = next((str(e.data.get("line", "")) for e in events
                             if e.kind == "log_line" and e.t_mono_ns > (ctx.t0_ns or 0)
                             and any(p.search(str(e.data.get("line", ""))) for p in log_patterns)), None)
        sqlstate = idle_check.get("termination_sqlstate")
        sqlstate_evidence = sqlstate if sqlstate in ctx.adapter.idle_timeout_sqlstates else None
        ctx.facts["idle_timeout_evidence"] = {"log_line": log_evidence, "sqlstate": sqlstate_evidence}
        if not idle_check.get("terminated_by_timeout"):
            m["idle_in_transaction_session_timeout_enforced"] = False
        elif log_evidence or sqlstate_evidence:
            m["idle_in_transaction_session_timeout_enforced"] = True
        else:
            m["idle_in_transaction_session_timeout_enforced"] = NOT_MEASURED
            ctx.not_measured["idle_in_transaction_session_timeout_enforced"] = (
                "the idle session ended, but neither the server's idle-in-transaction timeout log line nor "
                f"SQLSTATE {'/'.join(ctx.adapter.idle_timeout_sqlstates) or '(none)'} was seen, so the cause "
                "is unknown")

        # --- context: planner statistics (reported, never the basis of a path)
        try:
            stats = await ctx.adapter.evaluate_vacuum_bloat()
        except Exception as exc:  # noqa: BLE001
            stats = {"error": f"{type(exc).__name__}: {exc}"}
        ctx.facts["vacuum_bloat_check"] = stats
        for name in ("dead_tuple_ratio", "unvacuumed_dead_tuples", "tuple_bloat_ratio", "oldest_transaction_age_s"):
            if stats.get(name) is not None:
                m[name] = stats[name]
            else:
                m[name] = NOT_MEASURED
                ctx.not_measured[name] = stats.get("error", "the engine reported no value")
        timing = {k: stats.get(k) for k in ("last_vacuum", "last_autovacuum", "last_analyze", "last_autoanalyze") if stats.get(k)}
        if timing:
            ctx.facts["vacuum_bloat_timing"] = timing
        ctx.disclosures.append(
            "dead-tuple metrics are from pg_stat_user_tables planner statistics, "
            "updated at analyze/vacuum and potentially stale while vacuum is blocked; "
            "reported as context only")
        if not stats.get("last_analyze") and not stats.get("last_autoanalyze"):
            ctx.disclosures.append(
                "No analyze ran during the hold; planner statistics may reflect pre-hold dead tuple estimates.")

        # --- path B: is vacuum actually blocked? Only meaningful while the session is open.
        if idle_check.get("still_idle"):
            try:
                probe = await ctx.adapter.probe_vacuum_horizon()
            except Exception as exc:  # noqa: BLE001
                probe = {"supported": False, "error": f"{type(exc).__name__}: {exc}"}
            ctx.facts["vacuum_horizon_probe"] = probe
            dead = probe.get("dead_not_removable")
            cutoff, xid = probe.get("removable_cutoff"), idle_fact.get("backend_xid")
            if not probe.get("supported") or dead is None:
                m["vacuum_blocked"] = NOT_MEASURED
                ctx.not_measured["vacuum_blocked"] = probe.get("error") or "vacuum did not report dead-but-not-removable tuples"
            else:
                m["dead_tuples_not_removable"] = dead
                # the cutoff must be held at (or behind) the idle session's own xid -- otherwise
                # something else is pinning the horizon and the fault is not the cause
                held_by_session = cutoff is None or xid is None or cutoff <= xid
                ctx.facts["vacuum_horizon_held_by_idle_session"] = held_by_session
                m["vacuum_blocked"] = dead > 0 and held_by_session
        else:
            m["vacuum_blocked"] = NOT_MEASURED
            ctx.not_measured["vacuum_blocked"] = "the idle session had already ended, so vacuum was no longer blocked by it"
        m["bloat_alert_fired"] = NOT_MEASURED
        ctx.not_measured["bloat_alert_fired"] = (
            "no monitoring alert source is connected to the harness; its own dead-tuple ratio is evidence of "
            "bloat, not of an alert")

        t_s = float(ctx.facts.get("idle_timeout_parsed_s") or 0.0)
        if t_s > 0 and not ctx.facts.get("idle_timeout_testable", False):
            ctx.disclosures.append(
                f"Configured idle_in_transaction_session_timeout ({t_s:.1f}s) exceeds the recovery soak bound; "
                "timeout enforcement was untestable within this ctx.")
        return m
