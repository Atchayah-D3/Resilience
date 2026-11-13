"""NL-N-I03: a lock pile-up behind a schema change -- an internal noisy neighbour whose shared
resource is a table's lock queue.

A schema change needs the strongest lock on a table. When it has to wait (behind a long read)
or holds that lock while it rewrites the table, every new query on the table queues behind it,
and through the workload's shared connections other work pauses too. The schema-change
session's own lock timeout is the protection under test: with it, the schema change gives up
and the queue clears within seconds; without it, the freeze lasts as long as the blocker does.

The hold ends when the queue clears -- the schema change gave up, or finished once its blocker
was released -- or when fault.duration runs out, whichever comes first. T1 is the moment the
queue cleared, so the return to normal service is measured from there. The pile-up is released
through the adapter; its ledger entry stays outstanding until cleanup's revert, after the
workload has stopped, ends every tagged session and drops what the schema change added (a revert
that cannot do so leaves the entry outstanding, and the run can then never report `passed`).
"""

from __future__ import annotations

import asyncio
import re
import time
from dataclasses import asdict
from types import SimpleNamespace
from typing import Any

from resilience_tests.analysis import rto_decomposer
from resilience_tests.analysis.predicates import NOT_MEASURED
from resilience_tests.control.faults.base import HeldFault, register_fault
from resilience_tests.execution.workload import contract
from resilience_tests.execution.workload.driver import CONTENDED_GROUP, OTHER_GROUP

STATE_POLL_S = 0.5
# After the release, the transactions queued behind the schema change still have to finish; the
# hold's load window stays open this long so their waits are counted in the hold, not after it.
QUEUE_DRAIN_S = 1.0
# Other work pauses through the shared connections while the table is frozen; its throughput
# is judged over the minute after the schema change was issued.
OTHERS_WINDOW_S = 60.0
# A table user's wait ends when the queue clears; its transaction completes a moment later.
WAIT_SLACK_S = 10.0
# Write probes on each side of the freeze, so a stall that spans its edges is measured whole.
PROBE_EDGE_S = 2.0
# "Back to normal" for this scenario (Scenario 3 document, section 2 working definition): TPS at
# least 95% of the baseline and p99 at most 1.5x, for the sustain period -- stricter than the
# Framework §6.2 80%. NEEDS SIGN-OFF: the document lists the exact values as an open decision.
NORMAL_TPS_FRACTION = 0.95
# The document's "for 60 s in a row" is judged as one window of the sustain period
# (rto_decomposer.SLO_SUSTAIN_S, 60 s): TPS over the whole window, p99 in every second of it. At
# the scenario's 60 TPS a 1 s count swings with when commits land (measured on .112: 3..108 per
# second with no fault, 36 of 119 baseline seconds under 95%), so a per-second 95% bound could
# never be met, not even by the undisturbed baseline. See slo_tps_window_s().
# Run 3a gates rto_to_slo_s <= 60 (counted from the moment the queue cleared). The recovery watch
# must therefore last until a return starting at that limit has been held for the full sustain
# period, or a late-but-passing return would read as not measured.
RECOVERY_LIMIT_S = 60.0
RECOVERY_WATCH_SLACK_S = 10.0
# On TPC-C's stock table (lock_pileup.table tpcc_stock) the table users are the transaction types
# that read or write stock; the others never touch it (Scenario 3 document, section 1).
TPCC_TABLE_USERS = (contract.NEW_ORDER, contract.STOCK_LEVEL)
TPCC_OTHERS = (contract.PAYMENT, contract.ORDER_STATUS, contract.DELIVERY)


@register_fault
class LockPileupFault(HeldFault):
    fault_type = "lock_pileup"
    fact_key = "lock_pileup"

    async def observe_settings(self) -> dict[str, str]:
        return await self.ctx.adapter.observe_lock_settings()

    def slo_tps_fraction(self) -> float | None:
        return NORMAL_TPS_FRACTION

    def slo_tps_window_s(self) -> float | None:
        return rto_decomposer.SLO_SUSTAIN_S      # read at call time: the window IS the sustain period

    def phase_extension_s(self, phase: str) -> float:
        if phase != "recovery":
            return super().phase_extension_s(phase)
        needed = RECOVERY_LIMIT_S + rto_decomposer.SLO_SUSTAIN_S + RECOVERY_WATCH_SLACK_S
        return max(0.0, needed - self.ctx.phase_timeout_s("recovery"))

    def _on_tpcc(self) -> bool:
        plan = self.ctx.scenario.fault.lock_pileup
        return plan is not None and plan.table == "tpcc_stock"

    async def after_inject(self, detail: dict[str, Any]) -> None:
        ctx = self.ctx
        if self._on_tpcc():
            ctx.disclosures.append(
                f"The pile-up formed on TPC-C's stock table ({detail.get('table')}). Table users: "
                f"{', '.join(TPCC_TABLE_USERS)}; others: {', '.join(TPCC_OTHERS)} (per-type samples).")
        else:
            ctx.disclosures.append(
                f"The contended table is the harness's own ({detail.get('table')}), read and updated by a "
                f"{ctx.scenario.workload.contended_share:.0%} share of the marker workload; it stands in for an "
                "application table until the TPC-C workload exists.")
        ctx.disclosures.append(
            "Table users' transactions clear their own lock and statement timeouts, so a wait is measured "
            "as latency; the workload driver still abandons a transaction after its own 10 s bound "
            "(counted as indeterminate, never as lost).")
        ctx.disclosures.append(
            f"Back to normal (rto_to_slo_s) uses this scenario's definition: TPS >= {NORMAL_TPS_FRACTION:.0%} "
            f"of the baseline over a {rto_decomposer.SLO_SUSTAIN_S:g} s window and p99 <= "
            f"{rto_decomposer.SLO_P99_MULTIPLIER:g}x in every second of it ({rto_decomposer.SLO_SUSTAIN_S:g} s "
            f"straight), counted from the moment the lock queue cleared. TPS is judged over the window, "
            f"not per second, because a 1 s count at this rate swings with when commits land.")

    async def hold_and_release(self) -> dict[str, Any]:
        ctx = self.ctx
        hold_s = self._hold_s()
        assert hold_s is not None   # before_inject refused otherwise
        window: dict[str, Any] = {"planned_s": hold_s, "start_wall_s": time.time(),
                                  "start_mono_ns": time.monotonic_ns()}
        ctx.facts["hold"] = window
        ctx.emit("hold_start", planned_s=hold_s)
        ctx.open_load_window()
        await self.on_hold_start()
        deadline = time.monotonic() + hold_s
        state = await ctx.adapter.lock_pileup_state()
        while state.get("ddl_outcome") is None and time.monotonic() < deadline:
            await asyncio.sleep(min(STATE_POLL_S, max(0.0, deadline - time.monotonic())))
            state = await ctx.adapter.lock_pileup_state()
        await self.on_hold_end()

        # Released from the pile-up's own sessions: the read is committed, so a waiting schema
        # change gets its lock and finishes, or one still running is cancelled. The ledger entry
        # is left outstanding for cleanup's revert (see the module docstring). Released BEFORE
        # the load window closes, which stays open until the queue has drained: a table user
        # that waited out the freeze completes only after it, and its wait belongs to the hold.
        released = await ctx.adapter.end_lock_pileup()
        await asyncio.sleep(QUEUE_DRAIN_S)
        held = ctx.close_load_window()
        window.update(end_wall_s=time.time(), end_mono_ns=time.monotonic_ns(), load=asdict(held))
        self._held = held
        ctx.emit("hold_end", tps=held.tps, p99_ms=held.p99_ms)

        state = await ctx.adapter.lock_pileup_state()
        ctx.facts["lock_pileup_state"] = state
        ended = state.get("ddl_ended_mono_ns")
        t1 = ended if isinstance(ended, int) else time.monotonic_ns()
        ctx.set_t1(t1)
        ctx.emit("t1", t1_mono_ns=t1, action="lock queue cleared", ddl_outcome=state.get("ddl_outcome"))
        self._released = {"held_s": round((window["end_mono_ns"] - window["start_mono_ns"]) / 1e9, 3),
                          "released": released, "ddl_outcome": state.get("ddl_outcome")}
        return self._released

    async def measure(self, events: list[Any]) -> dict[str, Any]:
        ctx = self.ctx
        m = await super().measure(events)   # oltp_tps_ratio, oltp_p99_ratio over the hold
        state = ctx.facts.get("lock_pileup_state") or await ctx.adapter.lock_pileup_state()
        issued, ended = state.get("ddl_issued_mono_ns"), state.get("ddl_ended_mono_ns")
        outcome = state.get("ddl_outcome")
        never = "the schema change was never issued, so the pile-up never formed"

        if issued is None:
            for name in ("ddl_lock_timeout_fired", "ddl_completed", "queue_freeze_s", "queue_max_sessions",
                         "blocker_visible"):
                m[name] = NOT_MEASURED
                ctx.not_measured[name] = never
        else:
            m["ddl_lock_timeout_fired"] = outcome == "lock_timeout"
            m["ddl_completed"] = outcome == "completed"
            m["queue_max_sessions"] = int(state.get("queue_max") or 0)
            if isinstance(ended, int):
                m["queue_freeze_s"] = round((ended - issued) / 1e9, 3)
            else:
                m["queue_freeze_s"] = NOT_MEASURED
                ctx.not_measured["queue_freeze_s"] = "the schema change had not ended when measurement stopped"
            chain = state.get("chain") or {}
            if state.get("chain_confirmed") is None:
                m["blocker_visible"] = NOT_MEASURED
                ctx.not_measured["blocker_visible"] = "the blocking chain was not evaluated"
            else:
                # visible: the server named the schema change, and what held it up, by pid
                ddl = chain.get("ddl") or {}
                held_up = state.get("reader_pid") is None or state.get("reader_pid") in (ddl.get("blocked_by") or [])
                m["blocker_visible"] = bool(chain) and ddl.get("pid") is not None and held_up

        m.update(self._table_user_wait(events, issued, ended))
        m.update(self._others_tps_lost(events, issued))
        m.update(self._writer_stall(events, issued, ended))
        m.update(self._crash_restarts(events))
        m.update(self._lock_waits_logged(events, issued))
        m.update(await self._leftovers())
        return m

    async def _leftovers(self) -> dict[str, Any]:
        """Scenario 3 document, check 7 and table 20: after the run the probe column is gone and
        no harness session is left. Measured in validate, with the workload already stopped (so
        dropping a column cannot pile up a queue of its own): the revert is applied here --
        cleanup's revert then finds nothing left and closes the ledger entry -- and what is left
        is read back from the server."""
        ctx = self.ctx
        names = ("probe_column_absent", "harness_sessions_left")
        try:
            ctx.facts["lock_pileup_revert"] = await ctx.adapter.revert_lock_pileup()
        except Exception as exc:  # noqa: BLE001 -- what is left is read back below either way
            ctx.facts["lock_pileup_revert"] = {"error": f"{type(exc).__name__}: {exc}"}
        try:
            left = await ctx.adapter.lock_pileup_leftovers()
        except Exception as exc:  # noqa: BLE001 -- recorded; both measures are then not measured
            for name in names:
                ctx.not_measured[name] = f"what the pile-up left could not be read: {type(exc).__name__}: {exc}"
            return {name: NOT_MEASURED for name in names}
        ctx.facts["lock_pileup_leftovers"] = left
        return {"probe_column_absent": not left.get("probe_columns"),
                "harness_sessions_left": int(left.get("sessions") or 0)}

    # --- measures from the run's own evidence ------------------------------------------------

    def _group_samples(self, events: list[Any], group: str) -> list[Any]:
        """One entry per second for the table users (CONTENDED_GROUP) or the others (OTHER_GROUP),
        each with t_mono_ns and data {interval_s, commits, max_ms}. Marker workload: its own
        group samples. TPC-C: the per-type samples of the types in that group, summed."""
        if not self._on_tpcc():
            return [e for e in events if e.kind == "group_sample" and e.source == "workload"
                    and e.data.get("group") == group]
        types = TPCC_TABLE_USERS if group == CONTENDED_GROUP else TPCC_OTHERS
        out = []
        for e in events:
            if e.kind != contract.SAMPLE_KIND or e.source != contract.SAMPLE_SOURCE:
                continue
            by_type = e.data.get(contract.BY_TYPE) or {}
            parts = [by_type[t] for t in types if t in by_type]
            if not parts:
                continue
            peaks = [p["max_ms"] for p in parts if p.get("max_ms") is not None]
            out.append(SimpleNamespace(t_mono_ns=e.t_mono_ns, data={
                "interval_s": e.data.get("interval_s", 0.0),
                "commits": sum(int(p.get("commits") or 0) for p in parts),
                "max_ms": max(peaks) if peaks else None}))
        return out

    def _table_user_wait(self, events: list[Any], issued: int | None, ended: int | None) -> dict[str, Any]:
        """The longest a table user's transaction took while the table was frozen: the largest
        per-second maximum latency of the table users from the schema change until just after
        the queue cleared."""
        name = "longest_table_wait_s"
        if issued is None:
            self.ctx.not_measured[name] = "the schema change was never issued"
            return {name: NOT_MEASURED}
        until = (ended if isinstance(ended, int) else time.monotonic_ns()) + int(WAIT_SLACK_S * 1e9)
        peaks = [e.data["max_ms"] for e in self._group_samples(events, CONTENDED_GROUP)
                 if issued < e.t_mono_ns <= until and e.data.get("max_ms") is not None]
        if not peaks:
            self.ctx.not_measured[name] = "no table-user transaction finished while the table was frozen"
            return {name: NOT_MEASURED}
        return {name: round(max(peaks) / 1000, 3)}

    def _others_tps_lost(self, events: list[Any], issued: int | None) -> dict[str, Any]:
        """Throughput the others (who never touch the table) lost over the minute after the
        schema change was issued, against their own throughput in the baseline."""
        name = "other_tables_tps_lost_pct"
        ctx = self.ctx
        t0, baseline = ctx.t0_ns, ctx.baseline
        if issued is None or t0 is None or baseline is None:
            ctx.not_measured[name] = "the schema change was never issued, or no baseline was measured"
            return {name: NOT_MEASURED}
        samples = self._group_samples(events, OTHER_GROUP)
        base_from = t0 - int(baseline.duration_s * 1e9)
        before = [e for e in samples if base_from <= e.t_mono_ns < t0]
        during = [e for e in samples if issued < e.t_mono_ns <= issued + int(OTHERS_WINDOW_S * 1e9)]

        def tps(window: list[Any]) -> float | None:
            span = sum(e.data.get("interval_s", 0.0) for e in window)
            return sum(e.data.get("commits", 0) for e in window) / span if span > 0 else None

        base_tps, during_tps = tps(before), tps(during)
        if not base_tps or during_tps is None:
            ctx.not_measured[name] = "the others' throughput was not sampled in the baseline or after the schema change"
            return {name: NOT_MEASURED}
        ctx.facts["others_tps"] = {"baseline": round(base_tps, 2), "after_schema_change": round(during_tps, 2),
                                   "window_s": OTHERS_WINDOW_S}
        return {name: round(100.0 * (1.0 - during_tps / base_tps), 2)}

    def _writer_stall(self, events: list[Any], issued: int | None, ended: int | None) -> dict[str, Any]:
        """The write prober inserts into its own table on its own connection, outside the
        workload's connections: the longest gap between its successful writes across the
        freeze shows whether the freeze spread beyond the contended table."""
        name = "writer_max_stall_s"
        if issued is None:
            self.ctx.not_measured[name] = "the schema change was never issued"
            return {name: NOT_MEASURED}
        until = (ended if isinstance(ended, int) else time.monotonic_ns()) + int(PROBE_EDGE_S * 1e9)
        since = issued - int(PROBE_EDGE_S * 1e9)
        ok = sorted(e.t_mono_ns for e in events
                    if e.kind == "write_probe" and e.data.get("ok") and since <= e.t_mono_ns <= until)
        if len(ok) < 2:
            self.ctx.not_measured[name] = "the write prober recorded fewer than two successful writes around the freeze"
            return {name: NOT_MEASURED}
        return {name: round(max(b - a for a, b in zip(ok, ok[1:])) / 1e9, 3)}

    def _crash_restarts(self, events: list[Any]) -> dict[str, Any]:
        """Server processes that crashed and made the server end every session and restart,
        from the engine's own log after the fault -- a restart the start time does not show."""
        name = "server_crash_restarts"
        patterns = [re.compile(p) for p in self.ctx.adapter.crash_restart_log_patterns()]
        t0 = self.ctx.t0_ns
        if not patterns or t0 is None:
            self.ctx.not_measured[name] = "this engine names no crash-restart log lines"
            return {name: NOT_MEASURED}
        if not any(e.kind == "log_line" for e in events):
            self.ctx.not_measured[name] = "no line of the engine's log reached the harness during the run"
            return {name: NOT_MEASURED}
        hits = [e.data.get("line", "") for e in events if e.kind == "log_line" and e.t_mono_ns > t0
                and any(p.search(e.data.get("line", "")) for p in patterns)]
        if hits:
            self.ctx.facts["crash_restart_lines"] = hits[:20]
        return {name: len(hits)}

    def _lock_waits_logged(self, events: list[Any], issued: int | None) -> dict[str, Any]:
        """The engine's own lock-wait log lines after the schema change was issued (PostgreSQL
        writes them only with log_lock_waits on, after deadlock_timeout of waiting). Reported:
        the server's record of the pile-up, independent of the harness's own polling. The
        harness never turns the setting on -- configuration is observed, not written."""
        name = "lock_waits_logged"
        ctx = self.ctx
        patterns = [re.compile(p) for p in ctx.adapter.lock_wait_log_patterns()]
        if issued is None:
            ctx.not_measured[name] = "the schema change was never issued"
            return {name: NOT_MEASURED}
        if not patterns:
            ctx.not_measured[name] = "this engine names no lock-wait log lines"
            return {name: NOT_MEASURED}
        setting = (ctx.facts.get("scenario_observed") or {}).get("log_lock_waits")
        if setting is not None and str(setting).lower() not in ("on", "true", "1"):
            ctx.not_measured[name] = (f"log_lock_waits is {setting!r} on the target, so the server logs no lock "
                                      "waits; the harness does not change server configuration")
            return {name: NOT_MEASURED}
        if not any(e.kind == "log_line" for e in events):
            ctx.not_measured[name] = "no line of the engine's log reached the harness during the run"
            return {name: NOT_MEASURED}
        hits = [e.data.get("line", "") for e in events if e.kind == "log_line" and e.t_mono_ns >= issued
                and any(p.search(e.data.get("line", "")) for p in patterns)]
        if hits:
            ctx.facts["lock_wait_lines"] = hits[:20]
        return {name: len(hits)}

    @classmethod
    def summary_lines(cls, r: dict[str, Any]) -> list[str]:
        facts = r.get("facts") or {}
        state = facts.get("lock_pileup_state")
        if not state:
            return []
        measured = r.get("measured") or {}
        others = facts.get("others_tps") or {}
        return [
            "",
            "lock pile-up behind a schema change (NL-N-I03):",
            f"  blocker: {state.get('blocker')}  schema change: {state.get('ddl')}  "
            f"its lock timeout: {_timeout_text(state.get('ddl_lock_timeout_s'))}",
            f"  schema change ended: {state.get('ddl_outcome')} (sqlstate {state.get('ddl_sqlstate')})  "
            f"chain seen: {state.get('chain_confirmed')}  largest queue: {state.get('queue_max')} sessions",
            f"  freeze: {measured.get('queue_freeze_s')} s  longest table-user wait: "
            f"{measured.get('longest_table_wait_s')} s  writer stall: {measured.get('writer_max_stall_s')} s",
            f"  others' TPS: baseline {others.get('baseline')}, first minute after the schema change "
            f"{others.get('after_schema_change')} -> lost {measured.get('other_tables_tps_lost_pct')}%",
            f"  back to normal after the queue cleared: {measured.get('rto_to_slo_s')} s  "
            f"lock waits in the server log: {measured.get('lock_waits_logged')}  "
            f"per-second lock samples: {len(state.get('samples') or [])}",
            f"  after the run: probe column absent {measured.get('probe_column_absent')}  "
            f"harness sessions left {measured.get('harness_sessions_left')}",
        ]


def _timeout_text(seconds: Any) -> str:
    """0 is "no lock timeout" to the server, so it is never printed as "0.0 s"."""
    if seconds is None:
        return "unknown"
    return "none" if float(seconds) == 0 else f"{float(seconds):g} s"
