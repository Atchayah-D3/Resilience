"""Operations a fault must land inside (`fault.during`): a checkpoint (NL-C-02), a large
transaction (NL-C-03), a concurrent index build (NL-C-06), an autovacuum worker (NL-M-03)."""

from __future__ import annotations

from typing import Any, ClassVar

from resilience_tests.analysis.predicates import NOT_MEASURED
from resilience_tests.control.faults.base import DuringBehaviour, PhaseAbort, register_during


@register_during
class Checkpoint(DuringBehaviour):
    during = "checkpoint"

    async def create_objects(self) -> dict[str, Any]:
        return await self.ctx.adapter.prepare_checkpoint_objects()

    async def establish(self) -> tuple[dict[str, Any], Any]:
        # The kill must land while the checkpoint is still running. Everything the kill
        # needs (SSH session, postmaster identity) is prepared BEFORE the checkpoint
        # starts, so once the checkpointer is seen working the kill is a single command.
        # Whether it actually landed in time is proven after recovery (validate).
        ctx = self.ctx
        await ctx.arm_fault()
        try:
            # Give the checkpoint a known amount of data to write, so it is still running
            # when the kill arrives rather than finishing in the gap before it.
            changed = await ctx.adapter.change_pages_before_checkpoint()
        except Exception as exc:
            await ctx.disarm_fault()
            raise PhaseAbort(f"could not prepare data for the checkpoint to write: "
                         f"{type(exc).__name__}: {exc}") from None
        except BaseException:
            await ctx.disarm_fault()
            raise
        ctx.facts["checkpoint_pages_changed"] = changed
        try:
            checkpoint_detail = await ctx.adapter.trigger_checkpoint_and_await_active()
        except Exception as exc:
            ctx.facts["checkpoint_trigger_error"] = f"{type(exc).__name__}: {exc}"
            checkpoint_detail = {"checkpointer_active": False, "error": str(exc)}
        except BaseException:
            await ctx.disarm_fault()
            raise
        active = checkpoint_detail.get("checkpointer_active", False)
        ctx.facts["checkpointer_seen_active_before_kill"] = active
        ctx.facts["checkpoint_injection"] = checkpoint_detail
        if not active:
            await ctx.disarm_fault()
            raise PhaseAbort(f"checkpointer was not active: {checkpoint_detail.get('error', 'wait event indicates idle checkpointer')}")
        return checkpoint_detail, None

    async def verify(self, events: list[Any]) -> tuple[dict[str, Any], str | None]:
        # Proven from where crash recovery started, not from what was seen before the kill:
        # a checkpoint observed running can still finish before an SSH command arrives.
        ctx = self.ctx
        m: dict[str, Any] = {}
        fault_not_landed: str | None = None
        log_lines = [str(e.data.get("line", "")) for e in events
                     if e.kind == "log_line" and e.t_mono_ns > ctx.t0_ns]
        try:
            verification = await ctx.adapter.checkpoint_in_flight_at_kill(log_lines)
        except Exception as exc:  # noqa: BLE001 -- recorded; the run is then aborted
            verification = {"in_flight": None, "note": f"{type(exc).__name__}: {exc}"}
        ctx.facts["checkpoint_verification"] = verification
        in_flight = verification.get("in_flight")
        if in_flight is None:
            m["checkpoint_in_flight_at_kill"] = NOT_MEASURED
            ctx.not_measured["checkpoint_in_flight_at_kill"] = verification.get("note", "no evidence")
            fault_not_landed = ("could not show the checkpoint was still running when the kill landed: "
                                + verification.get("note", "no evidence"))
        else:
            m["checkpoint_in_flight_at_kill"] = in_flight
            if not in_flight:
                fault_not_landed = ("the checkpoint completed before the kill landed, so this run tested an "
                                    "ordinary crash, not a crash during a checkpoint: " + verification.get("note", ""))
        return m, fault_not_landed


class _InterruptedOperation(DuringBehaviour):
    """An operation the engine runs in the background, judged after recovery by what it left
    behind (Framework §10.2 NL-C-03 / NL-C-06), or by whether the process killed came back
    (Framework §10.7 NL-M-03)."""

    # measure name -> key in the adapter's verification result
    MEASURES: ClassVar[dict[str, str]] = {}

    async def _verify_with_adapter(self) -> dict[str, Any]:
        raise NotImplementedError

    async def verify(self, events: list[Any]) -> tuple[dict[str, Any], str | None]:
        """Any value the engine could not produce is NOT_MEASURED, never a default."""
        ctx = self.ctx
        m: dict[str, Any] = {"operation_in_progress_at_fault": bool((ctx.facts.get("during") or {}).get("in_progress"))}
        await ctx.adapter.abandon_background_operation()
        try:
            result = await self._verify_with_adapter()
            error = None
        except Exception as exc:  # noqa: BLE001 -- recorded; every measure is then not measured
            result, error = {}, f"{type(exc).__name__}: {exc}"
        ctx.facts["during_verification"] = result if error is None else {"error": error}
        for name, key in self.MEASURES.items():
            if key in result and result[key] is not None:
                m[name] = result[key]
            else:
                m[name] = NOT_MEASURED
                ctx.not_measured[name] = (error or result.get(f"{key}_why")
                                          or f"the engine did not report {key}")
        if "kills_landed" in ctx.scenario.measure:
            m["kills_landed"] = ctx.kills_landed
        return m, None

    async def cleanup(self) -> None:
        ctx = self.ctx
        try:
            await ctx.adapter.abandon_background_operation()
            ctx.facts["scenario_objects_cleanup"] = await ctx.adapter.cleanup_scenario_objects()
        except Exception as exc:  # noqa: BLE001 -- the next run's init removes them too
            ctx.facts["scenario_objects_cleanup_error"] = f"{type(exc).__name__}: {exc}"


class _StartedOperation(_InterruptedOperation):
    """Started by the harness, injected only once the engine itself reports it in progress.
    Whether the kill really caught it is judged again after recovery (an index left
    INVALID, no bulk rows visible)."""

    async def _start(self) -> dict[str, Any]:
        raise NotImplementedError

    async def establish(self) -> tuple[dict[str, Any], Any]:
        ctx = self.ctx
        during_detail = await self._start()
        ctx.facts["during"] = {"operation": self.during, **during_detail}
        if not during_detail.get("in_progress"):
            await ctx.adapter.abandon_background_operation()
            raise PhaseAbort(f"{self.during} was not in progress at the fault: "
                         f"{during_detail.get('note', 'not confirmed')}")
        return {}, None


@register_during
class LargeTransaction(_StartedOperation):
    during = "large_transaction"
    MEASURES = {"large_txn_rows_visible": "rows_visible",
                "large_txn_parent_rows_visible": "parent_rows_visible",
                "fk_violations": "fk_violations"}

    async def _start(self) -> dict[str, Any]:
        return await self.ctx.adapter.start_large_transaction()

    async def _verify_with_adapter(self) -> dict[str, Any]:
        return await self.ctx.adapter.verify_large_transaction()


@register_during
class ConcurrentIndexBuild(_StartedOperation):
    during = "concurrent_index_build"
    MEASURES = {"index_left_invalid": "index_left_invalid",
                "table_readable": "table_readable",
                "rebuild_succeeds": "rebuild_succeeds"}

    async def create_objects(self) -> dict[str, Any]:
        return await self.ctx.adapter.prepare_index_build_objects()

    async def _start(self) -> dict[str, Any]:
        return await self.ctx.adapter.start_concurrent_index_build()

    async def _verify_with_adapter(self) -> dict[str, Any]:
        return await self.ctx.adapter.verify_concurrent_index()


@register_during
class AutovacuumWorker(_InterruptedOperation):
    during = "autovacuum_worker"
    MEASURES = {"autovacuum_worker_respawned": "autovacuum_worker_respawned",
                "relations_eligible": "relations_eligible",
                "relations_left_unvacuumed": "relations_left_unvacuumed"}

    async def create_objects(self) -> dict[str, Any]:
        return await self.ctx.adapter.prepare_autovacuum_objects()

    async def observe_settings(self) -> dict[str, str]:
        return await self.ctx.adapter.observe_autovacuum_settings()

    def check_settings(self, observed: dict[str, Any]) -> None:
        ctx = self.ctx
        off = [k for k in ("autovacuum", "track_counts") if observed.get(k) == "off"]
        if off:
            # no worker can ever appear: refuse rather than report a vacuum failure
            raise PhaseAbort(f"{', '.join(off)} is off in this deployment, so no autovacuum worker can run "
                         "(observed, not changed)")
        options = (ctx.facts.get("during_objects") or {}).get("table_options")
        ctx.disclosures.append(
            "Each kill of an autovacuum worker is a crash-restart of the whole instance: every "
            "session is terminated and crash recovery runs."
            + (f" The harness-owned table it vacuums carries its own storage options {options}, "
               "so its vacuum lasts long enough to be killed; no global setting is changed." if options else ""))

    async def establish(self) -> tuple[dict[str, Any], Any]:
        # Give autovacuum work AFTER the latest recovery, wait for a worker of this instance
        # to be vacuuming it, and hand the injector exactly that process (it re-checks the
        # parent and title before killing).
        ctx = self.ctx
        during_detail = await ctx.adapter.start_autovacuum_worker()
        ctx.facts["during"] = {"operation": self.during, **during_detail}
        ctx.facts.setdefault("during_attempts", []).append(during_detail)
        if not during_detail.get("in_progress"):
            raise PhaseAbort(f"{self.during} was not in progress at the fault: "
                         f"{during_detail.get('note', 'not confirmed')}")
        ctx.aim_fault_at(during_detail["kill_target"])
        return {}, None

    async def _verify_with_adapter(self) -> dict[str, Any]:
        return await self.ctx.adapter.verify_autovacuum_resumed()
