"""What a fault or `during` behaviour may see and do in its run.

Behaviours are given this view instead of the orchestrator itself, so they depend on a short,
named list -- the scenario, the database adapter, the run's records, T0 and a few actions on
the fault -- and not on how the orchestrator keeps its state. It is a live view: behaviours
are built when the run is, before the injector, the event stream and T0 exist, so every
attribute is read from the run at the moment it is used.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from catalog.schema import Scenario
    from resilience_tests.adapters.base import BaseDatabaseAdapter
    from resilience_tests.control.orchestrator import TestOrchestrator


class RunContext:
    def __init__(self, run: TestOrchestrator) -> None:
        self._run = run

    # ------------------------------------------------------------------ what the run is

    @property
    def scenario(self) -> Scenario:
        return self._run.scenario

    @property
    def adapter(self) -> BaseDatabaseAdapter:
        return self._run.adapter

    def phase_timeout_s(self, phase: str) -> float:
        """The profile's time bound for one phase."""
        return self._run.profile.phase_timeouts_s[phase]

    # ------------------------------------------------------------------ the run's records

    @property
    def facts(self) -> dict[str, Any]:
        """Observations kept in results.json; analysis and the report read them by key."""
        return self._run.facts

    @property
    def not_measured(self) -> dict[str, str]:
        """measure name -> why it could not be taken."""
        return self._run.not_measured

    @property
    def disclosures(self) -> list[str]:
        """Limitations of this run, printed with its verdict."""
        return self._run.disclosures

    @property
    def kills_landed(self) -> int:
        return self._run.kills_landed

    # ------------------------------------------------------------------ the timeline

    @property
    def t0_ns(self) -> int | None:
        """Monotonic time the fault was injected (None before it is)."""
        return self._run.t0_ns

    @property
    def t1_ns(self) -> int | None:
        return self._run.t1_ns

    def set_t1(self, t1_ns: int) -> None:
        """T1: the moment recovery may begin (Arch §7.2)."""
        self._run.t1_ns = t1_ns

    def events(self) -> list[Any]:
        """Every event recorded so far, the server's log lines included."""
        assert self._run.stream is not None
        return self._run.stream.events()

    def emit(self, kind: str, **data: Any) -> None:
        assert self._run.stream is not None
        self._run.stream.emit("orchestrator", kind, **data)

    # ------------------------------------------------------------------ the fault itself

    async def arm_fault(self) -> None:
        """See FaultInjector.arm."""
        assert self._run.injector is not None
        await self._run.injector.arm(self._run.node)

    async def disarm_fault(self) -> None:
        assert self._run.injector is not None
        await self._run.injector.disarm()

    def aim_fault_at(self, target: dict[str, Any]) -> None:
        """A single process the fault must hit instead of the whole service (FaultInjector.kill_target)."""
        assert self._run.injector is not None
        self._run.injector.kill_target = target

    async def revert_fault(self) -> tuple[Any, dict[str, Any]]:
        """Undo this run's injection now (its ledger entry stays outstanding until
        `mark_fault_reverted`). Returns (ledger entry, the driver's revert detail)."""
        run = self._run
        assert run.injector is not None
        entry = next(e for e in run.ledger.outstanding() if e.injection_id == run.facts["injection_id"])
        return entry, await run.injector.revert(run.node, entry.detail)

    def mark_fault_reverted(self, entry: Any, revert: dict[str, Any]) -> None:
        self._run.ledger.transition(entry, "reverted", revert=revert, by="orchestrator")
