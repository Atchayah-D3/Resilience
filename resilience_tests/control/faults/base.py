"""What the run does differently for one fault type, or for one `fault.during` operation.

The orchestrator runs the same nine phases for every scenario. Where a fault needs more --
its own recovery action, its own measures, an operation the fault must land inside -- the
orchestrator calls a hook on the behaviour registered for that fault type (or `during`
value), and never compares a fault type itself. A new fault type is added by registering a
new behaviour, without editing the orchestrator.

A behaviour is built once per run and sees the run only through `self.ctx`, a RunContext
(context.py): the scenario, the adapter, the run's records and a few actions on the fault.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, ClassVar

from resilience_tests.control.faults.context import RunContext

if TYPE_CHECKING:
    from resilience_tests.control.orchestrator import TestOrchestrator


class PhaseAbort(RuntimeError):
    """The run cannot proceed for a reason that is a finding, not a harness defect."""


class FaultBehaviour:
    """Defaults: a fault that may or may not interrupt service, and whose recovery the harness
    has not been built to drive -- the recovery phase then refuses rather than guessing."""

    fault_type: ClassVar[str] = ""
    # facts key the injection detail is kept under, where analysis and the report look for it
    fact_key: ClassVar[str | None] = None
    # the fault must interrupt writes; if the probes never see an outage, the availability
    # gap is not measured rather than reported as ~0 s
    expects_outage: ClassVar[bool] = False
    # the service recovers on its own; the ledger entry stays outstanding until cleanup's
    # revert has confirmed the node is back in its pre-fault state
    recovers_unattended: ClassVar[bool] = False

    def __init__(self, ctx: RunContext) -> None:
        self.ctx = ctx

    async def begin_recovery(self) -> dict[str, Any]:
        """Set T1 (the moment recovery may begin) and return the recovery phase's detail."""
        ctx = self.ctx
        if not self.recovers_unattended:
            raise NotImplementedError(f"recovery for {ctx.scenario.fault.type}/{ctx.scenario.fault.duration} not built yet")
        # No harness action: the service comes back on its own (restart policy, or the
        # restart command itself). T1 = T0, so rto_first_write_s and "starts unattended"
        # are measured from the fault. The ledger entry is deliberately left outstanding:
        # cleanup's revert confirms the service is running (and, for a reload, restores the
        # parameter), and a harness crash before then leaves it to the kill switch.
        ctx.set_t1(ctx.t0_ns)
        ctx.emit("t1", t1_mono_ns=ctx.t1_ns, action="unattended recovery expected")
        return {"recovery": "unattended (no harness action)"}

    def recovery_hold_s(self, available_bound_s: float) -> float | None:
        """A fixed time to observe after the fault instead of waiting for the return to SLO,
        or None to wait for the SLO. `available_bound_s` is what the recovery phase allows."""
        return None

    def measure_client_impact(self, m: dict[str, Any]) -> None:
        """Measures read from the client-visible disturbance already in `m`."""

    async def before_integrity_check(self) -> None:
        """Observations that must be made before the whole-database integrity check."""

    async def measure(self, events: list[Any]) -> dict[str, Any]:
        """This fault's own measures, taken last in validate."""
        return {}


class PowerRestoredRecovery(FaultBehaviour):
    """The harness restores power and nothing else; the database must start unattended
    (Framework §10.1). T1 = power restored."""

    async def begin_recovery(self) -> dict[str, Any]:
        ctx = self.ctx
        entry, power_on = await ctx.revert_fault()
        ctx.set_t1(time.monotonic_ns())
        ctx.mark_fault_reverted(entry, power_on)
        ctx.emit("t1", t1_mono_ns=ctx.t1_ns, action="power restored")
        return {"power_on": power_on}


class DuringBehaviour:
    """An operation the fault must land inside (`fault.during`): the harness establishes it
    and confirms it from the engine before injecting, then judges what it left behind."""

    during: ClassVar[str] = ""

    def __init__(self, ctx: RunContext) -> None:
        self.ctx = ctx

    async def prepare(self) -> None:
        """Init: create the harness-owned objects the operation works on."""
        ctx = self.ctx
        ctx.facts["during_objects"] = await ctx.adapter.prepare_scenario_objects(self.during)

    def check_settings(self, observed: dict[str, Any]) -> None:
        """Init: refuse or disclose on the deployment settings observed for this fault."""

    async def establish(self) -> tuple[dict[str, Any], Any]:
        """Bring the target into the state the fault must land inside. Returns (checkpoint
        evidence, running index build) -- either empty."""
        return {}, None

    async def verify(self, events: list[Any]) -> tuple[dict[str, Any], str | None]:
        """Validate: (measures, reason the fault did not land inside the operation or None)."""
        return {}, None

    async def cleanup(self) -> None:
        """Cleanup, after the revert: remove what the operation left behind."""


_FAULTS: dict[str, type[FaultBehaviour]] = {}
_DURING: dict[str, type[DuringBehaviour]] = {}


def _register(registry: dict[str, Any], kind: str, name: str, cls: type) -> None:
    """Refuse a nameless or second registration: silently replacing a behaviour would change
    what a scenario runs depending on import order."""
    if not name:
        raise ValueError(f"{cls.__qualname__} does not set its {kind}")
    existing = registry.get(name)
    if existing is not None and existing is not cls:
        raise ValueError(f"{kind} {name!r} is registered twice: {existing.__module__}.{existing.__qualname__} "
                         f"and {cls.__module__}.{cls.__qualname__}")
    registry[name] = cls


def register_fault(cls: type[FaultBehaviour]) -> type[FaultBehaviour]:
    _register(_FAULTS, "fault_type", cls.fault_type, cls)
    return cls


def register_during(cls: type[DuringBehaviour]) -> type[DuringBehaviour]:
    _register(_DURING, "during", cls.during, cls)
    return cls


def fault_behaviour(run: TestOrchestrator) -> FaultBehaviour:
    """The behaviour for the run's fault type; the defaults when none is registered."""
    return _FAULTS.get(run.scenario.fault.type, FaultBehaviour)(RunContext(run))


def during_behaviour(run: TestOrchestrator) -> DuringBehaviour | None:
    during = run.scenario.fault.during
    if during is None:
        return None
    cls = _DURING.get(during)
    if cls is None:
        # the catalog schema only accepts FaultDuring values, so this is a value added there
        # without a behaviour in control/faults/during.py
        raise NotImplementedError(f"no behaviour is registered for fault.during {during!r} "
                                  f"(registered: {', '.join(sorted(_DURING))})")
    return cls(RunContext(run))
