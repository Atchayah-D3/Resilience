"""What the run does differently for one fault type, or for one `fault.during` operation.

The orchestrator runs the same nine phases for every scenario. Where a fault needs more --
its own recovery action, its own measures, an operation the fault must land inside -- the
orchestrator calls a hook on the behaviour registered for that fault type (or `during`
value), and never compares a fault type itself. A new fault type is added by registering a
new behaviour, without editing the orchestrator.

A behaviour is built once per run and reads and writes the run's own state (facts,
measures, disclosures) through `self.run`.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, ClassVar

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

    def __init__(self, run: TestOrchestrator) -> None:
        self.run = run

    async def begin_recovery(self) -> dict[str, Any]:
        """Set T1 (the moment recovery may begin) and return the recovery phase's detail."""
        run = self.run
        if not self.recovers_unattended:
            raise NotImplementedError(f"recovery for {run.scenario.fault.type}/{run.scenario.fault.duration} not built yet")
        # No harness action: the service comes back on its own (restart policy, or the
        # restart command itself). T1 = T0, so rto_first_write_s and "starts unattended"
        # are measured from the fault. The ledger entry is deliberately left outstanding:
        # cleanup's revert confirms the service is running (and, for a reload, restores the
        # parameter), and a harness crash before then leaves it to the kill switch.
        assert run.stream is not None
        run.t1_ns = run.t0_ns
        run.stream.emit("orchestrator", "t1", t1_mono_ns=run.t1_ns, action="unattended recovery expected")
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
        run = self.run
        assert run.injector is not None and run.stream is not None
        entry = next(e for e in run.ledger.outstanding() if e.injection_id == run.facts["injection_id"])
        power_on = await run.injector.revert(run.node, entry.detail)
        run.t1_ns = time.monotonic_ns()
        run.ledger.transition(entry, "reverted", revert=power_on, by="orchestrator")
        run.stream.emit("orchestrator", "t1", t1_mono_ns=run.t1_ns, action="power restored")
        return {"power_on": power_on}


class DuringBehaviour:
    """An operation the fault must land inside (`fault.during`): the harness establishes it
    and confirms it from the engine before injecting, then judges what it left behind."""

    during: ClassVar[str] = ""

    def __init__(self, run: TestOrchestrator) -> None:
        self.run = run

    async def prepare(self) -> None:
        """Init: create the harness-owned objects the operation works on."""
        run = self.run
        run.facts["during_objects"] = await run.adapter.prepare_scenario_objects(self.during)

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


def register_fault(cls: type[FaultBehaviour]) -> type[FaultBehaviour]:
    _FAULTS[cls.fault_type] = cls
    return cls


def register_during(cls: type[DuringBehaviour]) -> type[DuringBehaviour]:
    _DURING[cls.during] = cls
    return cls


def fault_behaviour(run: TestOrchestrator) -> FaultBehaviour:
    """The behaviour for the run's fault type; the defaults when none is registered."""
    return _FAULTS.get(run.scenario.fault.type, FaultBehaviour)(run)


def during_behaviour(run: TestOrchestrator) -> DuringBehaviour | None:
    during = run.scenario.fault.during
    return None if during is None else _DURING[during](run)
