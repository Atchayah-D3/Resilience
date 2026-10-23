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

import asyncio
import time
from collections.abc import Callable
from dataclasses import asdict
from typing import TYPE_CHECKING, Any, ClassVar, Protocol

from resilience_tests.analysis.degradation import load_ratios
from resilience_tests.control.faults.context import RunContext

if TYPE_CHECKING:
    from resilience_tests.control.orchestrator import TestOrchestrator
    from resilience_tests.execution.workload.driver import MeasuredWindow


class PhaseAbort(RuntimeError):
    """The run cannot proceed for a reason that is a finding, not a harness defect."""


class Sampler(Protocol):
    """A background measurement started with the run's probes (the probers' own interface)."""

    def start(self) -> None: ...

    async def stop(self) -> None: ...


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
    # keys of the driver's injection detail kept in each cycle's row of a repeated scenario
    # (facts["cycles"]), as that cycle's evidence of the fault
    cycle_evidence_keys: ClassVar[tuple[str, ...]] = ()

    def __init__(self, ctx: RunContext) -> None:
        self.ctx = ctx

    # Every hook below defaults to doing nothing, so a behaviour overrides only what its
    # fault needs. In the order the phases call them:

    async def observe_settings(self) -> dict[str, str]:
        """Init: the engine settings that decide how this fault plays out, read through the
        adapter -- never written. Recorded and disclosed with the result."""
        return {}

    def phase_extension_s(self, phase: str) -> float:
        """Seconds this fault adds to `phase`'s profile bound -- the time it is held there, so a
        fault held longer than the bound is not cut off by it. The bound itself still comes
        from the profile."""
        return 0.0

    def samplers(self) -> list[Sampler]:
        """Extra background measurements for this fault (OS counters, cgroup files, ...). Built
        when the baseline starts, after the run's own probes, and stopped with them. Each
        records into the event stream (`self.ctx.stream`)."""
        return []

    async def baseline_started(self) -> None:
        """The baseline measurement window has just opened (snapshot counters here)."""

    async def baseline_ended(self) -> None:
        """The baseline measurement window has just closed."""

    async def before_inject(self) -> None:
        """Once per injection, before the fault (and before any `fault.during` operation is
        established) -- never between establishing that operation and the fault itself."""

    async def after_inject(self, detail: dict[str, Any]) -> None:
        """Right after the fault landed and T0 was recorded; `detail` is the driver's."""

    async def hold_and_release(self) -> dict[str, Any] | None:
        """Fault-inject phase, after the injection: a fault the harness holds for a set time and
        then removes itself does both here and sets T1 at the removal (see HeldFault). Returns
        the hold's detail, or None for a fault that is not held (the default)."""
        return None

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

    def recovery_soak_s(self, available_bound_s: float) -> float | None:
        """Recovery phase: a fixed time to keep observing the target after the fault instead of
        waiting for the return to SLO, or None to wait for the SLO (the default). For a fault
        whose recovery is the engine's own doing, such as a timeout ending a session (NL-M-05).
        Not to be confused with a held fault, which the harness itself releases in the
        fault-inject phase. `available_bound_s` is what the recovery phase allows."""
        return None

    def slo_clock_from_t1(self) -> bool:
        """Measure time back to SLO from T1 instead of T0. For a fault the harness holds and
        then releases, degradation during the hold is the point of the test, not slowness to
        recover from it."""
        return False

    def measure_client_impact(self, m: dict[str, Any]) -> None:
        """Measures read from the client-visible disturbance already in `m`."""

    async def before_integrity_check(self) -> None:
        """Observations that must be made before the whole-database integrity check."""

    async def measure(self, events: list[Any]) -> dict[str, Any]:
        """This fault's own measures, taken last in validate."""
        return {}

    @classmethod
    def summary_lines(cls, r: dict[str, Any]) -> list[str]:
        """Report: the lines this fault adds to summary.txt, built from the results file alone
        (`r`), and none for a run it does not concern. Default: none."""
        return []


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


class HeldFault(FaultBehaviour):
    """A fault held for `fault.duration` and then released by the harness: contention and
    pressure (Framework §10.6 NL-N recovery procedure: contention injected -> the database
    degrades without crashing -> contention clears -> performance returns to baseline, with no
    recovery action by anyone).

    The driver's `inject` applies the fault and returns at once (T0). Still in the fault-inject
    phase, `hold_and_release` holds it with the workload running, measured in its own exact
    window, then reverts it through the driver (T1 = released). The recovery phase then only
    watches the return to SLO from T1, so each phase's duration in the report means what its
    name says. The hold is added to the fault-inject phase's bound. Subclasses add snapshots in
    `on_hold_start` / `on_hold_end`.

    Measures produced for every held fault: oltp_tps_ratio and oltp_p99_ratio (load during
    the hold over the baseline, analysis/degradation.py)."""

    def __init__(self, ctx: RunContext) -> None:
        super().__init__(ctx)
        self._held: MeasuredWindow | None = None          # the load measured during the hold
        self._released: dict[str, Any] | None = None      # the hold's detail, once released

    def _hold_s(self) -> float | None:
        duration = self.ctx.scenario.fault.duration
        return None if duration == "permanent" else float(duration)

    def phase_extension_s(self, phase: str) -> float:
        return (self._hold_s() or 0.0) if phase == "fault_inject" else 0.0

    async def before_inject(self) -> None:
        # refused before anything is applied
        if self._hold_s() is None:
            # a held fault with no end would never clear
            raise PhaseAbort(f"{self.ctx.scenario.fault.type} is held and released by the harness, "
                             "so the scenario must set fault.duration (seconds)")
        if self.ctx.scenario.repeat is not None:
            raise PhaseAbort(f"{self.ctx.scenario.fault.type} is held and released by the harness; "
                             "repeating a held fault is not built yet")

    async def on_hold_start(self) -> None:
        """The fault is applied and the hold window has opened."""

    async def on_hold_end(self) -> None:
        """The hold is over; the fault is still applied (released right after this)."""

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
        await asyncio.sleep(hold_s)
        await self.on_hold_end()
        held = ctx.close_load_window()
        window.update(end_wall_s=time.time(), end_mono_ns=time.monotonic_ns(), load=asdict(held))
        self._held = held
        ctx.emit("hold_end", tps=held.tps, p99_ms=held.p99_ms)

        entry, released = await ctx.revert_fault()
        ctx.set_t1(time.monotonic_ns())
        ctx.mark_fault_reverted(entry, released)
        ctx.emit("t1", t1_mono_ns=ctx.t1_ns, action="fault released after hold")
        self._released = {"held_s": round((window["end_mono_ns"] - window["start_mono_ns"]) / 1e9, 3),
                          "released": released}
        return self._released

    async def begin_recovery(self) -> dict[str, Any]:
        """Released in the fault-inject phase (T1); recovery only watches from there."""
        if self._released is None:
            raise RuntimeError(f"{self.ctx.scenario.fault.type} reached recovery without being held and released")
        return dict(self._released)

    def slo_clock_from_t1(self) -> bool:
        return True

    async def measure(self, events: list[Any]) -> dict[str, Any]:
        ratios, why = load_ratios(self._held, self.ctx.baseline)
        self.ctx.not_measured.update(why)
        return ratios


class DuringBehaviour:
    """An operation the fault must land inside (`fault.during`): the harness establishes it
    and confirms it from the engine before injecting, then judges what it left behind."""

    during: ClassVar[str] = ""

    def __init__(self, ctx: RunContext) -> None:
        self.ctx = ctx

    async def prepare(self) -> None:
        """Init: create the harness-owned objects the operation works on."""
        self.ctx.facts["during_objects"] = await self.create_objects()

    async def create_objects(self) -> dict[str, Any]:
        """The objects this operation needs, created through the adapter. Default: none."""
        return {}

    async def observe_settings(self) -> dict[str, str]:
        """Init: the engine settings that decide whether the operation can run (read only)."""
        return {}

    def check_settings(self, observed: dict[str, Any]) -> None:
        """Init: refuse or disclose on the deployment settings observed for this fault."""

    async def establish(self) -> dict[str, Any]:
        """Bring the target into the state the fault must land inside, just before each
        injection attempt. Returns evidence of that state to record with the injection's own
        detail (e.g. {"checkpoint": ...}), or {} for none."""
        return {}

    async def after_inject(self, detail: dict[str, Any]) -> None:
        """Right after each injection that landed; `detail` is the driver's."""

    async def verify(self, events: list[Any]) -> tuple[dict[str, Any], str | None]:
        """Validate: (measures, reason the fault did not land inside the operation or None)."""
        return {}, None

    async def cleanup(self) -> None:
        """Cleanup, after the revert: remove what the operation left behind."""

    @classmethod
    def summary_lines(cls, r: dict[str, Any]) -> list[str]:
        """Report: the lines this operation adds to summary.txt, built from the results file alone
        (`r`), and none for a run it does not concern. Default: none."""
        return []


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


def has_fault_behaviour(fault_type: str) -> bool:
    """Whether a behaviour is registered for this fault type. A scenario whose fault type has
    none is not built: the run plan skips it and init refuses it (control/matrix.py)."""
    return fault_type in _FAULTS


def fault_behaviour(run: TestOrchestrator) -> FaultBehaviour:
    """The behaviour for the run's fault type. When none is registered, the defaults -- so the
    run can still be built and its init phase refuse it, with the reason, before anything is
    applied (see has_fault_behaviour)."""
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


def summary_sections() -> list[Callable[[dict[str, Any]], list[str]]]:
    """The summary.txt sections that belong to one fault or `during` operation: each registered
    class's own `summary_lines`, the faults' first. Asked for when a report is written, so the
    report names no fault and does not depend on which modules happen to be imported. Every
    section returns no lines for a run it does not concern."""
    defaults = (FaultBehaviour.summary_lines.__func__, DuringBehaviour.summary_lines.__func__)
    sections: list[Callable[[dict[str, Any]], list[str]]] = []
    for cls in [*_FAULTS.values(), *_DURING.values()]:
        section = cls.summary_lines.__func__
        if section not in defaults and all(s.__func__ is not section for s in sections):
            sections.append(cls.summary_lines)
    return sections
