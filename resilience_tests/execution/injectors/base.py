"""FaultInjector interface and driver resolution (Arch §5).

One FaultInjector interface, several drivers. The scenario names a fault type; the
environment profile decides which driver serves it -- this is what lets the same scenario
run on bare metal and on a cloud VM without edit.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import Any, ClassVar

from catalog.schema import DEFAULT_DRIVER_SECTION, FAULT_DRIVER_SECTION, Fault
from resilience_tests.adapters.base import BaseDatabaseAdapter
from resilience_tests.control.profile import EnvProfile, Node


class DriverNotAvailable(RuntimeError):
    """The profile resolves the fault to a driver that is not configured or not built.
    Always fails closed -- a scenario never silently runs without its fault."""


class FaultNotLanded(RuntimeError):
    """The fault was attempted and the target did not end up in the faulted state. The run
    is aborted -- a statement about the run, not the database. `detail` is what was observed."""

    def __init__(self, message: str, detail: Mapping[str, Any] | None = None) -> None:
        super().__init__(message)
        self.detail = dict(detail or {})


class FaultInjector(ABC):
    """A driver for one or more fault types.

    Timing contract (Arch §7.2): `inject` returns at T0 -- when the injection call returns,
    or when the control plane confirms the fault (e.g. power off).
    """

    fault_types: ClassVar[frozenset[str]]
    driver_name: ClassVar[str]

    def __init__(self, profile: EnvProfile, fault_type: str) -> None:
        self.profile = profile
        self.fault_type = fault_type
        # The run's database adapter. Faults that live inside the database (a held session,
        # a connection flood) are applied and reverted through it, so the session the fault
        # opened is the same one the run observes and cleanup ends. Set by the orchestrator
        # and by the kill switch; None when no adapter was handed over.
        self.adapter: BaseDatabaseAdapter | None = None
        # (cycles, interval_s) when the scenario repeats its fault, so preflight can check the
        # target can actually take that cadence (Framework NL-C-05)
        self.repeat_plan: tuple[int, float] | None = None
        # the scenario's fault.duration in seconds, for faults that are held and then released
        # (None for a permanent fault)
        self.duration_s: float | None = None
        # A single process the fault must hit instead of the whole service, found by the
        # adapter: {"pid", "parent_pid", "title_marker"}. Generic -- the driver re-checks
        # parent and title before acting. Set per attempt by the orchestrator, cleared after.
        self.kill_target: Mapping[str, Any] | None = None

    async def confirm(self, node: Node, detail: Mapping[str, Any]) -> dict[str, Any]:
        """After recovery: independent evidence that the fault took effect, beyond the
        injection call having returned. {"fault_confirmed": True/False/None, ...}; None means
        this driver cannot tell, which is reported as not measured -- never as confirmed.
        `detail` is the injection's ledger detail (`preflight` and `inject`)."""
        return {"fault_confirmed": None, "note": f"{self.driver_name} has no confirmation for {self.fault_type}"}

    @abstractmethod
    async def preflight(self, node: Node) -> dict[str, Any]:
        """Verify the driver can act on `node` right now. Raise DriverNotAvailable if not."""

    @abstractmethod
    async def inject(self, node: Node) -> dict[str, Any]:
        """Apply the fault. Returns driver detail recorded in the ledger and event stream."""

    async def arm(self, node: Node) -> None:
        """Do everything `inject` needs EXCEPT the fault itself, so that a later `inject` is a
        single action. Used when the fault must land inside a short window the harness has
        just observed (NL-C-02: while a checkpoint is running). Default: nothing to prepare."""

    async def disarm(self) -> None:
        """Release what `arm` prepared, when the fault will not be injected after all.
        Idempotent. Default: nothing to release."""

    @abstractmethod
    async def revert(self, node: Node, detail: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Undo the fault (for power loss: power on). Must be idempotent -- the kill switch
        may call it for an injection whose state is unknown.

        `detail` is the injection's ledger detail: `preflight` (what preflight observed before
        the fault) and, once applied, `inject`. It may be partial or empty -- the harness can
        die between journalling the intent and applying the fault."""


_REGISTRY: dict[tuple[str, str], type[FaultInjector]] = {}


def register(section: str, cls: type[FaultInjector]) -> type[FaultInjector]:
    _REGISTRY[(section, cls.driver_name)] = cls
    return cls


def driver_for(fault_type: str, profile: EnvProfile) -> tuple[str, str]:
    """(profile section, driver name) serving `fault_type` in this environment."""
    section = FAULT_DRIVER_SECTION.get(fault_type, DEFAULT_DRIVER_SECTION)
    config = getattr(profile, section, None)
    driver = getattr(config, "driver", None)
    if driver is None:
        raise DriverNotAvailable(f"profile {profile.name} has no driver for section {section!r} ({fault_type})")
    return section, driver


def resolve(fault: Fault, profile: EnvProfile) -> FaultInjector:
    section, driver = driver_for(fault.type, profile)
    cls = _REGISTRY.get((section, driver))
    if cls is None or fault.type not in cls.fault_types:
        raise DriverNotAvailable(
            f"{section}.driver={driver!r} is not built for fault type {fault.type!r} "
            "(Arch §16: Phase 1 builds one power driver per environment class as needed)"
        )
    return cls(profile, fault.type)


def resolve_by_name(section: str, driver: str, profile: EnvProfile, fault_type: str = "") -> FaultInjector:
    """Used by the kill switch, which works from ledger entries rather than scenarios."""
    cls = _REGISTRY.get((section, driver))
    if cls is None:
        raise DriverNotAvailable(f"no driver {section}/{driver}")
    return cls(profile, fault_type)
