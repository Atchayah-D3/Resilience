"""Workload profiles: one class per transaction shape, registered by name (TPC-C build step 3).

The same pattern as the fault behaviours (control/faults/base.py, @register_fault): a new
workload is a new class with @register_profile; the engine and the orchestrator do not change.

    name                      the scenario file's workload.profile value, e.g. "tpcc"
    built                     False while the profile is still being built: the run plan skips a
                              scenario that uses it and init refuses it, before anything is applied
    required_capabilities()   engine capabilities it needs: an engine without them is skipped
    prepare(adapter)          init: the profile's data is present, the right size and consistent
    terminal(worker_id)       one clerk: its group and home (e.g. its TPC-C warehouse)
    next_request(terminal)    pure: the next transaction's type, inputs and whether it needs a marker
    execute(session, ...)     one call through the adapter; returns how it ended
    consistency_checks(...)   validate: profile-specific checks after recovery -> measure name: value

The custom workload (marker, churn, list_append) is NOT moved behind this interface: TPC-C is
added beside it (add-only, docs/tpcc-workload-proposal.md 7.2). Its hooks are hooks.NoProfileHooks.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar

from catalog.schema import Workload
from resilience_tests.adapters.base import Capability


@dataclass(frozen=True)
class Terminal:
    """One clerk of a profile: which worker it is, its group, and its home (profile-specific)."""

    worker_id: int
    group: str
    home: int | None = None          # TPC-C: the home warehouse
    district: int | None = None      # TPC-C: the clerk's fixed district for Stock-Level (Clause 2.8.1.1)


@dataclass(frozen=True)
class Request:
    """The next transaction of one clerk, chosen by the profile (pure, no I/O)."""

    txn_type: str                    # contract: e.g. new_order, payment, ...
    inputs: dict[str, Any] = field(default_factory=dict)
    needs_marker: bool = True        # read-only transactions write no marker and no journal line


@dataclass(frozen=True)
class Result:
    """How one transaction ended (contract outcomes) and what the journal must record."""

    outcome: str                     # committed / definitely_aborted / business_rollback / unknown
    journal: dict[str, Any] = field(default_factory=dict)   # e.g. New-Order: w, d, o_id, ol_cnt
    sqlstate: str | None = None      # the server's error code when it rolled the transaction back


class WorkloadProfile(ABC):
    name: ClassVar[str] = ""
    built: ClassVar[bool] = True
    # False while the profile's checks after a fault are not built: only dry runs
    # (--stop-before-fault) are allowed, so no fault is injected that could not be judged.
    ready_for_faults: ClassVar[bool] = True

    def __init__(self, workload: Workload) -> None:
        self.workload = workload

    def required_capabilities(self) -> set[Capability]:
        return set()

    async def prepare(self, adapter: Any) -> dict[str, Any]:
        return {}

    @abstractmethod
    def terminal(self, worker_id: int) -> Terminal: ...

    @abstractmethod
    def next_request(self, terminal: Terminal) -> Request: ...

    @abstractmethod
    async def execute(self, session: Any, request: Request, marker: tuple[int, str] | None) -> Result: ...

    async def consistency_checks(self, adapter: Any, run_dir: Path) -> dict[str, Any]:
        return {}

    def make_driver(self, adapter: Any, journals: Any, stream: Any) -> Any:
        """The engine that runs this profile's clerks (contract.WorkloadDriverContract)."""
        raise NotImplementedError(f"workload profile {self.name!r} has no engine")


# --- registry ----------------------------------------------------------------------------

_PROFILES: dict[str, type[WorkloadProfile]] = {}


def register_profile(cls: type[WorkloadProfile]) -> type[WorkloadProfile]:
    """Register a profile under its name. A nameless or second registration is refused: silently
    replacing a profile would change what a scenario runs depending on import order."""
    if not cls.name:
        raise ValueError(f"{cls.__qualname__} does not set its name")
    existing = _PROFILES.get(cls.name)
    if existing is not None and existing is not cls:
        raise ValueError(f"workload profile {cls.name!r} is registered twice: "
                         f"{existing.__module__}.{existing.__qualname__} and {cls.__module__}.{cls.__qualname__}")
    _PROFILES[cls.name] = cls
    return cls


def profile_class(name: str) -> type[WorkloadProfile] | None:
    """The registered profile of this name, or None (the custom workload's names are not registered)."""
    return _PROFILES.get(name)


def registered_profiles() -> dict[str, type[WorkloadProfile]]:
    return dict(_PROFILES)
