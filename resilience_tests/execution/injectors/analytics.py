"""OS/SSH-section driver for noisy analytics (fault type analytics_load, NL-N-I01).

The fault lives inside the database, so it is applied and reverted through the run's database
adapter, as the environment's analytics login (node.analytics), and its sessions carry an
application_name, so the kill switch can end them from a fresh process after a harness crash.
The catalog's fault.analytics_load states the rules; the numbers come from the target:

  sessions   full-table sessions: ceil(cores / 3) (`by_cores`) -- one session and its two helpers per
             three cores -- at least 2 with the limit tests; plus the index sessions
  warehouses the index reports read the warehouses the workload does not use (the TPC-C layout the
             workload handed to the adapter)

  preflight  the service is running, the login is named, no report session of an earlier run is
             left, the plan can be made
  inject     the adapter opens the sessions and starts their queries
  confirm    from the adapter's 1-second samples: at minute 1 every report session active and at
             least one helper working for a report (document check 1)
  revert     every report query cancelled, every session closed, every tagged session ended on the
             server. Idempotent.
"""

from __future__ import annotations

import math
import shlex
import time
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Any

from catalog.schema import AnalyticsLoad, Fault, Repeat
from resilience_tests.adapters.base import BaseDatabaseAdapter
from resilience_tests.control.profile import EnvProfile, Node
from resilience_tests.execution.injectors.base import DriverNotAvailable, FaultInjector, FaultNotLanded, register
from resilience_tests.execution.injectors.os_ssh import SSH_TIMEOUT_S
from resilience_tests.execution.remote import RemoteHost

q = shlex.quote
CORES_PER_FULL_TABLE_SESSION = 3    # one report process and its two helpers
CONFIRM_AT_S = 60.0                 # document check 1: the reports are seen running at minute 1


@dataclass(frozen=True)
class SessionPlan:
    full_table: int
    index: int
    limit_tests: bool
    cores: int | None

    @property
    def total(self) -> int:
        return self.full_table + self.index

    def report_processes(self, helpers_per_report: int) -> int:
        """Processes the reports can keep busy: each full-table report and its helpers, plus the
        index sessions (their small reads run without helpers)."""
        return self.full_table * (1 + helpers_per_report) + self.index

    def as_dict(self) -> dict[str, Any]:
        return {**asdict(self), "total": self.total}


def session_plan(plan: AnalyticsLoad, cores: int | None) -> SessionPlan:
    """The report sessions for this target (see the module docstring)."""
    if plan.full_table_sessions == "by_cores":
        if not cores:
            raise DriverNotAvailable("analytics_load: the target's CPU cores could not be read, so the number of "
                                     "report sessions (by_cores) cannot be set")
        full = math.ceil(cores / CORES_PER_FULL_TABLE_SESSION)
    else:
        full = int(plan.full_table_sessions)
    if plan.limit_tests:
        full = max(full, 2)            # one full-table session per limit test
    return SessionPlan(full_table=full, index=plan.index_sessions, limit_tests=plan.limit_tests, cores=cores)


def minute_one(samples: Sequence[Mapping[str, Any]], started_ns: int, sessions: int,
               at_s: float = CONFIRM_AT_S) -> tuple[bool | None, Mapping[str, Any] | None]:
    """Document check 1 from the 1-second samples: the first sample at or after `at_s` after the
    start shows every report session active and at least one helper working for a report.
    (None, None) when no sample was taken that late -- not measured, never confirmed."""
    due = started_ns + int(at_s * 1e9)
    late = [s for s in samples if int(s.get("t_mono_ns", 0)) >= due]
    if not late:
        return None, None
    sample = min(late, key=lambda s: int(s["t_mono_ns"]))
    ok = int(sample.get("sessions_active") or 0) >= sessions and int(sample.get("helpers_total") or 0) >= 1
    return ok, sample


def as_int(value: Any) -> int | None:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


class OsSshAnalyticsDriver(FaultInjector):
    fault_types = frozenset({"analytics_load"})
    driver_name = "os_ssh"

    def __init__(self, profile: EnvProfile, fault_type: str) -> None:
        super().__init__(profile, fault_type)
        self.plan: AnalyticsLoad | None = None      # the catalog's fault.analytics_load
        self.sessions: SessionPlan | None = None

    def configure(self, fault: Fault, repeat: Repeat | None, adapter: BaseDatabaseAdapter | None) -> None:
        super().configure(fault, repeat, adapter)
        self.plan = fault.analytics_load

    async def preflight(self, node: Node) -> dict[str, Any]:
        if self.plan is None:
            raise DriverNotAvailable("analytics_load needs the scenario's fault.analytics_load")
        if node.analytics is None:
            raise DriverNotAvailable(f"{node.name}: the environment names no analytics login (node.analytics)")
        async with RemoteHost(node.ssh) as host:
            active = (await host.run(f"systemctl is-active {q(node.service)}",
                                     timeout_s=SSH_TIMEOUT_S, check=False)).stdout.strip()
        if active != "active":
            raise DriverNotAvailable(f"{node.name}: service {node.service} is {active!r}")
        adapter = self._database_adapter(node)
        left = await adapter.analytics_leftovers()
        if left.get("sessions"):
            raise DriverNotAvailable(f"{left['sessions']} report session(s) of an earlier run are still open")
        settings = await adapter.observe_analytics_settings()
        self.sessions = session_plan(self.plan, as_int(settings.get("cpu_cores")))
        layout = adapter.tpcc_layout() or {}
        extra = layout.get("extra_warehouses")
        if self.plan.index_sessions and not extra:
            raise DriverNotAvailable("analytics_load: index reports read the warehouses the workload does not use, "
                                     "and the TPC-C layout has none")
        return {"service": node.service, "login": node.analytics.user, "plan": self.plan.model_dump(),
                "sessions": self.sessions.as_dict(), "extra_warehouses": extra}

    async def inject(self, node: Node) -> dict[str, Any]:
        if self.plan is None or self.sessions is None:
            raise DriverNotAvailable("analytics_load: preflight must run first")
        layout = self._database_adapter(node).tpcc_layout() or {}
        extra = layout.get("extra_warehouses")
        tenant: dict[str, Any] = {}        # run 1b only: an engine without it is never asked
        if self.plan.session_overrides:
            tenant["session_overrides"] = dict(self.plan.session_overrides)
        if self.plan.hard_limit_attempts:
            tenant["hard_limit_attempts"] = dict(self.plan.hard_limit_attempts)
        detail = dict(await self._database_adapter(node).start_analytics(
            full_table_sessions=self.sessions.full_table, index_sessions=self.sessions.index,
            limit_tests=self.sessions.limit_tests, extra_warehouses=tuple(extra) if extra else None, **tenant))
        if not detail.get("supported"):
            raise FaultNotLanded(f"reports not started: {detail.get('error')}", detail)
        detail.setdefault("t0_mono_ns", time.monotonic_ns())
        detail["action"] = "analytics_load"
        return detail

    async def confirm(self, node: Node, detail: Mapping[str, Any]) -> dict[str, Any]:
        state = await self._database_adapter(node).analytics_state()
        sessions = len(state.get("sessions") or [])
        if not state.get("started") or not sessions:
            return {"fault_confirmed": None, "note": "the reports were never started in this process"}
        ok, sample = minute_one(state.get("samples") or [], int(state["started_mono_ns"]), sessions, at_s=CONFIRM_AT_S)
        out: dict[str, Any] = {"fault_confirmed": ok, "sessions": sessions, "sample_at_minute_1": dict(sample or {})}
        if ok is None:
            out["note"] = f"no activity sample was taken {CONFIRM_AT_S:.0f} s or later after the reports started"
        return out

    async def revert(self, node: Node, detail: Mapping[str, Any] | None = None) -> dict[str, Any]:
        return await self._database_adapter(node).revert_analytics()


register("os_ssh", OsSshAnalyticsDriver)
