"""OS/SSH-section driver for a lock pile-up behind a schema change (fault type lock_pileup,
NL-N-I03).

The fault lives inside the database, so it is applied and reverted through the run's database
adapter -- the sessions it opens are the ones the run observes and cleanup ends -- and its
sessions carry an application_name, so the kill switch can end them from a fresh process after
a harness crash. The catalog's fault.lock_pileup says what holds the table, which schema change
queues behind it and the schema change's own lock timeout.

  preflight  the service is running, nothing of an earlier pile-up is left on the server
  inject     the adapter starts the pile-up and returns once the server shows it running
  confirm    the blocking chain the adapter saw from the server: the schema change held up
             (by the read, or holding its lock) AND a workload session queued behind it
  revert     every tagged session ended on the server; the probe columns the schema change
             added are dropped. Idempotent.

The pile-up is released by the fault's behaviour (control/faults/lock_pileup.py) at the moment
its queue clears; the ledger entry stays outstanding until cleanup's revert has removed what
the schema change left, after the workload has stopped.
"""

from __future__ import annotations

import shlex
import time
from collections.abc import Mapping
from typing import Any

from catalog.schema import Fault, LockPileup, Repeat
from resilience_tests.adapters.base import BaseDatabaseAdapter
from resilience_tests.control.profile import EnvProfile, Node
from resilience_tests.execution.injectors.base import DriverNotAvailable, FaultInjector, FaultNotLanded, register
from resilience_tests.execution.injectors.os_ssh import SSH_TIMEOUT_S
from resilience_tests.execution.remote import RemoteHost

q = shlex.quote


class OsSshLockPileupDriver(FaultInjector):
    fault_types = frozenset({"lock_pileup"})
    driver_name = "os_ssh"

    def __init__(self, profile: EnvProfile, fault_type: str) -> None:
        super().__init__(profile, fault_type)
        self.plan: LockPileup | None = None   # the catalog's fault.lock_pileup

    def configure(self, fault: Fault, repeat: Repeat | None, adapter: BaseDatabaseAdapter | None) -> None:
        super().configure(fault, repeat, adapter)
        self.plan = fault.lock_pileup

    async def preflight(self, node: Node) -> dict[str, Any]:
        if self.plan is None:
            raise DriverNotAvailable("lock_pileup needs the scenario's fault.lock_pileup")
        async with RemoteHost(node.ssh) as host:
            active = (await host.run(f"systemctl is-active {q(node.service)}",
                                     timeout_s=SSH_TIMEOUT_S, check=False)).stdout.strip()
        if active != "active":
            raise DriverNotAvailable(f"{node.name}: service {node.service} is {active!r}")
        # a pile-up left by a crashed run would hold the table before this one starts
        leftover = await self._database_adapter(node).lock_pileup_state()
        if leftover.get("started") and leftover.get("ddl_outcome") is None:
            raise DriverNotAvailable("a lock pile-up of this adapter is still running")
        return {"service": node.service, "plan": self.plan.model_dump()}

    async def inject(self, node: Node) -> dict[str, Any]:
        """Started through the run's adapter, confirmed from the server or not landed."""
        if self.plan is None:
            raise DriverNotAvailable("lock_pileup needs the scenario's fault.lock_pileup")
        detail = dict(await self._database_adapter(node).start_lock_pileup(
            blocker=self.plan.blocker, ddl=self.plan.ddl,
            ddl_lock_timeout_s=self.plan.ddl_lock_timeout_s, ddl_after_s=self.plan.ddl_after_s,
            table=self.plan.table))
        if not detail.get("supported"):
            raise FaultNotLanded(f"lock pile-up not started: {detail.get('error')}", detail)
        detail.setdefault("t0_mono_ns", time.monotonic_ns())
        detail["action"] = "lock_pileup"
        return detail

    async def confirm(self, node: Node, detail: Mapping[str, Any]) -> dict[str, Any]:
        """The blocking chain as the adapter saw it from the server while the schema change was
        unfinished. None (not measured) when the schema change was never issued."""
        state = await self._database_adapter(node).lock_pileup_state()
        confirmed = state.get("chain_confirmed")
        out: dict[str, Any] = {"fault_confirmed": confirmed if isinstance(confirmed, bool) else None,
                               "chain": state.get("chain"), "queue_max": state.get("queue_max")}
        if out["fault_confirmed"] is None:
            out["note"] = ("the schema change was never seen in progress, so the blocking chain could not be looked for"
                           if state.get("ddl_issued_mono_ns") is None else "the blocking chain was not evaluated")
        return out

    async def revert(self, node: Node, detail: Mapping[str, Any] | None = None) -> dict[str, Any]:
        return await self._database_adapter(node).revert_lock_pileup()


register("os_ssh", OsSshLockPileupDriver)
