"""OS/SSH-section driver for the cluster-level settings a scenario runs under (catalog
server_settings). Not a fault: a change the harness makes to the environment for the run.

It is journalled in the injection ledger like an injection, for the same reason: a crashed
harness must not leave the server running with settings the operator never chose. So the
kill switch restores it from the ledger, and a node still carrying it is refused by the next
run until it has.

  plan     read only: what would change; refuses what the configuration file cannot decide
  inject   write the change (one marked block in the configuration file), restart, verify
  confirm  the change was verified in effect after the restart
  revert   remove the block, restart if it was there, verify the recorded values are back.
           Idempotent; works from the ledger's detail alone (the kill switch has no scenario).

Through the run's database adapter (ConfigurationSupport), which is the only place that knows
the configuration file's syntax and how the engine reports a setting.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, ClassVar

from resilience_tests.control.profile import EnvProfile, Node
from resilience_tests.execution.injectors.base import DriverNotAvailable, FaultInjector, register

SERVER_SETTINGS = "server_settings"


class OsSshServerSettingsDriver(FaultInjector):
    fault_types = frozenset({SERVER_SETTINGS})
    driver_name = "os_ssh"
    harness_setup: ClassVar[bool] = True     # a change for the run, not a fault: it has no fault behaviour

    def __init__(self, profile: EnvProfile, fault_type: str = SERVER_SETTINGS) -> None:
        super().__init__(profile, fault_type)
        self._plan: dict[str, Any] | None = None
        self._tag: str | None = None

    async def plan(self, node: Node, wanted: Mapping[str, str]) -> dict[str, Any]:
        """Read only. Raises ServerSettingsRefused (nothing changed) for what cannot be applied."""
        return await self._database_adapter(node).plan_server_settings(wanted)

    def prepare(self, plan: Mapping[str, Any], tag: str) -> dict[str, Any]:
        """Hold the plan and its tag for `inject`. Returns what the ledger needs to revert the
        change even if the harness dies in the middle of applying it."""
        self._plan, self._tag = dict(plan), tag
        return {"tag": tag, "config_file": plan["config_file"], "changes": dict(plan["changes"])}

    async def preflight(self, node: Node) -> dict[str, Any]:
        if self._plan is None:
            raise DriverNotAvailable("server settings: plan() and prepare() come first")
        return {"config_file": self._plan["config_file"], "changes": sorted(self._plan["changes"])}

    async def inject(self, node: Node) -> dict[str, Any]:
        if self._plan is None or self._tag is None:
            raise DriverNotAvailable("server settings: plan() and prepare() come first")
        return await self._database_adapter(node).apply_server_settings(self._plan, self._tag)

    async def confirm(self, node: Node, detail: Mapping[str, Any]) -> dict[str, Any]:
        return {"fault_confirmed": bool(detail.get("verified"))}

    async def revert(self, node: Node, detail: Mapping[str, Any] | None = None) -> dict[str, Any]:
        if not detail or not detail.get("tag"):
            raise RuntimeError("server settings: the ledger entry has no tag, so the change cannot be found")
        return await self._database_adapter(node).restore_server_settings(detail)


register("os_ssh", OsSshServerSettingsDriver)
