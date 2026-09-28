"""Power-loss drivers (Arch §5 row "Power loss": virsh destroy / ipmitool / Redfish / cloud
force-stop -- build one per environment class).

Only libvirt (E2) is built: it is the driver the reference lab needs. `virsh destroy` gives
true force-off semantics (Arch §12) -- no guest shutdown, no guest page-cache flush.
virsh runs on the driver host against the remote URI from the profile
(`qemu+ssh://<hypervisor>/system`), so no agent is needed on the hypervisor.
"""

from __future__ import annotations

import asyncio
import shutil
import time
from collections.abc import Mapping
from typing import Any

from resilience_tests.control.profile import Node
from resilience_tests.execution.injectors.base import DriverNotAvailable, FaultInjector, register

VIRSH_TIMEOUT_S = 60.0


class LibvirtPowerDriver(FaultInjector):
    fault_types = frozenset({"host_power_loss"})
    driver_name = "libvirt"

    def _target(self, node: Node) -> tuple[str, str]:
        uri = self.profile.power_control.uri
        if not uri:
            raise DriverNotAvailable(f"profile {self.profile.name}: power_control.uri is not set")
        if not node.power_domain:
            raise DriverNotAvailable(f"profile {self.profile.name}: node {node.name} has no power_domain")
        return uri, node.power_domain

    async def _virsh(self, *args: str) -> str:
        if shutil.which("virsh") is None:
            raise DriverNotAvailable("virsh not installed on the driver host (apt install libvirt-clients)")
        proc = await asyncio.create_subprocess_exec(
            "virsh", *args, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        try:
            out, err = await asyncio.wait_for(proc.communicate(), timeout=VIRSH_TIMEOUT_S)
        except TimeoutError:
            proc.kill()
            raise
        if proc.returncode != 0:
            raise RuntimeError(f"virsh {' '.join(args)} exited {proc.returncode}: {err.decode().strip()}")
        return out.decode().strip()

    async def domstate(self, node: Node) -> str:
        uri, domain = self._target(node)
        return await self._virsh("-c", uri, "domstate", domain)

    async def preflight(self, node: Node) -> dict[str, Any]:
        state = await self.domstate(node)
        if state != "running":
            raise DriverNotAvailable(f"{node.name}: domain state is {state!r}, expected 'running'")
        return {"domstate": state}

    async def inject(self, node: Node) -> dict[str, Any]:
        uri, domain = self._target(node)
        await self._virsh("-c", uri, "destroy", domain)
        t0_mono_ns = time.monotonic_ns()  # T0: the injection call returned (Arch §7.2)
        state = await self.domstate(node)
        if state != "shut off":
            raise RuntimeError(f"{node.name}: after destroy the domain is {state!r}, not 'shut off'")
        return {"action": "destroy", "domstate_after": state, "t0_mono_ns": t0_mono_ns}

    async def revert(self, node: Node, detail: Mapping[str, Any] | None = None) -> dict[str, Any]:
        state = await self.domstate(node)
        if state == "running":
            return {"action": "start", "skipped": "already running"}
        uri, domain = self._target(node)
        await self._virsh("-c", uri, "start", domain)
        return {"action": "start", "domstate_before": state}


register("power_control", LibvirtPowerDriver)
