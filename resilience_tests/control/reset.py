"""L0 reset drivers (Arch §3, §4.2: a reset phase before init delegates to the L0 reset driver).

Reset must be snapshot-based, not rebuild-based. `lvm_snapshot` is built together with the
Ansible storage role once PGDATA sits on the dedicated DM volume; until then the lab profile
declares `none`, which is recorded as a disclosure on every run -- never silently skipped.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from resilience_tests.control.profile import EnvProfile, Node


class ResetNotAvailable(RuntimeError):
    pass


class ResetDriver(ABC):
    def __init__(self, profile: EnvProfile) -> None:
        self.profile = profile

    @abstractmethod
    async def reset(self, node: Node) -> dict[str, Any]: ...


class NoReset(ResetDriver):
    DISCLOSURE = "No baseline reset: the target was not rolled back to a snapshot before this run."

    async def reset(self, node: Node) -> dict[str, Any]:
        return {"driver": "none", "disclosure": self.DISCLOSURE}


def resolve_reset(profile: EnvProfile) -> ResetDriver:
    if profile.reset.driver == "none":
        return NoReset(profile)
    raise ResetNotAvailable(
        f"reset driver {profile.reset.driver!r} is not built yet (built with the Ansible storage role, Arch §3)"
    )
