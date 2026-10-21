"""Did monitoring raise an alert? (Framework §10.6: "exposed and alerted", "WAL stall alerted",
"detected and alerted as degradation"; Framework §10.7 NL-M-05.)

The harness's own probes show that a condition existed; whether anyone was TOLD is a question
for the monitoring system, so it is asked there. The environment profile names the source
(`alerting.source`). One AlertSource interface, one class per monitoring system, chosen by
name like the fault drivers -- so a scenario never names a monitoring product.

No source is built yet beyond `none`: it answers "cannot tell", which the run reports as
NOT_MEASURED with the reason. Note: docs/infra-requirements.md asks for host alerting to be
suppressed on the lab hosts, so a source must be one the harness's own rules feed, not the
suppressed one.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from resilience_tests.control.profile import EnvProfile


class AlertSource(ABC):
    name: ClassVar[str]

    def __init__(self, profile: EnvProfile) -> None:
        self.profile = profile

    @abstractmethod
    async def fired(self, alert: str, since_wall_s: float, until_wall_s: float) -> dict[str, Any]:
        """Whether `alert` fired between the two wall-clock times: {"fired": True/False/None,
        "note": ...}. None means this source cannot tell -- reported as not measured, never as
        "no alert"."""


class NoAlertSource(AlertSource):
    name = "none"

    async def fired(self, alert: str, since_wall_s: float, until_wall_s: float) -> dict[str, Any]:
        return {"fired": None, "note": f"no alert source is connected to the harness (profile "
                                       f"{self.profile.name!r} alerting.source: none), so whether "
                                       f"{alert!r} was alerted cannot be observed"}


_SOURCES: dict[str, type[AlertSource]] = {NoAlertSource.name: NoAlertSource}


def alert_source_for(profile: EnvProfile) -> AlertSource:
    return _SOURCES[profile.alerting.source](profile)
