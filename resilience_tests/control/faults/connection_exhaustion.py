"""NL-R-04: connection exhaustion under load."""

from __future__ import annotations

from typing import Any

from resilience_tests.analysis.predicates import NOT_MEASURED
from resilience_tests.control.faults.base import FaultBehaviour, register_fault


@register_fault
class ConnectionExhaustion(FaultBehaviour):
    fault_type = "connection_exhaustion"
    fact_key = "connection_exhaustion"
    recovers_unattended = True

    def measure_client_impact(self, m: dict[str, Any]) -> None:
        ctx = self.ctx
        exhaust = ctx.facts.get("connection_exhaustion", {})
        for name in ("rejections_explicit", "superuser_slot_honoured", "connections_recover_after_release"):
            value = exhaust.get(name)
            if isinstance(value, bool):
                m[name] = value
            else:
                m[name] = NOT_MEASURED
                note = (exhaust.get("superuser_probe") or {}) if name == "superuser_slot_honoured" else {}
                ctx.not_measured[name] = (note.get("note") or note.get("error")
                                          or "the connection flood did not report it")
        m["existing_sessions_unaffected"] = (m.get("dropped_connections", 0) == 0 and m.get("failed_transactions", 0) == 0)
