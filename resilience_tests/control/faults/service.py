"""Faults on the database service or its host, with no measures of their own: the process
kill, the restart, the reload and the power cut."""

from __future__ import annotations

from resilience_tests.control.faults.base import FaultBehaviour, PowerRestoredRecovery, register_fault


@register_fault
class ProcessKill(FaultBehaviour):
    fault_type = "process_kill"
    expects_outage = True
    recovers_unattended = True


@register_fault
class ServiceRestart(FaultBehaviour):
    fault_type = "service_restart"
    expects_outage = True
    recovers_unattended = True


@register_fault
class ConfigReload(FaultBehaviour):
    fault_type = "config_reload"
    recovers_unattended = True


@register_fault
class HostPowerLoss(PowerRestoredRecovery):
    fault_type = "host_power_loss"
    expects_outage = True
