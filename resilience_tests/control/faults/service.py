"""Faults on the database service or its host, with no measures of their own: the process
kill, the restart, the reload and the power cut."""

from __future__ import annotations

from resilience_tests.control.faults.base import FaultBehaviour, PowerRestoredRecovery, register_fault


@register_fault
class ProcessKill(FaultBehaviour):
    fault_type = "process_kill"
    expects_outage = True
    recovers_unattended = True
    # a repeated kill (NL-C-05, NL-M-03): which process each cycle hit, whether its death was
    # confirmed, and any attempts that missed (a targeted kill whose process ended first)
    cycle_evidence_keys = ("target_pid", "target", "landed", "death_confirmed_s", "postmaster_pid",
                           "postmaster_survived", "missed_attempts")


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
