"""Workload driver interface and factory (Arch §6.1, contracts/workload-driver.md)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable

from catalog.schema import Workload
from resilience_tests.adapters.base import BaseDatabaseAdapter, Capability
from resilience_tests.control.profile import EnvProfile
from resilience_tests.execution.workload.driver import (
    UnsupportedWorkload,
    WorkloadDriver,
)
from resilience_tests.execution.workload.history_writer import HistoryWriter
from resilience_tests.execution.workload.markers import MarkerJournals
from resilience_tests.execution.workload.pgbench_driver import (
    PgbenchWorkloadDriver,
    probe_pgbench,
)
from resilience_tests.observability.event_stream import EventStream

if TYPE_CHECKING:
    from resilience_tests.execution.workload.record_channel import RecordChannel

# Global registry for R1 record channel factory and certification status
_RECORD_CHANNEL_FACTORY: Callable[[], RecordChannel] | None = None
_RECORD_CHANNEL_TESTS_PASSED: bool = False


def register_record_channel_factory(
    factory: Callable[[], RecordChannel] | None,
    tests_passed: bool = False,
) -> None:
    """Register the R1 record channel factory and flag whether contract tests passed."""
    global _RECORD_CHANNEL_FACTORY, _RECORD_CHANNEL_TESTS_PASSED
    _RECORD_CHANNEL_FACTORY = factory
    _RECORD_CHANNEL_TESTS_PASSED = tests_passed


def derive_shape(workload: Workload) -> str:
    """Derive the shape (marker, churn, list_append) for the scenario workload.

    Enforces the same invariants and refusals as the built-in driver.
    """
    if workload.profile not in ("oltp_write_heavy", "mixed") or not workload.transaction_markers:
        raise UnsupportedWorkload(
            f"workload profile {workload.profile!r} (markers={workload.transaction_markers}) is not built yet"
        )
    churn = workload.profile == "mixed"
    list_append = workload.history == "list_append"
    if churn and list_append:
        raise UnsupportedWorkload("list-append history and the churn workload are not combined")
    if list_append:
        return "list_append"
    if churn:
        return "churn"
    return "marker"


async def make_workload_driver(
    profile: EnvProfile,
    adapter: BaseDatabaseAdapter,
    workload: Workload,
    journals: MarkerJournals,
    stream: EventStream,
    history: HistoryWriter | None = None,
    channel: RecordChannel | None = None,
) -> WorkloadDriver | PgbenchWorkloadDriver:
    """Construct the workload driver selected by the environment profile.

    Raises UnsupportedWorkload if the driver cannot meet its contract.
    NEVER silently substitutes one driver for another (contracts/workload-driver.md).
    """
    generator = profile.workload.generator

    if generator == "builtin":
        return WorkloadDriver(
            adapter=adapter,
            workload=workload,
            journals=journals,
            stream=stream,
            history=history,
        )

    if generator == "pgbench":
        # Check adapter capability (FR-004)
        if not adapter.has(Capability.PGBENCH_WORKLOAD):
            raise UnsupportedWorkload(
                f"adapter for engine {adapter.engine!r} lacks Capability.PGBENCH_WORKLOAD "
                f"(set workload.generator: builtin in profile {profile.name!r} to use the built-in driver)"
            )

        # Check pgbench availability and version match against server (FR-004)
        server_version_str = await adapter.server_version()
        pgbench_version = probe_pgbench(profile.workload.pgbench_bin, server_version_str)

        # Check R1 record channel if transaction markers are required (FR-005)
        rec_channel = channel
        if workload.transaction_markers:
            if rec_channel is None and _RECORD_CHANNEL_FACTORY is not None and _RECORD_CHANNEL_TESTS_PASSED:
                rec_channel = _RECORD_CHANNEL_FACTORY()
            if rec_channel is None:
                raise UnsupportedWorkload(
                    "pgbench generator requested for scenario with transaction_markers: true, but no verified "
                    "R1 record channel is registered (gated on research R1; see specs/002-pgbench-workload-driver/research.md; "
                    f"set workload.generator: builtin in profile {profile.name!r} to use the built-in driver)"
                )

        return PgbenchWorkloadDriver(
            adapter=adapter,
            workload=workload,
            journals=journals,
            stream=stream,
            pgbench_bin=profile.workload.pgbench_bin,
            pgbench_version=pgbench_version,
            channel=rec_channel,
            history=history,
        )

    raise UnsupportedWorkload(
        f"unknown workload generator {generator!r} in profile {profile.name!r}; must be 'pgbench' or 'builtin'"
    )
