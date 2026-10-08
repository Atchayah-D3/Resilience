"""Workload driver interface and factory (Arch §6.1, contracts/workload-driver.md)."""

from __future__ import annotations

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
from resilience_tests.execution.workload.pgbench_driver import derive_shape as _derive_shape
from resilience_tests.observability.event_stream import EventStream


def derive_shape(workload: Workload) -> str:
    """The transaction shape (marker, churn, list_append) a scenario's workload declares, with
    the built-in driver's refusals of combinations it does not build."""
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

        # The shape and its capabilities first: a scenario pgbench cannot run is refused before
        # anything is probed on the driver host.
        _derive_shape(workload, adapter, history)

        # Check pgbench availability and version match against server (FR-004)
        server_version_str = await adapter.server_version()
        pgbench_version = probe_pgbench(profile.workload.pgbench_bin, server_version_str)

        return PgbenchWorkloadDriver(
            adapter=adapter,
            workload=workload,
            journals=journals,
            stream=stream,
            pgbench_bin=profile.workload.pgbench_bin,
            pgbench_version=pgbench_version,
            history=history,
        )

    raise UnsupportedWorkload(
        f"unknown workload generator {generator!r} in profile {profile.name!r}; must be 'pgbench' or 'builtin'"
    )
