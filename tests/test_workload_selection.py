"""Tests for workload selection and refusals (spec User Story 1, tasks T011)."""

import asyncio
from pathlib import Path
import tempfile
import pytest

from catalog.schema import load_catalog
from resilience_tests.adapters.base import (
    BaseDatabaseAdapter,
    Capability,
    DatabaseSession,
    IntegrityResult,
    PgbenchLaunchSpec,
)
from resilience_tests.control.profile import EnvProfile, WorkloadConfig, load_profile
from resilience_tests.execution.workload.driver import UnsupportedWorkload, WorkloadDriver
from resilience_tests.execution.workload.interface import (
    make_workload_driver,
    register_record_channel_factory,
)
from resilience_tests.execution.workload.markers import MarkerJournals
from resilience_tests.execution.workload.pgbench_driver import PgbenchWorkloadDriver
from resilience_tests.observability.event_stream import EventStream

FAKE_PGBENCH = str(Path(__file__).parent / "fakes" / "fake_pgbench.py")


class MockAdapter(BaseDatabaseAdapter):
    engine = "mock-engine"
    capabilities = frozenset({Capability.TRANSACTIONAL_MARKERS, Capability.PGBENCH_WORKLOAD})

    def __init__(self, node, server_version_str="PostgreSQL 17.11.1.0 on x86_64"):
        super().__init__(node)
        self._server_version_str = server_version_str

    async def session(self, endpoint=None, timeout_s=5.0) -> DatabaseSession:
        raise NotImplementedError

    async def prepare_harness_state(self) -> None:
        pass

    async def marker_ids(self) -> set[str]:
        return set()

    async def sentinel(self, table: str, hostname: str):
        return {}

    async def durability_settings(self) -> dict[str, str]:
        return {}

    async def certification_blockers(self) -> list[str]:
        return []

    async def integrity_check(self, timeout_s: float) -> IntegrityResult:
        return IntegrityResult(structural_errors=None, checksum_failures=None)

    async def server_version(self) -> str:
        return self._server_version_str

    def pgbench_launch(self, shape: str, launch: int, client: int) -> PgbenchLaunchSpec:
        return PgbenchLaunchSpec(
            script="SELECT 1;\n",
            variables={},
            connection=self.node.client,
            application_name=f"resilience-pgbench-test-{launch}",
        )


def _base_profile(workload_config: WorkloadConfig | None = None) -> EnvProfile:
    # Load profile and return copy with specific workload config
    profile = load_profile("e2-dedicated-vm")
    if workload_config is None:
        # Default WorkloadConfig (no workload section in yaml gives default WorkloadConfig)
        return profile.model_copy(update={"workload": WorkloadConfig()})
    return profile.model_copy(update={"workload": workload_config})


def test_no_workload_section_defaults_to_pgbench():
    """(a) no workload section -> generator pgbench."""
    raw_profile = load_profile("e2-dedicated-vm")
    # Reset workload to default
    default_cfg = WorkloadConfig()
    assert default_cfg.generator == "pgbench"
    assert default_cfg.pgbench_bin == "pgbench"


def test_builtin_selection_returns_workload_driver(tmp_path):
    """(b) generator: builtin -> the existing WorkloadDriver, with facts['workload_generator'] == 'builtin'."""
    profile = _base_profile(WorkloadConfig(generator="builtin"))
    node = profile.nodes[0]
    adapter = MockAdapter(node)
    scenario = load_catalog().scenarios["NL-C-01"]
    journals = MarkerJournals(tmp_path)
    stream = EventStream(tmp_path / "events.jsonl")

    driver = asyncio.run(
        make_workload_driver(
            profile,
            adapter,
            scenario.workload,
            journals,
            stream,
        )
    )
    assert isinstance(driver, WorkloadDriver)
    assert not isinstance(driver, PgbenchWorkloadDriver)
    assert driver.generator == "builtin"


def test_pgbench_bin_not_executable_refuses(tmp_path):
    """(c) pgbench_bin not executable -> UnsupportedWorkload naming the path, raised before baseline."""
    bad_bin = str(tmp_path / "nonexistent-pgbench-bin")
    profile = _base_profile(WorkloadConfig(generator="pgbench", pgbench_bin=bad_bin))
    node = profile.nodes[0]
    adapter = MockAdapter(node)
    scenario = load_catalog().scenarios["NL-C-01"]
    journals = MarkerJournals(tmp_path)
    stream = EventStream(tmp_path / "events.jsonl")

    with pytest.raises(UnsupportedWorkload, match="not found or not executable") as exc_info:
        asyncio.run(
            make_workload_driver(
                profile,
                adapter,
                scenario.workload,
                journals,
                stream,
            )
        )
    assert bad_bin in str(exc_info.value)


def test_pgbench_version_mismatch_refuses(tmp_path, monkeypatch):
    """(d) pgbench major version != target server major version -> refusal naming both versions (FR-004)."""
    profile = _base_profile(WorkloadConfig(generator="pgbench", pgbench_bin=FAKE_PGBENCH))
    node = profile.nodes[0]
    # Server is version 16, while fake_pgbench is version 17
    adapter = MockAdapter(node, server_version_str="PostgreSQL 16.3 on x86_64")
    scenario = load_catalog().scenarios["NL-C-01"]
    journals = MarkerJournals(tmp_path)
    stream = EventStream(tmp_path / "events.jsonl")

    with pytest.raises(UnsupportedWorkload) as exc_info:
        asyncio.run(
            make_workload_driver(
                profile,
                adapter,
                scenario.workload,
                journals,
                stream,
            )
        )
    err = str(exc_info.value)
    assert "17" in err
    assert "16" in err
    assert "FR-004" in err


def test_adapter_without_capability_refuses(tmp_path):
    """(e) adapter without Capability.PGBENCH_WORKLOAD -> refusal."""
    class IncompatibleAdapter(MockAdapter):
        capabilities = frozenset({Capability.TRANSACTIONAL_MARKERS})  # Missing PGBENCH_WORKLOAD

    profile = _base_profile(WorkloadConfig(generator="pgbench", pgbench_bin=FAKE_PGBENCH))
    node = profile.nodes[0]
    adapter = IncompatibleAdapter(node)
    scenario = load_catalog().scenarios["NL-C-01"]
    journals = MarkerJournals(tmp_path)
    stream = EventStream(tmp_path / "events.jsonl")

    with pytest.raises(UnsupportedWorkload, match="Capability.PGBENCH_WORKLOAD"):
        asyncio.run(
            make_workload_driver(
                profile,
                adapter,
                scenario.workload,
                journals,
                stream,
            )
        )


def test_transaction_markers_without_r1_channel_refuses(tmp_path):
    """(f) a scenario with transaction_markers: true while no record channel is registered -> refusal citing research R1."""
    # Ensure no record channel is registered
    register_record_channel_factory(None, tests_passed=False)

    profile = _base_profile(WorkloadConfig(generator="pgbench", pgbench_bin=FAKE_PGBENCH))
    node = profile.nodes[0]
    adapter = MockAdapter(node, server_version_str="PostgreSQL 17.11 on x86_64")
    scenario = load_catalog().scenarios["NL-C-01"]  # transaction_markers: true
    journals = MarkerJournals(tmp_path)
    stream = EventStream(tmp_path / "events.jsonl")

    with pytest.raises(UnsupportedWorkload, match="R1"):
        asyncio.run(
            make_workload_driver(
                profile,
                adapter,
                scenario.workload,
                journals,
                stream,
            )
        )


def test_never_returns_other_driver_on_failure(tmp_path):
    """(g) in none of these failure cases is the built-in driver returned instead."""
    profile = _base_profile(WorkloadConfig(generator="pgbench", pgbench_bin="/nonexistent/path"))
    node = profile.nodes[0]
    adapter = MockAdapter(node)
    scenario = load_catalog().scenarios["NL-C-01"]
    journals = MarkerJournals(tmp_path)
    stream = EventStream(tmp_path / "events.jsonl")

    with pytest.raises(UnsupportedWorkload):
        driver = asyncio.run(
            make_workload_driver(
                profile,
                adapter,
                scenario.workload,
                journals,
                stream,
            )
        )
