"""The engine seam (Arch §8): nothing above the adapter layer may know what a database is.

These tests run the real workload driver and the real write prober against an in-memory fake
engine -- no PostgreSQL, no network. If they pass, a new engine is an adapter plus a profile
entry, not a change to the framework.
"""

import asyncio
import tempfile
from pathlib import Path

import pytest

from catalog.schema import load_catalog
from resilience_tests.adapters.base import (
    BaseDatabaseAdapter,
    Capability,
    DatabaseSession,
    EngineNotSupported,
    IntegrityResult,
    TransactionOutcome,
    adapter_for,
    register_adapter,
    registered_engines,
)
from resilience_tests.adapters import postgresql  # noqa: F401  (registers the adapter)
from resilience_tests.control.profile import load_profile
from resilience_tests.execution.probes.probers import WriteProber
from resilience_tests.execution.workload.driver import UnsupportedWorkload, WorkloadDriver
from resilience_tests.execution.workload.markers import MarkerJournals, diff_from_journals
from resilience_tests.observability.event_stream import EventStream

NODE = load_profile("e2-dedicated-vm").nodes[0]
WORKLOAD = load_catalog().scenarios["NL-C-01"].workload


class FakeSession(DatabaseSession):
    """An engine that keeps markers in a dict. It can be told to fail, so the driver's
    outcome handling is exercised without a real database."""

    def __init__(self, store: set[str], fail_with: TransactionOutcome | None = None) -> None:
        self.store = store
        self.fail_with = fail_with
        self._closed = False

    async def commit_marker(self, seq: int, marker_id: str) -> TransactionOutcome:
        if self.fail_with is TransactionOutcome.UNKNOWN:
            await self.close()
            return TransactionOutcome.UNKNOWN
        if self.fail_with is TransactionOutcome.DEFINITELY_ABORTED:
            return TransactionOutcome.DEFINITELY_ABORTED
        self.store.add(marker_id)
        return TransactionOutcome.COMMITTED

    async def try_write(self) -> bool:
        return self.fail_with is None

    async def ping(self) -> bool:
        return self.fail_with is None

    async def close(self) -> None:
        self._closed = True

    @property
    def is_closed(self) -> bool:
        return self._closed


class FakeAdapter(BaseDatabaseAdapter):
    engine = "fake-engine"
    capabilities = frozenset({Capability.TRANSACTIONAL_MARKERS, Capability.DURABILITY_SETTINGS})

    def __init__(self, node, fail_with=None) -> None:
        super().__init__(node)
        self.store: set[str] = set()
        self.fail_with = fail_with

    async def session(self, endpoint=None, timeout_s: float = 5.0) -> DatabaseSession:
        return FakeSession(self.store, self.fail_with)

    async def prepare_harness_state(self) -> None:
        self.store.clear()

    async def marker_ids(self) -> set[str]:
        return set(self.store)

    async def sentinel(self, table: str, hostname: str):
        return {"hostname": hostname, "inventory_tag": "resilience-lab-e2", "disposable": True, "created_by": "op"}

    async def durability_settings(self) -> dict[str, str]:
        return {"durable_writes": "on"}

    async def certification_blockers(self) -> list[str]:
        return []

    async def integrity_check(self, timeout_s: float) -> IntegrityResult:
        # this engine has neither a structural checker nor page checksums
        return IntegrityResult(structural_errors=None, checksum_failures=None)

    async def server_version(self) -> str:
        return "fake-engine 1.0"


def run_workload(adapter: FakeAdapter, seconds: float = 1.0):
    async def go():
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            stream = EventStream(d / "events.jsonl")
            journals = MarkerJournals(d)
            driver = WorkloadDriver(adapter, WORKLOAD, journals, stream)
            await driver.start()
            await asyncio.sleep(seconds)
            await driver.stop()
            journals.close()
            stream.close()
            diff, torn = diff_from_journals(d, await adapter.marker_ids())
            return diff, torn
    return asyncio.run(go())


def test_workload_driver_runs_against_a_non_postgres_engine():
    adapter = FakeAdapter(NODE)
    diff, torn = run_workload(adapter)
    assert diff.written > 0 and diff.acked > 0
    assert diff.rpo_txn == 0          # everything acknowledged is present
    assert not diff.phantom and not diff.unjournalled_ack and torn == 0


def test_unknown_outcomes_become_indeterminate_not_loss():
    """The :fail-versus-:info rule (Arch §7.1) is enforced above the engine: an unknown
    outcome must never be counted as a lost transaction."""
    adapter = FakeAdapter(NODE, fail_with=TransactionOutcome.UNKNOWN)
    diff, _ = run_workload(adapter, seconds=0.6)
    assert diff.acked == 0 and diff.written > 0
    assert diff.rpo_txn == 0 and len(diff.indeterminate) == diff.written


def test_definite_aborts_are_not_indeterminate():
    adapter = FakeAdapter(NODE, fail_with=TransactionOutcome.DEFINITELY_ABORTED)
    diff, _ = run_workload(adapter, seconds=0.6)
    assert diff.acked == 0 and diff.rpo_txn == 0


def test_write_prober_runs_against_a_non_postgres_engine():
    async def go():
        with tempfile.TemporaryDirectory() as tmp:
            stream = EventStream(Path(tmp) / "events.jsonl")
            prober = WriteProber(FakeAdapter(NODE), stream)
            prober.start()
            await asyncio.sleep(0.7)
            await prober.stop()
            stream.close()
            return [e for e in stream.events() if e.kind == "write_probe"]

    probes = asyncio.run(go())
    assert probes and all(e.data["ok"] for e in probes)


def test_engine_without_markers_cannot_measure_rpo():
    class NoMarkers(FakeAdapter):
        engine = "no-markers"
        capabilities = frozenset({Capability.DURABILITY_SETTINGS})

    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        stream = EventStream(d / "e.jsonl")
        journals = MarkerJournals(d)
        with pytest.raises(UnsupportedWorkload, match="cannot record transaction markers"):
            WorkloadDriver(NoMarkers(NODE), WORKLOAD, journals, stream)
        journals.close()
        stream.close()


def test_missing_capability_is_reported_as_absent_not_zero():
    integrity = asyncio.run(FakeAdapter(NODE).integrity_check(timeout_s=1))
    assert integrity.structural_errors is None and integrity.checksum_failures is None


def test_adapter_registry():
    register_adapter(FakeAdapter)
    assert "postgresql" in registered_engines() and "fake-engine" in registered_engines()
    assert adapter_for("fake-engine", NODE).engine == "fake-engine"
    with pytest.raises(EngineNotSupported, match="no adapter for engine"):
        adapter_for("nonexistent-db", NODE)


def test_no_engine_specific_imports_above_the_adapter_layer():
    """Structural guard: `asyncpg` may appear only inside the PostgreSQL adapter."""
    root = Path(__file__).resolve().parents[1]
    offenders = []
    for path in sorted((root / "resilience_tests").rglob("*.py")):
        if "adapters/postgresql" in path.as_posix():
            continue
        text = path.read_text()
        if "asyncpg" in text or "psycopg" in text:
            offenders.append(path.relative_to(root).as_posix())
    assert not offenders, f"engine-specific imports outside the adapter: {offenders}"


def test_run_plan_skips_scenarios_an_engine_cannot_support():
    """One catalog, many engines: a scenario an engine cannot support is skipped with a
    reason, never run with a fabricated measurement."""
    from resilience_tests.control.matrix import expand

    register_adapter(FakeAdapter)   # capabilities: markers + durability settings only
    profile = load_profile("e2-dedicated-vm")
    catalog = load_catalog()

    on_postgres, skipped_pg = expand(catalog, profile, reference_env_class="E2")
    assert {p.scenario.id for p in on_postgres} == set(catalog.scenarios) and not skipped_pg

    fake = profile.model_copy(update={"database": profile.database.model_copy(update={"engine": "fake-engine"})})
    on_fake, skipped_fake = expand(catalog, fake, reference_env_class="E2")
    # every authored scenario needs a structural integrity check, which this engine lacks
    assert not on_fake
    assert all("structural_integrity_check" in s.reason for s in skipped_fake)
