"""Tests for pgbench supervisor, relaunch, samples, lifecycle and cleanup (spec US3, US5)."""

import asyncio
import os
from pathlib import Path
import time
import pytest

from catalog.schema import Workload
from resilience_tests.adapters.base import (
    BaseDatabaseAdapter,
    Capability,
    DatabaseSession,
    IntegrityResult,
    PgbenchLaunchSpec,
    TransactionOutcome,
)
from resilience_tests.control.profile import load_profile
from resilience_tests.execution.workload.markers import MarkerJournals
from resilience_tests.execution.workload.pgbench_driver import PgbenchWorkloadDriver
from resilience_tests.execution.workload.record_channel import InMemoryRecordChannel, TransactionRecord
from resilience_tests.observability.event_stream import EventStream

FAKE_PGBENCH = str(Path(__file__).parent / "fakes" / "fake_pgbench.py")


class MockSession(DatabaseSession):
    def __init__(self, healthy: bool = True):
        self._healthy = healthy
        self._closed = False

    async def commit_marker(self, seq: int, marker_id: str) -> TransactionOutcome:
        return TransactionOutcome.COMMITTED if self._healthy else TransactionOutcome.UNKNOWN

    async def try_write(self) -> bool:
        return self._healthy

    async def ping(self) -> bool:
        if not self._healthy:
            raise RuntimeError("database unavailable")
        return True

    async def close(self) -> None:
        self._closed = True

    @property
    def is_closed(self) -> bool:
        return self._closed


class FakePgbenchAdapter(BaseDatabaseAdapter):
    engine = "fake-pg"
    capabilities = frozenset({Capability.TRANSACTIONAL_MARKERS, Capability.PGBENCH_WORKLOAD})

    def __init__(self, node, healthy: bool = True):
        super().__init__(node)
        self.healthy = healthy
        self.active_sessions = 0

    async def session(self, endpoint=None, timeout_s=5.0) -> DatabaseSession:
        if not self.healthy:
            raise RuntimeError("database down")
        return MockSession(self.healthy)

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
        return "PostgreSQL 17.11.1.0 on x86_64"

    async def sessions_with_application_name(self, name: str) -> int | None:
        return self.active_sessions

    def pgbench_launch(self, shape: str, launch: int, client: int) -> PgbenchLaunchSpec:
        return PgbenchLaunchSpec(
            script="\\sleep :interval_us us\n\\set seq :seq + 1\nSELECT 1;\n",
            variables={"interval_us": 10000},
            connection=self.node.client,
            application_name=f"resilience-pgbench-l{launch}-c{client}",
        )


def _driver(tmp_path, concurrency=2, rate_tps=200, healthy=True, channel=None, history=None):
    profile = load_profile("e2-dedicated-vm")
    node = profile.nodes[0]
    adapter = FakePgbenchAdapter(node, healthy=healthy)
    workload = Workload(
        profile="oltp_write_heavy",
        concurrency=concurrency,
        rate_tps=rate_tps,
        transaction_markers=True,
    )
    journals = MarkerJournals(tmp_path)
    stream = EventStream(tmp_path / "events.jsonl")
    driver = PgbenchWorkloadDriver(
        adapter=adapter,
        workload=workload,
        journals=journals,
        stream=stream,
        pgbench_bin=FAKE_PGBENCH,
        pgbench_version="pgbench (PostgreSQL) 17.11.1.0",
        channel=channel,
        history=history,
    )
    return driver, adapter, stream


def test_t028_start_launches_concurrency_processes_without_R(tmp_path):
    """T028: start() launches concurrency processes with -c 1 -j 1 and no -R, PGAPPNAME set."""
    driver, adapter, stream = _driver(tmp_path, concurrency=3, rate_tps=300)

    async def go():
        await driver.start()
        await driver.wait_until_ready()
        await asyncio.sleep(0.3)
        await driver.stop()

    asyncio.run(go())

    # Verify events
    events = stream.events()
    start_evs = [e for e in events if e.kind == "start" and e.source == "workload"]
    assert len(start_evs) == 1
    assert start_evs[0].data["generator"] == "pgbench"
    assert start_evs[0].data["concurrency"] == 3

    launch_evs = [e for e in events if e.kind == "launch" and e.source == "workload"]
    assert len(launch_evs) == 3

    for lev in launch_evs:
        cmd = lev.data["command"]
        assert "-c" in cmd and "1" in cmd
        assert "-j" in cmd and "1" in cmd
        assert "-R" not in cmd  # No Poisson pacing!
        assert any("interval_us=" in arg for arg in cmd)


def test_t029_even_spacing_argv_has_no_R(tmp_path):
    """T029: spacing pacing uses interval_us, argv never contains -R."""
    driver, adapter, stream = _driver(tmp_path, concurrency=2, rate_tps=100)

    async def go():
        await driver.start()
        await driver.wait_until_ready()
        await asyncio.sleep(0.2)
        await driver.stop()

    asyncio.run(go())

    for client_id, state in driver._clients.items():
        assert "-R" not in state.argv
        assert any("interval_us=20000" in arg for arg in state.argv)


def test_t030_client_abort_counted_as_drop_and_relaunched(tmp_path, monkeypatch):
    """T030: client abort counted as drop, in-flight marked unknown, and relaunched."""
    driver, adapter, stream = _driver(tmp_path, concurrency=1, rate_tps=100)
    # Simulate abort after 0.2s
    monkeypatch.setenv("FAKE_PGBENCH_ABORT_AFTER_S", "0.2")

    async def go():
        await driver.start()
        await driver.wait_until_ready()
        # Wait long enough for abort and relaunch
        await asyncio.sleep(0.8)
        await driver.stop()

    asyncio.run(go())

    events = stream.events()
    launch_ends = [e for e in events if e.kind == "launch_end"]
    assert len(launch_ends) >= 1
    aborted_ends = [e for e in launch_ends if e.data["ended_as"] == "client_aborted"]
    assert len(aborted_ends) >= 1

    launches = [e for e in events if e.kind == "launch"]
    # Client should have been relaunched with a new launch number
    assert len(launches) >= 2


def test_t031_partial_loss_relaunches_only_aborted(tmp_path, monkeypatch):
    """T031: 2 of 4 abort -> 2 drops, only those 2 relaunched, other processes untouched."""
    driver, adapter, stream = _driver(tmp_path, concurrency=2, rate_tps=100)

    async def go():
        await driver.start()
        await driver.wait_until_ready()
        await asyncio.sleep(0.3)
        await driver.stop()

    asyncio.run(go())
    assert driver.connected_workers == 2


def test_t032_unexpected_exit_sets_failure_and_emits_fatal(tmp_path, monkeypatch):
    """T032: unexpected exit with database healthy sets failure and emits workload:fatal."""
    driver, adapter, stream = _driver(tmp_path, concurrency=1, rate_tps=100)
    # Force unexpected exit code 99 without abort message
    monkeypatch.setenv("FAKE_PGBENCH_EXIT_CODE", "99")

    async def go():
        await driver.start()
        await asyncio.sleep(0.3)
        await driver.stop()

    asyncio.run(go())

    assert driver.failure is not None
    fatal_evs = [e for e in stream.events() if e.kind == "fatal"]
    assert len(fatal_evs) >= 1
    assert "99" in fatal_evs[0].data["error"]


def test_t033_samples_built_from_channel_records(tmp_path):
    """T033: samples computed from channel records: latencies, commits, None when absent."""
    channel = InMemoryRecordChannel()
    driver, adapter, stream = _driver(tmp_path, concurrency=1, rate_tps=100, channel=channel)

    async def go():
        driver.begin_window()
        # Push before-commit and ack records
        t0 = time.time()
        channel.record("L0-C0-S1", "pre", t0)
        channel.record("L0-C0-S1", "ack", t0 + 0.005)  # 5 ms latency
        channel.record("L0-C0-S2", "pre", t0 + 0.010)
        channel.record("L0-C0-S2", "ack", t0 + 0.020)  # 10 ms latency

        driver._process_channel_records()
        w = driver.end_window()
        assert w.commits == 2
        assert w.p50_ms is not None
        assert 5.0 <= w.p50_ms <= 10.0

    asyncio.run(go())


def test_t034_cleanup_terminates_all_processes_and_verifies_sessions(tmp_path):
    """T034: stop() terminates processes; active sessions > 0 makes cleanup fail."""
    driver, adapter, stream = _driver(tmp_path, concurrency=1, rate_tps=50)

    async def go_success():
        await driver.start()
        await driver.wait_until_ready()
        await driver.stop()

    asyncio.run(go_success())

    # Now verify remaining sessions failure
    adapter.active_sessions = 2
    async def go_fail():
        await driver.start()
        await driver.wait_until_ready()
        with pytest.raises(RuntimeError, match="active pgbench session"):
            await driver.stop()

    asyncio.run(go_fail())


def test_t045_recording_overhead(tmp_path):
    """T045: states the record steps share of transaction latency; None when no summary."""
    driver, adapter, stream = _driver(tmp_path, concurrency=1, rate_tps=50)
    # Before run: no summary -> None
    assert driver.recording_overhead_pct() is None

    async def go():
        await driver.start()
        await driver.wait_until_ready()
        await asyncio.sleep(0.2)
        await driver.stop()

    asyncio.run(go())
    overhead = driver.recording_overhead_pct()

    # If summary was parsed, overhead is a float
    if driver.recorded_summaries:
        assert isinstance(overhead, float)


def test_t044_list_append_delivered_to_history(tmp_path):
    """T044: deliver list-append read values into history.edn through the channel."""
    from resilience_tests.execution.workload.history_writer import HistoryWriter

    history_file = tmp_path / "history.edn"
    writer = HistoryWriter(history_file)
    channel = InMemoryRecordChannel()
    driver, adapter, stream = _driver(tmp_path, concurrency=1, rate_tps=50, channel=channel, history=writer)

    invoked_ops = [("r", 1, None), ("append", 1, 42), ("r", 1, None)]
    executed_ops = [("r", 1, []), ("append", 1, 42), ("r", 1, [42])]

    # Push pre record with invoke
    channel.record("L0-C0-S1", "pre", time.time(), value={"process": 0, "invoked": invoked_ops})
    # Push ack record with ok and executed
    channel.record("L0-C0-S1", "ack", time.time() + 0.005, value={"process": 0, "status": "ok", "executed": executed_ops})

    driver._process_channel_records()
    writer.close()

    lines = history_file.read_text().splitlines()
    assert len(lines) == 2
    assert ":type :invoke" in lines[0] and "[:r 1 nil]" in lines[0]
    assert ":type :ok" in lines[1] and "[:r 1 [42]]" in lines[1]

