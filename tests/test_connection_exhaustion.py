"""Tests for NL-R-04 (Connection exhaustion under load)."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

from catalog.schema import load_catalog
from resilience_tests.analysis import rto_decomposer
from resilience_tests.control import orchestrator as orch_mod
from resilience_tests.control.matrix import RunPlanItem
from resilience_tests.control.orchestrator import RunOptions, TestOrchestrator
from resilience_tests.control.profile import load_profile
from resilience_tests.execution.injectors.process import OsSshProcessDriver
from resilience_tests.execution.remote import RemoteResult
from tests.test_orchestrator import OutageAdapter, Engine


def test_os_ssh_connection_exhaustion_driver():
    """Verify OsSshProcessDriver can inject and revert connection_exhaustion with realistic mock."""
    import asyncpg
    profile = load_profile("e2-dedicated-vm")
    driver = OsSshProcessDriver(profile, "connection_exhaustion")
    node = profile.nodes[0]

    call_count = 0

    async def fake_connect(**kwargs):
        nonlocal call_count
        call_count += 1
        # GUC query connection (first call)
        if call_count == 1:
            mock_conn = AsyncMock()
            mock_conn.fetchrow.side_effect = [("100",), ("3",)]
            return mock_conn
        # First 97 flood connections succeed
        if call_count <= 98:
            return AsyncMock(spec=asyncpg.Connection)
        # Superuser probe after flood succeeds
        if call_count > 151:
            return AsyncMock(spec=asyncpg.Connection)
        # Excess 50+ flood connections fail with 53300
        raise asyncpg.TooManyConnectionsError("FATAL: 53300: remaining connection slots are reserved for non-replication superuser connections")

    with patch("asyncpg.connect", side_effect=fake_connect):
        detail = asyncio.run(driver.inject(node))
        assert detail["t0_mono_ns"] > 0
        assert detail["max_connections"] == 100
        assert detail["superuser_reserved"] == 3
        assert detail["attempted_connections"] == 150
        assert detail["held_connections"] == 97
        assert detail["rejected_connections"] > 0
        assert detail["rejections_explicit"] is True
        assert detail["superuser_slot_honoured"] is True

        rev = asyncio.run(driver.revert(node, detail))
        assert rev["action"] == "connection_exhaustion_drained"


def test_connection_exhaustion_fails_if_no_explicit_rejections():
    """Negative test: if flood fails to hold or excess connections don't get 53300, rejections_explicit is False."""
    import asyncpg
    profile = load_profile("e2-dedicated-vm")
    driver = OsSshProcessDriver(profile, "connection_exhaustion")
    node = profile.nodes[0]

    async def fake_connect_all_fail(**kwargs):
        raise ConnectionRefusedError("Connection refused")

    with patch("asyncpg.connect", side_effect=fake_connect_all_fail):
        detail = asyncio.run(driver.inject(node))
        assert detail["held_connections"] == 0
        assert detail["rejections_explicit"] is False
        assert detail["superuser_slot_honoured"] is False


def test_nl_r_04_orchestrator_evaluation(tmp_path, monkeypatch):
    """Verify NL-R-04 runs through orchestrator and evaluates rejections_explicit,
    superuser_slot_honoured, and existing_sessions_unaffected."""
    Engine.down = False
    Engine.store = set()
    catalog = load_catalog()
    sc = catalog.scenarios["NL-R-04"]
    fast_sc = sc.model_copy(update={
        "steady_state": sc.steady_state.model_copy(update={"duration_s": 0.5, "tps_min": 1}),
        "workload": sc.workload.model_copy(update={"rate_tps": 20, "concurrency": 2}),
    })
    base_profile = load_profile("e2-dedicated-vm")
    node = base_profile.nodes[0]
    item = RunPlanItem(scenario=fast_sc, env_class="E2", role="standalone", node=node)

    timeouts = dict(base_profile.phase_timeouts_s, recovery=12.0)
    profile = base_profile.model_copy(update={
        "database": base_profile.database.model_copy(update={"engine": "orch-fake"}),
        "driver_host": base_profile.driver_host.model_copy(update={"host": "127.0.0.1", "run_dir": str(tmp_path)}),
        "phase_timeouts_s": timeouts,
    })

    async def fake_host_run(command, *args, timeout_s=30.0, check=True, **kwargs):
        if "is-active" in command:
            return RemoteResult(0, "active\n", "")
        if "postmaster.pid" in command:
            return RemoteResult(0, "4242\n", "")
        if "show -p Restart" in command:
            return RemoteResult(0, "on-failure\n", "")
        if "SHOW max_connections" in command:
            return RemoteResult(0, "100\n3\n", "")
        return RemoteResult(0, "BDB-QA-U22-30\n", "")

    async def fake_run_once(endpoint, command, *args, timeout_s=30.0, check=True, **kwargs):
        return RemoteResult(0, "BDB-QA-U22-30\n", "")

    async def no_offset(node, stream, samples=5):
        return 0.0

    class NoTail:
        def __init__(self, *a, **k):
            pass
        def start(self):
            pass
        async def stop(self):
            pass

    monkeypatch.setattr(TestOrchestrator, "_assert_on_driver_host", lambda self: None)
    monkeypatch.setattr(orch_mod, "run_once", fake_run_once)
    monkeypatch.setattr(orch_mod, "measure_clock_offset", no_offset)
    monkeypatch.setattr(orch_mod, "LogTailer", NoTail)
    monkeypatch.setattr(orch_mod, "WORKLOAD_RAMP_S", 0.05)
    monkeypatch.setattr(rto_decomposer, "SLO_SUSTAIN_S", 0.5)

    orch = TestOrchestrator(item, profile, RunOptions())
    orch.adapter = OutageAdapter(node)

    with patch("resilience_tests.execution.injectors.process.RemoteHost") as MockRemoteHost:
        mock_host = AsyncMock()
        mock_host.run.side_effect = fake_host_run
        MockRemoteHost.return_value.__aenter__.return_value = mock_host

        results = asyncio.run(orch.run())
        assert results["status"] == "passed", f"Status: {results.get('status')}, Error: {results.get('error')}, Evaluations: {results.get('evaluations')}, Measured: {results.get('measured')}"
        measured = results["measured"]
        assert measured["rejections_explicit"] is True
        assert measured["superuser_slot_honoured"] is True
        assert measured["existing_sessions_unaffected"] is True
        assert measured["rpo_txn"] == 0
