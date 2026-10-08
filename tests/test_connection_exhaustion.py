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


ADAPTER_MODULE = "resilience_tests.adapters.postgresql.adapter"
TOO_MANY = "FATAL: 53300: remaining connection slots are reserved for roles with the SUPERUSER attribute"


def flood_driver(hold_s: float = 0.01) -> tuple[OsSshProcessDriver, object]:
    profile = load_profile("e2-dedicated-vm")
    driver = OsSshProcessDriver(profile, "connection_exhaustion")
    driver.duration_s = hold_s
    return driver, profile.nodes[0]


def fake_server(free_slots: int = 97, other_failures: int = 0):
    """asyncpg.connect stand-in: the settings connection, then `free_slots` flood sessions,
    then refusals (a few unexplained ones first, if asked), then the post-release probe."""
    import asyncpg
    state = {"calls": 0, "flood_done": False}

    async def connect(**kwargs):
        state["calls"] += 1
        n = state["calls"]
        if n == 1:
            conn = AsyncMock()
            conn.fetchval.side_effect = ["100", "3"]
            return conn
        flood_index = n - 2           # 150 flood attempts follow
        if flood_index < 150:
            assert kwargs.get("server_settings") == {"application_name": "resilience-flood"}
            if flood_index < free_slots:
                return AsyncMock(spec=asyncpg.Connection)
            if flood_index < free_slots + other_failures:
                raise TimeoutError("connect timed out")
            raise asyncpg.TooManyConnectionsError(TOO_MANY)
        return AsyncMock(spec=asyncpg.Connection)    # accepted again after release
    return connect


def fake_ssh(rolsuper: str = "t", remaining: str = "0", fail: bool = False):
    host = AsyncMock()

    async def run(command, *a, **k):
        if fail:
            raise OSError("ssh unreachable")
        if "rolsuper" in command:
            return RemoteResult(0, f"{rolsuper}\n", "")
        if "pg_terminate_backend" in command:
            return RemoteResult(0, "53\n", "")
        if "count(*)" in command:
            return RemoteResult(0, f"{remaining}\n", "")
        return RemoteResult(0, "", "")
    host.run.side_effect = run
    remote = MagicMock()
    remote.return_value.__aenter__.return_value = host
    return remote


def test_flood_is_held_released_and_judged():
    driver, node = flood_driver()
    with patch("asyncpg.connect", side_effect=fake_server()), patch(f"{ADAPTER_MODULE}.RemoteHost", fake_ssh()):
        detail = asyncio.run(driver.inject(node))
    assert detail["attempted_connections"] == 150
    assert detail["held_connections"] == 97
    assert detail["rejected_explicit"] == 53 and detail["rejected_other"] == 0
    assert detail["rejections_explicit"] is True
    assert detail["superuser_slot_honoured"] is True
    assert detail["released_connections"] == 97          # released inside the fault
    assert detail["connections_recover_after_release"] is True
    assert detail["hold_s"] == 0.01


def test_one_explicit_rejection_among_unexplained_ones_is_not_explicit():
    """Was: rejections_explicit passed if any single refusal carried 53300."""
    driver, node = flood_driver()
    with patch("asyncpg.connect", side_effect=fake_server(other_failures=5)), \
            patch(f"{ADAPTER_MODULE}.RemoteHost", fake_ssh()):
        detail = asyncio.run(driver.inject(node))
    assert detail["rejected_other"] == 5 and detail["rejected_explicit"] == 48
    assert detail["rejections_explicit"] is False


def test_superuser_slot_is_not_measured_without_a_superuser_probe():
    """Was: fell back to connecting as the harness role -- a non-superuser -- and passed."""
    driver, node = flood_driver()
    with patch("asyncpg.connect", side_effect=fake_server()), \
            patch(f"{ADAPTER_MODULE}.RemoteHost", fake_ssh(fail=True)):
        detail = asyncio.run(driver.inject(node))
    assert detail["superuser_slot_honoured"] is None
    with patch("asyncpg.connect", side_effect=fake_server()), \
            patch(f"{ADAPTER_MODULE}.RemoteHost", fake_ssh(rolsuper="f")):
        detail = asyncio.run(driver.inject(node))
    assert detail["superuser_slot_honoured"] is None


def test_preflight_refuses_a_flood_without_a_duration():
    from resilience_tests.execution.injectors.base import DriverNotAvailable
    driver, node = flood_driver()
    driver.duration_s = None
    host = fake_ssh()

    async def healthy_unit(command, *a, **k):
        if "is-active" in command:
            return RemoteResult(0, "active\n", "")
        if "postmaster.pid" in command:
            return RemoteResult(0, "4242\n", "")
        if "-p Restart" in command:
            return RemoteResult(0, "on-failure\n", "")
        return RemoteResult(0, "", "")
    host.return_value.__aenter__.return_value.run.side_effect = healthy_unit
    with patch("resilience_tests.execution.injectors.process.RemoteHost", host):
        with pytest.raises(DriverNotAvailable, match="fault.duration"):
            asyncio.run(driver.preflight(node))


def test_revert_terminates_flood_sessions_from_a_fresh_injector():
    """Was: the kill switch built a new adapter holding no connections, drained 0, and the
    ledger said reverted while the flood stayed open."""
    driver, node = flood_driver()
    with patch(f"{ADAPTER_MODULE}.RemoteHost", fake_ssh()):
        rev = asyncio.run(driver.revert(node, {}))
    assert rev["terminated_on_server"] == 53 and rev["remaining"] == 0


def test_revert_fails_loudly_if_flood_sessions_remain():
    driver, node = flood_driver()
    with patch(f"{ADAPTER_MODULE}.RemoteHost", fake_ssh(remaining="4")), \
            patch(f"{ADAPTER_MODULE}.asyncio.sleep", AsyncMock()):
        with pytest.raises(RuntimeError, match="still open"):
            asyncio.run(driver.revert(node, {}))


@pytest.mark.parametrize("generator", ["builtin", "pgbench"])
def test_nl_r_04_orchestrator_evaluation(tmp_path, monkeypatch, generator):
    """Verify NL-R-04 runs through orchestrator and evaluates rejections_explicit,
    superuser_slot_honoured, and existing_sessions_unaffected."""
    from tests.fakes.pgbench_env import FAKE_PGBENCH, FakeDB

    monkeypatch.setattr(Engine, "pgbench_db", FakeDB(tmp_path / "fakedb") if generator == "pgbench" else None)
    if generator == "pgbench":
        Engine.pgbench_db.flag("commit-delay", value="0.02")
        monkeypatch.setenv("FAKE_PGBENCH_DB", str(Engine.pgbench_db.path))
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
        "workload": base_profile.workload.model_copy(update={"generator": generator, "pgbench_bin": FAKE_PGBENCH}),
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
        assert measured["connections_recover_after_release"] is True
        assert measured["rpo_txn"] == 0
