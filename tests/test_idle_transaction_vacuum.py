"""NL-M-05 -- Idle-in-transaction blocking vacuum: production-level resilience testing,
vacuum horizon pinning, timeout enforcement, and operational bloat alerting.

Framework §10.7, Arch §5, Arch §8, Arch §10.2:
An industry-standard resilience test harness validating production database systems
against lingering idle transactions must eliminate guesswork:
1. Dirties relations continuously with high-concurrency transactional load and churn.
2. Injects an open transaction that acquires an active snapshot and pins backend_xmin,
   holding back the global vacuum horizon and blocking autovacuum from reclaiming dead tuples.
3. Tests both standard production defenses:
   - Path A: idle_in_transaction_session_timeout is enforced by the database engine,
     terminating the lingering session with FATAL (SQLSTATE 25P03/57P01) and unblocking vacuum.
   - Path B: When timeout is unconfigured (0), unvacuumed dead tuples accumulate under write load;
     telemetry probes detect dead-tuple accumulation and trigger an operational bloat alert
     (dead_tuple_ratio >= 0.20 per Framework §10.7 NL-M-04).
4. Verifies zero transaction data loss (rpo_txn == 0) and zero relation/index corruption
   (structural_integrity_errors == 0, corruption_count == 0).
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from catalog.schema import load_catalog
from resilience_tests.adapters.base import (
    Capability,
    DatabaseSession,
    IntegrityResult,
    TransactionOutcome,
    register_adapter,
)
from resilience_tests.adapters.postgresql.adapter import PostgreSQLAdapter
from resilience_tests.analysis.report import render_summary
from resilience_tests.control.matrix import RunPlanItem
from resilience_tests.control.orchestrator import RunOptions, TestOrchestrator
from resilience_tests.control.profile import Node
from resilience_tests.execution.injectors.base import FaultInjector
from tests.test_adapter_seam import FakeAdapter
from tests.test_orchestrator import BASE_PROFILE, Engine, FakeFault, OutageAdapter, OutageSession, env, scenario, why


def test_nl_m_05_catalog_specification():
    """Verify that NL-M-05 adheres to Framework §10.7 / Arch §5 / Arch §8 requirements."""
    catalog = load_catalog()
    assert "NL-M-05" in catalog.scenarios
    sc = catalog.scenarios["NL-M-05"]

    assert sc.id == "NL-M-05"
    assert sc.name == "Idle-in-transaction blocking vacuum"
    assert sc.tier == "node_local"
    assert sc.category == "NL-M"
    assert sc.priority == "P1"
    assert sc.fault.type == "idle_in_transaction"
    assert sc.fault.driver == "os_ssh"
    assert sc.workload.profile == "mixed"
    assert sc.workload.transaction_markers is True

    # Required capabilities per Arch §8
    assert "transactional_markers" in sc.requires
    assert "workload_churn" in sc.requires
    assert "structural_integrity_check" in sc.requires

    # Acceptance criteria per Framework §10.7 table
    accept_text = " ".join(sc.accept)
    assert "idle_in_transaction_session_timeout_enforced == true or bloat_alert_fired == true" in accept_text
    assert "rpo_txn == 0" in accept_text
    assert "structural_integrity_errors == 0" in accept_text
    assert "corruption_count == 0" in accept_text


def test_nl_m_05_path_a_timeout_enforced(env):
    """End-to-end execution of Path A: idle_in_transaction_session_timeout terminates the session."""
    item = RunPlanItem(
        scenario=scenario("NL-M-05"),
        env_class=env.env_class,
        role="standalone",
        node=env.nodes[0],
    )
    results = asyncio.run(TestOrchestrator(item, env, RunOptions()).run())

    assert results["status"] == "passed", why(results)
    m = results["measured"]

    # Production benchmark acceptance criteria
    assert m["rpo_txn"] == 0
    assert m["structural_integrity_errors"] == 0
    assert m["corruption_count"] == 0
    assert m["idle_in_transaction_session_timeout_enforced"] is True

    # Idle transaction telemetry facts
    facts = results["facts"]
    assert "idle_transaction" in facts
    assert facts["idle_transaction"].get("supported") is True
    assert facts["idle_transaction"].get("pid") == 8888
    assert "idle_transaction_check" in facts
    assert facts["idle_transaction_check"].get("terminated_by_timeout") is True


def test_nl_m_05_path_b_bloat_alert_fires(env, monkeypatch):
    """End-to-end execution of Path B: timeout is disabled; dead tuples accumulate and bloat alert fires."""
    # Simulate timeout NOT enforced (session still lingering)
    async def lingering_check(self, pid: int | None = None):
        return {
            "pid": pid or 8888,
            "terminated_by_timeout": False,
            "still_idle": True,
            "backend_xmin": "5000",
            "age_s": 75.0,
        }

    # Simulate blocked vacuum causing dead-tuple ratio to exceed 20% threshold
    async def bloat_alert_check(self):
        return {
            "dead_tuple_ratio": 0.28,
            "unvacuumed_dead_tuples": 450,
            "live_tuples": 1150,
            "oldest_transaction_age_s": 75.0,
            "bloat_alert_fired": True,
            "bloat_ratio": 1.35,
        }

    monkeypatch.setattr(OutageAdapter, "check_idle_transaction", lingering_check)
    monkeypatch.setattr(OutageAdapter, "evaluate_vacuum_bloat", bloat_alert_check)

    item = RunPlanItem(
        scenario=scenario("NL-M-05"),
        env_class=env.env_class,
        role="standalone",
        node=env.nodes[0],
    )
    results = asyncio.run(TestOrchestrator(item, env, RunOptions()).run())

    assert results["status"] == "passed", why(results)
    m = results["measured"]

    # Path B criteria
    assert m["idle_in_transaction_session_timeout_enforced"] is False
    assert m["bloat_alert_fired"] is True
    assert m["dead_tuple_ratio"] == 0.28
    assert m["unvacuumed_dead_tuples"] == 450
    assert m["rpo_txn"] == 0
    assert m["structural_integrity_errors"] == 0
    assert m["corruption_count"] == 0


def test_nl_m_05_fails_closed_when_neither_defense_triggers(env, monkeypatch):
    """If neither the timeout is enforced nor a bloat alert fires, the system failed
    to defend against vacuum starvation and MUST fail closed."""
    async def no_timeout(self, pid: int | None = None):
        return {"pid": pid or 8888, "terminated_by_timeout": False, "still_idle": True}

    async def no_bloat_alert(self):
        return {"dead_tuple_ratio": 0.05, "unvacuumed_dead_tuples": 10, "bloat_alert_fired": False,
                "oldest_transaction_age_s": 10.0}

    monkeypatch.setattr(OutageAdapter, "check_idle_transaction", no_timeout)
    monkeypatch.setattr(OutageAdapter, "evaluate_vacuum_bloat", no_bloat_alert)

    item = RunPlanItem(
        scenario=scenario("NL-M-05"),
        env_class=env.env_class,
        role="standalone",
        node=env.nodes[0],
    )
    results = asyncio.run(TestOrchestrator(item, env, RunOptions()).run())

    assert results["status"] == "failed"
    assert results["verdict"]["passed"] is False
    assert any(r["outcome"] != "pass" and "idle_in_transaction_session_timeout_enforced" in r["predicate"]
               for r in results["verdict"]["results"])


def test_nl_m_05_fails_closed_on_data_loss(env, monkeypatch):
    """If acknowledged transactions are lost during or after the fault, NL-M-05 must fail."""
    import resilience_tests.control.orchestrator as orch
    from resilience_tests.execution.workload.markers import MarkerDiff

    real_diff = orch.diff_from_journals

    def lost_diff(run_dir, ids):
        d, torn = real_diff(run_dir, ids)
        lost_set = frozenset(d.lost | {"lost-uuid-0001"})
        return MarkerDiff(
            written=d.written,
            acked=d.acked,
            in_db=d.in_db,
            lost=lost_set,
            indeterminate=d.indeterminate,
            indeterminate_committed=d.indeterminate_committed,
            phantom=d.phantom,
            unjournalled_ack=d.unjournalled_ack,
        ), torn

    monkeypatch.setattr(orch, "diff_from_journals", lost_diff)

    item = RunPlanItem(
        scenario=scenario("NL-M-05"),
        env_class=env.env_class,
        role="standalone",
        node=env.nodes[0],
    )
    results = asyncio.run(TestOrchestrator(item, env, RunOptions()).run())

    assert results["status"] == "failed"
    assert results["measured"]["rpo_txn"] > 0
    assert any(r["outcome"] != "pass" and "rpo_txn == 0" in r["predicate"]
               for r in results["verdict"]["results"])


def test_nl_m_05_fails_closed_on_corruption(env, monkeypatch):
    """If relation corruption is detected, NL-M-05 must fail closed."""
    async def corrupted_integrity(self, timeout_s: float):
        return IntegrityResult(structural_errors=3, checksum_failures=1)

    monkeypatch.setattr(OutageAdapter, "integrity_check", corrupted_integrity)

    item = RunPlanItem(
        scenario=scenario("NL-M-05"),
        env_class=env.env_class,
        role="standalone",
        node=env.nodes[0],
    )
    results = asyncio.run(TestOrchestrator(item, env, RunOptions()).run())

    assert results["status"] == "failed"
    assert results["measured"]["structural_integrity_errors"] == 3
    assert results["measured"]["corruption_count"] == 4
    assert any(r["outcome"] != "pass" and "structural_integrity_errors == 0" in r["predicate"]
               for r in results["verdict"]["results"])


def test_nl_m_05_summary_report_rendering(env):
    """Summary rendering format must present the idle transaction vacuum blocking telemetry."""
    item = RunPlanItem(
        scenario=scenario("NL-M-05"),
        env_class=env.env_class,
        role="standalone",
        node=env.nodes[0],
    )
    results = asyncio.run(TestOrchestrator(item, env, RunOptions()).run())
    summary = render_summary(results)

    assert "idle-in-transaction vacuum blocking (NL-M-05):" in summary
    assert "idle backend pid: 8888" in summary
    assert "backend_xmin: 5000" in summary
    assert "timeout enforced: True" in summary
    assert "bloat alert fired: False" in summary
    assert "note: baseline, TPS floor, and SLO recovery were measured under this tuning" not in summary


def test_nl_m_05_cleanup_closes_idle_transaction(env):
    """Cleanup must call close_idle_transaction to guarantee no lingering session pins xmin."""
    closed = False

    class TrackingAdapter(OutageAdapter):
        async def close_idle_transaction(self):
            nonlocal closed
            closed = True
            return {"closed": True}

    register_adapter(TrackingAdapter)
    profile = env.model_copy(update={
        "database": env.database.model_copy(update={"engine": "orch-fake"})
    })
    item = RunPlanItem(
        scenario=scenario("NL-M-05"),
        env_class=profile.env_class,
        role="standalone",
        node=profile.nodes[0],
    )
    orch = TestOrchestrator(item, profile, RunOptions())
    results = asyncio.run(orch.run())

    assert results["status"] == "passed"
    assert closed is True


def _node() -> Node:
    return Node(
        name="test-node",
        role="standalone",
        topology_role="primary",
        ssh={"host": "127.0.0.1", "port": 22, "user": "test"},
        db={"host": "127.0.0.1", "port": 5432, "dbname": "test", "user": "test"},
        client={"host": "127.0.0.1", "port": 5432, "dbname": "test", "user": "test"},
        pgdata="/data",
        pg_bin="/bin",
        os_user="postgres",
        service="postgresql.service",
        log_file="/data/logfile",
    )


def test_check_idle_transaction_fails_closed_when_no_session_was_established():
    """A failed/absent injection must NEVER read as `idle_in_transaction_session_timeout` being
    enforced: `terminated_by_timeout=True` with no session is a fail-open path A pass."""

    async def _test():
        adapter = PostgreSQLAdapter(_node())
        result = await adapter.check_idle_transaction(pid=None)
        assert result["terminated_by_timeout"] is False
        assert result["pid"] is None
        assert "no idle transaction session was established" in result["note"]
        # and an adapter that never recorded a session takes the same guarded path
        result2 = await adapter.check_idle_transaction()
        assert result2["terminated_by_timeout"] is False

    asyncio.run(_test())


def test_check_idle_transaction_absent_backend_without_closed_session_is_not_timeout_enforcement(monkeypatch):
    """An absent backend is only evidence of timeout enforcement when the harness-held session
    existed and is now closed. The SSH-fallback shell pid (never a backend pid) must fail closed."""

    async def _test():
        adapter = PostgreSQLAdapter(_node())
        adapter._idle_conn = None

        stat_row = AsyncMock(return_value=None)
        stat_conn = AsyncMock()
        stat_conn.fetchrow = stat_row
        stat_conn.close = AsyncMock()
        adapter._connect = AsyncMock(return_value=stat_conn)

        result = await adapter.check_idle_transaction(pid=7777)
        assert result["terminated_by_timeout"] is False
        assert "did not close" in result["note"]
        assert stat_row.await_count == 1

    asyncio.run(_test())


def test_evaluate_vacuum_bloat_alert_is_ratio_driven_not_age_driven(monkeypatch):
    """The NL-M-05 acceptance is `timeout enforced OR bloat alert fires`, and NL-M-04 keys the
    alert to dead_tuple_ratio >= 0.20. An old idle session WITH ZERO dead tuples must not fire
    the alert: that would pass the guard clause on the injected fault's age alone."""

    async def _test():
        adapter = PostgreSQLAdapter(_node())
        conn = AsyncMock()
        conn.fetchrow = AsyncMock(return_value={"live_tup": 0, "dead_tup": 0})
        conn.fetchval = AsyncMock(return_value=599.0)  # a very old idle-in-transaction session
        conn.close = AsyncMock()
        adapter._connect = AsyncMock(return_value=conn)
        result = await adapter.evaluate_vacuum_bloat()
        assert result["dead_tuple_ratio"] == 0.0
        assert result["oldest_transaction_age_s"] == 599.0
        assert result["bloat_alert_fired"] is False

    asyncio.run(_test())


def test_nl_m_05_tuning_is_observed_not_applied(monkeypatch):
    """NL-M-05 must SHOW the deployment's idle_in_transaction_session_timeout and STOP. It must
    not be recorded in _scenario_config_applied (which would make cleanup `ALTER SYSTEM RESET` a
    value the harness never wrote), and it is not 'tuning applied'."""

    async def _test():
        adapter = PostgreSQLAdapter(_node())
        conn = AsyncMock()
        conn.fetchval = AsyncMock(return_value="0")
        conn.close = AsyncMock()
        adapter._connect = AsyncMock(return_value=conn)

        tuning = await adapter.configure_for_scenario("NL-M-05")
        assert tuning == {"observed_idle_in_transaction_session_timeout": "0"}
        assert adapter._scenario_config_applied == {}
        assert adapter._scenario_observed == {"idle_in_transaction_session_timeout": "0"}
        assert conn.execute.call_count == 0  # nothing written

        # restore must be a no-op: nothing was applied
        await adapter.restore_scenario_configuration()
        assert conn.execute.call_count == 0

    asyncio.run(_test())


def test_nl_m_05_is_a_single_injection_not_a_double(env, monkeypatch):
    """The generic SSH injector and the adapter must both target the SAME session. When the
    adapter injects a supported idle transaction, the orchestrator must NOT also fire the
    os_ssh injector -- two concurrent idle transactions would be an unmeasured second fault
    (Events showed pids 1866342 AND 1866395 in the field run)."""

    import resilience_tests.control.orchestrator as orch
    calls = []

    class RecordingFault(FakeFault):
        async def inject(self, node):
            calls.append(self.fault_type)
            return {"action": self.fault_type}

    monkeypatch.setattr(orch, "resolve", lambda fault, prof: RecordingFault(prof, fault.type))

    item = RunPlanItem(
        scenario=scenario("NL-M-05"),
        env_class=env.env_class,
        role="standalone",
        node=env.nodes[0],
    )
    results = asyncio.run(TestOrchestrator(item, env, RunOptions()).run())

    assert results["status"] == "passed", why(results)
    assert calls == [], f"generic SSH injector must not double-inject when the adapter session exists: {calls}"


def test_nl_m_05_fails_closed_when_injection_fails_even_if_bloat_alert_fires(env, monkeypatch):
    """Path B must be gated on the fault actually existing. If injection fails (supported: False)
    but the workload alone bloats a table past 0.20, the test MUST fail closed."""
    async def failed_injection(self):
        return {"supported": False, "error": "simulated injection failure"}

    async def firing_bloat_alert(self):
        return {
            "dead_tuple_ratio": 0.25,
            "unvacuumed_dead_tuples": 500,
            "bloat_alert_fired": True,
            "oldest_transaction_age_s": 0.0,
        }

    monkeypatch.setattr(OutageAdapter, "inject_idle_transaction", failed_injection)
    monkeypatch.setattr(OutageAdapter, "evaluate_vacuum_bloat", firing_bloat_alert)

    item = RunPlanItem(
        scenario=scenario("NL-M-05"),
        env_class=env.env_class,
        role="standalone",
        node=env.nodes[0],
    )
    results = asyncio.run(TestOrchestrator(item, env, RunOptions()).run())

    assert results["status"] == "failed"
    assert results["measured"]["bloat_alert_fired"] is False
    assert results["measured"]["idle_in_transaction_session_timeout_enforced"] is False


def test_check_idle_transaction_does_not_mutate_session_when_backend_present():
    """When pg_stat_activity shows the backend is alive, check_idle_transaction must NOT
    touch _idle_conn (e.g. running SELECT 1), which would re-snapshot the transaction and advance xmin."""
    async def _test():
        adapter = PostgreSQLAdapter(_node())
        idle_conn = AsyncMock()
        idle_conn.fetchval = AsyncMock(side_effect=AssertionError("Must not probe active held session!"))
        adapter._idle_conn = idle_conn

        stat_row = AsyncMock(return_value={
            "pid": 8888, "state": "idle in transaction", "backend_xmin": "5000",
            "xact_age_s": 10.0, "wait_event_type": "Client", "wait_event": "ClientRead"
        })
        stat_conn = AsyncMock()
        stat_conn.fetchrow = stat_row
        stat_conn.close = AsyncMock()
        adapter._connect = AsyncMock(return_value=stat_conn)

        result = await adapter.check_idle_transaction(pid=8888)
        assert result["terminated_by_timeout"] is False
        assert result["still_idle"] is True
        assert result["backend_xmin"] == "5000"
        assert idle_conn.fetchval.await_count == 0

    asyncio.run(_test())


def test_inject_idle_transaction_rolls_back_partial_connection_before_fallback(monkeypatch):
    """If an exception occurs after opening the connection, inject_idle_transaction must
    roll back and close the connection before attempting the SSH fallback."""
    from resilience_tests.execution.remote import RemoteHost

    async def _test():
        adapter = PostgreSQLAdapter(_node())
        mock_conn = AsyncMock()
        mock_conn.fetchval = AsyncMock(return_value=1234)
        mock_conn.execute = AsyncMock(return_value=None)
        # Fail when querying pg_stat_activity after BEGIN
        mock_conn.fetchrow = AsyncMock(side_effect=RuntimeError("pg_stat_activity connection dropped"))
        mock_conn.is_closed = MagicMock(return_value=False)
        mock_conn.close = AsyncMock()
        adapter._connect = AsyncMock(return_value=mock_conn)

        # Mock SSH fallback to succeed
        fake_host = AsyncMock()
        fake_host.run = AsyncMock(return_value=MagicMock(stdout="5678\n", exit_status=0))
        fake_remote = MagicMock()
        fake_remote.__aenter__ = AsyncMock(return_value=fake_host)
        fake_remote.__aexit__ = AsyncMock(return_value=False)
        monkeypatch.setattr("resilience_tests.adapters.postgresql.adapter.RemoteHost", lambda ssh: fake_remote)

        result = await adapter.inject_idle_transaction()
        assert result["supported"] is True
        assert result["pid"] == 5678
        assert result["method"] == "ssh_background"
        # The partial connection must have had ROLLBACK called and been closed
        mock_conn.execute.assert_any_call("ROLLBACK")
        mock_conn.close.assert_awaited_once()
        assert adapter._idle_conn is None

    asyncio.run(_test())


def test_evaluate_vacuum_bloat_returns_stats_timing_metadata():
    """evaluate_vacuum_bloat must include last_vacuum, last_autovacuum, last_analyze, and
    last_autoanalyze metadata from pg_stat_user_tables to determine statistics freshness."""
    async def _test():
        adapter = PostgreSQLAdapter(_node())
        conn = AsyncMock()
        conn.fetchrow = AsyncMock(return_value={
            "live_tup": 100,
            "dead_tup": 30,
            "last_vacuum": "2026-09-30 18:00:00+00",
            "last_autovacuum": "2026-09-30 18:10:00+00",
            "last_analyze": None,
            "last_autoanalyze": "2026-09-30 18:15:00+00",
        })
        conn.fetchval = AsyncMock(return_value=120.0)
        conn.close = AsyncMock()
        adapter._connect = AsyncMock(return_value=conn)

        result = await adapter.evaluate_vacuum_bloat()
        assert result["dead_tuple_ratio"] == round(30 / 130, 4)
        assert result["unvacuumed_dead_tuples"] == 30
        assert result["last_vacuum"] == "2026-09-30 18:00:00+00"
        assert result["last_autovacuum"] == "2026-09-30 18:10:00+00"
        assert result["last_analyze"] is None
        assert result["last_autoanalyze"] == "2026-09-30 18:15:00+00"

    asyncio.run(_test())


def test_nl_m_05_disclosures_and_timing_recorded(env, monkeypatch):
    """Running NL-M-05 must add planner statistics precision disclosure, record
    vacuum_bloat_timing, and include a caveat if no analyze ran during the hold."""
    async def bloat_with_timing(self):
        return {
            "dead_tuple_ratio": 0.05,
            "unvacuumed_dead_tuples": 10,
            "live_tuples": 200,
            "oldest_transaction_age_s": 0.0,
            "bloat_alert_fired": False,
            "last_autovacuum": "2026-09-30 18:10:00+00",
            "last_analyze": None,
            "last_autoanalyze": None,
        }

    monkeypatch.setattr(OutageAdapter, "evaluate_vacuum_bloat", bloat_with_timing)

    item = RunPlanItem(
        scenario=scenario("NL-M-05"),
        env_class=env.env_class,
        role="standalone",
        node=env.nodes[0],
    )
    results = asyncio.run(TestOrchestrator(item, env, RunOptions()).run())

    disclosures_text = " ".join(results["disclosures"])
    assert "dead-tuple metrics are from pg_stat_user_tables planner statistics" in disclosures_text
    assert "No analyze ran during the hold" in disclosures_text
    assert "vacuum_bloat_timing" in results["facts"]
    assert results["facts"]["vacuum_bloat_timing"]["last_autovacuum"] == "2026-09-30 18:10:00+00"


def test_integrity_file_writes_clean_sentinel_when_raw_output_empty(env, monkeypatch):
    """When pg_amcheck output is empty (clean run), integrity.txt must contain the
    'pg_amcheck run clean (exit 0)' sentinel rather than being a 0-byte file."""
    async def clean_integrity(self, timeout_s: float = 30.0):
        return IntegrityResult(
            structural_errors=0,
            checksum_failures=0,
            raw_output="",
            detail={"exit_status": 0},
        )

    monkeypatch.setattr(OutageAdapter, "integrity_check", clean_integrity)

    item = RunPlanItem(
        scenario=scenario("NL-C-01"),
        env_class=env.env_class,
        role="standalone",
        node=env.nodes[0],
    )
    results = asyncio.run(TestOrchestrator(item, env, RunOptions()).run())
    evidence_dir = Path(results["evidence_dir"])
    integrity_path = evidence_dir / "integrity.txt"
    assert integrity_path.exists()
    content = integrity_path.read_text()
    assert content == "pg_amcheck run clean (exit 0)\n"


def test_parse_pg_interval_s():
    from resilience_tests.control.orchestrator import parse_pg_interval_s

    assert parse_pg_interval_s(None) == 0.0
    assert parse_pg_interval_s("0") == 0.0
    assert parse_pg_interval_s("0s") == 0.0
    assert parse_pg_interval_s("disabled") == 0.0
    assert parse_pg_interval_s("30s") == 30.0
    assert parse_pg_interval_s("30") == 30.0
    assert parse_pg_interval_s("500ms") == 0.5
    assert parse_pg_interval_s("2min") == 120.0
    assert parse_pg_interval_s("1h") == 3600.0
    assert parse_pg_interval_s("2h") == 7200.0


def test_nl_m_05_calibrated_soak_hold_and_untestable_disclosure(env, monkeypatch):
    """When configured timeout is large (e.g. 2h), recovery soak holds for MIN_SOAK_S,
    records testable=False, and issues an explicit disclosure explaining Path B fallback."""
    item = RunPlanItem(
        scenario=scenario("NL-M-05"),
        env_class=env.env_class,
        role="standalone",
        node=env.nodes[0],
    )
    orch_inst = TestOrchestrator(item, env, RunOptions())
    orch_inst.facts["scenario_observed"] = {"idle_in_transaction_session_timeout": "2h"}

    async def run_recovery():
        orch_inst.facts["injection_id"] = "test-inj"
        orch_inst.baseline = MagicMock(tps=200, p99_ms=10.0)
        orch_inst.t0_ns = 1000
        orch_inst.injector = MagicMock()
        orch_inst.stream = MagicMock()
        orch_inst.stream.events.return_value = []
        return await orch_inst._p_recovery()

    detail = asyncio.run(run_recovery())
    assert orch_inst.facts["idle_timeout_parsed_s"] == 7200.0
    assert orch_inst.facts["idle_timeout_testable"] is False
    # Hold was clamped to available bound / min soak, not 7200s
    assert orch_inst.facts["idle_hold_s"] <= 7.0



