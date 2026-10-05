"""NL-M-05 -- Idle-in-transaction blocking vacuum (Framework §10.7).

Each acceptance path is accepted only on evidence the harness observed:
- Path A: the session ended AND the server said it was the idle-in-transaction timeout (its
  log line, or SQLSTATE 25P03). A session that merely vanished is NOT_MEASURED.
- Path B: VACUUM, run while the session is open, could not remove dead tuples at a cutoff no
  newer than the session's xid (vacuum_blocked) AND monitoring alerted (bloat_alert_fired).
  No alert source is connected, so bloat_alert_fired is NOT_MEASURED and path B cannot pass.
The fault is confirmed by the server at injection, or the run aborts.
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
    assert ("idle_in_transaction_session_timeout_enforced == true or "
            "(vacuum_blocked == true and bloat_alert_fired == true)") in accept_text
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


def _run_nlm05(env):
    item = RunPlanItem(scenario=scenario("NL-M-05"), env_class=env.env_class,
                       role="standalone", node=env.nodes[0])
    return asyncio.run(TestOrchestrator(item, env, RunOptions()).run())


async def _lingering(self, pid: int | None = None):
    return {"pid": pid or 8888, "terminated_by_timeout": False, "still_idle": True,
            "backend_xmin": "5000", "age_s": 75.0}


def test_nl_m_05_path_a_needs_evidence_of_why_the_session_ended(env, monkeypatch):
    """Was: 'backend gone and connection closed' was read as the timeout firing. Anything can
    end a session; without the server's own reason the cause is unknown."""
    async def vanished(self, pid=None):
        return {"pid": 8888, "terminated_by_timeout": True, "still_idle": False, "termination_sqlstate": None}

    monkeypatch.setattr(OutageAdapter, "check_idle_transaction", vanished)
    results = _run_nlm05(env)
    assert results["status"] == "failed", why(results)
    assert str(results["measured"]["idle_in_transaction_session_timeout_enforced"]) == "NOT_MEASURED"
    assert "cause is unknown" in results["facts"]["not_measured"]["idle_in_transaction_session_timeout_enforced"]


def test_nl_m_05_path_b_cannot_pass_without_an_alert_source(env, monkeypatch):
    """Was: the harness's own dead_tuple_ratio >= 0.20 was reported as 'a bloat alert fired'.
    Now vacuum blocking is proven, but an alert is not something the harness can see."""
    monkeypatch.setattr(OutageAdapter, "check_idle_transaction", _lingering)
    results = _run_nlm05(env)
    m = results["measured"]
    assert m["idle_in_transaction_session_timeout_enforced"] is False
    assert m["vacuum_blocked"] is True and m["dead_tuples_not_removable"] == 4200
    assert str(m["bloat_alert_fired"]) == "NOT_MEASURED"
    assert results["status"] == "failed", why(results)
    (path,) = [r for r in results["verdict"]["results"] if "vacuum_blocked" in r["predicate"]]
    assert path["outcome"] == "not_measured" and "alert source" in path["reason"]


def test_nl_m_05_vacuum_that_removed_everything_was_not_blocked(env, monkeypatch):
    async def clean_vacuum(self):
        return {"supported": True, "dead_not_removable": 0, "removable_cutoff": 6000}

    monkeypatch.setattr(OutageAdapter, "check_idle_transaction", _lingering)
    monkeypatch.setattr(OutageAdapter, "probe_vacuum_horizon", clean_vacuum)
    results = _run_nlm05(env)
    assert results["measured"]["vacuum_blocked"] is False
    (path,) = [r for r in results["verdict"]["results"] if "vacuum_blocked" in r["predicate"]]
    assert path["outcome"] == "fail"


def test_nl_m_05_a_horizon_held_by_something_else_is_not_this_fault(env, monkeypatch):
    """Dead tuples kept at a cutoff NEWER than the idle session's xid are being held by some
    other transaction -- the fault under test is not the cause."""
    async def other_holder(self):
        return {"supported": True, "dead_not_removable": 900, "removable_cutoff": 7000}

    monkeypatch.setattr(OutageAdapter, "check_idle_transaction", _lingering)
    monkeypatch.setattr(OutageAdapter, "probe_vacuum_horizon", other_holder)
    results = _run_nlm05(env)
    assert results["measured"]["vacuum_blocked"] is False
    assert results["facts"]["vacuum_horizon_held_by_idle_session"] is False


def test_nl_m_05_fails_closed_when_neither_defense_triggers(env, monkeypatch):
    """If neither the timeout is enforced nor a bloat alert fires, the system failed
    to defend against vacuum starvation and MUST fail closed."""
    async def no_timeout(self, pid: int | None = None):
        return {"pid": pid or 8888, "terminated_by_timeout": False, "still_idle": True}

    async def no_bloat_alert(self):
        return {"dead_tuple_ratio": 0.05, "unvacuumed_dead_tuples": 10, "tuple_bloat_ratio": 1.05,
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
    assert "bloat alert fired: NOT_MEASURED" in summary
    assert "tuning" not in summary.lower()


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


def test_evaluate_vacuum_bloat_reports_counters_and_never_an_alert():
    """Planner statistics are context. The adapter must not turn a ratio into an 'alert'."""

    async def _test():
        adapter = PostgreSQLAdapter(_node())
        conn = AsyncMock()
        conn.fetchrow = AsyncMock(return_value={"live_tup": 2000, "dead_tup": 6000})
        conn.fetchval = AsyncMock(return_value=599.0)
        conn.close = AsyncMock()
        adapter._connect = AsyncMock(return_value=conn)
        result = await adapter.evaluate_vacuum_bloat()
        assert result["dead_tuple_ratio"] == 0.75 and result["tuple_bloat_ratio"] == 4.0
        assert "bloat_alert_fired" not in result

    asyncio.run(_test())


def test_settings_are_observed_never_written():
    """Was: every non-NL-C-05 run ALTER SYSTEM RESET autovacuum_naptime,
    autovacuum_vacuum_cost_delay and idle_in_transaction_session_timeout -- silently changing
    the very setting NL-M-05 is meant to observe."""

    async def _test():
        adapter = PostgreSQLAdapter(_node())
        conn = AsyncMock()
        conn.fetchval = AsyncMock(return_value="30s")
        conn.close = AsyncMock()
        adapter._connect = AsyncMock(return_value=conn)
        assert await adapter.observe_fault_settings("idle_in_transaction") == {
            "idle_in_transaction_session_timeout": "30s"}
        assert await adapter.observe_fault_settings("process_kill") == {}
        assert conn.execute.call_count == 0
        assert not hasattr(adapter, "cleanup_leftover_configuration")
        assert not hasattr(adapter, "configure_for_scenario")

    asyncio.run(_test())


def test_nl_m_05_is_one_injection_through_the_injector(env, monkeypatch):
    """The fault goes through the FaultInjector like every other fault (Arch §5), and the
    injector applies it on the run's own adapter -- exactly one idle session, the one the run
    observes. (Was: the orchestrator injected through the adapter and bypassed the injector,
    with a fallback that could open a second, unmeasured session.)"""
    injected: list[Any] = []
    real = OutageAdapter.inject_idle_transaction

    async def counting(self):
        injected.append(self)
        return await real(self)

    monkeypatch.setattr(OutageAdapter, "inject_idle_transaction", counting)
    item = RunPlanItem(scenario=scenario("NL-M-05"), env_class=env.env_class,
                       role="standalone", node=env.nodes[0])
    orch = TestOrchestrator(item, env, RunOptions())
    results = asyncio.run(orch.run())
    assert results["status"] == "passed", why(results)
    assert injected == [orch.adapter]
    # and cleanup reverted it with that same adapter, so the same session is the one ended
    (revert,) = [r for r in FakeFault.reverts if r["fault_type"] == "idle_in_transaction"]
    assert revert["adapter"] is orch.adapter


def test_nl_m_05_aborts_when_the_injection_did_not_land(env, monkeypatch):
    """Nothing is scored against a fault that was never confirmed by the server."""
    async def failed_injection(self):
        return {"supported": False, "error": "simulated injection failure"}

    monkeypatch.setattr(OutageAdapter, "inject_idle_transaction", failed_injection)
    results = _run_nlm05(env)
    assert results["status"] == "aborted", why(results)
    assert "idle_in_transaction fault did not land" in results["error"]
    assert results["verdict"] is None
    # the outstanding entry was still reverted by cleanup, not left for the next run
    from resilience_tests.control.killswitch import ledger_for
    assert not ledger_for(env).outstanding()


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


def test_inject_idle_transaction_rolls_back_and_does_not_fall_back(monkeypatch):
    """Was: on any error a `nohup ... sleep 7200 | psql` session was started over SSH, holding
    the horizon for up to 2 h under a shell PID nothing could later find or end."""

    async def _test():
        adapter = PostgreSQLAdapter(_node())
        mock_conn = AsyncMock()
        mock_conn.fetchval = AsyncMock(return_value=1234)
        mock_conn.execute = AsyncMock(side_effect=[None, RuntimeError("connection dropped"), None])
        mock_conn.is_closed = MagicMock(return_value=False)
        mock_conn.close = AsyncMock()
        adapter._connect = AsyncMock(return_value=mock_conn)
        monkeypatch.setattr("resilience_tests.adapters.postgresql.adapter.RemoteHost",
                            MagicMock(side_effect=AssertionError("no SSH fallback")))

        result = await adapter.inject_idle_transaction()
        assert result["supported"] is False and "connection dropped" in result["error"]
        mock_conn.execute.assert_any_call("ROLLBACK")
        mock_conn.close.assert_awaited_once()
        assert adapter._idle_conn is None

    asyncio.run(_test())


def test_inject_idle_transaction_is_confirmed_from_a_separate_connection():
    """The session must be SEEN idle in transaction holding an xid, from another connection."""
    from resilience_tests.adapters.postgresql.adapter import IDLE_SESSION_APPLICATION_NAME

    async def _test():
        adapter = PostgreSQLAdapter(_node())
        idle = AsyncMock()
        idle.fetchval = AsyncMock(return_value=8888)
        stat = AsyncMock()
        stat.fetchrow = AsyncMock(side_effect=[
            {"state": "active", "backend_xid": 5000, "backend_xmin": None, "xact_age_s": 0.0},
            {"state": "idle in transaction", "backend_xid": 5000, "backend_xmin": None, "xact_age_s": 0.1},
        ])
        adapter._connect = AsyncMock(side_effect=[idle, stat])
        result = await adapter.inject_idle_transaction()
        assert result["supported"] is True and result["backend_xid"] == 5000
        assert stat.fetchrow.await_count == 2
        assert idle.fetchrow.await_count == 0                 # never queried from itself
        settings = adapter._connect.await_args_list[0].kwargs["server_settings"]
        assert settings == {"application_name": IDLE_SESSION_APPLICATION_NAME}

    asyncio.run(_test())


def test_cleanup_ends_only_the_harness_session(monkeypatch):
    """Was: `kill -9 <ledger pid>` as root (a PID that may since belong to anything) and
    pg_terminate_backend on EVERY idle-in-transaction session in the cluster."""
    from resilience_tests.adapters.postgresql.adapter import IDLE_SESSION_APPLICATION_NAME
    import resilience_tests.execution.injectors.process as process_mod
    from resilience_tests.execution.injectors.process import OsSshProcessDriver
    from tests.test_measurement_and_safety import NODE, PROFILE, use_host

    calls = use_host(monkeypatch, process_mod, {"pg_terminate_backend": "1"})
    detail = asyncio.run(OsSshProcessDriver(PROFILE, "idle_in_transaction").revert(NODE, {"inject": {"pid": 8888}}))
    assert detail["terminated"] == "1"
    (cmd,) = calls
    assert IDLE_SESSION_APPLICATION_NAME in cmd and "kill" not in cmd.replace("pg_terminate_backend", "")
    assert "idle in transaction" not in cmd

    async def _test():
        adapter = PostgreSQLAdapter(_node())
        stat = AsyncMock()
        stat.fetchval = AsyncMock(return_value=1)
        adapter._connect = AsyncMock(return_value=stat)
        adapter._idle_pid = 8888
        detail = await adapter.close_idle_transaction()
        assert detail["terminated"] == 1
        assert stat.fetchval.await_args.args[1] == IDLE_SESSION_APPLICATION_NAME

    asyncio.run(_test())


def test_probe_vacuum_horizon_reads_what_vacuum_could_not_remove():
    async def _test():
        adapter = PostgreSQLAdapter(_node())
        conn = AsyncMock()
        conn.add_log_listener = MagicMock()

        async def execute(sql):
            listener = conn.add_log_listener.call_args.args[0]
            listener(conn, MagicMock(message='finished vacuuming "resilience.resilience.churn": index scans: 0\n'
                                             "tuples: 12 removed, 2000 remain, 5310 are dead but not yet removable\n"
                                             "removable cutoff: 74121, which was 6022 XIDs old when operation ended",
                                     detail=None))
        conn.execute = execute
        adapter._connect = AsyncMock(return_value=conn)
        result = await adapter.probe_vacuum_horizon()
        assert result["dead_not_removable"] == 5310 and result["removable_cutoff"] == 74121

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
            "tuple_bloat_ratio": 1.05,
            "oldest_transaction_age_s": 0.0,
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
    from resilience_tests.adapters.postgresql.adapter import parse_pg_interval_s

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



