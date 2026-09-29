"""NL-C-02 -- Crash during checkpoint: deterministic state synchronization,
active wait event tracking, unattended crash recovery, and zero corruption.

Arch §5, Framework §10.2:
A standard resilience testing harness breaking a database system at production level
must eliminate timing guesswork. Instead of guessing sleep intervals, the harness:
1. Dirties buffers continuously with high-concurrency transactional load (oltp_write_heavy).
2. Initiates a checkpoint and deterministically monitors the checkpointer process
   until it leaves idle 'CheckpointDelay' and is actively flushing and syncing dirty buffers.
3. Instantly delivers SIGKILL to the postmaster and whole unit cgroup mid-checkpoint.
4. Verifies crash recovery starts unattended, replays WAL from the prior valid checkpoint's
   REDO point, resolves any torn or partial page writes without corruption, and preserves
   all acknowledged transactions with RPO == 0.
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
from resilience_tests.analysis.report import render_summary
from resilience_tests.control.matrix import RunPlanItem
from resilience_tests.control.orchestrator import RunOptions, TestOrchestrator
from resilience_tests.execution.injectors.base import FaultInjector
from tests.test_adapter_seam import FakeAdapter
from tests.test_orchestrator import BASE_PROFILE, Engine, OutageAdapter, OutageSession, env, scenario, why


def test_nl_c_02_catalog_specification():
    """Verify that NL-C-02 adheres to Framework §10.2 / Arch §5 catalog requirements."""
    catalog = load_catalog()
    assert "NL-C-02" in catalog.scenarios
    sc = catalog.scenarios["NL-C-02"]

    assert sc.id == "NL-C-02"
    assert sc.name == "Crash during checkpoint"
    assert sc.tier == "node_local"
    assert sc.category == "NL-C"
    assert sc.priority == "P0"
    assert sc.fault.type == "process_kill"
    assert sc.fault.driver == "os_ssh"

    # Capability requirements
    assert "transactional_markers" in sc.requires
    assert "structural_integrity_check" in sc.requires

    # Acceptance criteria per Framework §10.2 table
    accept_text = " ".join(sc.accept)
    assert "corruption_count == 0" in accept_text
    assert "starts_unattended == true" in accept_text
    assert "rpo_txn == 0" in accept_text
    assert "structural_integrity_errors == 0" in accept_text


def test_nl_c_02_deterministic_execution_and_verdict(env):
    """End-to-end execution of NL-C-02 through the orchestrator state machine."""
    item = RunPlanItem(
        scenario=scenario("NL-C-02"),
        env_class=env.env_class,
        role="standalone",
        node=env.nodes[0],
    )
    results = asyncio.run(TestOrchestrator(item, env, RunOptions()).run())

    assert results["status"] == "passed", why(results)
    m = results["measured"]

    # Production benchmark acceptance criteria
    assert m["corruption_count"] == 0
    assert m["structural_integrity_errors"] == 0
    assert m["starts_unattended"] is True
    assert m["rpo_txn"] == 0

    # Checkpoint synchronization facts
    facts = results["facts"]
    assert facts.get("checkpoint_active_at_kill") is True
    assert "checkpoint_injection" in facts
    assert facts["checkpoint_injection"].get("checkpointer_active") is True
    assert "checkpoint_verification" in facts
    assert facts["checkpoint_verification"].get("checkpoint_aborted") is True


def test_nl_c_02_fails_if_checkpoint_not_active(env, monkeypatch):
    """If the checkpointer was not actively running at the moment of SIGKILL,
    the scenario must abort and fail closed rather than give a false pass."""
    async def inactive_checkpoint(self, timeout_s: float = 10.0):
        return {"checkpointer_active": False, "method": "test"}

    monkeypatch.setattr(OutageAdapter, "trigger_checkpoint_and_await_active", inactive_checkpoint)

    item = RunPlanItem(
        scenario=scenario("NL-C-02"),
        env_class=env.env_class,
        role="standalone",
        node=env.nodes[0],
    )
    results = asyncio.run(TestOrchestrator(item, env, RunOptions()).run())

    assert results["status"] == "aborted", why(results)
    assert "checkpointer was not active" in (results.get("error") or "")


def test_nl_c_02_fails_on_structural_corruption(env, monkeypatch):
    """If crash recovery leaves page or index corruption, NL-C-02 must fail closed."""
    async def corrupt_integrity(self, timeout_s: float):
        return IntegrityResult(structural_errors=3, checksum_failures=0)

    monkeypatch.setattr(OutageAdapter, "integrity_check", corrupt_integrity)

    item = RunPlanItem(
        scenario=scenario("NL-C-02"),
        env_class=env.env_class,
        role="standalone",
        node=env.nodes[0],
    )
    results = asyncio.run(TestOrchestrator(item, env, RunOptions()).run())

    assert results["status"] == "failed", why(results)
    assert results["measured"]["structural_integrity_errors"] == 3
    assert results["measured"]["corruption_count"] == 3


def test_nl_c_02_summary_report_formatting():
    """Verify that checkpoint facts are rendered truthfully in summary.txt.
    Must never report fake fallback strings like 'wait event: active' when events are None."""
    sample_results = {
        "scenario": {"id": "NL-C-02", "name": "Crash during checkpoint"},
        "run_id": "test-run-123",
        "environment": {"profile": "e2-dedicated-vm", "class": "E2"},
        "target": {"node": "shaktidb-standalone"},
        "status": "passed",
        "phases": [{"phase": "fault_inject", "outcome": "ok", "duration_s": 0.5}],
        "facts": {
            "checkpoint_injection": {
                "checkpointer_active": True,
                "checkpointer_pid": 48102,
                "wait_event": "CheckpointWriteDelay",
                "wait_event_type": "Timeout",
                "buffers_written_during_cp": 42,
                "io_writes_during_cp": 15,
            }
        },
        "measured": {"checkpoint_active_at_kill": True, "corruption_count": 0},
    }
    summary = render_summary(sample_results)
    assert "checkpoint fault injection (NL-C-02):" in summary
    assert "checkpointer active at kill: True" in summary
    assert "checkpointer pid: 48102" in summary
    assert "wait event: CheckpointWriteDelay (Timeout)" in summary
    assert "stat counter delta (pg_stat_checkpointer.buffers_written): 42" in summary
    assert "io writes delta (pg_stat_io): 15" in summary

    # Case 2: When wait events are null, must report truthfully as none, NEVER fake 'active'
    sample_null_events = {
        "scenario": {"id": "NL-C-02", "name": "Crash during checkpoint"},
        "run_id": "test-run-124",
        "environment": {"profile": "e2-dedicated-vm", "class": "E2"},
        "target": {"node": "shaktidb-standalone"},
        "status": "passed",
        "facts": {
            "checkpoint_injection": {
                "checkpointer_active": True,
                "checkpointer_pid": 48102,
                "wait_event": None,
                "wait_event_type": None,
            }
        },
    }
    summary_null = render_summary(sample_null_events)
    assert "wait event: none (checkpointer running, not waiting)" in summary_null
    assert "wait event: active" not in summary_null  # Fallback word 'active' must NEVER be printed


def test_postgres_adapter_checkpoint_methods_unit():
    """Unit test for PostgreSQLAdapter checkpoint methods with mock connection.

    The mock connection sequence faithfully models the real PostgreSQL wait event types:
    - CheckpointerMain: Activity (idle main loop)
    - CheckpointDelay: Activity (idle inter-checkpoint sleep)
    - CheckpointWriteDelay: Timeout (throttling between buffer writes)
    - CheckpointSync: IO (syncing relation files)
    - DataFileWrite: IO (writing data pages)
    """
    async def _test():
        from resilience_tests.adapters.postgresql.adapter import PostgreSQLAdapter
        from resilience_tests.control.profile import Node

        node = Node(
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
        adapter = PostgreSQLAdapter(node)

        # --- trigger_checkpoint_and_await_active ---
        # Connection 1: initial checkpointer lookup + baseline checkpoint
        mock_conn_init = AsyncMock()
        mock_conn_init.fetchrow.side_effect = [
            {"pid": 1234, "wait_event_type": "Activity", "wait_event": "CheckpointerMain"},
            # pg_control_checkpoint baseline (asyncpg returns pg_lsn as int)
            {"checkpoint_lsn": 23068672, "redo_lsn": 23068672, "checkpoint_time": "2026-09-28 12:00:00"},
        ]

        # Connection 2: CHECKPOINT execution (async)
        mock_conn_cp = AsyncMock()

        # Connection 3: polling loop — first sees idle CheckpointerMain (must skip),
        # then CheckpointDelay (must also skip), then active CheckpointWriteDelay (must break).
        mock_conn_poll = AsyncMock()
        mock_conn_poll.fetchrow.side_effect = [
            # Poll 1: still in CheckpointerMain (idle) — must NOT break
            {"wait_event_type": "Activity", "wait_event": "CheckpointerMain"},
            # Poll 2: now in CheckpointDelay (idle) — must NOT break
            {"wait_event_type": "Activity", "wait_event": "CheckpointDelay"},
            # Poll 3: actively writing — CheckpointWriteDelay is Timeout type, not IO
            {"wait_event_type": "Timeout", "wait_event": "CheckpointWriteDelay"},
        ]

        connect_returns = [mock_conn_init, mock_conn_cp, mock_conn_poll]
        adapter._connect = AsyncMock(side_effect=connect_returns)

        detail = await adapter.trigger_checkpoint_and_await_active(timeout_s=2.0)
        assert detail["checkpointer_active"] is True
        assert detail["checkpointer_pid"] == 1234
        assert detail["wait_event"] == "CheckpointWriteDelay"
        assert detail["wait_event_type"] == "Timeout"
        # Verify the poll actually polled 3 times (skipped 2 idle states)
        assert mock_conn_poll.fetchrow.call_count == 3

        # --- verify_checkpoint_aborted ---
        # After crash recovery, pg_control_checkpoint shows the end-of-recovery checkpoint
        # with a HIGHER checkpoint_lsn than the pre-kill baseline.
        mock_conn_verify = AsyncMock()
        mock_conn_verify.fetchrow.return_value = {
            "checkpoint_lsn": 23134208,   # higher than baseline 23068672
            "redo_lsn": 23100000,         # also advanced past baseline
            "checkpoint_time": "2026-09-28 12:00:05",
        }
        adapter._connect = AsyncMock(return_value=mock_conn_verify)
        verify = await adapter.verify_checkpoint_aborted()
        assert verify["checkpoint_aborted"] is True
        assert "current_checkpoint" in verify
        assert "prior_checkpoint" in verify
        assert verify.get("redo_advanced") is True

    asyncio.run(_test())


def test_postgres_adapter_rejects_null_wait_event_without_buffers():
    """Verify that a NULL/NULL wait event (running on CPU) is NOT accepted as active
    unless confirmed by buffers_written advancing beyond baseline (Material Defect 1)."""
    async def _test():
        from resilience_tests.adapters.postgresql.adapter import PostgreSQLAdapter
        from resilience_tests.control.profile import Node

        node = Node(
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
        adapter = PostgreSQLAdapter(node)

        mock_conn_init = AsyncMock()
        mock_conn_init.fetchrow.side_effect = [
            {"pid": 1234, "wait_event_type": "Activity", "wait_event": "CheckpointerMain"},
            {"checkpoint_lsn": 1000, "redo_lsn": 1000, "checkpoint_time": "2026-01-01"},
        ]
        # Baseline buffers = 100
        mock_conn_init.fetchval.return_value = 100

        mock_conn_cp = AsyncMock()

        # Poll loop:
        # Poll 1: NULL/NULL with 100 buffers (delta = 0) -> MUST NOT BREAK!
        # Poll 2: NULL/NULL with 100 buffers (delta = 0) -> MUST NOT BREAK!
        # Poll 3: Active DataFileWrite with 105 buffers -> BREAKS!
        mock_conn_poll = AsyncMock()
        mock_conn_poll.fetchrow.side_effect = [
            {"wait_event_type": None, "wait_event": None, "buffers": 100},
            {"wait_event_type": None, "wait_event": None, "buffers": 100},
            {"wait_event_type": "IO", "wait_event": "DataFileWrite", "buffers": 105},
        ]

        adapter._connect = AsyncMock(side_effect=[mock_conn_init, mock_conn_cp, mock_conn_poll])

        detail = await adapter.trigger_checkpoint_and_await_active(timeout_s=2.0)
        assert detail["checkpointer_active"] is True
        assert detail["wait_event"] == "DataFileWrite"
        assert detail["wait_event_type"] == "IO"
        assert detail["buffers_written_during_cp"] == 5
        # Crucial check: polled 3 times, meaning the two NULL/NULL states were NOT accepted as active
        assert mock_conn_poll.fetchrow.call_count == 3

    asyncio.run(_test())


def test_postgres_adapter_buffers_advancement_confirms_active():
    """Verify that when buffers_written advances, active state is quantitatively confirmed
    even if the checkpointer was running on CPU (NULL wait event)."""
    async def _test():
        from resilience_tests.adapters.postgresql.adapter import PostgreSQLAdapter
        from resilience_tests.control.profile import Node

        node = Node(
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
        adapter = PostgreSQLAdapter(node)

        mock_conn_init = AsyncMock()
        mock_conn_init.fetchrow.side_effect = [
            {"pid": 1234, "wait_event_type": "Activity", "wait_event": "CheckpointerMain"},
            {"checkpoint_lsn": 1000, "redo_lsn": 1000, "checkpoint_time": "2026-01-01"},
        ]
        # Baseline buffers = 50
        mock_conn_init.fetchval.return_value = 50

        mock_conn_cp = AsyncMock()

        # Poll 1: 50 buffers (delta = 0) -> does not break
        # Poll 2: 65 buffers (delta = 15 > 0) -> breaks with quantitative proof!
        mock_conn_poll = AsyncMock()
        mock_conn_poll.fetchrow.side_effect = [
            {"wait_event_type": None, "wait_event": None, "buffers": 50},
            {"wait_event_type": None, "wait_event": None, "buffers": 65},
        ]

        adapter._connect = AsyncMock(side_effect=[mock_conn_init, mock_conn_cp, mock_conn_poll])

        detail = await adapter.trigger_checkpoint_and_await_active(timeout_s=2.0)
        assert detail["checkpointer_active"] is True
        assert detail["buffers_written_during_cp"] == 15
        assert mock_conn_poll.fetchrow.call_count == 2

    asyncio.run(_test())


def test_recovery_slo_unmet_note_propagates_to_disclosures(env, monkeypatch):
    """Verify that when recovery phase times out without reaching SLO,
    the note is propagated directly into disclosures and not hidden."""
    from resilience_tests.control import orchestrator as orch
    test_profile = env
    test_profile.phase_timeouts_s["recovery"] = 0.2
    monkeypatch.setattr(orch, "RECOVERY_EXIT_MARGIN_S", 0.15)
    monkeypatch.setattr(orch, "RECOVERY_POLL_S", 0.01)
    orig_decompose = orch.decompose
    def mock_decompose(*args, **kwargs):
        res = orig_decompose(*args, **kwargs)
        from dataclasses import replace
        return replace(res, rto_to_slo_s=None)
    monkeypatch.setattr(orch, "decompose", mock_decompose)

    item = RunPlanItem(
        scenario=scenario("NL-C-02"),
        env_class=env.env_class,
        role="standalone",
        node=env.nodes[0],
    )
    results = asyncio.run(TestOrchestrator(item, test_profile, RunOptions()).run())
    # Verify note in disclosures
    disclosures_text = " ".join(results.get("disclosures", []))
    assert "Recovery limitation: service did not return to SLO within the recovery bound" in disclosures_text
    # Verify deduplication
    assert len(results["disclosures"]) == len(set(results["disclosures"]))

