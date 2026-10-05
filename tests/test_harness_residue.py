"""What one run leaves behind for the next (review findings S2 and NL-C-06 cleanup)."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

from resilience_tests.adapters.postgresql.adapter import PostgreSQLAdapter
from resilience_tests.control.profile import load_profile

NODE = load_profile("e2-dedicated-vm").nodes[0]


def adapter_with(deviations: dict[str, str]) -> tuple[PostgreSQLAdapter, AsyncMock]:
    adapter = PostgreSQLAdapter(NODE)
    conn = AsyncMock()
    adapter._connect = AsyncMock(return_value=conn)            # type: ignore[method-assign]
    adapter.config_deviations = AsyncMock(return_value=deviations)  # type: ignore[method-assign]
    return adapter, conn


def executed(conn: AsyncMock) -> list[str]:
    return [c.args[0] for c in conn.execute.await_args_list]


def test_only_the_harness_own_tuning_values_are_reset():
    """Was: any autovacuum_naptime / cost_delay / idle_in_transaction_session_timeout in
    postgresql.auto.conf was RESET on every run -- the operator's own settings included."""
    adapter, conn = adapter_with({
        "autovacuum_naptime": "5s",                        # NL-C-05's value: harness residue
        "autovacuum_vacuum_cost_delay": "10ms",            # operator's: kept
        "idle_in_transaction_session_timeout": "60s",      # never written by the harness: kept
        "work_mem": "64MB",
    })
    reset = asyncio.run(adapter.cleanup_leftover_configuration())
    assert reset == {"autovacuum_naptime": "5s"}
    assert executed(conn) == ["ALTER SYSTEM RESET autovacuum_naptime", "SELECT pg_reload_conf()"]


def test_nothing_is_touched_when_no_harness_value_leaked():
    adapter, conn = adapter_with({"autovacuum_naptime": "30s"})
    assert asyncio.run(adapter.cleanup_leftover_configuration()) == {}
    assert executed(conn) == []


def test_prepare_drops_leftover_indexes_by_qualified_name():
    """Was: DROP INDEX idx_nlc06_concurrent, unqualified -- it resolved through search_path,
    missed the resilience schema, and the index survived to make the next build a no-op."""
    adapter = PostgreSQLAdapter(NODE)
    conn = AsyncMock()
    adapter._connect = AsyncMock(return_value=conn)            # type: ignore[method-assign]
    asyncio.run(adapter.prepare_harness_state())
    drops = [sql for sql in executed(conn) if sql.startswith("DROP INDEX")]
    assert drops == ["DROP INDEX CONCURRENTLY IF EXISTS resilience.idx_nlc06_concurrent",
                     "DROP INDEX CONCURRENTLY IF EXISTS resilience.cic_nlc06_idx"]
