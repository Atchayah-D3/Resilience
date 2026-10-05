"""What one run leaves behind for the next (review finding NL-C-06 cleanup).

S2 (silent ALTER SYSTEM RESET of operator settings) is closed by design: the harness never
writes configuration at all -- it only observes it (observe_fault_settings; see
test_idle_transaction_vacuum.py, which asserts the tuning methods no longer exist).
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

from resilience_tests.adapters.postgresql.adapter import PostgreSQLAdapter
from resilience_tests.control.profile import load_profile

NODE = load_profile("e2-dedicated-vm").nodes[0]


def executed(conn: AsyncMock) -> list[str]:
    return [c.args[0] for c in conn.execute.await_args_list]


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
