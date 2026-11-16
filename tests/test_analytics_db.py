"""NL-N-I01 report sessions against a real ShaktiDB/PostgreSQL (adapters/postgresql/analytics.py).

Only against a throw-away database whose user may create roles (it creates and drops a report role):
    RESILIENCE_TEST_PG=127.0.0.1:55433/resilience/tester .venv/bin/python -m pytest tests/test_analytics_db.py -q

The role's limits are scaled to the small test data, so both limit tests trip within seconds:
temp_file_limit 64 MB (limit test 1 sorts about 150 MB; Q3 needs about 15 MB per process) and
statement_timeout 4 s (limit test 2 would run for minutes; the reports take well under a second).
"""

from __future__ import annotations

import asyncio
import os
import time

import asyncpg
import pytest

from resilience_tests.adapters.postgresql.adapter import PostgreSQLAdapter
from resilience_tests.adapters.postgresql.analytics import ANALYTICS_APPLICATION_NAME
from resilience_tests.control.profile import DbEndpoint
from tests.test_adapter_seam import NODE

TARGET = os.environ.get("RESILIENCE_TEST_PG")
pytestmark = pytest.mark.skipif(not TARGET, reason="RESILIENCE_TEST_PG (host:port/db/user of a throw-away database) not set")
ROLE = "resilience_test_analytics"
TIMEOUT_S = 4.0


def _endpoint(user: str | None = None) -> DbEndpoint:
    hostport, db, owner = TARGET.split("/")
    host, port = hostport.split(":")
    return DbEndpoint(host=host, port=int(port), dbname=db, user=user or owner)


async def _sql(*statements: str) -> None:
    ep = _endpoint()
    conn = await asyncpg.connect(host=ep.host, port=ep.port, database=ep.dbname, user=ep.user)
    try:
        for s in statements:
            await conn.execute(s)
    finally:
        await conn.close()


@pytest.fixture(scope="module")
def adapter():
    ep = _endpoint()
    a = PostgreSQLAdapter(NODE.model_copy(update={"db": ep, "client": ep, "analytics": _endpoint(ROLE)}))
    asyncio.run(a.prepare_harness_state())
    asyncio.run(a.tpcc_load(3, jobs=3, replace=True, seed=7))
    asyncio.run(_sql(
        f"DROP ROLE IF EXISTS {ROLE}",
        f"CREATE ROLE {ROLE} LOGIN CONNECTION LIMIT 4",
        f"ALTER ROLE {ROLE} SET max_parallel_workers_per_gather = 2",
        f"ALTER ROLE {ROLE} SET work_mem = '1MB'",
        f"ALTER ROLE {ROLE} SET temp_file_limit = '64MB'",
        f"ALTER ROLE {ROLE} SET statement_timeout = '{TIMEOUT_S:g}s'",
        f"GRANT USAGE ON SCHEMA resilience_tpcc TO {ROLE}",
        f"GRANT SELECT ON ALL TABLES IN SCHEMA resilience_tpcc TO {ROLE}"))
    yield a
    asyncio.run(a.stop_analytics())
    asyncio.run(_sql(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = '{ROLE}'"))
    time.sleep(0.5)
    asyncio.run(_sql(f"REVOKE ALL ON ALL TABLES IN SCHEMA resilience_tpcc FROM {ROLE}",
                     f"REVOKE ALL ON SCHEMA resilience_tpcc FROM {ROLE}", f"DROP ROLE IF EXISTS {ROLE}"))
    asyncio.run(a.tpcc_drop())


def test_the_login_and_its_limits_are_read_not_written(adapter):
    observed = asyncio.run(adapter.observe_analytics_settings())
    assert observed["analytics_login"] == ROLE and observed["role_exists"] == "true"
    assert observed["role_can_login"] == "true" and observed["role_connection_limit"] == "4"
    assert observed["role_max_parallel_workers_per_gather"] == "2" and observed["role_work_mem"] == "1MB"
    assert observed["role_temp_file_limit"] == "64MB" and observed["role_statement_timeout"] == f"{TIMEOUT_S:g}s"
    assert observed["shared_buffers"] and observed["max_parallel_workers"]


def test_the_reports_run_the_limit_tests_trip_and_nothing_is_left(adapter):
    async def go():
        before = await adapter.customer_cache_counters()
        started = await adapter.start_analytics(full_table_sessions=2, index_sessions=1, limit_tests=True,
                                                extra_warehouses=(2, 3))
        assert started["supported"], started
        assert [s["first"] for s in started["sessions"]] == ["limit_temp", "limit_timeout", None]
        samples = []
        deadline = time.monotonic() + TIMEOUT_S + 20
        while time.monotonic() < deadline:
            samples.append(await adapter.analytics_sample())
            state = await adapter.analytics_state()
            kinds = state["by_kind"]
            if len(state["limit_tests"]) == 2 and all((kinds.get(k) or {}).get("completed") for k in ("q1", "q2", "q3", "q4")):
                break
            await asyncio.sleep(0.5)
        stopped = await adapter.stop_analytics()
        reverted = await adapter.revert_analytics()
        left = await adapter.analytics_leftovers()
        after = await adapter.customer_cache_counters()
        return started, samples, await adapter.analytics_state(), stopped, reverted, left, before, after

    started, samples, state, stopped, reverted, left, before, after = asyncio.run(go())
    tests = state["limit_tests"]
    # limit test 1: stopped by temp_file_limit; limit test 2: by statement_timeout, at about the limit
    assert (tests["limit_temp"]["outcome"], tests["limit_temp"]["sqlstate"]) == ("error", "53400")
    assert (tests["limit_timeout"]["outcome"], tests["limit_timeout"]["sqlstate"]) == ("error", "57014")
    assert TIMEOUT_S - 0.5 <= tests["limit_timeout"]["seconds"] <= TIMEOUT_S + 1.0
    kinds = state["by_kind"]
    for k in ("q1", "q2", "q3", "q4"):
        assert kinds[k]["completed"] >= 1, (k, kinds)
    # the full-table reports used helpers and wrote no error; the index report read its pages
    assert max(kinds[k]["workers_launched_max"] for k in ("q1", "q2", "q3")) >= 1
    assert kinds["q4"]["shared_hit"] + kinds["q4"]["shared_read"] > 0
    assert not any(kinds[k]["errors"] for k in ("q1", "q2", "q3", "q4")), kinds
    # samples saw the sessions and their helpers, and were kept for the minute-1 confirmation
    assert max(s["sessions_open"] for s in samples) == 3
    assert any(s["helpers_total"] >= 1 for s in samples)
    assert len(state["samples"]) == len(samples)
    assert stopped["stopped"] and reverted["sessions_remaining"] == 0
    assert left == {"sessions": 0, "temp_files": 0}
    assert set(after) >= {"customers", "reports"} and after["reports"]["hit"] + after["reports"]["read"] >= \
        before["reports"]["hit"] + before["reports"]["read"]


def test_a_login_over_its_connection_limit_does_not_land(adapter):
    async def go():
        return await adapter.start_analytics(full_table_sessions=4, index_sessions=1, limit_tests=False,
                                             extra_warehouses=(2, 3))
    out = asyncio.run(go())
    assert out["supported"] is False and "not opened" in out["error"]
    asyncio.run(adapter.stop_analytics())


def test_revert_ends_tagged_sessions_left_by_another_process(adapter):
    """The kill switch's case: a fresh adapter with no state of its own ends what is tagged. On a
    local test database the server-side backstop cannot run (no SSH); sessions closed by their own
    process are what remains to check."""
    async def go():
        conn = await asyncpg.connect(host=_endpoint().host, port=_endpoint().port, database=_endpoint().dbname,
                                     user=ROLE, server_settings={"application_name": ANALYTICS_APPLICATION_NAME})
        fresh = PostgreSQLAdapter(adapter.node)
        assert (await fresh.analytics_leftovers())["sessions"] == 1
        await conn.close()
        for _ in range(20):
            if (await fresh.analytics_leftovers())["sessions"] == 0:
                break
            await asyncio.sleep(0.1)
        return await fresh.revert_analytics()
    assert asyncio.run(go())["sessions_remaining"] == 0
