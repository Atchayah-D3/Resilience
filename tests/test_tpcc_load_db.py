"""TPC-C build step 4 against a REAL PostgreSQL/ShaktiDB: load 1 warehouse, check it, break it,
see the checks fail for the right reason, drop it.

Skipped unless a throw-away test database is named (never the lab target):

    RESILIENCE_TEST_PG=127.0.0.1:55433/resilience/tester .venv/bin/python -m pytest tests/test_tpcc_load_db.py -q

The load of one warehouse takes about a minute.
"""

from __future__ import annotations

import asyncio
import os

import asyncpg
import pytest

from catalog.schema import Workload
from resilience_tests.adapters.postgresql.adapter import PostgreSQLAdapter
from resilience_tests.control.profile import DbEndpoint
from resilience_tests.execution.workload.contract import TPCC_SCHEMA
from resilience_tests.execution.workload.tpcc import data
from resilience_tests.execution.workload.tpcc.profile import TpccDataError, TpccProfile
from tests.test_adapter_seam import NODE

TARGET = os.environ.get("RESILIENCE_TEST_PG")
pytestmark = pytest.mark.skipif(not TARGET, reason="RESILIENCE_TEST_PG (host:port/db/user of a throw-away database) not set")


def _endpoint() -> DbEndpoint:
    hostport, db, user = TARGET.split("/")
    host, port = hostport.split(":")
    return DbEndpoint(host=host, port=int(port), dbname=db, user=user)


@pytest.fixture(scope="module")
def adapter():
    ep = _endpoint()
    a = PostgreSQLAdapter(NODE.model_copy(update={"db": ep, "client": ep}))
    asyncio.run(a.tpcc_drop())
    yield a
    asyncio.run(a.tpcc_drop())


@pytest.fixture(scope="module")
def loaded(adapter):
    messages: list[str] = []
    result = asyncio.run(adapter.tpcc_load(1, jobs=1, progress=messages.append, seed=7))
    return result, messages


def _sql(adapter, query: str, *args):
    async def go():
        conn = await asyncpg.connect(host=adapter.node.db.host, port=adapter.node.db.port,
                                     database=adapter.node.db.dbname, user=adapter.node.db.user)
        try:
            return await conn.fetchval(query, *args)
        finally:
            await conn.close()
    return asyncio.run(go())


def test_one_warehouse_loads_with_exact_counts_and_every_rule_holding(loaded):
    result, messages = loaded
    assert data.row_count_problems(result["rows"], 1) == []
    assert result["rows"]["order_line"] == pytest.approx(300_000, rel=0.01)
    assert all(v == 0 for v in result["rules"].values()) and set(result["rules"]) == set(range(1, 13))
    assert any("warehouse 1 loaded" in m for m in messages)
    assert 0 <= result["c_last_load"] <= 255


def test_population_rules_of_clause_4_3_3_1(adapter, loaded):
    s = TPCC_SCHEMA
    # the first 1,000 customers of a district carry the names of 0..999 in order
    assert _sql(adapter, f"SELECT c_last FROM {s}.customer WHERE c_w_id=1 AND c_d_id=1 AND c_id=1") == "BARBARBAR"
    assert _sql(adapter, f"SELECT c_last FROM {s}.customer WHERE c_w_id=1 AND c_d_id=1 AND c_id=372") == "PRICALLYOUGHT"
    # orders 1..2100 delivered (carrier set), 2101..3000 new (no carrier, a new_order row)
    assert _sql(adapter, f"SELECT count(*) FROM {s}.orders WHERE o_id < 2101 AND o_carrier_id IS NULL") == 0
    assert _sql(adapter, f"SELECT count(*) FROM {s}.orders WHERE o_id >= 2101 AND o_carrier_id IS NOT NULL") == 0
    assert _sql(adapter, f"SELECT min(no_o_id) FROM {s}.new_order") == 2101
    # each district's customers appear exactly once among its orders (a permutation)
    assert _sql(adapter, f"SELECT count(DISTINCT (o_d_id, o_c_id)) FROM {s}.orders") == 30_000
    # about 10% of items carry "ORIGINAL"; credit "BC" for about 10% of customers
    share = _sql(adapter, f"SELECT avg((i_data LIKE '%ORIGINAL%')::int)::float FROM {s}.item")
    assert 0.08 < share < 0.12
    bc = _sql(adapter, f"SELECT avg((c_credit = 'BC')::int)::float FROM {s}.customer")
    assert 0.08 < bc < 0.12
    assert _sql(adapter, f"SELECT min(s_quantity) FROM {s}.stock") >= 10
    assert _sql(adapter, f"SELECT max(s_quantity) FROM {s}.stock") <= 100


def test_python_and_sql_last_names_are_identical_for_all_1000_numbers(adapter, loaded):
    """The clerks (Python, inputs.last_name) must look up exactly the names the loader (SQL) wrote."""
    from resilience_tests.execution.workload.tpcc.inputs import last_name
    sql = _sql(adapter, f"SELECT array_agg({TPCC_SCHEMA}.last_name(n) ORDER BY n) FROM generate_series(0, 999) n")
    assert list(sql) == [last_name(n) for n in range(1000)]


def test_status_reports_the_load(adapter, loaded):
    st = asyncio.run(adapter.tpcc_data_status())
    assert st["loaded"] and st["warehouses"] == 1 and st["items"] == data.ITEMS
    assert st["loader_version"] == data.LOADER_VERSION and st["current_orders"] == st["loaded_orders"] == 30_000
    assert data.growth(st) == 1.0 and data.data_problems(st, 1) == []


def test_a_second_load_is_refused_without_replace(adapter, loaded):
    with pytest.raises(RuntimeError, match="already exists"):
        asyncio.run(adapter.tpcc_load(1, jobs=1))


def _workload(warehouses: int) -> Workload:
    return Workload(profile="tpcc", concurrency=4, transaction_markers=True, rate_tps=10,
                    tpcc={"warehouses": warehouses})


def test_prepare_accepts_the_loaded_data_and_refuses_a_different_size(adapter, loaded):
    facts = asyncio.run(TpccProfile(_workload(1)).prepare(adapter))["tpcc_data"]
    assert facts["warehouses"] == 1 and set(facts["consistency"].values()) == {0}
    with pytest.raises(TpccDataError, match="expects 2"):
        asyncio.run(TpccProfile(_workload(2)).prepare(adapter))


def test_broken_data_is_caught_by_the_rule_that_owns_it(adapter, loaded):
    """Break one thing at a time; the matching rule must report it, then repair it."""
    s = TPCC_SCHEMA
    cases = [
        (f"UPDATE {s}.district SET d_ytd = d_ytd + 1 WHERE d_w_id=1 AND d_id=1", {1, 9},
         f"UPDATE {s}.district SET d_ytd = d_ytd - 1 WHERE d_w_id=1 AND d_id=1"),
        (f"UPDATE {s}.district SET d_next_o_id = d_next_o_id + 1 WHERE d_w_id=1 AND d_id=2", {2},
         f"UPDATE {s}.district SET d_next_o_id = d_next_o_id - 1 WHERE d_w_id=1 AND d_id=2"),
        (f"DELETE FROM {s}.order_line WHERE ol_w_id=1 AND ol_d_id=3 AND ol_o_id=5 AND ol_number=1", {4, 6},
         None),   # a half-saved order: lines missing
    ]
    for breaking, owners, repair in cases:
        _sql(adapter, breaking)
        found = asyncio.run(adapter.tpcc_consistency([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12]))
        failing = {r for r, n in found.items() if n}
        assert owners <= failing, (breaking, failing)
        if repair:
            _sql(adapter, repair)
    # the half-saved order is still there: prepare must refuse (rule 4 sees all 10 districts of 1 warehouse)
    with pytest.raises(TpccDataError, match="rule 4"):
        asyncio.run(TpccProfile(_workload(1)).prepare(adapter))


def test_growth_beyond_the_limit_is_refused(adapter, loaded):
    s = TPCC_SCHEMA
    _sql(adapter, f"UPDATE {s}.meta SET loaded_orders = loaded_orders / 2")     # as if orders had doubled
    try:
        st = asyncio.run(adapter.tpcc_data_status())
        assert data.growth(st) == 2.0
        assert any("grown to 2.00 times" in p for p in data.data_problems(st, 1))
    finally:
        _sql(adapter, f"UPDATE {s}.meta SET loaded_orders = loaded_orders * 2")


def test_a_load_that_did_not_finish_is_not_used(adapter, loaded):
    s = TPCC_SCHEMA
    _sql(adapter, f"ALTER TABLE {s}.meta RENAME TO meta_hidden")
    _sql(adapter, f"CREATE TABLE {s}.meta (LIKE {s}.meta_hidden)")
    try:
        st = asyncio.run(adapter.tpcc_data_status())
        assert st == {"loaded": False, "reason": "the TPC-C load did not finish (no meta row)"}
    finally:
        _sql(adapter, f"DROP TABLE {s}.meta")
        _sql(adapter, f"ALTER TABLE {s}.meta_hidden RENAME TO meta")


def test_drop_leaves_nothing(adapter, loaded):
    assert asyncio.run(adapter.tpcc_drop()) is True
    assert asyncio.run(adapter.tpcc_data_status()) == {"loaded": False, "reason": f"schema {TPCC_SCHEMA} does not exist"}
