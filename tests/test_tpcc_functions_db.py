"""TPC-C build step 6 against a REAL PostgreSQL/ShaktiDB: the five transaction functions, one
call each, and how each outcome is classified.

Skipped unless a throw-away test database is named (never the lab target):

    RESILIENCE_TEST_PG=127.0.0.1:55433/resilience/tester .venv/bin/python -m pytest tests/test_tpcc_functions_db.py -q
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections import Counter

import asyncpg
import pytest

from catalog.schema import Workload
from resilience_tests.adapters.postgresql.adapter import PostgreSQLAdapter
from resilience_tests.control.profile import DbEndpoint
from resilience_tests.execution.workload.contract import (
    DELIVERY,
    NEW_ORDER,
    ORDER_STATUS,
    PAYMENT,
    STOCK_LEVEL,
    TPCC_SCHEMA,
)
from resilience_tests.execution.workload.tpcc.profile import TpccProfile
from tests.test_adapter_seam import NODE

TARGET = os.environ.get("RESILIENCE_TEST_PG")
pytestmark = pytest.mark.skipif(not TARGET, reason="RESILIENCE_TEST_PG (host:port/db/user of a throw-away database) not set")
S = TPCC_SCHEMA


def _endpoint() -> DbEndpoint:
    hostport, db, user = TARGET.split("/")
    host, port = hostport.split(":")
    return DbEndpoint(host=host, port=int(port), dbname=db, user=user)


@pytest.fixture(scope="module")
def adapter():
    ep = _endpoint()
    a = PostgreSQLAdapter(NODE.model_copy(update={"db": ep, "client": ep}))
    asyncio.run(a.prepare_harness_state())          # resilience.markers, as init creates it
    asyncio.run(a.tpcc_load(2, jobs=2, replace=True, seed=11))
    yield a
    asyncio.run(a.tpcc_drop())


def q(adapter, query, *args):
    async def go():
        ep = adapter.node.db
        c = await asyncpg.connect(host=ep.host, port=ep.port, database=ep.dbname, user=ep.user)
        try:
            return await c.fetchval(query, *args)
        finally:
            await c.close()
    return asyncio.run(go())


def run(adapter, txn_type, inputs, write=True):
    async def go():
        s = await adapter.session()
        try:
            marker = (1, str(uuid.uuid4())) if write else None
            outcome, journal, sqlstate = await s.tpcc_execute(txn_type, inputs, marker)
            in_txn = s._conn.is_in_transaction() if not s.is_closed else None
            return outcome, journal, sqlstate, marker, in_txn
        finally:
            await s.close()
    return asyncio.run(go())


def marker_present(adapter, marker) -> bool:
    return bool(q(adapter, "SELECT count(*) FROM resilience.markers WHERE uuid = $1", uuid.UUID(marker[1])))


NO = {"w_id": 1, "d_id": 3, "c_id": 42, "items": [10, 20, 30], "supply_w": [1, 1, 2], "quantities": [5, 3, 1]}


def test_new_order_commits_when_the_call_returns(adapter):
    before = q(adapter, f"SELECT d_next_o_id FROM {S}.district WHERE d_w_id=1 AND d_id=3")
    outcome, journal, sqlstate, marker, in_txn = run(adapter, NEW_ORDER, NO)
    assert outcome == "committed" and sqlstate is None and in_txn is False      # not left inside a transaction
    assert journal == {"w": 1, "d": 3, "o_id": before, "ol_cnt": 3}
    # visible at once from ANOTHER connection: the call returning really meant committed
    o = journal["o_id"]
    assert q(adapter, f"SELECT o_ol_cnt FROM {S}.orders WHERE o_w_id=1 AND o_d_id=3 AND o_id=$1", o) == 3
    assert q(adapter, f"SELECT o_all_local FROM {S}.orders WHERE o_w_id=1 AND o_d_id=3 AND o_id=$1", o) == 0
    assert q(adapter, f"SELECT count(*) FROM {S}.order_line WHERE ol_w_id=1 AND ol_d_id=3 AND ol_o_id=$1", o) == 3
    assert q(adapter, f"SELECT count(*) FROM {S}.new_order WHERE no_w_id=1 AND no_d_id=3 AND no_o_id=$1", o) == 1
    assert q(adapter, f"SELECT d_next_o_id FROM {S}.district WHERE d_w_id=1 AND d_id=3") == before + 1
    assert q(adapter, f"SELECT s_remote_cnt FROM {S}.stock WHERE s_w_id=2 AND s_i_id=30") == 1
    assert marker_present(adapter, marker)


def test_business_rollback_leaves_nothing_behind(adapter):
    before = q(adapter, f"SELECT d_next_o_id FROM {S}.district WHERE d_w_id=1 AND d_id=4")
    orders = q(adapter, f"SELECT count(*) FROM {S}.orders")
    bad = {**NO, "d_id": 4, "items": [10, 20, 100_001], "supply_w": [1, 1, 1]}
    outcome, journal, sqlstate, marker, _ = run(adapter, NEW_ORDER, bad)
    assert outcome == "business_rollback" and sqlstate == "TP001" and journal == {}
    assert not marker_present(adapter, marker)                                    # the marker rolled back too
    assert q(adapter, f"SELECT count(*) FROM {S}.orders") == orders
    assert q(adapter, f"SELECT d_next_o_id FROM {S}.district WHERE d_w_id=1 AND d_id=4") == before


def test_payment_by_number_and_by_last_name(adapter):
    w_before = q(adapter, f"SELECT w_ytd FROM {S}.warehouse WHERE w_id=1")
    pay = {"w_id": 1, "d_id": 2, "c_w_id": 2, "c_d_id": 5, "c_id": 77, "c_last": None, "amount_cents": 12_345}
    outcome, _, _, marker, _ = run(adapter, PAYMENT, pay)
    assert outcome == "committed" and marker_present(adapter, marker)
    assert float(q(adapter, f"SELECT w_ytd FROM {S}.warehouse WHERE w_id=1") - w_before) == pytest.approx(123.45)
    assert float(q(adapter, f"SELECT c_balance FROM {S}.customer WHERE c_w_id=2 AND c_d_id=5 AND c_id=77")) == pytest.approx(-10 - 123.45)
    assert q(adapter, f"SELECT count(*) FROM {S}.history WHERE h_c_w_id=2 AND h_c_d_id=5 AND h_c_id=77 AND h_w_id=1") == 1
    # by last name: the middle customer of those with the name, sorted by first name (Clause 2.5.2.2)
    expected = q(adapter, f"""SELECT (array_agg(c_id ORDER BY c_first))[(count(*)::int + 1) / 2] FROM {S}.customer
                              WHERE c_w_id=1 AND c_d_id=1 AND c_last='BARBARBAR'""")
    by_name = {**pay, "c_w_id": 1, "c_d_id": 1, "c_id": None, "c_last": "BARBARBAR"}
    count = q(adapter, f"SELECT c_payment_cnt FROM {S}.customer WHERE c_w_id=1 AND c_d_id=1 AND c_id=$1", expected)
    assert run(adapter, PAYMENT, by_name)[0] == "committed"
    assert q(adapter, f"SELECT c_payment_cnt FROM {S}.customer WHERE c_w_id=1 AND c_d_id=1 AND c_id=$1", expected) == count + 1


def test_read_only_transactions_write_nothing(adapter):
    markers = q(adapter, "SELECT count(*) FROM resilience.markers")
    assert run(adapter, ORDER_STATUS, {"w_id": 1, "d_id": 1, "c_id": 5, "c_last": None}, write=False)[0] == "committed"
    assert run(adapter, ORDER_STATUS, {"w_id": 1, "d_id": 1, "c_id": None, "c_last": "PRICALLYOUGHT"}, write=False)[0] == "committed"
    assert run(adapter, STOCK_LEVEL, {"w_id": 1, "d_id": 1, "threshold": 15}, write=False)[0] == "committed"
    assert q(adapter, "SELECT count(*) FROM resilience.markers") == markers


def test_delivery_delivers_the_oldest_order_of_every_district(adapter):
    oldest = q(adapter, f"SELECT min(no_o_id) FROM {S}.new_order WHERE no_w_id=2 AND no_d_id=7")
    waiting = q(adapter, f"SELECT count(*) FROM {S}.new_order WHERE no_w_id=2")
    outcome, _, _, marker, _ = run(adapter, DELIVERY, {"w_id": 2, "o_carrier_id": 4})
    assert outcome == "committed" and marker_present(adapter, marker)
    assert q(adapter, f"SELECT count(*) FROM {S}.new_order WHERE no_w_id=2") == waiting - 10
    assert q(adapter, f"SELECT o_carrier_id FROM {S}.orders WHERE o_w_id=2 AND o_d_id=7 AND o_id=$1", oldest) == 4
    assert q(adapter, f"""SELECT count(*) FROM {S}.order_line WHERE ol_w_id=2 AND ol_d_id=7 AND ol_o_id=$1
                          AND ol_delivery_d IS NULL""", oldest) == 0


def test_a_broken_connection_gives_unknown_never_committed(adapter):
    async def go():
        s = await adapter.session()
        await s._conn.close()                               # as if the server went away
        marker = (1, str(uuid.uuid4()))
        return await s.tpcc_execute(NEW_ORDER, NO, marker), marker
    (outcome, journal, _), marker = asyncio.run(go())
    assert outcome == "unknown" and journal == {}
    assert not marker_present(adapter, marker)


def test_a_lock_timeout_is_a_sure_rollback(adapter):
    """The clerk's own lock_timeout (scenarios 2 and 4) ends the call; the server says so: a
    definite rollback, counted as an error, never as unknown."""
    async def go():
        ep = adapter.node.db
        holder = await asyncpg.connect(host=ep.host, port=ep.port, database=ep.dbname, user=ep.user)
        tx = holder.transaction()
        await tx.start()
        await holder.execute(f"SELECT 1 FROM {S}.district WHERE d_w_id=1 AND d_id=9 FOR UPDATE")
        try:
            s = await adapter.session(settings={"lock_timeout": "300ms"})
            marker = (1, str(uuid.uuid4()))
            result = await s.tpcc_execute(NEW_ORDER, {**NO, "d_id": 9}, marker)
            await s.close()
            return result, marker
        finally:
            await tx.rollback()
            await holder.close()
    (outcome, _, sqlstate), marker = asyncio.run(go())
    assert outcome == "definitely_aborted" and sqlstate == "55P03"
    assert not marker_present(adapter, marker)


def test_a_mixed_run_keeps_every_consistency_rule_and_every_acknowledged_order_complete(adapter):
    """2,300 transactions from the tpcc profile (100 decks), through the real functions: the
    mix is exact, about 1% of New-Orders roll back, every committed write has its marker, every
    committed New-Order exists with exactly its lines, and consistency rules 1-10 and 12 hold."""
    w = Workload(profile="tpcc", concurrency=4, transaction_markers=True, rate_tps=100, tpcc={"warehouses": 2})
    profile = TpccProfile(w)
    profile.set_run_constants(q(adapter, f"SELECT c_last_load FROM {S}.meta"))

    async def go():
        sessions = [await adapter.session() for _ in range(4)]
        outcomes, committed_writes, orders, seq = Counter(), [], [], 1000
        for _round in range(575):
            for i, s in enumerate(sessions):
                req = profile.next_request(profile.terminal(i))
                seq += 1
                marker = (seq, str(uuid.uuid4())) if req.needs_marker else None
                outcome, journal, _ = await s.tpcc_execute(req.txn_type, req.inputs, marker)
                outcomes[(req.txn_type, outcome)] += 1
                if outcome == "committed" and marker:
                    committed_writes.append(marker[1])
                    if req.txn_type == NEW_ORDER:
                        orders.append(journal)
        for s in sessions:
            await s.close()
        return outcomes, committed_writes, orders

    outcomes, committed_writes, orders = asyncio.run(go())
    by_type = Counter()
    for (t, _o), n in outcomes.items():
        by_type[t] += n
    assert by_type == {NEW_ORDER: 1000, PAYMENT: 1000, ORDER_STATUS: 100, DELIVERY: 100, STOCK_LEVEL: 100}
    assert {o for (_t, o) in outcomes} <= {"committed", "business_rollback"}       # nothing failed or unknown
    assert 2 <= outcomes[(NEW_ORDER, "business_rollback")] <= 25                  # about 1% of 1,000
    present = set(asyncio.run(_markers(adapter)))
    assert set(committed_writes) <= present                                       # no committed write lost
    missing_or_short = sum(
        1 for o in orders
        if q(adapter, f"SELECT count(*) FROM {S}.order_line WHERE ol_w_id=$1 AND ol_d_id=$2 AND ol_o_id=$3",
             o["w"], o["d"], o["o_id"]) != o["ol_cnt"])
    assert missing_or_short == 0                                                   # the per-order check
    rules = asyncio.run(adapter.tpcc_consistency([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12]))
    assert rules == {r: 0 for r in rules}, rules


async def _markers(adapter):
    return await adapter.marker_ids()


def test_the_engine_runs_real_tpcc_load_and_loses_nothing(adapter, tmp_path):
    """The single-process engine for 6 s with 8 clerks at 300 TPS against the real database.
    Afterwards, from the evidence alone: RPO 0, no phantom, no rolled-back transaction kept,
    every acknowledged New-Order complete, consistency rules hold, samples show all 5 types."""
    import json as _json

    from resilience_tests.execution.workload.markers import ABORTED_JOURNAL, MarkerJournals, diff_from_journals, read_journal
    from resilience_tests.observability.event_stream import EventStream

    asyncio.run(adapter.prepare_harness_state())            # empty markers table, as init does
    w = Workload(profile="tpcc", concurrency=8, transaction_markers=True, rate_tps=300, tpcc={"warehouses": 2})
    profile = TpccProfile(w)
    asyncio.run(profile.prepare(adapter))                   # data checks, functions, run constants

    async def go():
        stream, journals = EventStream(tmp_path / "events.jsonl"), MarkerJournals(tmp_path)
        drv = profile.make_driver(adapter, journals, stream)
        await drv.start()
        await asyncio.wait_for(drv.wait_until_ready(), 10)
        drv.begin_window()
        await asyncio.sleep(6)
        window = drv.end_window()
        await drv.stop()
        journals.close()
        stream.close()
        return drv, window, [e.data for e in stream.events() if e.kind == "sample"]

    drv, window, samples = asyncio.run(go())
    assert drv.failure is None and window.tps > 200, (drv.failure, window)
    diff, torn = diff_from_journals(tmp_path, asyncio.run(adapter.marker_ids()))
    assert torn == 0 and diff.rpo_txn == 0 and not diff.phantom and not diff.unjournalled_ack
    aborted = read_journal(tmp_path / ABORTED_JOURNAL).records if (tmp_path / ABORTED_JOURNAL).exists() else []
    db = asyncio.run(adapter.marker_ids())
    assert not ({r["uuid"] for r in aborted} & db)           # nothing rolled back was kept
    acked_orders = [r for r in read_journal(tmp_path / "acked.jrnl").records if "o_id" in r]
    assert acked_orders
    rules = asyncio.run(adapter.tpcc_consistency([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12]))
    assert rules == {r: 0 for r in rules}, rules
    assert asyncio.run(adapter.tpcc_orders_incomplete(acked_orders)) == 0      # the per-order check
    # ...and it catches a half-saved order: remove one line of one acknowledged order
    o = acked_orders[0]
    q(adapter, f"DELETE FROM {S}.order_line WHERE ol_w_id=$1 AND ol_d_id=$2 AND ol_o_id=$3 AND ol_number=1",
      o["w"], o["d"], o["o_id"])
    assert asyncio.run(adapter.tpcc_orders_incomplete(acked_orders)) == 1
    assert asyncio.run(adapter.tpcc_orders_incomplete([{**o, "o_id": 99_999}])) == 1   # an order that never existed
    seen = Counter()
    for s in samples:
        for t, st in s["by_type"].items():
            seen[t] += st["commits"]
    assert all(seen[t] > 0 for t in (NEW_ORDER, PAYMENT, ORDER_STATUS, DELIVERY, STOCK_LEVEL)), seen
    assert sum(s["errors"] for s in samples) == 0 and sum(s["indeterminate"] for s in samples) == 0
    _json.dumps(samples)
