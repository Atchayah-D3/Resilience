"""TPC-C build step 6 against a REAL PostgreSQL/ShaktiDB: the five transaction functions, one
call each, and how each outcome is classified.

Skipped unless a throw-away test database is named (never the lab target):

    RESILIENCE_TEST_PG=127.0.0.1:55433/resilience/tester .venv/bin/python -m pytest tests/test_tpcc_functions_db.py -q
"""

from __future__ import annotations

import asyncio
import os
import time
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
    # ...and it catches a half-saved order: remove one line of one acknowledged order (then put
    # it back, so the tests after this one start from consistent data)
    o = acked_orders[0]
    where = f"ol_w_id={int(o['w'])} AND ol_d_id={int(o['d'])} AND ol_o_id={int(o['o_id'])} AND ol_number=1"
    q(adapter, f"CREATE TABLE {S}.test_hold AS SELECT * FROM {S}.order_line WHERE {where}")
    q(adapter, f"DELETE FROM {S}.order_line WHERE {where}")
    try:
        assert asyncio.run(adapter.tpcc_orders_incomplete(acked_orders)) == 1
        assert asyncio.run(adapter.tpcc_order_check([o]))["incomplete"] == 1
        assert asyncio.run(adapter.tpcc_order_check([{**o, "o_id": 99_999}]))["missing"] == 1   # never existed
    finally:
        q(adapter, f"INSERT INTO {S}.order_line SELECT * FROM {S}.test_hold")
        q(adapter, f"DROP TABLE {S}.test_hold")
    assert asyncio.run(adapter.tpcc_orders_incomplete(acked_orders)) == 0
    seen = Counter()
    for s in samples:
        for t, st in s["by_type"].items():
            seen[t] += st["commits"]
    assert all(seen[t] > 0 for t in (NEW_ORDER, PAYMENT, ORDER_STATUS, DELIVERY, STOCK_LEVEL)), seen
    assert sum(s["errors"] for s in samples) == 0 and sum(s["indeterminate"] for s in samples) == 0
    _json.dumps(samples)


def test_the_checks_after_a_fault_find_every_kind_of_half_saved_transaction(adapter, tmp_path):
    """Step 7 on a real database: a clean run gives 0 everywhere; then four different atomicity
    failures are made by hand, and each must be found with its own label."""
    import json as _json

    from resilience_tests.execution.workload.hooks import split_checks
    from resilience_tests.execution.workload.markers import MarkerJournals, read_journal
    from resilience_tests.observability.event_stream import EventStream

    asyncio.run(adapter.prepare_harness_state())
    w = Workload(profile="tpcc", concurrency=4, transaction_markers=True, rate_tps=100, tpcc={"warehouses": 2})
    profile = TpccProfile(w)
    asyncio.run(profile.prepare(adapter))

    async def go():
        stream, journals = EventStream(tmp_path / "events.jsonl"), MarkerJournals(tmp_path)
        drv = profile.make_driver(adapter, journals, stream)
        await drv.start()
        await asyncio.wait_for(drv.wait_until_ready(), 10)
        await asyncio.sleep(4)
        await drv.stop()
        journals.close()
        stream.close()
    asyncio.run(go())

    clean, facts, reasons = split_checks(asyncio.run(profile.consistency_checks(adapter, tmp_path)))
    assert reasons == {}, reasons
    assert clean["partial_commits"] == 0 and clean["aborted_markers_present"] == 0
    assert clean["tpcc_consistency_violations"] == 0, (clean, facts)
    # the stop cancels any transaction still in progress: each is "unknown" (never lost, never
    # committed) -- exactly those written but neither confirmed nor rolled back, at most one per clerk
    assert clean["indeterminate_txn"] == facts["written"] - facts["acked"] - facts["aborted"] <= 4, (clean, facts)

    acked = read_journal(tmp_path / "acked.jrnl").records
    orders = [r for r in acked if "o_id" in r]
    payments = [r for r in acked if "o_id" not in r]
    assert len(orders) >= 2 and payments
    one, two = orders[0], orders[1]
    # 1. an acknowledged New-Order lost one of its lines (marker kept, rows incomplete)
    q(adapter, f"DELETE FROM {S}.order_line WHERE ol_w_id=$1 AND ol_d_id=$2 AND ol_o_id=$3 AND ol_number=1",
      one["w"], one["d"], one["o_id"])
    # 2. an acknowledged New-Order lost its marker but kept its rows
    q(adapter, "DELETE FROM resilience.markers WHERE uuid = $1", uuid.UUID(two["uuid"]))
    # 3. a transaction the journal says was rolled back is in the database
    q(adapter, "SELECT 1")  # (no-op: the payment below is committed; its uuid is added to the rolled-back journal)
    with open(tmp_path / "aborted.jrnl", "a") as fh:
        fh.write(_json.dumps({"uuid": payments[0]["uuid"], "business_rollback": False, "sqlstate": "40P01"}) + "\n")
    # 4. an unknown New-Order kept its marker but created no order
    ghost = str(uuid.uuid4())
    with open(tmp_path / "marker.jrnl", "a") as fh:
        fh.write(_json.dumps({"seq": 999_999, "uuid": ghost, "t_pre": 0, "type": NEW_ORDER}) + "\n")
    q(adapter, "INSERT INTO resilience.markers (uuid, seq) VALUES ($1, 999999)", uuid.UUID(ghost))

    broken, facts, reasons = split_checks(asyncio.run(profile.consistency_checks(adapter, tmp_path)))
    assert reasons == {}, reasons
    parts = facts["partial_commit_parts"]
    assert parts["acknowledged_order_incomplete"] == 1                 # case 1
    assert parts["lost_but_rows_present"] == 1 and facts["lost"] == 1   # case 2: a partial commit, not just a loss
    assert broken["aborted_markers_present"] == 1                       # case 3
    assert parts["unexplained_order_count"] == -1                       # case 4
    assert broken["partial_commits"] == 3                               # cases 1, 2 and 4
    assert broken["tpcc_consistency_violations"] > 0                    # rules 4 and 6 see case 1 too


def test_several_worker_processes_lose_nothing_and_their_numbers_add_up(adapter, tmp_path):
    """Step 8: the same engine in 3 worker processes under the coordinator (12 clerks, 450 TPS,
    6 s) against the real database. Each process writes its own journals; the checks read them
    all. Afterwards: RPO 0, no phantom, nothing rolled back kept, every acknowledged New-Order
    complete, the rules hold, every sample has all 3 processes, and the sample totals equal the
    acknowledgement journals (nothing counted twice or lost in the merge)."""
    from resilience_tests.execution.workload.markers import (
        ABORTED_JOURNAL, ACKED_JOURNAL, MarkerJournals, diff_from_journals, journal_paths, read_journal)
    from resilience_tests.execution.workload.tpcc.coordinator import TpccCoordinator
    from resilience_tests.observability.event_stream import EventStream

    asyncio.run(adapter.tpcc_load(2, jobs=2, replace=True, seed=11))   # the step 7 test leaves broken data on purpose
    asyncio.run(adapter.prepare_harness_state())
    w = Workload(profile="tpcc", concurrency=12, transaction_markers=True, rate_tps=450, tpcc={"warehouses": 2})
    profile = TpccProfile(w)
    asyncio.run(profile.prepare(adapter))

    async def go():
        stream, journals = EventStream(tmp_path / "events.jsonl"), MarkerJournals(tmp_path)
        drv = profile.make_driver(adapter, journals, stream, processes=3)
        assert isinstance(drv, TpccCoordinator)
        await drv.start()
        await asyncio.wait_for(drv.wait_until_ready(), 60)
        ready_s = time.monotonic()
        drv.begin_window()
        await asyncio.sleep(6)
        window = drv.end_window()
        stop_s = time.monotonic()
        await drv.stop()
        journals.close()
        stream.close()
        return drv, window, list(stream.events()), ready_s, stop_s

    drv, window, events, ready_s, stop_s = asyncio.run(go())
    assert drv.failure is None, drv.failure
    assert window.tps > 300, window
    assert len(journal_paths(tmp_path, ACKED_JOURNAL)) == 4              # acked.jrnl (empty) + one per process
    db = asyncio.run(adapter.marker_ids())
    diff, torn = diff_from_journals(tmp_path, db)
    assert torn == 0 and diff.rpo_txn == 0 and not diff.phantom and not diff.unjournalled_ack
    aborted = [r for p in journal_paths(tmp_path, ABORTED_JOURNAL) for r in read_journal(p).records]
    assert aborted == [] or not ({r["uuid"] for r in aborted} & db)
    acked = [r for p in journal_paths(tmp_path, ACKED_JOURNAL) for r in read_journal(p).records]
    orders = [r for r in acked if "o_id" in r]
    assert orders and asyncio.run(adapter.tpcc_orders_incomplete(orders)) == 0
    rules = asyncio.run(adapter.tpcc_consistency([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12]))
    assert rules == {r: 0 for r in rules}, rules
    samples = [e.data for e in events if e.kind == "sample"]
    loads = [e.data for e in events if e.kind == "driver_load"]
    start = next(e.data for e in events if e.kind == "start")
    assert start["processes"] == 3 and start["clerks_per_process"] == [4, 4, 4]
    # every second between "all processes ready" and the stop has all 3 processes (before it a
    # process may still be starting; the stop's last part-second may come from some only)
    inside = [d for d in loads if ready_s + 1 < d["second"] < stop_s - 1]
    assert samples and len(inside) >= 4 and all(d["processes_reporting"] == 3 for d in inside), loads
    # every committed write transaction is in a sample once: sample totals = acknowledgement journal lines
    written = sum(s["by_type"][t]["commits"] for s in samples for t in (NEW_ORDER, PAYMENT, DELIVERY))
    assert written == len(acked), (written, len(acked))
    assert all(set(s["by_type"]) == {NEW_ORDER, PAYMENT, ORDER_STATUS, DELIVERY, STOCK_LEVEL} for s in samples)


def test_a_worker_process_that_dies_stops_the_run(adapter, tmp_path):
    """Step 8: if one worker process is killed, the coordinator records the failure (the
    orchestrator then aborts the run): its clerks' load silently missing would be a false baseline."""
    import os as _os
    import signal

    from resilience_tests.execution.workload.markers import MarkerJournals
    from resilience_tests.observability.event_stream import EventStream

    asyncio.run(adapter.tpcc_load(2, jobs=2, replace=True, seed=11))   # the step 7 test leaves broken data on purpose
    asyncio.run(adapter.prepare_harness_state())
    w = Workload(profile="tpcc", concurrency=4, transaction_markers=True, rate_tps=100, tpcc={"warehouses": 2})
    profile = TpccProfile(w)
    asyncio.run(profile.prepare(adapter))

    async def go():
        stream, journals = EventStream(tmp_path / "events.jsonl"), MarkerJournals(tmp_path)
        drv = profile.make_driver(adapter, journals, stream, processes=2)
        await drv.start()
        await asyncio.wait_for(drv.wait_until_ready(), 60)
        _os.kill(drv._procs[1].pid, signal.SIGKILL)
        for _ in range(40):
            if drv.failure:
                break
            await asyncio.sleep(0.25)
        failure = drv.failure
        await drv.stop()
        journals.close()
        stream.close()
        return failure, list(stream.events())

    failure, events = asyncio.run(go())
    assert failure and "worker process 1 exited unexpectedly" in failure, failure
    assert any(e.kind == "fatal" and e.data.get("worker_process") == 1 for e in events)


def test_the_reset_before_a_run_reloads_changed_data_and_keeps_the_capacity(adapter):
    """Step 9 on a real database: a column added to `history` and not removed (as noisy-neighbour
    scenario 3 does when its cleanup fails) makes the reset reload the data. Afterwards the
    columns are as the loader makes them, the dropped-column count is back to 0, the growth is
    1.0, the stored capacity is kept, a CHECKPOINT ran, and prepare accepts the data."""
    from resilience_tests.execution.workload.tpcc import data as tdata

    asyncio.run(adapter.tpcc_load(2, jobs=2, replace=True, seed=11))
    asyncio.run(adapter.tpcc_record_capacity(321.5))
    q(adapter, f"ALTER TABLE {S}.history ADD COLUMN nn3_test integer")
    q(adapter, f"ALTER TABLE {S}.history ADD COLUMN nn3_gone integer")
    q(adapter, f"ALTER TABLE {S}.history DROP COLUMN nn3_gone")
    cols = asyncio.run(adapter.tpcc_columns())
    assert cols["history"] == {"live": 9, "dropped": 1}

    w = Workload(profile="tpcc", concurrency=4, transaction_markers=True, rate_tps=10, tpcc={"warehouses": 2})
    profile = TpccProfile(w)
    asked = []
    out = asyncio.run(profile.reset_data(adapter, lambda s, why: asked.append(s)))
    r = out["tpcc_reset"]
    assert r["reloaded"] and any("history has 9 columns" in x for x in r["reasons"]), r
    assert asked and r["checkpoint"] == "done" and r["capacity_kept"] == 321.5
    cols = asyncio.run(adapter.tpcc_columns())
    assert {t: c["live"] for t, c in cols.items() if t in tdata.EXPECTED_COLUMNS} == tdata.EXPECTED_COLUMNS
    assert all(c["dropped"] == 0 for c in cols.values())
    status = asyncio.run(adapter.tpcc_data_status())
    assert status["capacity_tps"] == 321.5 and tdata.growth(status) == 1.0
    assert asyncio.run(profile.reset_data(adapter))["tpcc_reset"]["reloaded"] is False    # now clean
    asyncio.run(profile.prepare(adapter))                                                 # accepted


def test_each_confirmed_payment_is_checked_against_its_history_row(adapter, tmp_path):
    """Review fix 2 on a real database: after a run, every confirmed Payment has its history row;
    with one row removed by hand (a half-saved Payment), the per-payment check and the checks after
    a fault both find exactly that one."""
    from resilience_tests.execution.workload.hooks import split_checks
    from resilience_tests.execution.workload.markers import MarkerJournals, read_journal
    from resilience_tests.observability.event_stream import EventStream

    asyncio.run(adapter.tpcc_load(2, jobs=2, replace=True, seed=11))
    asyncio.run(adapter.prepare_harness_state())
    w = Workload(profile="tpcc", concurrency=4, transaction_markers=True, rate_tps=150, tpcc={"warehouses": 2})
    profile = TpccProfile(w)
    asyncio.run(profile.prepare(adapter))
    assert profile.history_since is not None

    async def go():
        stream, journals = EventStream(tmp_path / "events.jsonl"), MarkerJournals(tmp_path)
        drv = profile.make_driver(adapter, journals, stream)
        await drv.start()
        await asyncio.wait_for(drv.wait_until_ready(), 10)
        await asyncio.sleep(4)
        await drv.stop()
        journals.close()
        stream.close()
    asyncio.run(go())

    paid = [r for r in read_journal(tmp_path / "acked.jrnl").records if "amt" in r]
    assert paid and all({"w", "d", "c_w", "c_d", "c", "amt"} <= set(r) for r in paid)
    assert asyncio.run(adapter.tpcc_payment_check(paid, profile.history_since)) == {"checked": len(paid), "missing": 0}
    clean, _, reasons = split_checks(asyncio.run(profile.consistency_checks(adapter, tmp_path)))
    assert reasons == {} and clean["partial_commits"] == 0

    p = paid[0]
    where = (f"h_w_id={int(p['w'])} AND h_d_id={int(p['d'])} AND h_c_w_id={int(p['c_w'])} AND h_c_d_id={int(p['c_d'])} "
             f"AND h_c_id={int(p['c'])} AND h_amount={int(p['amt'])}/100.0")
    q(adapter, f"CREATE TABLE {S}.test_hold_h AS SELECT * FROM {S}.history WHERE {where}")
    q(adapter, f"DELETE FROM {S}.history WHERE {where}")
    try:
        assert asyncio.run(adapter.tpcc_payment_check(paid, profile.history_since))["missing"] >= 1
        broken, facts, _ = split_checks(asyncio.run(profile.consistency_checks(adapter, tmp_path)))
        assert facts["partial_commit_parts"]["acknowledged_payment_missing"] >= 1 and broken["partial_commits"] >= 1
    finally:
        q(adapter, f"INSERT INTO {S}.history SELECT * FROM {S}.test_hold_h")
        q(adapter, f"DROP TABLE {S}.test_hold_h")
    # rows from before the run (the load's own 10.00 payments) are never taken for this run's payments
    assert asyncio.run(adapter.tpcc_payment_check(
        [{"w": 1, "d": 1, "c_w": 1, "c_d": 1, "c": 1, "amt": 1000}], profile.history_since))["missing"] == 1
