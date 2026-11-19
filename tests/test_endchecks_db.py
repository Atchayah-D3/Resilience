"""The common workload's end checks compared in the database (service/endchecks.py,
adapters/postgresql/journal_check.py) against a REAL PostgreSQL/ShaktiDB: they must give exactly
the numbers of the in-memory checks they replace, on a clean run and on a run damaged on purpose
in every way the checks look for. And their memory must not grow with the number of lines.

Skipped unless a throw-away test database is named (never the lab target):

    RESILIENCE_TEST_PG=127.0.0.1:55432/resilience/harness .venv/bin/python -m pytest tests/test_endchecks_db.py -q
"""

from __future__ import annotations

import asyncio
import json
import os
import tracemalloc
import uuid

import asyncpg
import pytest

from catalog.schema import Workload
from resilience_tests.adapters.postgresql.adapter import PostgreSQLAdapter
from resilience_tests.control.profile import DbEndpoint
from resilience_tests.execution.workload.contract import NEW_ORDER, PAYMENT, TPCC_SCHEMA
from resilience_tests.execution.workload.markers import (
    ACKED_JOURNAL,
    DB_MARKERS_FILE,
    MarkerJournals,
    diff_from_journals,
    journal_paths,
    read_db_markers,
    read_journal,
    write_db_markers,
)
from resilience_tests.execution.workload.service import endchecks
from resilience_tests.execution.workload.tpcc.profile import TpccProfile
from resilience_tests.observability.event_stream import EventStream
from tests.test_adapter_seam import NODE

TARGET = os.environ.get("RESILIENCE_TEST_PG")
pytestmark = pytest.mark.skipif(not TARGET, reason="RESILIENCE_TEST_PG (host:port/db/user of a throw-away database) not set")
S = TPCC_SCHEMA
WAREHOUSES = 2


def _endpoint() -> DbEndpoint:
    hostport, db, user = TARGET.split("/")
    host, port = hostport.split(":")
    return DbEndpoint(host=host, port=int(port), dbname=db, user=user)


@pytest.fixture(scope="module")
def adapter():
    ep = _endpoint()
    a = PostgreSQLAdapter(NODE.model_copy(update={"db": ep, "client": ep}))
    a.free_bytes = 10**12

    async def free():                    # never SSH from a test: the node's host is a lab machine
        return a.free_bytes
    a.data_fs_free_bytes = free
    a._grant_harness_roles = _no_grants
    asyncio.run(a.prepare_harness_state())
    asyncio.run(a.tpcc_load(WAREHOUSES, jobs=2, replace=True, seed=7))
    yield a
    asyncio.run(a.tpcc_drop())


async def _no_grants():
    return None


def q(adapter, query, *args):
    async def go():
        ep = adapter.node.db
        c = await asyncpg.connect(host=ep.host, port=ep.port, database=ep.dbname, user=ep.user)
        try:
            return await c.execute(query, *args) if query.lstrip().upper().startswith(("DELETE", "INSERT", "UPDATE"))\
                else await c.fetchval(query, *args)
        finally:
            await c.close()
    return asyncio.run(go())


class MemoryOnly:
    """The same adapter without journal_check: endchecks compares in memory."""

    def __init__(self, inner):
        self._inner = inner

    def __getattr__(self, name):
        if name == "journal_check":
            raise AttributeError(name)
        return getattr(self._inner, name)


# --- the in-memory end checks as the runner did them before (the reference) ------------------------


async def old_end_checks(adapter, folder, profile):
    """ServiceRunner._end_checks before the change: diff_from_journals and the profile's
    consistency_checks, every journal held in memory."""
    end = {"_errors": []}
    try:
        db = await adapter.marker_ids()
        db_file = write_db_markers(folder, db)
        diff, torn = diff_from_journals(folder, db)
        trusted = not torn and not diff.unjournalled_ack
        lost = set(diff.lost)
        lost_t = sorted(float(r.get("t_ack", 0.0)) for p in journal_paths(folder, ACKED_JOURNAL)
                        for r in read_journal(p).records if r.get("uuid") in lost)
        end.update({"rpo_txn": diff.rpo_txn if trusted else None, "phantom": len(diff.phantom),
                    "markers": {"written": diff.written, "acked": diff.acked, "in_db": diff.in_db,
                                "lost": sorted(diff.lost)[:100], "lost_count": len(diff.lost),
                                "indeterminate": len(diff.indeterminate),
                                "indeterminate_committed": len(diff.indeterminate_committed),
                                "rolled_back": len(diff.rolled_back), "phantom": sorted(diff.phantom)[:100],
                                "unjournalled_ack": len(diff.unjournalled_ack), "torn_journal_lines": torn}})
        if not trusted:
            end["_errors"].append("the journals disagree with themselves (torn lines or acknowledgements "
                                  "without a marker line): the lost-commit count is not trusted")
    except Exception as exc:  # noqa: BLE001
        end["_errors"].append(f"lost-commit check: {type(exc).__name__}: {exc}")
        db_file, lost_t = None, []
    checks = await profile.consistency_checks(adapter, folder)
    end["_not_measured"] = checks.pop("_not_measured", {})
    end["tpcc_facts"] = checks.pop("_facts", {})
    end.update(checks)
    return end, db_file, lost_t


def comparable(end):
    """Everything except the random sample of districts (all 20 districts of 2 warehouses are
    checked whatever the seed, so the rules' results do not depend on it)."""
    e = json.loads(json.dumps(end, default=str))
    e.get("tpcc_facts", {}).pop("consistency_sample_districts", None)
    return e


def new_end_checks(adapter, folder, profile, **kw):
    t = endchecks.TpccInputs(orders_before=profile.orders_at_prepare, history_since=profile.history_since,
                             warehouses=WAREHOUSES)
    return asyncio.run(endchecks.run_end_checks(adapter, folder, tpcc=t, **kw))


def run_engine(adapter, folder, clerks=6, rate=150, seconds=4):
    asyncio.run(adapter.prepare_harness_state())
    w = Workload(profile="tpcc", concurrency=clerks, transaction_markers=True, rate_tps=rate,
                 tpcc={"warehouses": WAREHOUSES, "worker_processes": 2})
    profile = TpccProfile(w)
    asyncio.run(profile.prepare(adapter))

    async def go():
        stream = EventStream(folder / "events.jsonl", keep_in_memory=False)
        journals = MarkerJournals(folder)
        drv = profile.make_driver(adapter, journals, stream)
        await drv.start()
        await asyncio.wait_for(drv.wait_until_ready(), 20)
        await asyncio.sleep(seconds)
        await drv.stop()
        journals.close()
        stream.close()
        assert drv.failure is None, drv.failure
    asyncio.run(go())
    return profile


def assert_same(adapter, folder, profile):
    old, old_file, old_lost_t = asyncio.run(old_end_checks(adapter, folder, profile))
    old_digest = endchecks.file_digest(folder / DB_MARKERS_FILE)
    in_db = new_end_checks(adapter, folder, profile)
    in_mem = new_end_checks(MemoryOnly(adapter), folder, profile)
    assert comparable(in_db.end) == comparable(old), (in_db.end, old)
    assert comparable(in_mem.end) == comparable(old), (in_mem.end, old)
    assert in_db.facts["markers_db_file"] == old_file == old_digest      # the same file, byte for byte
    assert sorted(in_db.lost_t_ack) == old_lost_t == sorted(in_mem.lost_t_ack)
    assert in_db.facts["journal_check"]["compared_in"].startswith("database")
    # checked again later from the saved markers file: the same answer
    again = new_end_checks(adapter, folder, profile, markers_from_file=True)
    assert comparable(again.end) == comparable(old)
    assert again.facts["markers_db_file"]["read_from_file"] is True
    return old, in_db


def test_a_clean_run_gives_exactly_the_old_checks_numbers(adapter, tmp_path):
    profile = run_engine(adapter, tmp_path)
    assert {x.name for x in journal_paths(tmp_path, "marker.jrnl")} >= {"marker.0.jrnl", "marker.1.jrnl"}  # per process
    old, new = assert_same(adapter, tmp_path, profile)
    assert new.end["rpo_txn"] == 0 and new.end["phantom"] == 0 and new.end["partial_commits"] == 0
    assert new.end["aborted_markers_present"] == 0 and new.end["tpcc_consistency_violations"] == 0
    assert new.end["markers"]["acked"] > 100 and not new.end["_errors"] and not new.end["_not_measured"]


def test_every_kind_of_damage_is_found_exactly_as_before(adapter, tmp_path):
    """Lost commits, a half-saved order, a payment whose history row is gone, a rolled-back
    transaction kept, a phantom, an unknown transaction that committed, duplicate and torn
    journal lines: the database comparison must count each exactly as the in-memory one."""
    profile = run_engine(adapter, tmp_path)
    acked = [r for p in journal_paths(tmp_path, ACKED_JOURNAL) for r in read_journal(p).records]
    orders = [r for r in acked if "o_id" in r]
    payments = [r for r in acked if "amt" in r]
    assert len(orders) >= 3 and len(payments) >= 3
    one, two = orders[:2]
    # 1. an acknowledged New-Order lost one of its lines (marker kept): incomplete
    q(adapter, f"DELETE FROM {S}.order_line WHERE ol_w_id=$1 AND ol_d_id=$2 AND ol_o_id=$3 AND ol_number=1",
      one["w"], one["d"], one["o_id"])
    # 2. an acknowledged New-Order lost its marker but kept its rows: lost, and a partial commit
    q(adapter, "DELETE FROM resilience.markers WHERE uuid = $1", uuid.UUID(two["uuid"]))
    def drop_history_row(p):
        q(adapter, f"""DELETE FROM {S}.history WHERE ctid = (SELECT ctid FROM {S}.history
                         WHERE h_w_id=$1 AND h_d_id=$2 AND h_c_w_id=$3 AND h_c_d_id=$4 AND h_c_id=$5
                           AND (h_amount * 100)::int = $6 AND h_date >= $7 LIMIT 1)""",
          p["w"], p["d"], p["c_w"], p["c_d"], p["c"], p["amt"], profile.history_since)
    # 3. an acknowledged Payment lost its marker AND its history row: a plain lost commit
    three = payments[2]
    q(adapter, "DELETE FROM resilience.markers WHERE uuid = $1", uuid.UUID(three["uuid"]))
    drop_history_row(three)
    # 4. an acknowledged Payment whose history row is gone (marker kept)
    drop_history_row(payments[0])
    marker0 = tmp_path / "marker.0.jrnl"
    acked0 = tmp_path / "acked.0.jrnl"
    aborted0 = tmp_path / "aborted.0.jrnl"
    with aborted0.open("a") as fh:
        # 5. the journal says rolled back, but it is in the database
        fh.write(json.dumps({"uuid": payments[1]["uuid"], "t_abort": 1.0, "type": PAYMENT, "sqlstate": "40P01",
                             "business_rollback": False}) + "\n")
    ghost, phantom, unknown = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
    with marker0.open("a") as fh:
        # 6. an unknown New-Order kept its marker but created no order
        fh.write(json.dumps({"seq": 999_991, "uuid": ghost, "t_pre": 0, "type": NEW_ORDER}) + "\n")
        # 7. an unknown Payment that did commit
        fh.write(json.dumps({"seq": 999_993, "uuid": unknown, "t_pre": 0, "type": PAYMENT}) + "\n")
        # 8. the same marker line twice (duplicates count once, as in the sets)
        first = read_journal(marker0).records[0]
        fh.write(json.dumps(first) + "\n")
    q(adapter, "INSERT INTO resilience.markers (uuid, seq) VALUES ($1, 999991), ($2, 999992), ($3, 999993)",
      uuid.UUID(ghost), uuid.UUID(phantom), uuid.UUID(unknown))      # 9. phantom: in the database, never written
    old, new = assert_same(adapter, tmp_path, profile)
    e, parts = new.end, new.end["tpcc_facts"]["partial_commit_parts"]
    assert e["rpo_txn"] == 2 and e["markers"]["lost_count"] == 2 and e["phantom"] == 1
    assert sorted(e["markers"]["lost"]) == sorted([two["uuid"], three["uuid"]])
    assert e["markers"]["phantom"] == [phantom] and e["markers"]["indeterminate_committed"] >= 2
    assert parts["acknowledged_order_incomplete"] == 1 and parts["lost_but_rows_present"] == 1
    assert parts["acknowledged_payment_missing"] == 1 and parts["unexplained_order_count"] == -1
    assert e["aborted_markers_present"] == 1 and e["partial_commits"] == 4
    assert e["tpcc_consistency_violations"] > 0
    # 10. a torn line (a crash in the middle of a write): the lost-commit count is no longer trusted
    with acked0.open("a") as fh:
        fh.write('{"uuid": "abc')
    old, new = assert_same(adapter, tmp_path, profile)
    assert new.end["rpo_txn"] is None and new.end["markers"]["torn_journal_lines"] == 1
    assert any("not trusted" in x for x in new.end["_errors"])


def test_a_line_that_is_not_a_journal_record_is_an_error_never_a_pass(adapter, tmp_path):
    profile = run_engine(adapter, tmp_path, seconds=1)
    with (tmp_path / "acked.1.jrnl").open("a") as fh:
        fh.write(json.dumps({"t_ack": 1.0}) + "\n")             # valid JSON, no uuid
    r = new_end_checks(adapter, tmp_path, profile)
    assert "rpo_txn" not in r.end and any("without a uuid" in x for x in r.end["_errors"])
    assert "partial_commits" in r.end["_not_measured"]           # not measured, never 0


def test_memory_does_not_grow_with_the_number_of_journal_lines(adapter, tmp_path):
    """300,000 committed transactions written straight into the journals and the marker table:
    the harness's Python memory stays small (one batch), where the in-memory comparison would
    hold every line."""
    n = 300_000
    ids = [str(uuid.uuid4()) for _ in range(n)]
    with (tmp_path / "marker.jrnl").open("w") as m, (tmp_path / "acked.jrnl").open("w") as a:
        for i, u in enumerate(ids):
            m.write(json.dumps({"seq": i + 1, "uuid": u, "t_pre": 1.0, "type": PAYMENT}) + "\n")
            a.write(json.dumps({"uuid": u, "t_ack": 1.1}) + "\n")

    async def load_markers():
        ep = adapter.node.db
        c = await asyncpg.connect(host=ep.host, port=ep.port, database=ep.dbname, user=ep.user)
        try:
            await c.execute("TRUNCATE resilience.markers")
            await c.copy_records_to_table("markers", schema_name="resilience", columns=["uuid", "seq"],
                                          records=[(uuid.UUID(u), i + 1) for i, u in enumerate(ids)])
        finally:
            await c.close()
    asyncio.run(load_markers())
    del ids
    tracemalloc.start()
    try:
        r = asyncio.run(endchecks.run_end_checks(adapter, tmp_path, tpcc=None))
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert r.end["rpo_txn"] == 0 and r.end["markers"]["acked"] == n and r.end["markers"]["in_db"] == n
    assert peak < 40e6, f"peak Python memory {peak / 1e6:.0f} MB for {n} lines"
    assert read_db_markers(tmp_path) and r.facts["markers_db_file"]["count"] == n


def test_the_check_never_starts_without_room_for_its_temporary_tables(adapter, tmp_path):
    profile = run_engine(adapter, tmp_path, seconds=1)
    need = endchecks.temp_space_needed(tmp_path, markers_from_file=False)
    adapter.free_bytes = int(need * 1.2)               # less than 1.5 x what the tables will take
    try:
        r = new_end_checks(adapter, tmp_path, profile)
    finally:
        adapter.free_bytes = 10**12
    assert "rpo_txn" not in r.end and any("NotEnoughSpace" in x for x in r.end["_errors"])
    assert "partial_commits" in r.end["_not_measured"]           # not measured, never a pass
    ok = new_end_checks(adapter, tmp_path, profile)
    assert ok.end["rpo_txn"] == 0 and ok.facts["temp_space"]["temp_bytes_expected"] == need
