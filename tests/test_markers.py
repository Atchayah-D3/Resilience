import asyncio
import json

from resilience_tests.execution.workload.markers import (
    ACKED_JOURNAL, MARKER_JOURNAL, MarkerJournals, diff_from_journals, diff_markers, read_journal,
)


def test_diff_semantics():
    d = diff_markers(written={"a", "b", "c", "d"}, acked={"a", "b"}, db={"a", "c", "x"})
    assert d.lost == {"b"} and d.rpo_txn == 1  # acked, not in db
    assert d.indeterminate == {"c", "d"}  # in flight at T0: reported, not counted
    assert d.indeterminate_committed == {"c"}
    assert d.phantom == {"x"}  # never written by the driver
    assert d.unjournalled_ack == frozenset()


def test_indeterminate_is_never_counted_as_loss():
    d = diff_markers(written={"a", "b"}, acked={"a"}, db={"a"})
    assert d.rpo_txn == 0 and len(d.indeterminate) == 1


def test_journals_roundtrip_and_ordering(tmp_path):
    async def go():
        j = MarkerJournals(tmp_path)
        for _ in range(50):
            seq, uid = j.next_marker()
            await j.written(seq, uid, 1.0)
            if seq % 2:
                await j.acknowledged(uid, 2.0)
        j.close()

    asyncio.run(go())
    written = read_journal(tmp_path / MARKER_JOURNAL).records
    acked = read_journal(tmp_path / ACKED_JOURNAL).records
    assert [r["seq"] for r in written] == list(range(1, 51))
    assert len(acked) == 25
    d, torn = diff_from_journals(tmp_path, {r["uuid"] for r in acked})
    assert d.rpo_txn == 0 and len(d.indeterminate) == 25 and torn == 0


def test_concurrent_appends_share_fsync_but_all_complete(tmp_path):
    async def go():
        j = MarkerJournals(tmp_path)

        async def one():
            seq, uid = j.next_marker()
            await j.written(seq, uid, 0.0)

        await asyncio.gather(*(one() for _ in range(500)))
        j.close()

    asyncio.run(go())
    assert len(read_journal(tmp_path / MARKER_JOURNAL).records) == 500


def test_torn_final_line_is_reported_not_guessed(tmp_path):
    p = tmp_path / MARKER_JOURNAL
    p.write_text(json.dumps({"seq": 1, "uuid": "a", "t_pre": 1}) + "\n" + '{"seq": 2, "uu')
    r = read_journal(p)
    assert len(r.records) == 1 and r.torn_lines == 1


def test_a_sure_rollback_is_not_indeterminate():
    """Was: the general marker count showed TPC-C's business rollbacks as unknown (223 shown,
    60 truly unknown, in the first lab NL-C-01 run with TPC-C). A transaction the database
    answered "rolled back" had an answer: it is counted as rolled back, not as unknown."""
    d = diff_markers(written={"a", "b", "c", "d", "e"}, acked={"a"}, db={"a", "c"}, aborted={"b", "c"})
    assert d.indeterminate == {"d", "e"}                 # no answer at all
    assert d.rolled_back == {"b", "c"}
    assert d.rolled_back_kept == {"c"}                   # answered "rolled back", yet in the database
    assert d.indeterminate_committed == frozenset()
    assert d.rpo_txn == 0


def test_an_acknowledgement_wins_over_a_rollback_line_so_its_loss_is_still_seen():
    d = diff_markers(written={"a"}, acked={"a"}, db=set(), aborted={"a"})
    assert d.rolled_back == frozenset() and d.lost == {"a"} and d.rpo_txn == 1


def test_without_a_rollback_journal_nothing_changes(tmp_path):
    """The custom workload writes no aborted journal: its counts are exactly as before."""
    async def go():
        j = MarkerJournals(tmp_path)
        for _ in range(4):
            seq, uid = j.next_marker()
            await j.written(seq, uid, 1.0)
            if seq % 2:
                await j.acknowledged(uid, 2.0)
        j.close()
    asyncio.run(go())
    acked = {r["uuid"] for r in read_journal(tmp_path / ACKED_JOURNAL).records}
    d, torn = diff_from_journals(tmp_path, acked)
    assert torn == 0 and len(d.indeterminate) == 2 and d.rolled_back == frozenset()


def test_the_rollback_journals_of_every_worker_process_are_read(tmp_path):
    async def go():
        for idx in (0, 1):
            j = MarkerJournals(tmp_path, index=idx, processes=2)
            for n in range(3):                       # one confirmed, one rolled back, one unknown
                seq, uid = j.next_marker()
                await j.written(seq, uid, 1.0)
                if n == 0:
                    await j.acknowledged(uid, 2.0)
                elif n == 1:
                    await j.aborted(uid, 2.0)
            j.close()
    asyncio.run(go())
    acked = {r["uuid"] for p in tmp_path.glob("acked.*.jrnl") for r in read_journal(p).records}
    d, _ = diff_from_journals(tmp_path, acked)
    assert len(d.rolled_back) == 2 and len(d.indeterminate) == 2 and d.rpo_txn == 0


def test_the_database_markers_are_kept_so_the_rpo_comparison_can_be_redone(tmp_path):
    """Audit, 12 November: the database's list of markers was used but not kept, so the journals-
    against-database comparison could not be checked again from the evidence."""
    import gzip
    import hashlib

    from resilience_tests.execution.workload.markers import DB_MARKERS_FILE, read_db_markers, write_db_markers
    ids = {"c", "a", "b"}
    info = write_db_markers(tmp_path, ids)
    assert info["file"] == DB_MARKERS_FILE and info["count"] == 3
    text = gzip.open(tmp_path / DB_MARKERS_FILE, "rb").read()
    assert text == b"a\nb\nc\n" and info["sha256"] == hashlib.sha256(text).hexdigest()   # sorted, checkable
    assert read_db_markers(tmp_path) == ids
    assert write_db_markers(tmp_path, set())["count"] == 0 and read_db_markers(tmp_path) == set()


def test_marker_numbers_are_unique_across_worker_processes(tmp_path):
    """Fix 3: each of N worker processes numbers its markers i+1, i+1+N, ...; one process 1, 2, 3."""
    import pytest
    single = MarkerJournals(tmp_path / "one" if (tmp_path / "one").mkdir() is None else tmp_path)
    assert [single.next_marker()[0] for _ in range(3)] == [1, 2, 3]
    single.close()
    procs = [MarkerJournals(tmp_path, index=i, processes=4) for i in range(4)]
    numbers = [n for j in procs for n in (j.next_marker()[0] for _ in range(50))]
    for j in procs:
        j.close()
    assert len(numbers) == len(set(numbers)) == 200
    with pytest.raises(ValueError):
        MarkerJournals(tmp_path, index=4, processes=4)
