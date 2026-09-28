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
