"""Contract tests for transaction record channel and RPO invariants (CT-1 to CT-6).

Validates contracts/transaction-record.md against fake workloads and databases:
- CT-1: pre-record precedes commit-send time observed by database.
- CT-2: database killed mid-load -> acknowledged ⊆ pre, rpo_txn == 0 for honest DB.
- CT-3: lying database -> rpo_txn >= 1 (fail-closed negative case, Constitution III).
- CT-4: pre record failure -> transaction never commits, never acknowledged without pre.
- CT-5: ack record failure -> channel marked incomplete, run aborts and issues no RPO figure.
- CT-6: repeated relaunches -> identities never repeat, 0 torn lines and 0 unjournalled.
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
import pytest

from resilience_tests.execution.workload.identity import identity_text, identity_uuid
from resilience_tests.execution.workload.markers import MarkerJournals, diff_from_journals
from resilience_tests.execution.workload.record_channel import (
    JournalRecordChannel,
    TransactionRecord,
)


def test_ct1_pre_record_precedes_commit_send(tmp_path: Path):
    """CT-1: every acknowledged identity's pre record precedes commit-send observed by DB."""
    journals = MarkerJournals(tmp_path)
    channel = JournalRecordChannel(journals)

    observed_commit_sends: dict[str, float] = {}

    async def run_workload():
        for i in range(1, 10):
            ident = identity_text(launch=1, client=0, seq=i)
            t_pre = time.time()
            await channel.record_pre(ident, t_pre, seq=i)

            # Database observes commit send strictly after pre-record is durable
            time.sleep(0.001)
            t_commit_send = time.time()
            observed_commit_sends[ident] = t_commit_send

            # Database acknowledges
            time.sleep(0.001)
            t_ack = time.time()
            await channel.record_ack(ident, t_ack)

    asyncio.run(run_workload())
    journals.close()

    pre_records = {r.identity: r for r in channel.all_records if r.kind == "pre"}
    ack_records = {r.identity: r for r in channel.all_records if r.kind == "ack"}

    assert len(ack_records) == 9
    for ident, ack_rec in ack_records.items():
        assert ident in pre_records
        pre_rec = pre_records[ident]
        t_commit_send = observed_commit_sends[ident]
        # TR-1: Pre record timestamp precedes database commit-send
        assert pre_rec.t_wall < t_commit_send
        # TR-2: Ack record timestamp follows database commit-send
        assert t_commit_send < ack_rec.t_wall


def test_ct2_kill_mid_load_honest_database(tmp_path: Path):
    """CT-2: database killed mid-load. Acknowledged ⊆ pre; rpo_txn == 0 for honest DB."""
    journals = MarkerJournals(tmp_path)
    channel = JournalRecordChannel(journals)
    db_committed_uuids: set[str] = set()

    async def run_workload():
        for i in range(1, 20):
            ident = identity_text(launch=1, client=0, seq=i)
            t_pre = time.time()
            await channel.record_pre(ident, t_pre, seq=i)

            if i <= 10:
                # Committed before kill
                db_committed_uuids.add(identity_uuid(ident))
                await channel.record_ack(ident, time.time())
            elif i <= 15:
                # Committed in DB but kill severed connection before ack received
                db_committed_uuids.add(identity_uuid(ident))
                # in-flight / indeterminate, no ack
            else:
                # In-flight and lost/aborted in DB before commit
                pass

    asyncio.run(run_workload())
    journals.close()

    pre_ids = {r.identity for r in channel.all_records if r.kind == "pre"}
    ack_ids = {r.identity for r in channel.all_records if r.kind == "ack"}

    # Acknowledged subset of pre
    assert ack_ids.issubset(pre_ids)
    assert len(ack_ids) == 10
    assert len(pre_ids) == 19

    # Diff from journals against honest DB
    diff, torn = diff_from_journals(tmp_path, db_committed_uuids)
    assert torn == 0
    assert diff.rpo_txn == 0  # 0 lost
    assert len(diff.lost) == 0
    assert diff.unjournalled_ack == frozenset()
    # Indeterminate transactions (written but unacked) reported
    assert len(diff.indeterminate) == 9
    assert len(diff.indeterminate_committed) == 5


def test_ct3_lying_database_detects_data_loss(tmp_path: Path):
    """CT-3: a fake database that acknowledges and then loses a commit -> rpo_txn >= 1."""
    journals = MarkerJournals(tmp_path)
    channel = JournalRecordChannel(journals)
    db_committed_uuids: set[str] = set()

    async def run_workload():
        for i in range(1, 10):
            ident = identity_text(launch=1, client=0, seq=i)
            await channel.record_pre(ident, time.time(), seq=i)
            await channel.record_ack(ident, time.time())
            if i != 5:
                # DB drops transaction 5 after acknowledging it!
                db_committed_uuids.add(identity_uuid(ident))

    asyncio.run(run_workload())
    journals.close()

    diff, torn = diff_from_journals(tmp_path, db_committed_uuids)
    # Constitution III: fail-closed negative test detecting data loss
    assert diff.rpo_txn == 1
    lost_ident = identity_text(1, 0, 5)
    assert identity_uuid(lost_ident) in diff.lost


def test_ct4_pre_record_failure_aborts_transaction(tmp_path: Path):
    """CT-4: the pre record fails -> transaction never commits, never acked-without-pre."""
    class FailingMarkerJournals(MarkerJournals):
        async def written(self, seq: int, uuid: str, t_pre: float) -> None:
            raise OSError("Disk full: cannot write pre-commit marker")

    journals = FailingMarkerJournals(tmp_path)
    channel = JournalRecordChannel(journals)

    async def run_attempt():
        ident = identity_text(launch=1, client=0, seq=1)
        with pytest.raises(OSError, match="Disk full"):
            await channel.record_pre(ident, time.time(), seq=1)

        # Invariant TR-3: Pre failed, so transaction must never be acknowledged
        with pytest.raises(RuntimeError, match="ack without pre record"):
            await channel.record_ack(ident, time.time())

    asyncio.run(run_attempt())
    assert channel.complete is False
    reason = channel.incomplete_reason or ""
    assert "failed writing pre-commit record" in reason or "ack without pre record" in reason



def test_ct5_ack_record_failure_marks_incomplete(tmp_path: Path):
    """CT-5: ack record fails after commit -> run detects incomplete channel, issues no RPO."""
    class FailingAckJournals(MarkerJournals):
        async def acknowledged(self, uuid: str, t_ack: float) -> None:
            raise OSError("Disk I/O error writing acked journal")

    journals = FailingAckJournals(tmp_path)
    channel = JournalRecordChannel(journals)

    async def run_attempt():
        ident = identity_text(launch=1, client=0, seq=1)
        await channel.record_pre(ident, time.time(), seq=1)
        with pytest.raises(OSError, match="Disk I/O error"):
            await channel.record_ack(ident, time.time())

    asyncio.run(run_attempt())
    journals.close()

    assert channel.complete is False
    assert "failed writing ack record" in (channel.incomplete_reason or "")


def test_ct6_repeated_relaunches_unique_identities(tmp_path: Path):
    """CT-6: repeated relaunches -> no identity repeats; diff_from_journals 0 torn, 0 unjournalled."""
    journals = MarkerJournals(tmp_path)
    channel = JournalRecordChannel(journals)
    all_seen_idents: set[str] = set()
    db_uuids: set[str] = set()

    async def simulate_launches():
        for launch_id in range(1, 5):
            for seq in range(1, 6):
                ident = identity_text(launch=launch_id, client=0, seq=seq)
                assert ident not in all_seen_idents, f"Duplicate identity {ident}"
                all_seen_idents.add(ident)

                await channel.record_pre(ident, time.time(), seq=seq)
                await channel.record_ack(ident, time.time())
                db_uuids.add(identity_uuid(ident))

    asyncio.run(simulate_launches())
    journals.close()

    assert len(all_seen_idents) == 20
    diff, torn = diff_from_journals(tmp_path, db_uuids)
    assert torn == 0
    assert diff.rpo_txn == 0
    assert diff.unjournalled_ack == frozenset()
    assert diff.phantom == frozenset()
