"""Contract tests CT-1 to CT-6 (contracts/transaction-record.md) for the pgbench record steps
(research R1, option B): every record is made by a pgbench transaction's own shell step,
through the harness's journal service, and checked against what the fake database holds."""

import asyncio

from resilience_tests.execution.workload.markers import diff_from_journals, read_journal
from tests.fakes.pgbench_env import close_evidence, make_driver, until


def _journal(driver, name):
    return read_journal(driver.journals.run_dir / name).records


def test_ct1_pre_record_is_durable_before_the_commit(tmp_path, monkeypatch):
    async def go():
        driver, _, db = make_driver(tmp_path, monkeypatch, concurrency=3, rate_tps=60)
        await driver.start()
        await until(lambda: len(db.committed()) >= 40, what="40 commits")
        await driver.stop()
        close_evidence(driver)
        return driver, db

    driver, db = asyncio.run(go())
    pre = {r["uuid"]: r["t_pre"] for r in _journal(driver, "marker.jrnl")}
    acked = {r["uuid"] for r in _journal(driver, "acked.jrnl")}
    committed = db.committed()
    assert acked and acked <= set(committed)
    for uuid, t_commit in committed.items():
        assert uuid in pre, "a committed transaction has no before-commit record"
        assert pre[uuid] < t_commit, "the before-commit record was made after the commit"


def test_ct2_database_killed_mid_load(tmp_path, monkeypatch):
    async def go():
        driver, adapter, db = make_driver(tmp_path, monkeypatch, concurrency=4, rate_tps=80)
        await driver.start()
        await driver.wait_until_ready()
        await until(lambda: len(db.committed()) >= 20, what="load before the kill")
        db.flag("down")
        await until(lambda: _ended(driver, "client_aborted") >= 4, what="every client dropped")
        await asyncio.sleep(0.5)
        db.flag("down", on=False)
        await until(lambda: len(db.committed()) >= 60, what="load after recovery")
        await driver.stop()
        close_evidence(driver)
        return driver, adapter

    driver, adapter = asyncio.run(go())
    diff, torn = diff_from_journals(driver.journals.run_dir, asyncio.run(adapter.marker_ids()))
    assert torn == 0
    assert diff.rpo_txn == 0
    assert not diff.unjournalled_ack and not diff.phantom
    assert diff.acked <= diff.written


def test_ct3_a_database_that_loses_acknowledged_commits_is_caught(tmp_path, monkeypatch):
    async def go():
        driver, adapter, db = make_driver(tmp_path, monkeypatch, concurrency=2, rate_tps=50)
        db.flag("lose")   # COMMIT acknowledged, nothing persists
        await driver.start()
        await until(lambda: len(_journal_now(driver)) >= 10, what="acknowledged commits")
        await driver.stop()
        close_evidence(driver)
        return driver, adapter

    driver, adapter = asyncio.run(go())
    diff, _ = diff_from_journals(driver.journals.run_dir, asyncio.run(adapter.marker_ids()))
    assert diff.rpo_txn >= 10, "acknowledged commits the database lost must count as lost"


def _ended(driver, how):
    return sum(1 for e in driver.stream.events() if e.kind == "launch_end" and e.data["ended_as"] == how)


def _journal_now(driver):
    p = driver.journals.run_dir / "acked.jrnl"
    return p.read_text().splitlines() if p.exists() else []


def test_ct4_a_failed_before_commit_record_stops_the_transaction(tmp_path, monkeypatch):
    async def go():
        driver, _, db = make_driver(tmp_path, monkeypatch, concurrency=2, rate_tps=50)
        real = driver.journals.written
        calls = {"n": 0}

        async def failing(seq, uuid, t_pre):
            calls["n"] += 1
            if calls["n"] > 5:
                raise OSError(28, "No space left on device")
            await real(seq, uuid, t_pre)

        monkeypatch.setattr(driver.journals, "written", failing)
        await driver.start()
        await until(lambda: driver.failure is not None, what="the run to fail")
        await driver.stop()
        close_evidence(driver)
        return driver, db

    driver, db = asyncio.run(go())
    assert "marker journal write failed" in driver.failure
    written = {r["uuid"] for r in _journal(driver, "marker.jrnl")}
    assert set(db.committed()) <= written, "a transaction committed without its before-commit record"


def test_ct5_a_failed_acknowledgement_record_fails_the_run(tmp_path, monkeypatch):
    async def go():
        driver, _, db = make_driver(tmp_path, monkeypatch, concurrency=2, rate_tps=50)

        async def failing(uuid, t_ack):
            raise OSError(5, "Input/output error")

        monkeypatch.setattr(driver.journals, "acknowledged", failing)
        await driver.start()
        await until(lambda: driver.failure is not None, what="the run to fail")
        await driver.stop()
        close_evidence(driver)
        return driver, db

    driver, db = asyncio.run(go())
    assert "acknowledgement journal write failed" in driver.failure
    # the commit happened, and no acknowledgement claims it: the run has no RPO to issue
    assert db.committed() and not _journal(driver, "acked.jrnl")


def test_ct6_relaunches_never_reuse_an_identity(tmp_path, monkeypatch):
    async def go():
        driver, adapter, db = make_driver(tmp_path, monkeypatch, concurrency=3, rate_tps=60)
        await driver.start()
        for _ in range(2):
            await until(lambda: len(db.committed()) >= 10, what="load")
            launches = driver._launch_counter
            db.flag("down")
            await asyncio.sleep(0.4)
            db.flag("down", on=False)
            await until(lambda: driver._launch_counter >= launches + 3, what="relaunch")
        n = len(db.committed())
        await until(lambda: len(db.committed()) >= n + 10, what="load after the relaunches")
        await driver.stop()
        close_evidence(driver)
        return driver, adapter

    driver, adapter = asyncio.run(go())
    records = _journal(driver, "marker.jrnl")
    assert len({r["uuid"] for r in records}) == len(records)
    assert len({r["seq"] for r in records}) == len(records)
    diff, torn = diff_from_journals(driver.journals.run_dir, asyncio.run(adapter.marker_ids()))
    assert torn == 0 and not diff.unjournalled_ack and diff.rpo_txn == 0
    assert driver._launch_counter >= 9
