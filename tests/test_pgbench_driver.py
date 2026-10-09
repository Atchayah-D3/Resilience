"""The pgbench workload driver: supervision, relaunch, pacing, outcomes, history, cleanup
(spec US3-US5). Runs the fake pgbench, whose record steps really run through /bin/sh."""

import asyncio
import os
import re

import pytest

from resilience_tests.execution.workload import pgbench_driver, shell_records
from resilience_tests.execution.workload.markers import read_journal
from tests.fakes.pgbench_env import close_evidence, history_ops, make_driver, until


def _events(driver, kind):
    return [e for e in driver.stream.events() if e.kind == kind]


def _ended(driver, how):
    return sum(1 for e in _events(driver, "launch_end") if e.data["ended_as"] == how)


def _alive(driver):
    return {n: st.process.pid for n, st in driver._launches.items()
            if st.process is not None and st.process.returncode is None}


def test_one_pgbench_per_client_with_the_record_steps(tmp_path, monkeypatch):
    async def go():
        driver, _, db = make_driver(tmp_path, monkeypatch, concurrency=3, rate_tps=30)
        await driver.start()
        await asyncio.wait_for(driver.wait_until_ready(), 10)
        alive = _alive(driver)
        await until(lambda: len(_events(driver, "sample")) >= 2, what="samples")
        await driver.stop()
        close_evidence(driver)
        return driver, alive

    driver, alive = asyncio.run(go())
    assert driver.connected_workers == 3 and len(alive) == 3
    argv = driver._launches[1].argv
    assert argv[argv.index("-c") + 1] == "1" and "-n" in argv and "-R" not in argv
    assert {"launch=1", "client=0", "reply=r/1"} <= set(argv)
    assert driver.app_name == f"resilience-pgbench-{driver.run_id}"
    script = (driver.pgbench_dir / pgbench_driver.SCRIPT_FILE).read_text()
    assert script.startswith("\\setshell tok") and "BEGIN;" in script and "COMMIT;" in script
    assert script.index("COMMIT;") < script.index(" ack ")
    assert _ended(driver, "stopped") == 3
    assert (driver.pgbench_dir / "launch-1.txt").exists()
    assert not _alive(driver)


def test_the_declared_rate_is_offered_evenly(tmp_path, monkeypatch):
    async def go():
        driver, _, _ = make_driver(tmp_path, monkeypatch, concurrency=4, rate_tps=40)
        await driver.start()
        await driver.wait_until_ready()
        await asyncio.sleep(0.5)
        driver.begin_window()
        await asyncio.sleep(3.0)
        window = driver.end_window()
        await driver.stop()
        close_evidence(driver)
        return driver, window

    driver, window = asyncio.run(go())
    # The shared limiter, as the built-in driver's: never above the rate (unthrottled pgbench would
    # be far above), and below it only by time lost to stalls -- the limiter never bursts to catch
    # up, and the lab driver host's flush stalls for over a second at times.
    assert 24 <= window.tps <= 44, window
    assert window.p99_ms is not None and window.journal_p99_ms is not None
    starts = sorted(r["t_pre"] for r in read_journal(driver.journals.run_dir / "marker.jrnl").records)[20:]
    gaps = sorted(b - a for a, b in zip(starts, starts[1:]))
    assert 0.020 <= gaps[len(gaps) // 2] <= 0.030, "transactions are not evenly spaced at 1/40 s"


def test_partial_loss_relaunches_only_the_dropped_clients(tmp_path, monkeypatch):
    async def go():
        driver, _, db = make_driver(tmp_path, monkeypatch, concurrency=4, rate_tps=40)
        await driver.start()
        await driver.wait_until_ready()
        before = _alive(driver)
        db.flag("drop-1")
        db.flag("drop-2")
        await until(lambda: _ended(driver, "client_aborted") == 2, what="two drops")
        await until(lambda: len(_alive(driver)) == 4, what="relaunch")
        await until(lambda: driver.connected_workers == 4 and sum(
            e.data["reconnects"] for e in _events(driver, "sample")) == 2, what="reconnects counted")
        after = _alive(driver)
        await driver.stop()
        close_evidence(driver)
        return driver, before, after

    driver, before, after = asyncio.run(go())
    assert {3: before[3], 4: before[4]}.items() <= after.items(), "a healthy client was disturbed"
    assert 1 not in after and 2 not in after
    samples = _events(driver, "sample")
    assert sum(e.data["drops"] for e in samples) == 2
    assert sum(e.data["indeterminate"] for e in samples) <= 2


def test_no_connection_at_start_is_retried_not_fatal(tmp_path, monkeypatch):
    async def go():
        driver, _, db = make_driver(tmp_path, monkeypatch, concurrency=2, rate_tps=20)
        db.flag("down")
        await driver.start()
        await until(lambda: _ended(driver, "not_connected") >= 2, what="refused launches")
        await asyncio.sleep(0.5)
        assert driver.failure is None and driver.connected_workers == 0
        db.flag("down", on=False)
        await asyncio.wait_for(driver.wait_until_ready(), 10)
        await until(lambda: len(_events(driver, "sample")) >= 1, what="a sample")
        await driver.stop()
        close_evidence(driver)
        return driver

    driver = asyncio.run(go())
    assert driver.failure is None
    assert sum(e.data["connect_failures"] for e in _events(driver, "sample")) >= 2


def test_a_serialization_failure_is_a_definite_abort(tmp_path, monkeypatch):
    async def go():
        driver, _, db = make_driver(tmp_path, monkeypatch, concurrency=2, rate_tps=40)
        db.flag("serialize-every", value="3")
        await driver.start()
        await driver.wait_until_ready()
        driver.begin_window()
        await asyncio.sleep(2.0)
        window = driver.end_window()
        await driver.stop()
        close_evidence(driver)
        return driver, window

    driver, window = asyncio.run(go())
    assert window.errors >= 5 and window.commits >= 2 * window.errors - 4
    assert window.drops == 0 and window.indeterminate == 0
    assert driver._launch_counter == 2, "a server-rejected transaction must not cost the connection"


def test_a_stuck_transaction_is_abandoned_as_unknown(tmp_path, monkeypatch):
    monkeypatch.setattr(pgbench_driver, "TXN_TIMEOUT_S", 0.6)

    async def go():
        driver, _, db = make_driver(tmp_path, monkeypatch, concurrency=1, rate_tps=20)
        await driver.start()
        await driver.wait_until_ready()
        db.flag("hang")
        await until(lambda: _ended(driver, "transaction_timeout") >= 1, what="the timeout")
        db.flag("hang", on=False)
        await until(lambda: driver._launch_counter >= 2 and len(_alive(driver)) == 1, what="relaunch")
        await until(lambda: len(_events(driver, "sample")) >= 2, what="samples")
        await driver.stop()
        close_evidence(driver)
        return driver

    driver = asyncio.run(go())
    samples = _events(driver, "sample")
    assert sum(e.data["indeterminate"] for e in samples) >= 1
    assert sum(e.data["drops"] for e in samples) >= 1
    assert driver.failure is None


def test_pgbench_exiting_on_its_own_fails_the_run(tmp_path, monkeypatch):
    async def go():
        driver, _, db = make_driver(tmp_path, monkeypatch, concurrency=2, rate_tps=40)
        db.flag("exit-after", value="3")
        await driver.start()
        await until(lambda: driver.failure is not None, what="the failure")
        await driver.stop()
        close_evidence(driver)
        return driver

    driver = asyncio.run(go())
    assert "ended unexpectedly" in driver.failure
    assert any(e.kind == "fatal" for e in driver.stream.events())


def test_a_record_step_that_cannot_run_fails_the_run(tmp_path, monkeypatch):
    """A meta-command abort is never the database's doing: the instrument is broken."""
    async def go():
        driver, _, _ = make_driver(tmp_path, monkeypatch, concurrency=1, rate_tps=20)
        await driver.start()
        await driver.wait_until_ready()
        os.chmod(driver.pgbench_dir / shell_records.REQUEST_FIFO, 0)   # `1<>q` now fails
        await until(lambda: driver.failure is not None, what="the failure")
        await driver.stop()
        close_evidence(driver)
        return driver

    driver = asyncio.run(go())
    assert "ended unexpectedly" in driver.failure and "meta-command" in driver.failure


def test_list_append_history_records_what_the_database_returned(tmp_path, monkeypatch):
    # tiny chunks, so lists span several of them as they do on a long run
    monkeypatch.setattr(pgbench_driver, "READ_CHUNK_CHARS", 6)
    monkeypatch.setattr(shell_records, "READ_CHUNK_CHARS", 6)
    monkeypatch.setattr(shell_records, "ELLE_KEYS", 2)
    monkeypatch.setattr(shell_records, "ELLE_TOKEN_BASE", 4)

    async def go():
        driver, _, db = make_driver(tmp_path, monkeypatch, shape="list_append", concurrency=2, rate_tps=40)
        db.flag("serialize-every", value="7")
        await driver.start()
        await until(lambda: sum(len(v) for v in db.lists().values()) >= 30, what="appends")
        await driver.stop()
        close_evidence(driver)
        return driver, db

    driver, db = asyncio.run(go())
    ops = history_ops(driver)
    kinds = [o["type"] for o in ops]
    assert kinds.count("invoke") == kinds.count("ok") + kinds.count("fail") + kinds.count("info")
    assert kinds.count("fail") >= 1
    final = db.lists()
    longest = 0
    for o in ops:
        if o["type"] != "ok":
            continue
        for key, val in re.findall(r"\[:r (\d+) (nil|\[[\d ]*\])\]", o["value"]):
            if val == "nil":
                continue
            vals = [int(x) for x in val.strip("[]").split()]
            longest = max(longest, len(vals))
            assert final[int(key)][:len(vals)] == vals, "a read differs from what the database holds"
    assert longest >= 4, "no read spanned several chunks"


def test_a_read_that_does_not_reassemble_fails_closed(tmp_path, monkeypatch):
    monkeypatch.setattr(pgbench_driver, "READ_CHUNKS", 1)
    monkeypatch.setattr(pgbench_driver, "READ_CHUNK_CHARS", 3)
    monkeypatch.setattr(shell_records, "READ_CHUNKS", 1)
    monkeypatch.setattr(shell_records, "READ_CHUNK_CHARS", 3)
    monkeypatch.setattr(shell_records, "ELLE_KEYS", 1)
    monkeypatch.setattr(shell_records, "ELLE_TOKEN_BASE", 1)

    async def go():
        driver, _, _ = make_driver(tmp_path, monkeypatch, shape="list_append", concurrency=1, rate_tps=40)
        await driver.start()
        await until(lambda: driver.failure is not None, what="the failure")
        await driver.stop()
        close_evidence(driver)
        return driver

    driver = asyncio.run(go())
    assert "arrived as 3 of" in driver.failure
    # the transaction whose read did not reassemble entered neither the history nor acked.jrnl
    acked = len(read_journal(driver.journals.run_dir / "acked.jrnl").records)
    assert sum(o["type"] == "ok" for o in history_ops(driver)) == acked


def test_churn_offers_the_built_in_drivers_key_sequence(tmp_path, monkeypatch):
    async def go():
        driver, _, db = make_driver(tmp_path, monkeypatch, shape="churn", concurrency=2, rate_tps=40)
        await driver.start()
        await until(lambda: db.churn_ops() >= 20, what="churn")
        await driver.stop()
        close_evidence(driver)
        return driver, db

    driver, db = asyncio.run(go())
    acked = len(read_journal(driver.journals.run_dir / "acked.jrnl").records)
    # committed churn is acknowledged churn, plus at most one unknown outcome per client at stop
    assert 0 <= db.churn_ops() - acked <= 2
    script = (driver.pgbench_dir / pgbench_driver.SCRIPT_FILE).read_text()
    assert f"\\set seq :tok / {2 * 50}" in script and "\\if :creplace" in script


def test_cleanup_fails_while_target_sessions_remain(tmp_path, monkeypatch):
    async def go():
        driver, adapter, _ = make_driver(tmp_path, monkeypatch, concurrency=1, rate_tps=20)
        await driver.start()
        await driver.wait_until_ready()
        adapter.open_sessions = 1
        with pytest.raises(RuntimeError, match="still open on the target"):
            await driver.stop()
        close_evidence(driver)
        return driver

    driver = asyncio.run(go())
    assert not _alive(driver)


def test_stop_leaves_no_process_behind_even_mid_transaction(tmp_path, monkeypatch):
    async def go():
        driver, _, db = make_driver(tmp_path, monkeypatch, concurrency=2, rate_tps=20)
        await driver.start()
        await driver.wait_until_ready()
        db.flag("hang")                       # both clients stuck inside COMMIT
        await asyncio.sleep(0.4)
        pids = list(_alive(driver).values())
        await driver.stop()
        close_evidence(driver)
        return pids

    for pid in asyncio.run(go()):
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)


def test_malformed_record_requests_fail_the_run(tmp_path, monkeypatch):
    async def go():
        driver, _, _ = make_driver(tmp_path, monkeypatch, concurrency=1, rate_tps=20)
        await driver.start()
        await driver.wait_until_ready()
        fd = os.open(driver.pgbench_dir / shell_records.REQUEST_FIFO, os.O_WRONLY)
        os.write(fd, b"ack 99 1\n")   # no such launch, no before-commit record
        os.close(fd)
        await until(lambda: driver.failure is not None, what="the failure")
        await driver.stop()
        close_evidence(driver)
        return driver

    driver = asyncio.run(go())
    assert "unknown launch 99" in driver.failure


def test_whitespace_in_the_run_directory_is_refused(tmp_path, monkeypatch):
    async def go():
        driver, _, _ = make_driver(tmp_path / "with space", monkeypatch)
        with pytest.raises(pgbench_driver.UnsupportedWorkload, match="whitespace"):
            await driver.start()
        close_evidence(driver)

    asyncio.run(go())


def test_report_facts_state_the_record_cost(tmp_path, monkeypatch):
    async def go():
        driver, _, db = make_driver(tmp_path, monkeypatch, concurrency=2, rate_tps=40)
        await driver.start()
        await until(lambda: len(db.committed()) >= 10, what="commits")
        await driver.stop()
        close_evidence(driver)
        return driver

    facts = asyncio.run(go()).report_facts()
    assert facts["pgbench_launches"] == 2
    assert facts["pgbench_record_journal_p99_ms"] is not None
    assert "before COMMIT" in facts["pgbench_record_mechanism"]


def test_sigterm_reaches_shell_steps_too(tmp_path, monkeypatch):
    """pgbench and its record steps share a process group, which stop() signals."""
    async def go():
        driver, _, _ = make_driver(tmp_path, monkeypatch, concurrency=1, rate_tps=20)
        await driver.start()
        await driver.wait_until_ready()
        st = driver._launches[1]
        pgid = os.getpgid(st.process.pid)
        await driver.stop()
        close_evidence(driver)
        return pgid

    pgid = asyncio.run(go())
    with pytest.raises(ProcessLookupError):
        os.killpg(pgid, 0)


def test_a_credentials_refusal_stops_the_run_instead_of_retrying(tmp_path, monkeypatch):
    async def go():
        driver, _, db = make_driver(tmp_path, monkeypatch, concurrency=2, rate_tps=20)
        db.flag("auth-fail")
        await driver.start()
        await until(lambda: driver.failure is not None, what="the failure")
        await driver.stop()
        close_evidence(driver)
        return driver

    driver = asyncio.run(go())
    assert "refused by the server for its credentials" in driver.failure
    assert "password authentication failed" in driver.failure
    assert driver._launch_counter <= 2, "a refused client must not be relaunched"


def test_a_statement_that_never_works_stops_the_run_naming_the_error(tmp_path, monkeypatch):
    async def go():
        driver, _, db = make_driver(tmp_path, monkeypatch, concurrency=2, rate_tps=20)
        db.flag("stmt-error")
        await driver.start()
        await until(lambda: driver.failure is not None, what="the failure")
        await driver.stop()
        close_evidence(driver)
        return driver

    driver = asyncio.run(go())
    assert "statement error before any has committed" in driver.failure
    assert 'column "ts" of relation "markers" does not exist' in driver.failure
    assert not read_journal(driver.journals.run_dir / "acked.jrnl").records


def test_statement_errors_after_commits_are_counted_apart_and_ridden_through(tmp_path, monkeypatch):
    """Once commits have worked, an ERROR can be the fault's doing (a full WAL disk): the load
    carries on, counted as the built-in driver counts it, and the report lists the message."""
    async def go():
        driver, _, db = make_driver(tmp_path, monkeypatch, concurrency=2, rate_tps=40)
        await driver.start()
        await until(lambda: len(db.committed()) >= 10, what="commits")
        driver.begin_window()
        db.flag("stmt-error")
        await until(lambda: _ended(driver, "statement_error") >= 4, what="statement errors")
        db.flag("stmt-error", on=False)
        n = len(db.committed())
        await until(lambda: len(db.committed()) >= n + 10, what="load after the errors")
        window = driver.end_window()
        await driver.stop()
        close_evidence(driver)
        return driver, window

    driver, window = asyncio.run(go())
    assert driver.failure is None
    facts = driver.report_facts()["pgbench_statement_errors"]
    assert sum(facts.values()) >= 4 and any("does not exist" in m for m in facts)
    assert window.drops >= 4    # counted as the built-in driver counts an unclassified server error
