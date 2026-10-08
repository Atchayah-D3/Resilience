"""The record service on its own: requests written straight into its pipe, as a pgbench
record step would, with nothing else running."""

import asyncio
import os

from resilience_tests.execution.workload.markers import MarkerJournals, read_journal
from resilience_tests.execution.workload.shell_records import REQUEST_FIFO, ShellRecordService


class Counts:
    def __init__(self):
        self.total = {}

    def __call__(self, **kw):
        for k, v in kw.items():
            if isinstance(v, int):
                self.total[k] = self.total.get(k, 0) + v


def _service(tmp_path, counts, fatal):
    journals = MarkerJournals(tmp_path)
    service = ShellRecordService(tmp_path / "pgbench", journals, "marker", rate_tps=None, concurrency=1,
                                 marker_uuid=lambda seq: f"00000000-0000-0000-0000-{seq:012d}",
                                 count=counts, on_connected=lambda c, r: None, on_fatal=fatal.append)
    return service, journals


async def _request(service, line):
    fd = os.open(service.directory / REQUEST_FIFO, os.O_WRONLY)
    os.write(fd, line.encode() + b"\n")
    os.close(fd)


async def _reply(service, rel):
    fd = os.open(service.directory / rel, os.O_RDONLY | os.O_NONBLOCK)
    try:
        for _ in range(200):
            try:
                data = os.read(fd, 100)
                if data:
                    return data.decode().strip()
            except BlockingIOError:
                pass
            await asyncio.sleep(0.01)
    finally:
        os.close(fd)
    raise AssertionError("no reply")


def test_an_acknowledgement_left_in_the_pipe_by_a_dead_client_still_counts(tmp_path):
    """The client sent its ack and died before the service read it: the server acknowledged,
    so the transaction is committed -- not an unknown outcome, and not a broken record."""
    counts, fatal = Counts(), []

    async def go():
        service, journals = _service(tmp_path, counts, fatal)
        await service.open()
        reply = service.new_launch(1, 0)
        await _request(service, "pre 1 0")
        seq = int(await _reply(service, reply))
        fd = os.open(service.directory / REQUEST_FIFO, os.O_WRONLY)
        os.write(fd, f"ack 1 {seq}\n".encode())
        os.close(fd)
        service.launch_ended(1, "aborted")      # before the reader task has run
        await service.close()
        journals.close()

    asyncio.run(go())
    assert not fatal
    assert counts.total.get("commits") == 1
    # the connection is gone too -- lost between transactions, nothing left in flight
    assert not counts.total.get("indeterminate") and counts.total.get("drops") == 1
    assert len(read_journal(tmp_path / "acked.jrnl").records) == 1


def test_a_client_lost_mid_transaction_is_unknown_and_dropped(tmp_path):
    counts, fatal = Counts(), []

    async def go():
        service, journals = _service(tmp_path, counts, fatal)
        await service.open()
        reply = service.new_launch(1, 0)
        await _request(service, "pre 1 0")
        await _reply(service, reply)
        service.launch_ended(1, "aborted")
        await service.close()
        journals.close()

    asyncio.run(go())
    assert not fatal
    assert counts.total == {"indeterminate": 1, "drops": 1}
    assert len(read_journal(tmp_path / "marker.jrnl").records) == 1
    assert not read_journal(tmp_path / "acked.jrnl").records


def test_a_skipped_acknowledgement_is_a_definite_abort(tmp_path):
    counts, fatal = Counts(), []

    async def go():
        service, journals = _service(tmp_path, counts, fatal)
        await service.open()
        reply = service.new_launch(1, 0)
        for _ in range(2):                   # the second pre arrives without an ack between
            await _request(service, "pre 1 0")
            await _reply(service, reply)
        await service.close()
        journals.close()

    asyncio.run(go())
    assert not fatal
    assert counts.total.get("errors") == 1 and not counts.total.get("indeterminate")


def test_a_client_stopped_during_its_flush_closes_its_invoke_in_order(tmp_path):
    """The :info of a transaction interrupted while its marker was being flushed must follow its
    :invoke: Elle reads a process that acts after an :info as two operations at once."""
    from resilience_tests.execution.workload.history_writer import HistoryWriter

    counts, fatal = Counts(), []

    async def go():
        journals = MarkerJournals(tmp_path)
        history = HistoryWriter(tmp_path / "history.edn")
        service = ShellRecordService(tmp_path / "pgbench", journals, "list_append", rate_tps=None,
                                     concurrency=1, marker_uuid=lambda seq: f"00000000-0000-0000-0000-{seq:012d}",
                                     count=counts, on_connected=lambda c, r: None, on_fatal=fatal.append,
                                     history=history)
        release, flushing = asyncio.Event(), asyncio.Event()
        real = journals.written

        async def slow(seq, uuid, t_pre):
            flushing.set()
            await release.wait()
            await real(seq, uuid, t_pre)

        journals.written = slow
        await service.open()
        service.new_launch(1, 0)
        await _request(service, "pre 1 0")
        await asyncio.wait_for(flushing.wait(), 5)
        service.launch_ended(1, "stopped")       # stopped mid-flush
        release.set()
        await service.close()
        journals.close()
        history.close()

    asyncio.run(go())
    types = [line.split(":type :")[1].split(",")[0] for line in (tmp_path / "history.edn").read_text().splitlines()]
    assert types == ["invoke", "info"]
    assert not fatal
