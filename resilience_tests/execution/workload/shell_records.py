"""The transaction marker protocol (Arch §6.2) for pgbench clients -- research R1, option B.

pgbench cannot write a file, so every pgbench client asks the harness to do it. Each
transaction of the script begins and ends with a record step, a pgbench shell command that
talks to this service over named pipes on the driver host:

    \\setshell tok  printf ... pre <launch> <client> 1<>q && read -r a < <reply> && echo "$a"
    BEGIN; <the adapter's transaction>; COMMIT;
    \\shell         printf ... ack <launch> <seq> 1<>q && read -r a < <reply> && test "$a" = ok

`pre`: the service takes the next slot of the shared rate limiter, assigns the run-wide
sequence number, appends {seq, uuid, t_pre} to marker.jrnl and fdatasyncs it -- the same
group-committed DurableJournal the built-in driver uses -- and only then replies with the
token the script decodes into its variables. The COMMIT cannot be sent before the record is
durable: the client is blocked on the reply until then.

`ack`: sent only after the server acknowledged the COMMIT. The service appends {uuid, t_ack}
to acked.jrnl, fdatasyncs, and replies `ok`. An acknowledgement the service cannot journal
makes the run fail (the client is answered `err`, and the evidence is incomplete).

Outcomes, without guessing (Arch §7.1):
- committed: an `ack` arrived;
- definitely aborted: the same client sent its next `pre` without an `ack`. pgbench continues a
  client only after a serialization or deadlock failure, which the server returned (it aborts
  the client on every other error), so a skipped ack means the server rejected the transaction;
- unknown: the client process ended (aborted, killed by the transaction timeout) with a
  transaction in flight.

The service also chooses each transaction's churn key and Elle keys, with the built-in
driver's seeded generators in the same order, so both generators offer the same load.

Elle reads travel back in chunks (`c` lines) because pgbench limits a shell command to 255
bytes. A read is sent as offsets from its key window's base -- lossless, and short enough to
fit -- and its length is checked on arrival: a read that does not reassemble exactly fails
the run rather than enter the history.
"""

from __future__ import annotations

import asyncio
import os
import random
import stat
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from resilience_tests.execution.workload.driver import (
    CHURN_REPLACE_EVERY,
    ELLE_KEYS,
    ELLE_TXNS_PER_WINDOW,
    RateLimiter,
)
from resilience_tests.execution.workload.history_writer import HistoryWriter
from resilience_tests.execution.workload.markers import MarkerJournals

REQUEST_FIFO = "q"
REPLY_DIR = "r"
# pgbench rejects a shell command of 255 bytes or more after variable substitution. A chunk
# line is ~70 bytes besides its text; 160 keeps every line well inside the limit.
READ_CHUNK_CHARS = 160
# A list in one key window holds ~ELLE_TXNS_PER_WINDOW / ELLE_KEYS = 125 values of at most 5
# characters each ("3999."): ~625 characters on average, with room for the variance.
READ_CHUNKS = 8
ELLE_TOKEN_BASE = ELLE_KEYS * ELLE_KEYS     # token = seq * 1024 + read_offset * 32 + append_offset


class RecordProtocolError(RuntimeError):
    """The record channel itself broke: the evidence is incomplete and the run must abort."""


@dataclass
class _Txn:
    seq: int
    uuid: str
    invoked: list[tuple[str, int, Any]]
    process: int
    vbase: int = 0
    t_sent: float | None = None      # monotonic: the moment the client was released to BEGIN
    journal_ms: float = 0.0
    acking: bool = False             # the server acknowledged; the ack is being journalled
    chunks: dict[int, list[str]] = field(default_factory=lambda: {1: [], 2: []})


@dataclass
class _Launch:
    launch: int
    client: int
    reply_path: Path
    reply_fd: int
    connected: bool = False
    ended: bool = False
    txn: _Txn | None = None


class ShellRecordService:
    """The harness side of the record steps. One per run; one reply pipe per pgbench launch."""

    def __init__(
        self,
        directory: Path,
        journals: MarkerJournals,
        shape: str,
        rate_tps: float | None,
        concurrency: int,
        marker_uuid: Callable[[int], str],
        count: Callable[..., None],
        on_connected: Callable[[int, bool], None],
        on_fatal: Callable[[str], None],
        history: HistoryWriter | None = None,
        churn_keys: int = 0,
    ) -> None:
        self.directory = Path(directory)
        self.journals = journals
        self.shape = shape
        self.concurrency = concurrency
        self.marker_uuid = marker_uuid
        self.count = count
        self.on_connected = on_connected
        self.on_fatal = on_fatal
        self.history = history
        self.churn_keys = churn_keys
        self.limiter = RateLimiter(rate_tps)
        # the built-in driver's generators and seeds: the same key sequence, run for run
        self._churn_rng = random.Random(0xC0FFEE)
        self._elle_rng = random.Random(0xE11E)
        self._seq = 0
        self._launches: dict[int, _Launch] = {}
        self._process: dict[int, int] = {}       # Elle process id per client
        self._request_fd: int | None = None
        self._buf = b""
        self._reader_task: asyncio.Task[None] | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self.failure: str | None = None
        self.journal_ms: list[float] = []        # per committed transaction, as journal_p99_ms

    # --- lifecycle ---------------------------------------------------------------------

    @property
    def token_base(self) -> int:
        """What the script divides its token by to get `seq` (see `decode_steps`)."""
        if self.shape == "list_append":
            return ELLE_TOKEN_BASE
        if self.shape == "churn":
            return 2 * self.churn_keys
        return 1

    async def open(self) -> None:
        (self.directory / REPLY_DIR).mkdir(parents=True, exist_ok=True)
        path = self.directory / REQUEST_FIFO
        if path.exists():
            path.unlink()
        os.mkfifo(path, 0o600)
        # O_RDWR: the service is always a writer too, so the pipe never reports end-of-file
        # between clients, and opening it never blocks.
        self._request_fd = os.open(path, os.O_RDWR | os.O_NONBLOCK)
        self._reader_task = asyncio.create_task(self._read_requests(), name="pgbench-records")

    def new_launch(self, launch: int, client: int) -> str:
        """Create the launch's reply pipe. Returns its path relative to the service directory."""
        rel = f"{REPLY_DIR}/{launch}"
        path = self.directory / rel
        os.mkfifo(path, 0o600)
        # held open for writing for the launch's life, so the client's `read < reply` never
        # blocks on open and never sees end-of-file while the service runs
        fd = os.open(path, os.O_RDWR | os.O_NONBLOCK)
        self._launches[launch] = _Launch(launch, client, path, fd)
        return rel

    def launch_ended(self, launch: int, how: str) -> None:
        """The launch's pgbench process is gone. `how`: "aborted" (the connection or the
        transaction timeout ended it), "not_connected" (it never connected), or "stopped"."""
        st = self._launches.get(launch)
        if st is None or st.ended:
            return
        # Everything the process wrote is in the pipe by now; read it first, so an
        # acknowledgement it sent just before it died is not mistaken for an unknown outcome.
        self._drain()
        st.ended = True
        txn = st.txn
        if txn is not None and not txn.acking:
            st.txn = None
            if how == "stopped":
                # the load is stopping mid-transaction: unknown outcome, never counted
                self._history_info(st, txn, "interrupted")
            else:
                latency = None if txn.t_sent is None else (time.monotonic() - txn.t_sent) * 1000
                self.count(indeterminate=1, drops=1, latency_ms=latency)
                self._history_info(st, txn, "indeterminate")
        elif how == "aborted":
            # lost between transactions: a dropped connection with nothing in flight
            self.count(drops=1)
        self._close_reply(st)

    def in_flight_since(self, launch: int) -> float | None:
        """Monotonic time the launch's transaction was released, or None if none is in flight
        (the transaction timeout is enforced by the driver, as the built-in driver's is)."""
        st = self._launches.get(launch)
        if st is None or st.txn is None or st.txn.acking:
            return None
        return st.txn.t_sent

    async def close(self) -> None:
        if self._reader_task is not None:
            self._reader_task.cancel()
            await asyncio.gather(self._reader_task, return_exceptions=True)
        if self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)
        for launch in list(self._launches):
            self.launch_ended(launch, "stopped")
        if self._request_fd is not None:
            os.close(self._request_fd)
            self._request_fd = None
        _unlink_fifo(self.directory / REQUEST_FIFO)

    # --- requests ----------------------------------------------------------------------

    async def _read_requests(self) -> None:
        loop = asyncio.get_running_loop()
        assert self._request_fd is not None
        readable = asyncio.Event()
        loop.add_reader(self._request_fd, readable.set)
        try:
            while True:
                await readable.wait()
                readable.clear()
                self._drain()
        finally:
            loop.remove_reader(self._request_fd)

    def _drain(self) -> None:
        """Dispatch every complete request line waiting in the pipe."""
        if self._request_fd is None:
            return
        while True:
            try:
                data = os.read(self._request_fd, 65536)
            except BlockingIOError:
                break
            if not data:
                break
            self._buf += data
        *lines, self._buf = self._buf.split(b"\n")
        for raw in lines:
            self._dispatch(raw.decode("ascii", errors="replace").split())

    def _dispatch(self, words: list[str]) -> None:
        try:
            kind = words[0] if words else ""
            if kind == "pre" and len(words) == 3:
                self._spawn(self._pre(int(words[1]), int(words[2])))
            elif kind == "c" and len(words) in (5, 6):
                self._chunk(int(words[1]), int(words[2]), int(words[3]), words[5] if len(words) == 6 else "")
            elif kind == "ack" and len(words) in (3, 5):
                lengths = (int(words[3]), int(words[4])) if len(words) == 5 else None
                self._ack_received(int(words[1]), int(words[2]), lengths)
            else:
                raise RecordProtocolError(f"malformed record request {' '.join(words)!r}")
        except (ValueError, RecordProtocolError) as exc:
            self._fatal(f"record channel: {exc}")

    def _spawn(self, coro: Any) -> None:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _launch(self, launch: int) -> _Launch:
        st = self._launches.get(launch)
        if st is None:
            raise RecordProtocolError(f"request from unknown launch {launch}")
        return st

    async def _pre(self, launch: int, client: int) -> None:
        try:
            st = self._launch(launch)
            if st.client != client:
                raise RecordProtocolError(f"launch {launch} belongs to client {st.client}, not {client}")
        except RecordProtocolError as exc:
            self._fatal(str(exc))
            return
        if st.ended:
            return
        if st.txn is not None:
            # the client went on without acknowledging its last transaction: pgbench does that
            # only after a serialization or deadlock failure the server returned
            self._settle_aborted(st)
        if not st.connected:
            st.connected = True
            self.on_connected(st.client, any(l.connected for l in self._launches.values()
                                             if l.client == st.client and l.launch != launch))
        if self.failure is not None:
            self._reply(st, "err")
            return
        await self.limiter.acquire()
        if st.ended:
            return
        self._seq += 1
        seq = self._seq
        uuid = self.marker_uuid(seq)
        aux, invoked, vbase = self._choose(seq)
        process = self._process.setdefault(st.client, st.client)
        txn = _Txn(seq=seq, uuid=uuid, invoked=invoked, process=process, vbase=vbase)
        st.txn = txn
        # The :invoke goes first, while nothing has been sent: the client can be stopped during
        # the flush below, and its :info must then follow the :invoke, never precede it.
        if self.history is not None and invoked:
            self.history.record("invoke", process, invoked)
        tj = time.monotonic()
        try:
            await self.journals.written(seq, uuid, time.time())
        except Exception as exc:  # noqa: BLE001 -- the journal is the evidence; nothing may commit
            self._fatal(f"marker journal write failed: {type(exc).__name__}: {exc}")
            self._reply(st, "err")
            return
        txn.t_sent = time.monotonic()
        txn.journal_ms = (txn.t_sent - tj) * 1000
        if not st.ended:
            self._reply(st, str(seq * self.token_base + aux))

    def _choose(self, seq: int) -> tuple[int, list[tuple[str, int, Any]], int]:
        if self.shape == "churn":
            key = self._churn_rng.randrange(1, self.churn_keys + 1)
            replace = self._churn_rng.randrange(CHURN_REPLACE_EVERY) == 0
            return (key - 1) * 2 + int(replace), [], 0
        if self.shape == "list_append":
            window = seq // ELLE_TXNS_PER_WINDOW
            base = window * ELLE_KEYS
            r_off = self._elle_rng.randrange(ELLE_KEYS)
            a_off = self._elle_rng.randrange(ELLE_KEYS)
            invoked = [("r", base + r_off, None), ("append", base + a_off, seq), ("r", base + a_off, None)]
            return r_off * ELLE_KEYS + a_off, invoked, window * ELLE_TXNS_PER_WINDOW
        return 0, [], 0

    def _chunk(self, launch: int, seq: int, read: int, text: str) -> None:
        st = self._launch(launch)
        if st.txn is None or st.txn.seq != seq or read not in (1, 2):
            raise RecordProtocolError(f"read chunk for launch {launch} seq {seq} has no transaction in flight")
        st.txn.chunks[read].append(text)

    def _ack_received(self, launch: int, seq: int, lengths: tuple[int, int] | None) -> None:
        """Runs as the line is read, before anything else can classify the transaction: the
        server acknowledged it, whatever happens to the client from here on."""
        t_ack = time.monotonic()
        try:
            st = self._launch(launch)
            txn = st.txn
            if txn is None or txn.seq != seq or txn.t_sent is None:
                # an acknowledgement without its before-commit record (TR-1): never counted
                raise RecordProtocolError(f"acknowledgement for launch {launch} seq {seq} matches no "
                                          "before-commit record")
            executed = self._executed(txn, lengths)
        except RecordProtocolError as exc:
            self._fatal(str(exc))
            st_ = self._launches.get(launch)
            if st_ is not None:
                self._reply(st_, "err")
            return
        txn.acking = True
        self._spawn(self._ack(st, txn, executed, t_ack))

    async def _ack(self, st: _Launch, txn: _Txn, executed: list[tuple[str, int, Any]], t_ack: float) -> None:
        assert txn.t_sent is not None
        latency = (t_ack - txn.t_sent) * 1000
        t2 = time.monotonic()
        try:
            await self.journals.acknowledged(txn.uuid, time.time())
        except Exception as exc:  # noqa: BLE001 -- a commit that cannot be journalled hides a loss
            self._fatal(f"acknowledgement journal write failed: {type(exc).__name__}: {exc}")
            self._reply(st, "err")
            return
        journal_ms = txn.journal_ms + (time.monotonic() - t2) * 1000
        self.journal_ms.append(journal_ms)
        self.count(commits=1, latency_ms=latency, journal_ms=journal_ms)
        if self.history is not None and txn.invoked:
            self.history.record("ok", txn.process, executed)
        if st.txn is txn:
            st.txn = None
        self._reply(st, "ok")

    def _executed(self, txn: _Txn, lengths: tuple[int, int] | None) -> list[tuple[str, int, Any]]:
        if self.shape != "list_append":
            if lengths is not None:
                raise RecordProtocolError(f"acknowledgement for seq {txn.seq} carries reads this shape has none of")
            return []
        if lengths is None:
            raise RecordProtocolError(f"list-append acknowledgement for seq {txn.seq} carries no reads")
        reads = []
        for i, expected in zip((1, 2), lengths):
            text = "".join(txn.chunks[i])
            if len(text) != expected:
                raise RecordProtocolError(
                    f"read {i} of seq {txn.seq} arrived as {len(text)} of {expected} characters "
                    f"(the record step carries at most {READ_CHUNKS * READ_CHUNK_CHARS})")
            reads.append(_decode_read(text, txn.vbase))
        (_, rk, _), (_, ak, value), _ = txn.invoked
        return [("r", rk, reads[0]), ("append", ak, value), ("r", ak, reads[1])]

    # --- outcomes ----------------------------------------------------------------------

    def _settle_aborted(self, st: _Launch) -> None:
        txn, st.txn = st.txn, None
        assert txn is not None
        latency = None if txn.t_sent is None else (time.monotonic() - txn.t_sent) * 1000
        self.count(errors=1, latency_ms=latency)
        if self.history is not None and txn.invoked:
            self.history.record("fail", txn.process, txn.invoked, error="aborted")

    def _history_info(self, st: _Launch, txn: _Txn, error: str) -> None:
        if self.history is None or not txn.invoked:
            return
        self.history.record("info", txn.process, txn.invoked, error=error)
        # Jepsen convention: a process whose operation ended :info may still be running it, so
        # the client continues under a fresh process id
        self._process[st.client] = txn.process + self.concurrency

    def _reply(self, st: _Launch, text: str) -> None:
        if st.ended:
            return
        try:
            os.write(st.reply_fd, (text + "\n").encode())
        except OSError as exc:
            self._fatal(f"reply to launch {st.launch} failed: {exc}")

    def _close_reply(self, st: _Launch) -> None:
        try:
            os.close(st.reply_fd)
        except OSError:
            pass
        _unlink_fifo(st.reply_path)

    def _fatal(self, message: str) -> None:
        if self.failure is None:
            self.failure = message
            self.on_fatal(message)


def _unlink_fifo(path: Path) -> None:
    try:
        if stat.S_ISFIFO(os.stat(path).st_mode):
            path.unlink()
    except FileNotFoundError:
        pass


def _decode_read(text: str, vbase: int) -> list[int] | None:
    if text == "nil":
        return None
    if not text:
        return []
    try:
        return [int(x) + vbase for x in text.split(".")]
    except ValueError as exc:
        raise RecordProtocolError(f"unparseable read {text!r}") from exc


# --- the record steps of the pgbench script (engine-agnostic pgbench syntax) -------------

_PRE_STEP = "\\setshell tok printf '%s %s %s\\n' pre :launch :client 1<>q && read -r a < :reply && echo \"$a\""
_REPLY_OK = "&& read -r a < :reply && test \"$a\" = ok"


def pre_steps(shape: str, token_base: int) -> str:
    """The script's first lines: the pre record, and the token decoded into the variables the
    adapter's transaction uses (`seq`; churn: `ckey`, `creplace`; list-append: `rk`, `ak`,
    `vbase`)."""
    lines = [_PRE_STEP]
    if shape == "churn":
        lines += [f"\\set seq :tok / {token_base}", f"\\set aux :tok % {token_base}",
                  "\\set ckey :aux / 2 + 1", "\\set creplace :aux % 2"]
    elif shape == "list_append":
        lines += [f"\\set seq :tok / {token_base}",
                  f"\\set kbase (:seq / {ELLE_TXNS_PER_WINDOW}) * {ELLE_KEYS}",
                  f"\\set rk :kbase + (:tok / {ELLE_KEYS}) % {ELLE_KEYS}",
                  f"\\set ak :kbase + :tok % {ELLE_KEYS}",
                  f"\\set vbase (:seq / {ELLE_TXNS_PER_WINDOW}) * {ELLE_TXNS_PER_WINDOW}"]
    else:
        lines.append("\\set seq :tok")
    return "\n".join(lines) + "\n"


def ack_steps(shape: str) -> str:
    """The script's last lines, run only after the COMMIT was acknowledged: the read chunks
    (list-append) and the acknowledgement record."""
    lines: list[str] = []
    if shape == "list_append":
        for r in (1, 2):
            for i in range(1, READ_CHUNKS + 1):
                step = f"\\shell printf '%s %s %s %s %s %s\\n' c :launch :seq {r} {i} :r{r}c{i} 1<>q"
                if i == 1:
                    lines.append(step)
                else:
                    lines += [f"\\if :r{r}len > {(i - 1) * READ_CHUNK_CHARS}", step, "\\endif"]
        lines.append(f"\\shell printf '%s %s %s %s %s\\n' ack :launch :seq :r1len :r2len 1<>q {_REPLY_OK}")
    else:
        lines.append(f"\\shell printf '%s %s %s\\n' ack :launch :seq 1<>q {_REPLY_OK}")
    return "\n".join(lines) + "\n"
