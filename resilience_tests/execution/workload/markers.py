"""Transaction marker protocol -- the RPO measurement of record (Arch §6.2, Fig. 4;
Framework §6.4, Fig. 8).

    for each transaction:
        seq += 1; uuid = uuid4()
        append {seq, uuid, t_pre} to marker.jrnl; fdatasync(marker.jrnl)     # driver host
        commit the marker through the target adapter                        # engine-agnostic
        on acknowledgement: append {uuid, t_ack} to acked.jrnl; fdatasync(acked.jrnl)
    after recovery:
        lost          = acked_set - db_set        # RPO. MUST be empty.
        indeterminate = written_set - acked_set   # in flight at T0; reported, NOT counted

The journals live on the driver host, never on the node under test: a power-loss scenario
would otherwise destroy the evidence with the database.

fdatasync is group-committed: many concurrent appends share one fdatasync, but no append
returns before the fdatasync that covers it -- so "journalled and flushed before the COMMIT
is sent" holds for every transaction individually.
"""

from __future__ import annotations

import asyncio
import json
import os
import queue
import threading
import uuid as uuidlib
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

MARKER_JOURNAL = "marker.jrnl"
ACKED_JOURNAL = "acked.jrnl"
# Sure rollbacks (the server said the transaction did not happen). Written by the TPC-C engine;
# created only when first used, so a run of the custom workload has the same files as before.
ABORTED_JOURNAL = "aborted.jrnl"

_STOP = object()


class DurableJournal:
    """Append-only JSONL journal; `append` returns only after the record is fdatasync'd."""

    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o640)
        self._q: queue.SimpleQueue[Any] = queue.SimpleQueue()
        self._thread = threading.Thread(target=self._run, name=f"journal:{path.name}", daemon=True)
        self._thread.start()

    async def append(self, record: dict[str, Any]) -> None:
        loop = asyncio.get_running_loop()
        fut: asyncio.Future[None] = loop.create_future()
        line = (json.dumps(record, separators=(",", ":")) + "\n").encode()
        self._q.put((line, loop, fut))
        await fut

    def _run(self) -> None:
        while True:
            item = self._q.get()
            if item is _STOP:
                return
            batch = [item]
            stop = False
            while True:
                try:
                    nxt = self._q.get_nowait()
                except queue.Empty:
                    break
                if nxt is _STOP:
                    stop = True
                    break
                batch.append(nxt)
            try:
                data = b"".join(line for line, _, _ in batch)
                view = memoryview(data)
                while view:
                    view = view[os.write(self._fd, view):]
                os.fdatasync(self._fd)
                err: BaseException | None = None
            except BaseException as exc:  # surfaced to every waiter; the run must abort
                err = exc
            # One cross-thread wake-up per loop per batch, not one per waiter: with 64
            # concurrent workers the per-waiter wake-ups cost more than the fdatasync and
            # would make the driver -- the measuring instrument -- the bottleneck.
            by_loop: dict[Any, list[asyncio.Future[None]]] = {}
            for _, loop, fut in batch:
                by_loop.setdefault(loop, []).append(fut)
            for loop, futures in by_loop.items():
                loop.call_soon_threadsafe(_settle_all, futures, err)
            if stop:
                return

    def close(self) -> None:
        self._q.put(_STOP)
        self._thread.join()
        os.close(self._fd)


def _settle_all(futures: list[asyncio.Future[None]], err: BaseException | None) -> None:
    for fut in futures:
        if fut.done():
            continue
        if err is None:
            fut.set_result(None)
        else:
            fut.set_exception(err)


class MarkerJournals:
    """The journals of the protocol, owned by the workload driver. Extra fields (`**extra`)
    are added to a line, e.g. the TPC-C transaction type, or the order number of a New-Order
    for the per-order check after a fault; without them each line is exactly as before."""

    def __init__(self, run_dir: Path, index: int | None = None) -> None:
        """`index`: one worker process of several writes marker.<index>.jrnl and so on (the
        TPC-C engine in several processes); without it the names are exactly as before."""
        self.run_dir = run_dir
        self._name = (lambda base: base) if index is None else (lambda base: base.replace(".jrnl", f".{index}.jrnl"))
        self.marker = DurableJournal(run_dir / self._name(MARKER_JOURNAL))
        self.acked = DurableJournal(run_dir / self._name(ACKED_JOURNAL))
        self._aborted: DurableJournal | None = None
        self._seq = 0

    def next_marker(self) -> tuple[int, str]:
        self._seq += 1
        return self._seq, str(uuidlib.uuid4())

    async def written(self, seq: int, uuid: str, t_pre: float, **extra: Any) -> None:
        await self.marker.append({"seq": seq, "uuid": uuid, "t_pre": t_pre, **extra})

    async def acknowledged(self, uuid: str, t_ack: float, **extra: Any) -> None:
        await self.acked.append({"uuid": uuid, "t_ack": t_ack, **extra})

    async def aborted(self, uuid: str, t_abort: float, **extra: Any) -> None:
        """A sure rollback: the server answered that the transaction did not happen. Never
        written for an unknown outcome -- that stays a marker line with no other line."""
        if self._aborted is None:
            self._aborted = DurableJournal(self.run_dir / self._name(ABORTED_JOURNAL))
        await self._aborted.append({"uuid": uuid, "t_abort": t_abort, **extra})

    def close(self) -> None:
        self.marker.close()
        self.acked.close()
        if self._aborted is not None:
            self._aborted.close()


# --- analysis (pure) ---------------------------------------------------------------------


@dataclass(frozen=True)
class JournalRead:
    records: list[dict[str, Any]]
    torn_lines: int  # a final partial line from a crashed driver; reported, never guessed at


def read_journal(path: Path) -> JournalRead:
    records, torn = [], 0
    with path.open("rb") as fh:
        for raw in fh:
            try:
                records.append(json.loads(raw))
            except (json.JSONDecodeError, UnicodeDecodeError):
                torn += 1
    return JournalRead(records, torn)


@dataclass(frozen=True)
class MarkerDiff:
    written: int
    acked: int
    in_db: int
    lost: frozenset[str]  # acked but absent from the database -> RPO
    indeterminate: frozenset[str]  # written, never acknowledged nor surely rolled back -> reported, not counted
    indeterminate_committed: frozenset[str]  # the subset of indeterminate that did commit
    phantom: frozenset[str]  # in the database but never written by the driver -> integrity violation
    unjournalled_ack: frozenset[str]  # acked without a marker record -> harness defect
    # Written, then answered "rolled back" by the database (the aborted journal; the TPC-C engine
    # writes it, the custom workload does not, so for it these stay empty). They had an answer,
    # so they are not indeterminate. One that is nevertheless in the database is a rollback
    # that was kept -- an atomicity failure, shown here and judged by the profile's checks.
    rolled_back: frozenset[str] = frozenset()
    rolled_back_kept: frozenset[str] = frozenset()

    @property
    def rpo_txn(self) -> int:
        return len(self.lost)


def diff_markers(written: Iterable[str], acked: Iterable[str], db: Iterable[str],
                 aborted: Iterable[str] = ()) -> MarkerDiff:
    w, a, d = frozenset(written), frozenset(acked), frozenset(db)
    # an acknowledgement wins over a rollback line (both would be a harness defect; the
    # acknowledged one is then still checked for loss)
    rolled_back = (frozenset(aborted) & w) - a
    indeterminate = w - a - rolled_back
    return MarkerDiff(
        written=len(w),
        acked=len(a),
        in_db=len(d),
        lost=a - d,
        indeterminate=indeterminate,
        indeterminate_committed=indeterminate & d,
        phantom=d - w,
        unjournalled_ack=a - w,
        rolled_back=rolled_back,
        rolled_back_kept=rolled_back & d,
    )


def journal_paths(run_dir: Path, name: str) -> list[Path]:
    """Every journal of one kind in a run: `marker.jrnl` (one process, as the custom workload
    writes) and `marker.<n>.jrnl` (one per worker process, as the TPC-C engine will write).
    Missing files are left out; the caller decides whether none at all is an error."""
    stem, suffix = name.rsplit(".", 1)
    single = run_dir / name
    paths = [single] if single.exists() else []
    paths += sorted(p for p in run_dir.glob(f"{stem}.*.{suffix}") if p != single)
    return paths


def diff_from_journals(run_dir: Path, db_uuids: Iterable[str]) -> tuple[MarkerDiff, int]:
    """Returns the diff and the number of torn journal lines, over every journal file present."""
    marker_paths = journal_paths(run_dir, MARKER_JOURNAL)
    acked_paths = journal_paths(run_dir, ACKED_JOURNAL)
    if not marker_paths:
        # as before: a run without a marker journal is an error, never an empty (perfect) RPO
        raise FileNotFoundError(run_dir / MARKER_JOURNAL)
    # Every marker journal must have its acknowledgement journal: without it the commits it
    # covers would silently count as never acknowledged, and their loss could not be seen.
    for mp in marker_paths:
        expected = mp.with_name(mp.name.replace(MARKER_JOURNAL.rsplit(".", 1)[0], ACKED_JOURNAL.rsplit(".", 1)[0], 1))
        if not expected.exists():
            raise FileNotFoundError(expected)
    markers = [read_journal(p) for p in marker_paths]
    acks = [read_journal(p) for p in acked_paths]
    aborts = [read_journal(p) for p in journal_paths(run_dir, ABORTED_JOURNAL)]
    result = diff_markers(
        (r["uuid"] for j in markers for r in j.records),
        (r["uuid"] for j in acks for r in j.records),
        db_uuids,
        (r["uuid"] for j in aborts for r in j.records),
    )
    return result, sum(j.torn_lines for j in markers + acks + aborts)
