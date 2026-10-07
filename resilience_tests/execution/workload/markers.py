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
    """The two journals of the protocol, owned by the workload driver."""

    def __init__(self, run_dir: Path) -> None:
        self.run_dir = Path(run_dir)
        self.marker = DurableJournal(self.run_dir / MARKER_JOURNAL)
        self.acked = DurableJournal(self.run_dir / ACKED_JOURNAL)
        self._seq = 0

    def next_marker(self) -> tuple[int, str]:
        self._seq += 1
        return self._seq, str(uuidlib.uuid4())

    async def written(self, seq: int, uuid: str, t_pre: float) -> None:
        await self.marker.append({"seq": seq, "uuid": uuid, "t_pre": t_pre})

    async def acknowledged(self, uuid: str, t_ack: float) -> None:
        await self.acked.append({"uuid": uuid, "t_ack": t_ack})

    def close(self) -> None:
        self.marker.close()
        self.acked.close()


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
    indeterminate: frozenset[str]  # written, never acknowledged -> reported, not counted
    indeterminate_committed: frozenset[str]  # the subset of indeterminate that did commit
    phantom: frozenset[str]  # in the database but never written by the driver -> integrity violation
    unjournalled_ack: frozenset[str]  # acked without a marker record -> harness defect

    @property
    def rpo_txn(self) -> int:
        return len(self.lost)


def diff_markers(written: Iterable[str], acked: Iterable[str], db: Iterable[str]) -> MarkerDiff:
    w, a, d = frozenset(written), frozenset(acked), frozenset(db)
    indeterminate = w - a
    return MarkerDiff(
        written=len(w),
        acked=len(a),
        in_db=len(d),
        lost=a - d,
        indeterminate=indeterminate,
        indeterminate_committed=indeterminate & d,
        phantom=d - w,
        unjournalled_ack=a - w,
    )


def diff_from_journals(run_dir: Path, db_uuids: Iterable[str]) -> tuple[MarkerDiff, int]:
    """Returns the diff and the number of torn journal lines."""
    marker = read_journal(run_dir / MARKER_JOURNAL)
    acked = read_journal(run_dir / ACKED_JOURNAL)
    result = diff_markers(
        (r["uuid"] for r in marker.records), (r["uuid"] for r in acked.records), db_uuids
    )
    return result, marker.torn_lines + acked.torn_lines
