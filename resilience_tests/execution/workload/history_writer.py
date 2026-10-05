"""Jepsen/Elle operation history export (Arch §6.3, §10.3).

Emits a Jepsen-format EDN operation history alongside the marker journals for
isolation and transactional anomaly verification with Elle.

Rules from Arch §6.3 / §7.1:
1. :invoke is emitted before the transaction is sent to the target.
2. :ok is emitted only when COMMIT is acknowledged by the target.
3. :info (indeterminate) is emitted when the connection drops, times out, or when
   the primary is killed. It must NEVER be marked :fail.
4. :fail is emitted ONLY when the server returns an explicit abort / serialization failure.
"""

from __future__ import annotations

import os
import queue
import threading
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

from resilience_tests.adapters.base import MicroOp

_STOP = object()


def edn_ops(ops: Sequence[MicroOp]) -> str:
    """[[:r 3 nil] [:append 4 17] [:r 4 [9 17]]] -- Elle's list-append transaction value."""
    parts = []
    for f, key, val in ops:
        if f == "append":
            parts.append(f"[:append {int(key)} {int(val)}]")
        elif f == "r":
            shown = "nil" if val is None else "[" + " ".join(str(int(v)) for v in val) + "]"
            parts.append(f"[:r {int(key)} {shown}]")
        else:
            raise ValueError(f"unknown micro-op {f!r}")
    return "[" + " ".join(parts) + "]"


class HistoryWriter:
    """Thread-safe, append-only Jepsen/Elle EDN history recorder."""

    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o640)
        self._q: queue.SimpleQueue[Any] = queue.SimpleQueue()
        self._index = 0
        self._lock = threading.Lock()
        self._thread = threading.Thread(target=self._run, name=f"history:{path.name}", daemon=True)
        self._thread.start()

    def next_index(self) -> int:
        with self._lock:
            idx = self._index
            self._index += 1
            return idx

    def record(self, type_: str, process_id: int, ops: Sequence[MicroOp],
               t_mono_ns: int | None = None, error: str | None = None) -> int:
        """One history operation. `ops` are the micro-operations: for :invoke, reads carry
        None (nothing has been read yet); for :ok, exactly what the database returned. For
        :fail and :info the invoke's ops are repeated -- Elle reads nothing into them."""
        if type_ not in ("invoke", "ok", "fail", "info"):
            raise ValueError(f"unknown history op type {type_!r}")
        idx = self.next_index()
        t = t_mono_ns if t_mono_ns is not None else time.monotonic_ns()
        err = f", :error :{error}" if error else ""
        edn = (f"{{:index {idx}, :type :{type_}, :f :txn, :process {process_id}, "
               f":time {t}{err}, :value {edn_ops(ops)}}}\n")
        self._q.put(edn.encode("utf-8"))
        return idx

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
            for chunk in batch:
                os.write(self._fd, chunk)
            os.fdatasync(self._fd)
            if stop:
                return

    def close(self) -> None:
        self._q.put(_STOP)
        self._thread.join()
        try:
            os.close(self._fd)
        except OSError:
            pass

