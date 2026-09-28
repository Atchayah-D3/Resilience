"""Append-only event stream with harness timestamps (Arch §7.3, Fig. 6).

Every prober, the injector and the workload driver append here. The RTO decomposer and
the other analysers are pure functions over the recorded stream, so they can be
unit-tested against recorded fixtures.

Clock discipline (Arch §7.2): every timestamp is taken on the harness clock --
`t_mono_ns` (monotonic, for intervals) plus `t_wall` (UTC epoch seconds, for correlation).
"""

from __future__ import annotations

import json
import os
import threading
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Event:
    t_mono_ns: int
    t_wall: float
    source: str  # write_prober | read_prober | injector | workload | log_tailer | orchestrator ...
    kind: str
    data: dict[str, Any] = field(default_factory=dict)

    @property
    def t_s(self) -> float:
        return self.t_mono_ns / 1e9

    def to_json(self) -> str:
        return json.dumps(
            {"t_mono_ns": self.t_mono_ns, "t_wall": self.t_wall, "source": self.source, "kind": self.kind, "data": self.data},
            separators=(",", ":"),
            sort_keys=True,
        )

    @classmethod
    def from_json(cls, line: str) -> Event:
        d = json.loads(line)
        return cls(d["t_mono_ns"], d["t_wall"], d["source"], d["kind"], d.get("data", {}))


class HarnessClock:
    """Single clock for every measurement taken on the driver host."""

    @staticmethod
    def mono_ns() -> int:
        return time.monotonic_ns()

    @staticmethod
    def wall() -> float:
        return time.time()


class EventStream:
    """Append-only JSONL file. Each record is flushed as it is written, so a crashed harness
    still leaves the stream up to the last event on disk (it is part of the evidence)."""

    def __init__(self, path: Path, clock: HarnessClock | None = None) -> None:
        self.path = path
        self.clock = clock or HarnessClock()
        path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = path.open("a", buffering=1)
        self._lock = threading.Lock()
        self._events: list[Event] = []

    def emit(self, source: str, kind: str, **data: Any) -> Event:
        event = Event(self.clock.mono_ns(), self.clock.wall(), source, kind, data)
        with self._lock:
            self._fh.write(event.to_json() + "\n")
            self._events.append(event)
        return event

    def sync(self) -> None:
        with self._lock:
            self._fh.flush()
            os.fsync(self._fh.fileno())

    def close(self) -> None:
        with self._lock:
            if not self._fh.closed:
                self._fh.flush()
                os.fsync(self._fh.fileno())
                self._fh.close()

    def events(self) -> list[Event]:
        with self._lock:
            return list(self._events)

    def __iter__(self) -> Iterator[Event]:
        return iter(self.events())


def read_stream(path: Path) -> list[Event]:
    with path.open() as fh:
        return [Event.from_json(line) for line in fh if line.strip()]
