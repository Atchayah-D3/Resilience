"""Background samplers of what the database and its host were doing (Resilience Testing document,
section 2.3: "wait events, locks, connections, WAL and checkpoints, dead rows, temporary files,
and operating-system counters, sampled every second or every few seconds").

  DbActivitySampler   the adapter's activity_snapshot(), every DB_ACTIVITY_INTERVAL_S
  HostStatsSampler    `vmstat -n -w 1` on the target over SSH: CPU, iowait, memory, swap, block I/O

Both are evidence only: they never stop or fail a run. A sample that cannot be taken is recorded
as unavailable with the reason, never as a number. The analysis (analysis/activity.py) turns the
samples into per-phase figures.
"""

from __future__ import annotations

import asyncio
import shlex
import time

import asyncssh

from resilience_tests.adapters.base import BaseDatabaseAdapter
from resilience_tests.control.profile import Node
from resilience_tests.execution.probes.probers import PROBE_ATTEMPT_TIMEOUT_S
from resilience_tests.execution.remote import RemoteHost
from resilience_tests.observability.event_stream import EventStream

DB_ACTIVITY_INTERVAL_S = 1.0
# a snapshot that keeps failing (target down, or an engine without the query) is retried slowly
DB_ACTIVITY_RETRY_S = 5.0
HOST_RECONNECT_PAUSE_S = 5.0
# vmstat's columns (procps-ng, -w): processes, memory (KiB), swap, block I/O, system, CPU %
VMSTAT_INTEGER_FIELDS = ("r", "b", "swpd", "free", "buff", "cache", "si", "so", "bi", "bo", "in", "cs",
                         "us", "sy", "id", "wa", "st", "gu")


class DbActivitySampler:
    """Emits db_activity/sample (the snapshot) or db_activity/unavailable {error}."""

    def __init__(self, adapter: BaseDatabaseAdapter, stream: EventStream) -> None:
        self.adapter = adapter
        self.stream = stream
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._loop(), name="DbActivitySampler")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        try:
            await self.adapter.close_activity()
        except Exception:  # noqa: BLE001 -- evidence only
            pass

    async def _loop(self) -> None:
        next_tick = time.monotonic()
        while True:
            delay = DB_ACTIVITY_INTERVAL_S
            try:
                snap = await asyncio.wait_for(self.adapter.activity_snapshot(), timeout=DB_ACTIVITY_INTERVAL_S * 3)
                self.stream.emit("db_activity", "sample", node=self.adapter.node.name, **snap)
            except NotImplementedError as exc:
                self.stream.emit("db_activity", "unavailable", node=self.adapter.node.name, error=str(exc))
                return                      # the engine cannot answer: say so once, then stop
            except Exception as exc:  # noqa: BLE001 -- the target may be down; evidence only
                self.stream.emit("db_activity", "unavailable", node=self.adapter.node.name,
                                 error=f"{type(exc).__name__}: {exc}"[:300])
                delay = DB_ACTIVITY_RETRY_S
            next_tick += delay
            pause = next_tick - time.monotonic()
            if pause > 0:
                await asyncio.sleep(pause)
            else:                           # overran: realign rather than burst
                next_tick = time.monotonic()


def parse_vmstat_line(header: list[str], line: str) -> dict[str, int] | None:
    """One `vmstat -n -w` data line as {column: value}, by the header's column names; None for
    a header or anything that is not a full row of numbers."""
    fields = line.split()
    if not fields or len(fields) != len(header) or not all(f.isdigit() for f in fields):
        return None
    return {name: int(value) for name, value in zip(header, fields) if name in VMSTAT_INTEGER_FIELDS}


class HostStatsSampler:
    """Streams `vmstat -n -w 1` from the target over one SSH session (reconnecting across reboots)
    and emits host_stats/sample per second. vmstat's first row is the average since boot, not a
    measurement of now: it is skipped. Emits host_stats/unavailable {error} when it cannot run."""

    def __init__(self, node: Node, stream: EventStream) -> None:
        self.node = node
        self.stream = stream
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._loop(), name="HostStatsSampler")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def _loop(self) -> None:
        command = "LC_ALL=C vmstat -n -w 1"
        while True:
            try:
                async with RemoteHost(self.node.ssh, connect_timeout_s=PROBE_ATTEMPT_TIMEOUT_S * 3) as host:
                    async with host.process(command) as proc:
                        header: list[str] = []
                        first_row = True
                        async for raw in proc.stdout:
                            line = raw.strip()
                            words = line.split()
                            if words and words[0] == "r":        # the column-name line
                                header = words
                                continue
                            if not header:
                                continue
                            row = parse_vmstat_line(header, line)
                            if row is None:
                                continue
                            if first_row:                        # since boot, not now
                                first_row = False
                                continue
                            self.stream.emit("host_stats", "sample", node=self.node.name, **row)
                        done = await proc.wait()
                        why = (done.stderr or "").strip().splitlines()
                        self.stream.emit("host_stats", "unavailable", node=self.node.name,
                                         error=(why[0][:300] if why else f"vmstat ended ({shlex.quote(command)})"))
            except (OSError, asyncssh.Error, TimeoutError) as exc:
                self.stream.emit("host_stats", "unavailable", node=self.node.name, error=type(exc).__name__)
            await asyncio.sleep(HOST_RECONNECT_PAUSE_S)
