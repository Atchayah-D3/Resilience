"""In-harness async probes -- the measurement of record for timing (Arch §7.2, §7.3, Fig. 6).

Prometheus scrape intervals cannot support 30-60 s budgets; these probes run at 100-250 ms
and write to the append-only event stream on the harness clock.

  WriteProber  INSERT via the client endpoint (pooler if present)  every 200 ms
  ReadProber   SELECT 1 per replica                                every 200 ms
  LogTailer    PG log on the node via SSH (reconnects across reboots)

PatroniProber and EtcdWatcher belong to the clustered tiers and arrive with them.
"""

from __future__ import annotations

import asyncio
import shlex
import time

import asyncssh

from resilience_tests.adapters.base import BaseDatabaseAdapter, DatabaseSession
from resilience_tests.control.profile import DbEndpoint, Node
from resilience_tests.execution.remote import RemoteHost, as_root
from resilience_tests.observability.event_stream import EventStream

WRITE_PROBE_INTERVAL_S = 0.2  # Fig. 6
READ_PROBE_INTERVAL_S = 0.2  # Fig. 6
PROBE_ATTEMPT_TIMEOUT_S = 1.0  # a probe attempt is bounded (Arch §15); it may span several ticks
LOG_RECONNECT_PAUSE_S = 1.0
DISK_PROBE_INTERVAL_S = 5.0  # disk fills over minutes; a 5 s reading is ample for an abort


class _Periodic:
    def __init__(self, stream: EventStream, interval_s: float) -> None:
        self.stream = stream
        self.interval_s = interval_s
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    def start(self) -> None:
        self._task = asyncio.create_task(self._loop(), name=type(self).__name__)

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        await self._close()

    async def _loop(self) -> None:
        next_tick = time.monotonic()
        while not self._stop.is_set():
            await self._tick()
            next_tick += self.interval_s
            delay = next_tick - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            else:  # an attempt overran: realign rather than burst
                next_tick = time.monotonic()

    async def _tick(self) -> None:
        raise NotImplementedError

    async def _close(self) -> None:
        pass


class WriteProber(_Periodic):
    """Emits write_probe {ok, latency_ms, error}. T_reconnect / rto_first_write_s is the
    first ok=true after T0 (Arch §7.2)."""

    def __init__(self, adapter: BaseDatabaseAdapter, stream: EventStream) -> None:
        super().__init__(stream, WRITE_PROBE_INTERVAL_S)
        self.adapter = adapter
        self._session: DatabaseSession | None = None

    async def _tick(self) -> None:
        # The event is stamped when the attempt completes, so the attempt's start is recorded
        # too: an attempt already in flight at T0 says nothing about recovery (Arch §7.3).
        t_start_ns = time.monotonic_ns()
        try:
            async with asyncio.timeout(PROBE_ATTEMPT_TIMEOUT_S):
                if self._session is None or self._session.is_closed:
                    self._session = await self.adapter.session(timeout_s=PROBE_ATTEMPT_TIMEOUT_S)
                ok = await self._session.try_write()
        except Exception as exc:  # noqa: BLE001 -- engine-specific; the adapter owns the taxonomy
            await self._close()
            self.stream.emit("write_prober", "write_probe", ok=False, error=type(exc).__name__,
                             latency_ms=(time.monotonic_ns() - t_start_ns) / 1e6, t_start_mono_ns=t_start_ns,
                             node=self.adapter.node.name)
            return
        if not ok:
            await self._close()
        self.stream.emit("write_prober", "write_probe", ok=ok, latency_ms=(time.monotonic_ns() - t_start_ns) / 1e6,
                         t_start_mono_ns=t_start_ns, node=self.adapter.node.name)

    async def _close(self) -> None:
        if self._session is not None:
            try:
                await self._session.close()
            except Exception:  # noqa: BLE001
                pass
            self._session = None


class ReadProber(_Periodic):
    def __init__(self, adapter: BaseDatabaseAdapter, endpoint: DbEndpoint, stream: EventStream) -> None:
        super().__init__(stream, READ_PROBE_INTERVAL_S)
        self.adapter = adapter
        self.endpoint = endpoint
        self._session: DatabaseSession | None = None

    async def _tick(self) -> None:
        t0 = time.monotonic()
        try:
            async with asyncio.timeout(PROBE_ATTEMPT_TIMEOUT_S):
                if self._session is None or self._session.is_closed:
                    self._session = await self.adapter.session(self.endpoint, timeout_s=PROBE_ATTEMPT_TIMEOUT_S)
                ok = await self._session.ping()
        except Exception as exc:  # noqa: BLE001 -- engine-specific; the adapter owns the taxonomy
            if self._session is not None:
                await self._session.close()
                self._session = None
            self.stream.emit("read_prober", "read_probe", ok=False, error=type(exc).__name__,
                             endpoint=f"{self.endpoint.host}:{self.endpoint.port}")
            return
        self.stream.emit("read_prober", "read_probe", ok=ok, latency_ms=(time.monotonic() - t0) * 1000,
                         endpoint=f"{self.endpoint.host}:{self.endpoint.port}")


class LogTailer:
    """Tails the node's PostgreSQL log over SSH during the fault window (Arch §9: direct
    journald/log tail for event corroboration). Lines carry the node's own timestamps, which
    are corrected by the measured clock offset before any use (Arch §7.2 clock discipline);
    harness-observable signals are always preferred."""

    def __init__(self, node: Node, stream: EventStream) -> None:
        self.node = node
        self.stream = stream
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    def start(self) -> None:
        self._task = asyncio.create_task(self._loop(), name="LogTailer")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def _loop(self) -> None:
        command = as_root(f"tail -n0 -F {shlex.quote(self.node.log_file)}")
        while not self._stop.is_set():
            try:
                async with RemoteHost(self.node.ssh, connect_timeout_s=PROBE_ATTEMPT_TIMEOUT_S * 3) as host:
                    self.stream.emit("log_tailer", "connected", node=self.node.name)
                    async with host.process(command) as proc:
                        async for line in proc.stdout:
                            self.stream.emit("log_tailer", "log_line", node=self.node.name, line=line.rstrip("\n"))
            except (OSError, asyncssh.Error, TimeoutError) as exc:
                self.stream.emit("log_tailer", "disconnected", node=self.node.name, error=type(exc).__name__)
            await asyncio.sleep(LOG_RECONNECT_PAUSE_S)


class DiskUsageProber:
    """How full the filesystem holding the data directory is, every DISK_PROBE_INTERVAL_S, over
    one persistent SSH session (reconnecting across restarts and reboots). Feeds the standing
    abort condition; a failed reading is recorded as unavailable, never as a number."""

    def __init__(self, node: Node, stream: EventStream) -> None:
        self.node = node
        self.stream = stream
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._loop(), name="DiskUsageProber")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def _loop(self) -> None:
        command = as_root(f"df -P {shlex.quote(self.node.pgdata)}")
        while True:
            try:
                async with RemoteHost(self.node.ssh, connect_timeout_s=PROBE_ATTEMPT_TIMEOUT_S * 3) as host:
                    while True:
                        result = await host.run(command, timeout_s=DISK_PROBE_INTERVAL_S, check=False)
                        used = parse_df_used_pct(result.stdout)
                        if used is None:
                            self.stream.emit("disk_prober", "disk_usage_unavailable", node=self.node.name,
                                             error=(result.stderr or result.stdout).strip()[:200])
                        else:
                            self.stream.emit("disk_prober", "disk_usage", node=self.node.name, used_pct=used)
                        await asyncio.sleep(DISK_PROBE_INTERVAL_S)
            except (OSError, asyncssh.Error, TimeoutError) as exc:
                self.stream.emit("disk_prober", "disk_usage_unavailable", node=self.node.name, error=type(exc).__name__)
            await asyncio.sleep(DISK_PROBE_INTERVAL_S)


def parse_df_used_pct(text: str) -> float | None:
    """The Use% column of `df -P <path>` (header line, then one line for the filesystem)."""
    lines = [line for line in text.strip().splitlines() if line.strip()]
    if len(lines) < 2:
        return None
    fields = lines[-1].split()
    if len(fields) < 5 or not fields[4].endswith("%") or not fields[4][:-1].isdigit():
        return None
    return float(fields[4][:-1])


async def measure_clock_offset(node: Node, stream: EventStream, samples: int = 5) -> float:
    """Node clock minus harness clock, in seconds (Arch §7.2: measured per node at test
    start). Uses the minimum-RTT sample; the RTT bounds the error."""
    best: tuple[float, float] | None = None  # (rtt, offset)
    async with RemoteHost(node.ssh) as host:
        for _ in range(samples):
            before = time.time()
            result = await host.run("date +%s.%N", timeout_s=5)
            after = time.time()
            offset = float(result.stdout.strip()) - (before + after) / 2
            rtt = after - before
            if best is None or rtt < best[0]:
                best = (rtt, offset)
    assert best is not None
    stream.emit("orchestrator", "clock_offset", node=node.name, offset_s=best[1], rtt_s=best[0])
    return best[1]
