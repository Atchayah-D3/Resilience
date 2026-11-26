"""Memory guard (run 1b, the misbehaving tenant, NL-N-I01b): ends the reports before the target runs
out of memory.

In run 1b the report sessions raise their own memory per sort (work_mem) far above the role's
default. A hash step may take work_mem x hash_mem_multiplier per process, so the database ALLOWS
more than the machine has; if the kernel then kills a server process, PostgreSQL restarts every
session, the customers included, and the target is shared with other software. The guard reads,
over one SSH session, the target's

  MemAvailable   free memory plus the cache the kernel can give back (vmstat's "free" leaves the
                 cache out, so it reads near 0 on a healthy machine)
  pswpout        pages swapped out since boot (/proc/vmstat): the swap-out since the guard was armed

every `interval_s` (at most 1 s), and once ARMED (before the first report session changes its
settings) calls `on_trigger(reason)` once when available memory falls below `available_min_mb`, the
swap-out since arming goes above `swap_out_max_mb`, or no reading arrives for `stale_s` (a guard
that sees nothing protects nothing). Each reading is also recorded (memory_guard/sample).
"""

from __future__ import annotations

import asyncio
import shlex
import time
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any

import asyncssh

from resilience_tests.control.profile import Node
from resilience_tests.execution.remote import RemoteHost
from resilience_tests.observability.event_stream import EventStream

SOURCE = "memory_guard"
END_OF_READING = "END"
RECONNECT_PAUSE_S = 1.0
CONNECT_TIMEOUT_S = 10.0


@dataclass(frozen=True)
class GuardRules:
    available_min_mb: float
    swap_out_max_mb: float
    interval_s: float
    stale_s: float


@dataclass(frozen=True)
class Reading:
    t_mono_ns: int
    available_kb: int
    swap_free_kb: int | None
    pswpout: int | None


def command(interval_s: float) -> str:
    """The remote loop: the page size once, then per reading the two meminfo lines, the pswpout
    line and an END line."""
    loop = (f"getconf PAGESIZE; while :; do grep -E '^(MemAvailable|SwapFree):' /proc/meminfo; "
            f"grep '^pswpout ' /proc/vmstat; echo {END_OF_READING}; sleep {interval_s:g}; done")
    return f"LC_ALL=C sh -c {shlex.quote(loop)}"


def parse_reading(lines: Iterable[str], t_mono_ns: int) -> Reading | None:
    """One reading from its lines ("MemAvailable:  13312000 kB", "SwapFree: ...", "pswpout 42");
    None without MemAvailable (never a reading of 0)."""
    values: dict[str, int] = {}
    for line in lines:
        words = line.replace(":", " ").split()
        if len(words) >= 2 and words[0] in ("MemAvailable", "SwapFree", "pswpout") and words[1].isdigit():
            values[words[0]] = int(words[1])
    if "MemAvailable" not in values:
        return None
    return Reading(t_mono_ns, values["MemAvailable"], values.get("SwapFree"), values.get("pswpout"))


def swapped_out_mb(reading: Reading, armed_pswpout: int | None, page_bytes: int) -> float | None:
    if reading.pswpout is None or armed_pswpout is None:
        return None
    return max(0, reading.pswpout - armed_pswpout) * page_bytes / 1024 ** 2


def trip_reason(reading: Reading, armed_pswpout: int | None, page_bytes: int, rules: GuardRules) -> str | None:
    """Why this reading ends the reports, or None."""
    available_mb = reading.available_kb / 1024
    if available_mb < rules.available_min_mb:
        return f"available memory {available_mb:.0f} MB, below {rules.available_min_mb:g} MB"
    swapped = swapped_out_mb(reading, armed_pswpout, page_bytes)
    if swapped is None:
        return "swap-out cannot be read (pswpout missing from /proc/vmstat)"
    if swapped > rules.swap_out_max_mb:
        return f"{swapped:.0f} MB swapped out since the guard was armed, above {rules.swap_out_max_mb:g} MB"
    return None


class MemoryGuard:
    """A fault sampler (started with the run's probes): readings from the start, judging only while
    armed. `on_trigger` runs at most once."""

    def __init__(self, node: Node, stream: EventStream, rules: GuardRules,
                 on_trigger: Callable[[str], Awaitable[Any]]) -> None:
        self.node = node
        self.stream = stream
        self.rules = rules
        self.on_trigger = on_trigger
        self.page_bytes = 4096
        self.last: Reading | None = None
        self.readings = 0
        self.armed_ns: int | None = None
        self.armed_pswpout: int | None = None
        self.disarmed_ns: int | None = None
        self.min_available_kb: int | None = None      # while armed
        self.max_swapped_mb: float | None = None      # while armed
        self.tripped: dict[str, Any] | None = None
        self.errors: list[str] = []
        self._tasks: list[asyncio.Task[None]] = []
        self._trigger_task: asyncio.Task[Any] | None = None

    # --- lifecycle (Sampler) ---------------------------------------------------------------------

    def start(self) -> None:
        self._tasks = [asyncio.create_task(self._read_loop(), name="MemoryGuard-read"),
                       asyncio.create_task(self._watch_loop(), name="MemoryGuard-watch")]

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        if self._trigger_task is not None:
            await asyncio.gather(self._trigger_task, return_exceptions=True)

    # --- arming ----------------------------------------------------------------------------------

    def fresh(self, now_ns: int | None = None) -> bool:
        now_ns = time.monotonic_ns() if now_ns is None else now_ns
        return self.last is not None and (now_ns - self.last.t_mono_ns) / 1e9 <= self.rules.stale_s

    def arm(self) -> None:
        """Start judging. Refused (RuntimeError) without a fresh reading: never armed blind."""
        if not self.fresh() or self.last is None:
            raise RuntimeError(f"no reading of the target's memory in the last {self.rules.stale_s:g} s"
                               + (f" ({self.errors[-1]})" if self.errors else ""))
        if self.last.pswpout is None:
            raise RuntimeError("the target's swap-out cannot be read (pswpout missing from /proc/vmstat)")
        self.armed_ns = time.monotonic_ns()
        self.armed_pswpout = self.last.pswpout
        self.min_available_kb = self.last.available_kb
        self.max_swapped_mb = 0.0
        self.stream.emit(SOURCE, "armed", available_kb=self.last.available_kb, pswpout=self.last.pswpout)

    def disarm(self) -> None:
        if self.armed_ns is not None and self.disarmed_ns is None:
            self.disarmed_ns = time.monotonic_ns()
            self.stream.emit(SOURCE, "disarmed")

    @property
    def armed(self) -> bool:
        return self.armed_ns is not None and self.disarmed_ns is None

    # --- judging ---------------------------------------------------------------------------------

    def observe(self, reading: Reading) -> None:
        """One reading: kept, recorded, and judged while armed."""
        self.last = reading
        self.readings += 1
        swapped = swapped_out_mb(reading, self.armed_pswpout, self.page_bytes) if self.armed else None
        self.stream.emit(SOURCE, "sample", available_kb=reading.available_kb, swap_free_kb=reading.swap_free_kb,
                         pswpout=reading.pswpout, swapped_out_mb=None if swapped is None else round(swapped, 1))
        if not self.armed:
            return
        self.min_available_kb = (reading.available_kb if self.min_available_kb is None
                                 else min(self.min_available_kb, reading.available_kb))
        if swapped is not None:
            self.max_swapped_mb = max(self.max_swapped_mb or 0.0, swapped)
        why = trip_reason(reading, self.armed_pswpout, self.page_bytes, self.rules)
        if why:
            self.trip(why)

    def trip(self, why: str) -> None:
        if self.tripped is not None or not self.armed:
            return
        now = time.monotonic_ns()
        self.tripped = {"reason": why, "t_mono_ns": now, "after_armed_s": round((now - (self.armed_ns or now)) / 1e9, 3)}
        self.stream.emit(SOURCE, "tripped", reason=why)
        self._trigger_task = asyncio.create_task(self._run_trigger(why), name="MemoryGuard-trigger")

    async def _run_trigger(self, why: str) -> None:
        try:
            result = await self.on_trigger(why)
            if self.tripped is not None:
                self.tripped["action"] = result
        except Exception as exc:  # noqa: BLE001 -- recorded; the revert at the end of the hold ends them too
            if self.tripped is not None:
                self.tripped["action_error"] = f"{type(exc).__name__}: {exc}"

    def state(self) -> dict[str, Any]:
        return {"rules": {"available_min_mb": self.rules.available_min_mb, "swap_out_max_mb": self.rules.swap_out_max_mb,
                          "interval_s": self.rules.interval_s, "stale_s": self.rules.stale_s},
                "readings": self.readings, "armed": self.armed_ns is not None,
                "armed_s": (round(((self.disarmed_ns or time.monotonic_ns()) - self.armed_ns) / 1e9, 1)
                            if self.armed_ns is not None else None),
                "min_available_mb": None if self.min_available_kb is None else round(self.min_available_kb / 1024, 1),
                "max_swapped_out_mb": None if self.max_swapped_mb is None else round(self.max_swapped_mb, 1),
                "page_bytes": self.page_bytes, "tripped": self.tripped, "errors": self.errors[-5:]}

    # --- the loops -------------------------------------------------------------------------------

    async def _watch_loop(self) -> None:
        """No reading for stale_s while armed ends the reports: a guard that sees nothing protects nothing."""
        while True:
            await asyncio.sleep(self.rules.interval_s)
            if self.armed and not self.fresh():
                age = (time.monotonic_ns() - self.last.t_mono_ns) / 1e9 if self.last else None
                self.trip(f"no memory reading for {age:.1f} s" if age is not None else "no memory reading")

    async def _read_loop(self) -> None:
        cmd = command(self.rules.interval_s)
        while True:
            try:
                async with RemoteHost(self.node.ssh, connect_timeout_s=CONNECT_TIMEOUT_S) as host:
                    async with host.process(cmd) as proc:
                        first = True
                        block: list[str] = []
                        async for raw in proc.stdout:
                            line = raw.strip()
                            if first:
                                first = False
                                if line.isdigit():
                                    self.page_bytes = int(line)
                                    continue
                            if line == END_OF_READING:
                                reading = parse_reading(block, time.monotonic_ns())
                                block = []
                                if reading is not None:
                                    self.observe(reading)
                                continue
                            block.append(line)
                        done = await proc.wait()
                        why = (done.stderr or "").strip().splitlines()
                        self.errors.append(why[0][:300] if why else "the reading loop ended")
            except (OSError, asyncssh.Error, TimeoutError) as exc:
                self.errors.append(f"{type(exc).__name__}: {exc}"[:300])
            self.stream.emit(SOURCE, "unavailable", error=self.errors[-1] if self.errors else None)
            await asyncio.sleep(RECONNECT_PAUSE_S)
