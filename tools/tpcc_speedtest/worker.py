"""One worker process of the speed test: N clerks on one asyncio event loop.

Each clerk does, per transaction, exactly the work the real TPC-C engine will do:

    create the New-Order inputs
    write the marker line and wait until it is on disk     (the harness's DurableJournal)
    one call:  SELECT resilience_speedtest.new_order(...)  (committed when it returns)
    committed      -> "committed" line (with the order number), on disk
    business/other rollback -> "rolled back" line, on disk
    connection lost or timeout -> outcome unknown, no line, reconnect

The journals are the harness's own DurableJournal (group-committed fdatasync), so the cost
measured here is the real cost.
"""

from __future__ import annotations

import asyncio
import os
import random
import threading
import time
import uuid as uuidlib
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import asyncpg

from resilience_tests.execution.workload.markers import DurableJournal
from tools.tpcc_speedtest.logic import BUSINESS_ROLLBACK_SQLSTATE, RunConstants, new_order_input

APPLICATION_NAME = "resilience-speedtest"
CONNECT_TIMEOUT_S = 2.0
CALL_TIMEOUT_S = 10.0
RECONNECT_PAUSE_S = 0.2

NEW_ORDER_SQL = (
    "SELECT resilience_speedtest.new_order($1, $2, $3, $4, $5, $6::int[], $7::int[], $8::int[])"
)


@dataclass
class WorkerConfig:
    index: int
    dsn: dict[str, Any]              # host, port, database, user (password from ~/.pgpass)
    step_dir: str
    home_warehouses: list[int]       # one per clerk
    warehouses: int
    items: int
    constants: tuple[int, int]       # RunConstants (c_customer, c_item), the same for all processes
    seed: int
    measure_from: float              # wall-clock time: measuring starts (warm-up before it)
    stop_at: float                   # wall-clock time: no new transaction starts after it


@dataclass
class WorkerResult:
    index: int
    clerks: int
    committed_total: int = 0         # all calls that returned, warm-up included
    committed_in_window: int = 0
    new_orders_total: int = 0        # all New-Orders attempted, warm-up included
    business_rollbacks: int = 0
    other_errors: int = 0
    unknown_outcomes: int = 0
    connect_failures: int = 0
    error_samples: list[str] = field(default_factory=list)
    cpu_s: float = 0.0               # CPU time of this process (all its threads) in the window
    wall_s: float = 0.0
    fsyncs: int = 0
    journal_lines: int = 0
    call_ms: list[float] = field(default_factory=list)     # database call, window only
    journal_ms: list[float] = field(default_factory=list)  # marker line on disk, window only


class _FsyncCounter:
    """Counts fdatasync calls in this process, to prove the journal flushes really happen.
    The worker process exists only for the speed test, so wrapping os.fdatasync is safe."""

    def __init__(self) -> None:
        self.count = 0
        self._lock = threading.Lock()
        self._real = os.fdatasync
        os.fdatasync = self._fdatasync  # type: ignore[assignment]

    def _fdatasync(self, fd: int) -> None:
        with self._lock:
            self.count += 1
        self._real(fd)


class _Journals:
    def __init__(self, step_dir: Path, index: int) -> None:
        self.marker = DurableJournal(step_dir / f"marker.{index}.jrnl")
        self.acked = DurableJournal(step_dir / f"acked.{index}.jrnl")
        self.aborted = DurableJournal(step_dir / f"aborted.{index}.jrnl")
        self.seq = 0
        self.lines = 0

    async def append(self, journal: DurableJournal, record: dict[str, Any]) -> None:
        await journal.append(record)
        self.lines += 1

    def close(self) -> None:
        self.marker.close()
        self.acked.close()
        self.aborted.close()


def _error_text(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"[:300]


async def _clerk(clerk: int, cfg: WorkerConfig, journals: _Journals, res: WorkerResult,
                 consts: RunConstants, rng: random.Random) -> None:
    home_w = cfg.home_warehouses[clerk]
    conn: asyncpg.Connection | None = None
    try:
        while time.time() < cfg.stop_at:
            if conn is None:
                try:
                    conn = await asyncpg.connect(**cfg.dsn, timeout=CONNECT_TIMEOUT_S,
                                                 server_settings={"application_name": APPLICATION_NAME})
                except (OSError, asyncio.TimeoutError, asyncpg.PostgresError, asyncpg.InterfaceError) as exc:
                    res.connect_failures += 1
                    if len(res.error_samples) < 5:
                        res.error_samples.append("connect: " + _error_text(exc))
                    await asyncio.sleep(RECONNECT_PAUSE_S)
                    continue

            inp = new_order_input(rng, home_w, cfg.warehouses, cfg.items, consts)
            journals.seq += 1
            seq, marker = journals.seq, uuidlib.uuid4()
            uid = str(marker)
            t_pre = time.time()
            in_window = cfg.measure_from <= t_pre < cfg.stop_at
            t0 = time.perf_counter()
            await journals.append(journals.marker, {"seq": seq, "uuid": uid, "t_pre": t_pre, "type": "new_order"})
            t1 = time.perf_counter()
            res.new_orders_total += 1
            try:
                o_id = await asyncio.wait_for(
                    conn.fetchval(NEW_ORDER_SQL, marker, seq, inp.w_id, inp.d_id, inp.c_id,
                                  list(inp.items), list(inp.supply_w), list(inp.quantities)),
                    CALL_TIMEOUT_S)
            except asyncpg.PostgresError as exc:
                # The server answered with an error: the call (marker row included) is rolled back.
                t2 = time.perf_counter()
                business = exc.sqlstate == BUSINESS_ROLLBACK_SQLSTATE
                if business:
                    res.business_rollbacks += 1
                else:
                    res.other_errors += 1
                    if len(res.error_samples) < 5:
                        res.error_samples.append(_error_text(exc))
                await journals.append(journals.aborted, {"uuid": uid, "t_abort": time.time(),
                                                         "sqlstate": exc.sqlstate, "business_rollback": business})
            except (OSError, asyncio.TimeoutError, asyncpg.InterfaceError) as exc:
                # No answer: the outcome is unknown. No journal line; reconnect.
                t2 = time.perf_counter()
                res.unknown_outcomes += 1
                if len(res.error_samples) < 5:
                    res.error_samples.append("unknown outcome: " + _error_text(exc))
                conn.terminate()
                conn = None
            else:
                t2 = time.perf_counter()
                await journals.append(journals.acked, {"uuid": uid, "t_ack": time.time(), "w": inp.w_id,
                                                       "d": inp.d_id, "o_id": o_id, "ol_cnt": inp.ol_cnt})
                res.committed_total += 1
                if in_window:
                    res.committed_in_window += 1
            if in_window:
                res.journal_ms.append((t1 - t0) * 1000.0)
                res.call_ms.append((t2 - t1) * 1000.0)
    finally:
        if conn is not None:
            await conn.close()


async def _cpu_window(cfg: WorkerConfig, res: WorkerResult) -> None:
    await asyncio.sleep(max(0.0, cfg.measure_from - time.time()))
    cpu0, wall0 = time.process_time(), time.perf_counter()
    await asyncio.sleep(max(0.0, cfg.stop_at - time.time()))
    res.cpu_s = time.process_time() - cpu0
    res.wall_s = time.perf_counter() - wall0


async def _main(cfg: WorkerConfig) -> WorkerResult:
    fsyncs = _FsyncCounter()
    journals = _Journals(Path(cfg.step_dir), cfg.index)
    res = WorkerResult(index=cfg.index, clerks=len(cfg.home_warehouses))
    consts = RunConstants(*cfg.constants)
    try:
        await asyncio.gather(
            _cpu_window(cfg, res),
            *(_clerk(c, cfg, journals, res, consts, random.Random(cfg.seed * 1000 + c))
              for c in range(len(cfg.home_warehouses))),
        )
    finally:
        journals.close()
    res.fsyncs = fsyncs.count
    res.journal_lines = journals.lines
    return res


def run_worker(cfg: WorkerConfig) -> dict[str, Any]:
    """Entry point of a worker process (spawned, so it starts clean)."""
    return asdict(asyncio.run(_main(cfg)))
