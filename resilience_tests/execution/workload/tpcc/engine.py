"""The TPC-C engine in one process (TPC-C build step 6; several processes follow in step 8).

It offers the same interface as the custom workload's driver (contract.WorkloadDriverContract),
so the orchestrator, the phases and the analysis use it unchanged. Per clerk and transaction:

    1. wait for the start slot from the shared rate limiter (the scenario's rate_tps)
    2. the profile picks the next transaction and its values (tpcc/inputs.py)
    3. write transactions: the marker line goes to the driver host's disk BEFORE the call
    4. one call through the adapter (adapters/postgresql/tpcc.py), bounded by TXN_TIMEOUT_S
    5. by outcome:
         committed           "committed" line (New-Order: with its order number and lines)
         business_rollback   "rolled back" line, marked as the expected TPC-C 1% -- not an error
         definitely_aborted  "rolled back" line; counted as an error
         unknown             no line (it may or may not have committed); the connection is
                             dropped and the clerk reconnects
Read-only transactions (Order-Status, Stock-Level) write no journal line.

Every second a `sample` event: the totals of all types and groups (today's fields, read by the
existing analysis), plus by_type (5 TPC-C types) and by_group (docs/tpcc-contract.md §2).
Latency is recorded for every attempt, committed or not, as the custom driver does.
"""

from __future__ import annotations

import asyncio
import time
from collections import Counter
from typing import Any

from catalog.schema import Workload
from resilience_tests.adapters.base import BaseDatabaseAdapter, DatabaseSession
from resilience_tests.execution.workload import contract
from resilience_tests.execution.workload.driver import (
    CONNECT_TIMEOUT_S,
    RECONNECT_PAUSE_S,
    TXN_TIMEOUT_S,
    MeasuredWindow,
    RateLimiter,
    _group_stats,
    _Window,
    percentile,
    p99,
)
from resilience_tests.execution.workload.markers import MarkerJournals
from resilience_tests.execution.workload.profiles.base import Request, Terminal, WorkloadProfile
from resilience_tests.observability.event_stream import EventStream

SAMPLE_INTERVAL_S = 1.0


class TpccWorkloadDriver:
    def __init__(self, adapter: BaseDatabaseAdapter, workload: Workload, journals: MarkerJournals,
                 stream: EventStream, profile: WorkloadProfile) -> None:
        if workload.tpcc is None:
            raise ValueError("the TPC-C engine needs workload.tpcc")
        self.adapter = adapter
        self.workload = workload
        self.journals = journals
        self.stream = stream
        self.profile = profile
        self.limiter = RateLimiter(workload.rate_tps)
        self.session_settings = dict(workload.session)
        self.terminals: list[Terminal] = [profile.terminal(i) for i in range(workload.concurrency)]
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._window = _Window()
        self._types = {t: _Window() for t in contract.TPCC_TYPES}
        self._business = Counter()
        self._groups = {g: _Window() for g in dict.fromkeys(t.group for t in self.terminals)}
        self._measure: _Window | None = None
        self._measure_t0 = 0.0
        self.window_t0_ns = 0
        self.failure: str | None = None
        self.connected_workers = 0

    # --- the driver interface (contract.WorkloadDriverContract) ----------------------------

    async def start(self) -> None:
        self.stream.emit("workload", "start", profile=self.workload.profile, concurrency=self.workload.concurrency,
                         rate_tps=self.workload.rate_tps, warehouses=self.workload.tpcc.warehouses)
        self._tasks = [asyncio.create_task(self._worker(i), name=f"tpcc-clerk-{i}")
                       for i in range(self.workload.concurrency)]
        self._tasks.append(asyncio.create_task(self._sampler(), name="tpcc-sampler"))

    async def wait_until_ready(self, poll_s: float = 0.1) -> None:
        while self.connected_workers < self.workload.concurrency and self.failure is None:
            await asyncio.sleep(poll_s)

    def begin_window(self) -> None:
        self._measure = _Window()
        self._measure_t0 = time.monotonic()
        self.window_t0_ns = time.monotonic_ns()

    def end_window(self) -> MeasuredWindow:
        if self._measure is None:
            raise RuntimeError("end_window() without begin_window()")
        w, self._measure = self._measure, None
        duration = time.monotonic() - self._measure_t0
        lat = w.latencies_ms or []
        return MeasuredWindow(
            duration_s=duration, commits=w.commits, tps=w.commits / duration if duration > 0 else 0.0,
            p99_ms=p99(lat), journal_p99_ms=p99(w.journal_ms or []), errors=w.errors,
            indeterminate=w.indeterminate, drops=w.drops, reconnects=w.reconnects,
            connect_failures=w.connect_failures, p50_ms=percentile(lat, 0.50), p95_ms=percentile(lat, 0.95))

    async def stop(self) -> None:
        self._stop.set()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self.stream.emit("workload", "stop")

    # --- clerks ----------------------------------------------------------------------------------

    async def _worker(self, worker_id: int) -> None:
        try:
            await self._work(worker_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 -- the measuring instrument failed: recorded, the run aborts
            if self.failure is None:
                self.failure = f"tpcc clerk {worker_id}: {type(exc).__name__}: {exc}"
                self.stream.emit("workload", "fatal", worker=worker_id, error=self.failure)
            self._stop.set()

    async def _connect(self) -> DatabaseSession:
        if self.session_settings:
            return await self.adapter.session(timeout_s=CONNECT_TIMEOUT_S, settings=self.session_settings)
        return await self.adapter.session(timeout_s=CONNECT_TIMEOUT_S)

    async def _work(self, worker_id: int) -> None:
        terminal = self.terminals[worker_id]
        session: DatabaseSession | None = None
        ever_connected = False
        try:
            while not self._stop.is_set():
                if session is not None and session.is_closed:
                    self._count(terminal, drops=1)
                    session = None
                if session is None:
                    try:
                        session = await self._connect()
                    except Exception:  # noqa: BLE001 -- engine-specific; the adapter owns the taxonomy
                        self._count(terminal, connect_failures=1)
                        await asyncio.sleep(RECONNECT_PAUSE_S)
                        continue
                    if ever_connected:
                        self._count(terminal, reconnects=1)
                    else:
                        self.connected_workers += 1
                    ever_connected = True
                session = await self._one_transaction(session, terminal)
        finally:
            if session is not None:
                try:
                    await session.close()
                except Exception:  # noqa: BLE001
                    pass

    async def _one_transaction(self, session: DatabaseSession, terminal: Terminal) -> DatabaseSession | None:
        await self.limiter.acquire()
        req: Request = self.profile.next_request(terminal)
        marker: tuple[int, str] | None = None
        journal_ms = 0.0
        if req.needs_marker:
            seq, uid = self.journals.next_marker()
            tj = time.monotonic()
            await self.journals.written(seq, uid, time.time(), type=req.txn_type)
            journal_ms = (time.monotonic() - tj) * 1000
            marker = (seq, uid)

        t0 = time.monotonic()
        try:
            async with asyncio.timeout(TXN_TIMEOUT_S):
                result = await self.profile.execute(session, req, marker)
            outcome, journal, sqlstate = result.outcome, dict(result.journal), result.sqlstate
        except TimeoutError:
            outcome, journal, sqlstate = contract.OUTCOME_UNKNOWN, {}, None
        except Exception:  # noqa: BLE001 -- anything not classified is unknown, never guessed
            outcome, journal, sqlstate = contract.OUTCOME_UNKNOWN, {}, None
        latency_ms = (time.monotonic() - t0) * 1000

        if outcome == contract.OUTCOME_COMMITTED:
            if marker is not None:
                t2 = time.monotonic()
                await self.journals.acknowledged(marker[1], time.time(), **journal)
                journal_ms += (time.monotonic() - t2) * 1000
            self._count(terminal, req.txn_type, commits=1, latency_ms=latency_ms,
                        journal_ms=journal_ms if marker is not None else None)
            return session
        if outcome in (contract.OUTCOME_BUSINESS_ROLLBACK, contract.OUTCOME_DEFINITELY_ABORTED):
            business = outcome == contract.OUTCOME_BUSINESS_ROLLBACK
            if marker is not None:
                await self.journals.aborted(marker[1], time.time(), type=req.txn_type, sqlstate=sqlstate,
                                            business_rollback=business)
            if business:
                self._business[req.txn_type] += 1
                self._count(terminal, req.txn_type, latency_ms=latency_ms)
            else:
                self._count(terminal, req.txn_type, errors=1, latency_ms=latency_ms)
            return session
        # unknown: no journal line; the connection is no longer trustworthy
        self._count(terminal, req.txn_type, indeterminate=1, drops=1, latency_ms=latency_ms)
        try:
            await session.close()
        except Exception:  # noqa: BLE001
            pass
        return None

    # --- counting and sampling ---------------------------------------------------------------

    def _count(self, terminal: Terminal, txn_type: str | None = None, *, commits: int = 0, errors: int = 0,
               indeterminate: int = 0, drops: int = 0, reconnects: int = 0, connect_failures: int = 0,
               latency_ms: float | None = None, journal_ms: float | None = None) -> None:
        targets = [self._window, self._measure, self._groups.get(terminal.group)]
        if txn_type is not None:
            targets.append(self._types.get(txn_type))
        for w in targets:
            if w is None:
                continue
            w.commits += commits
            w.errors += errors
            w.indeterminate += indeterminate
            w.drops += drops
            w.reconnects += reconnects
            w.connect_failures += connect_failures
            assert w.latencies_ms is not None and w.journal_ms is not None
            if latency_ms is not None:
                w.latencies_ms.append(latency_ms)
            if journal_ms is not None:
                w.journal_ms.append(journal_ms)

    async def _sampler(self) -> None:
        """One sample per interval, over the interval actually measured (as the custom driver)."""
        last = time.monotonic()
        while not self._stop.is_set():
            await asyncio.sleep(SAMPLE_INTERVAL_S)
            now = time.monotonic()
            elapsed, last = now - last, now
            w, self._window = self._window, _Window()
            types, self._types = self._types, {t: _Window() for t in contract.TPCC_TYPES}
            business, self._business = self._business, Counter()
            groups, self._groups = self._groups, {g: _Window() for g in self._groups}
            lat = w.latencies_ms or []
            self.stream.emit(
                "workload", "sample", interval_s=elapsed, commits=w.commits,
                tps=w.commits / elapsed if elapsed > 0 else 0.0,
                p99_ms=p99(lat), journal_p99_ms=p99(w.journal_ms or []),
                p50_ms=percentile(lat, 0.50), p95_ms=percentile(lat, 0.95),
                errors=w.errors, indeterminate=w.indeterminate, drops=w.drops,
                reconnects=w.reconnects, connect_failures=w.connect_failures,
                **{contract.BY_TYPE: {t: _tpcc_type_stats(tw, elapsed, business[t]) for t, tw in types.items()},
                   contract.BY_GROUP: {g: _group_stats(gw, elapsed) for g, gw in groups.items()}},
            )


def _tpcc_type_stats(w: _Window, elapsed: float, business_rollbacks: int) -> dict[str, Any]:
    lat = w.latencies_ms or []
    return {"commits": w.commits, "tps": w.commits / elapsed if elapsed > 0 else 0.0,
            "p90_ms": percentile(lat, 0.90), "p99_ms": p99(lat), "errors": w.errors,
            "business_rollbacks": business_rollbacks}
