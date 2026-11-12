"""The TPC-C engine in one process (TPC-C build step 6). With several processes (step 8,
coordinator.py) each process runs one of these for its share of the clerks and the rate, and
hands its raw numbers to the coordinator instead of writing samples itself.

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
import math
import time
from collections import Counter
from collections.abc import Callable, Sequence
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
    _Window,
    percentile,
    p99,
)
from resilience_tests.execution.workload.tpcc.samples import DriverLoadWatch, merge_raws
from resilience_tests.execution.workload.markers import MarkerJournals
from resilience_tests.execution.workload.profiles.base import Request, Terminal, WorkloadProfile
from resilience_tests.observability.event_stream import EventStream

SAMPLE_INTERVAL_S = 1.0
STOP_GRACE_S = 5.0          # at stop, each clerk may finish its running transaction for this long


class TpccWorkloadDriver:
    def __init__(self, adapter: BaseDatabaseAdapter, workload: Workload, journals: MarkerJournals,
                 stream: EventStream | None, profile: WorkloadProfile, *,
                 worker_ids: Sequence[int] | None = None, rate_tps: float | None = None,
                 raw_sink: Callable[[dict[str, Any]], None] | None = None) -> None:
        """One process: all clerks, the scenario's rate, samples written to `stream`. One of
        several processes (coordinator.py): `worker_ids` (its clerks), `rate_tps` (its share of
        the rate) and `raw_sink` (where its raw numbers go each interval)."""
        if workload.tpcc is None:
            raise ValueError("the TPC-C engine needs workload.tpcc")
        if stream is None and raw_sink is None:
            raise ValueError("the TPC-C engine needs a stream or a raw sink")
        self.adapter = adapter
        self.workload = workload
        self.journals = journals
        self.stream = stream
        self.raw_sink = raw_sink
        self.profile = profile
        self.limiter = RateLimiter(workload.rate_tps if rate_tps is None else rate_tps,
                                   keep_schedule=workload.latency_from == "intended_start")
        self.session_settings = dict(workload.session)
        ids = list(range(workload.concurrency)) if worker_ids is None else list(worker_ids)
        self.terminals: dict[int, Terminal] = {i: profile.terminal(i) for i in ids}
        self.load_watch = DriverLoadWatch()
        self._seq = 0
        self._last_t: float | None = None
        self._cpu_last = 0.0
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._window = _Window()
        self._types = {t: _Window() for t in contract.TPCC_TYPES}
        self._business = Counter()
        self._groups = {g: _Window() for g in dict.fromkeys(t.group for t in self.terminals.values())}
        self._measure: _Window | None = None
        self._measure_t0 = 0.0
        self.window_t0_ns = 0
        self.failure: str | None = None
        self.connected_workers = 0

    # --- the driver interface (contract.WorkloadDriverContract) ----------------------------

    async def start(self) -> None:
        if self.stream is not None:
            self.stream.emit("workload", "start", profile=self.workload.profile, concurrency=self.workload.concurrency,
                             rate_tps=self.workload.rate_tps, warehouses=self.workload.tpcc.warehouses, processes=1)
        self._tasks = [asyncio.create_task(self._worker(i), name=f"tpcc-clerk-{i}") for i in self.terminals]
        self._tasks.append(asyncio.create_task(self._sampler(), name="tpcc-sampler"))

    async def wait_until_ready(self, poll_s: float = 0.1) -> None:
        while self.connected_workers < len(self.terminals) and self.failure is None:
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
        """Graceful: no new transaction starts; each clerk finishes the one it is running, for at
        most STOP_GRACE_S (a clerk stuck on a database that does not answer is then cancelled,
        and its transaction stays unknown). So a transaction is not cut between its
        acknowledgement line and its count, and fewer end as unknown merely because the run ended."""
        self._stop.set()
        clerks, others = self._tasks[:-1], self._tasks[-1:]
        for t in others:
            t.cancel()
        if clerks:
            _done, pending = await asyncio.wait(clerks, timeout=STOP_GRACE_S)
            for t in pending:
                t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._publish_last_interval()
        if self.stream is not None:
            self.stream.emit("workload", "stop")

    # --- clerks ----------------------------------------------------------------------------------

    async def _worker(self, worker_id: int) -> None:
        try:
            await self._work(worker_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 -- the measuring instrument failed: recorded, the run aborts
            self._fail(f"tpcc clerk {worker_id}: {type(exc).__name__}: {exc}", worker=worker_id)

    def _fail(self, why: str, **detail: Any) -> None:
        if self.failure is None:
            self.failure = why
            if self.stream is not None:
                self.stream.emit("workload", "fatal", error=why, **detail)
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
        intended = await self.limiter.acquire()
        if self._stop.is_set():                 # stopping: start nothing new
            return session
        req: Request = self.profile.next_request(terminal)
        marker: tuple[int, str] | None = None
        journal_ms = 0.0
        if req.needs_marker:
            seq, uid = self.journals.next_marker()
            tj = time.monotonic()
            await self.journals.written(seq, uid, time.time(), type=req.txn_type)
            journal_ms = (time.monotonic() - tj) * 1000
            marker = (seq, uid)

        # latency is timed from the send, or from the intended start (workload.latency_from)
        t0 = intended if self.workload.latency_from == "intended_start" else time.monotonic()
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
        """One interval at a time, over the interval actually measured (as the custom driver):
        the raw numbers, this process's CPU use and loop delay, then a sample (one process) or a
        hand-over to the coordinator (several). The driver-limited guard watches every interval."""
        self._last_t = last = time.monotonic()
        self._cpu_last = cpu_last = time.process_time()
        while not self._stop.is_set():
            if self.raw_sink is not None:
                # one of several processes: end every interval on a whole second of the system's
                # monotonic clock (shared by all processes), so the coordinator merges the same
                # real second of every process
                target = (math.floor(last / SAMPLE_INTERVAL_S) + 1) * SAMPLE_INTERVAL_S
                await asyncio.sleep(max(0.0, target - time.monotonic()))
            else:
                target = last + SAMPLE_INTERVAL_S
                await asyncio.sleep(SAMPLE_INTERVAL_S)
            now, cpu_now = time.monotonic(), time.process_time()
            elapsed, last = now - last, now
            cpu_share, cpu_last = ((cpu_now - cpu_last) / elapsed if elapsed > 0 else 0.0), cpu_now
            self._last_t, self._cpu_last = last, cpu_last
            self._publish(self._raw_interval(elapsed, cpu_share, max(0.0, now - target)))
            if self.stream is not None:
                lag = max(0.0, now - target)
                self.stream.emit("workload", "driver_load", cpu_share=[round(cpu_share, 3)], loop_lag_s=[round(lag, 3)])
                limited = self.load_watch.update([cpu_share], [lag])
                if limited:
                    self._fail(limited, driver_limited=True)

    def _publish(self, raw: dict[str, Any]) -> None:
        if self.raw_sink is not None:
            self.raw_sink(raw)
        if self.stream is not None:
            self.stream.emit("workload", "sample", **merge_raws([raw]))

    def _publish_last_interval(self) -> None:
        """At stop: the part-second since the last sample, so every acknowledged transaction is
        in exactly one sample (the sample totals then equal the acknowledgement journal)."""
        if self._last_t is None:
            return
        now = time.monotonic()
        elapsed = now - self._last_t
        if elapsed <= 0:
            return
        cpu = (time.process_time() - self._cpu_last) / elapsed
        self._last_t = None
        self._publish(self._raw_interval(elapsed, cpu, 0.0))

    def _raw_interval(self, elapsed: float, cpu_share: float, loop_lag_s: float) -> dict[str, Any]:
        w, self._window = self._window, _Window()
        types, self._types = self._types, {t: _Window() for t in contract.TPCC_TYPES}
        business, self._business = self._business, Counter()
        groups, self._groups = self._groups, {g: _Window() for g in self._groups}
        self._seq += 1
        return {
            "seq": self._seq, "t_end_ns": time.monotonic_ns(),
            "second": round(time.monotonic() / SAMPLE_INTERVAL_S), "elapsed": elapsed,
            "cpu_share": cpu_share, "loop_lag_s": loop_lag_s,
            "totals": {"commits": w.commits, "errors": w.errors, "indeterminate": w.indeterminate, "drops": w.drops,
                       "reconnects": w.reconnects, "connect_failures": w.connect_failures},
            "latencies": list(w.latencies_ms or []), "journal_ms": list(w.journal_ms or []),
            "types": {t: {"commits": tw.commits, "errors": tw.errors, "business_rollbacks": business[t],
                          "latencies": list(tw.latencies_ms or [])} for t, tw in types.items()},
            "groups": {g: {"commits": gw.commits, "errors": gw.errors, "indeterminate": gw.indeterminate,
                           "drops": gw.drops, "latencies": list(gw.latencies_ms or [])} for g, gw in groups.items()},
        }
