"""Custom workload driver (Arch §6.1): used by every RPO-measuring scenario, because
transaction markers need application-level logic a generic load tool cannot express.

Engine-agnostic: it drives the target through the adapter interface (Arch §8) and never sees
a connection, a statement or an engine exception.

Profile `oltp_write_heavy` with transaction_markers: every transaction is exactly the
marker transaction of Arch §6.2 (BEGIN; INSERT marker; COMMIT). The driver keeps running
through the fault -- reconnecting as the target returns -- so post-recovery throughput
(T_warm / RTO-to-SLO) is measured by the same load.

Every second it emits a `sample` event (committed TPS, p99 commit latency, errors,
connection drops, reconnects, failed connection attempts) on the harness clock.
Per-transaction evidence lives in the marker journals, not the event stream.

The driver is the measuring instrument. If it cannot keep its evidence -- a marker journal
write fails, or a worker dies unexpectedly -- it records the failure in `failure` and stops;
the orchestrator aborts the run rather than evaluate incomplete evidence.
"""

from __future__ import annotations

import asyncio
import math
import random
import time
from dataclasses import dataclass

from catalog.schema import Workload
from resilience_tests.adapters.base import BaseDatabaseAdapter, Capability, DatabaseSession, TransactionOutcome
from resilience_tests.execution.workload.markers import MarkerJournals
from resilience_tests.observability.event_stream import EventStream

SAMPLE_INTERVAL_S = 1.0
# One transaction in CHURN_REPLACE_EVERY replaces its row rather than updating it.
CHURN_REPLACE_EVERY = 10

CONNECT_TIMEOUT_S = 2.0
RECONNECT_PAUSE_S = 0.2
TXN_TIMEOUT_S = 10.0


class UnsupportedWorkload(ValueError):
    pass


class RateLimiter:
    """Paces starts at `rate` per second across all workers (single event loop, no lock)."""

    def __init__(self, rate: float | None) -> None:
        self.interval = 1.0 / rate if rate else 0.0
        self._next = time.monotonic()

    async def acquire(self) -> None:
        if not self.interval:
            return
        now = time.monotonic()
        slot = max(now, self._next)
        self._next = slot + self.interval
        if slot > now:
            await asyncio.sleep(slot - now)


@dataclass
class _Window:
    commits: int = 0
    errors: int = 0
    indeterminate: int = 0
    drops: int = 0             # established connections lost without the client closing them
    reconnects: int = 0        # connections re-established after one was lost
    connect_failures: int = 0  # connection attempts the target refused or did not answer
    latencies_ms: list[float] | None = None
    journal_ms: list[float] | None = None

    def __post_init__(self) -> None:
        self.latencies_ms = []
        self.journal_ms = []


@dataclass(frozen=True)
class MeasuredWindow:
    duration_s: float
    commits: int
    tps: float
    p99_ms: float | None          # database transaction latency
    journal_p99_ms: float | None  # driver-side marker flush latency (Arch §6.2)
    errors: int
    indeterminate: int
    drops: int = 0
    reconnects: int = 0
    connect_failures: int = 0


def p99(values: list[float]) -> float | None:
    """Nearest-rank 99th percentile: the smallest value at or above 99% of the sample.

    `int(0.99 * n)` is one rank too high whenever 0.99n is integral -- at n = 100 it returns
    the maximum. An inflated baseline p99 widens the recovery SLO ceiling (1.5x baseline),
    which makes rto_to_slo_s optimistic, so the error is one-sided in the wrong direction."""
    if not values:
        return None
    ordered = sorted(values)
    rank = math.ceil(0.99 * len(ordered))  # 1-based
    return ordered[max(0, rank - 1)]


class WorkloadDriver:
    def __init__(
        self,
        adapter: BaseDatabaseAdapter,
        workload: Workload,
        journals: MarkerJournals,
        stream: EventStream,
    ) -> None:
        if workload.profile not in ("oltp_write_heavy", "mixed") or not workload.transaction_markers:
            raise UnsupportedWorkload(
                f"workload profile {workload.profile!r} (markers={workload.transaction_markers}) is not built yet"
            )
        if not adapter.has(Capability.TRANSACTIONAL_MARKERS):
            raise UnsupportedWorkload(
                f"engine {adapter.engine!r} cannot record transaction markers, so RPO cannot be measured"
            )
        # `mixed` adds UPDATE/DELETE traffic beside the marker insert. Without it the workload
        # only ever appends, no row is ever superseded, and a bloat criterion cannot fail.
        self.churn = workload.profile == "mixed"
        if self.churn and not adapter.has(Capability.WORKLOAD_CHURN):
            raise UnsupportedWorkload(
                f"engine {adapter.engine!r} cannot run the churn workload, so cumulative bloat "
                "cannot be measured -- an insert-only load leaves no dead rows to find"
            )
        # the engine's own seeded key space, never a second copy of the number
        self.churn_keys = adapter.churn_key_space if self.churn else 0
        if self.churn and self.churn_keys < 1:
            raise UnsupportedWorkload(
                f"engine {adapter.engine!r} declares the churn capability but no key space"
            )
        self.adapter = adapter
        self.workload = workload
        self.journals = journals
        self.stream = stream
        self.limiter = RateLimiter(workload.rate_tps)
        self._churn_rng = random.Random(0xC0FFEE)   # seeded: the same key sequence every run
        self._stop = asyncio.Event()
        self._window = _Window()
        self._tasks: list[asyncio.Task[None]] = []
        self._measure: _Window | None = None
        self._measure_t0 = 0.0
        self.failure: str | None = None

    def begin_window(self) -> None:
        """Start an exact measurement window (baseline / steady-state check)."""
        self._measure = _Window()
        self._measure_t0 = time.monotonic()

    def end_window(self) -> MeasuredWindow:
        if self._measure is None:
            raise RuntimeError("end_window() without begin_window()")
        w, self._measure = self._measure, None
        duration = time.monotonic() - self._measure_t0
        return MeasuredWindow(
            duration_s=duration, commits=w.commits, tps=w.commits / duration if duration > 0 else 0.0,
            p99_ms=p99(w.latencies_ms or []), journal_p99_ms=p99(w.journal_ms or []),
            errors=w.errors, indeterminate=w.indeterminate, drops=w.drops,
            reconnects=w.reconnects, connect_failures=w.connect_failures,
        )

    async def start(self) -> None:
        self.stream.emit("workload", "start", profile=self.workload.profile,
                         concurrency=self.workload.concurrency, rate_tps=self.workload.rate_tps)
        self._tasks = [asyncio.create_task(self._worker(i), name=f"worker-{i}") for i in range(self.workload.concurrency)]
        self._tasks.append(asyncio.create_task(self._sampler(), name="sampler"))

    async def stop(self) -> None:
        self._stop.set()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self.stream.emit("workload", "stop")

    async def _sampler(self) -> None:
        """Samples carry the interval that was actually measured, not the nominal one.

        `asyncio.sleep(1.0)` yields at least 1 s, and more whenever the loop is busy -- which
        is exactly what happens during a recovery storm, the window these samples are used to
        judge. Dividing by the nominal 1 s would report a throughput the target never reached
        and could certify an SLO recovery that never happened (Arch §7.2)."""
        last = time.monotonic()
        while not self._stop.is_set():
            await asyncio.sleep(SAMPLE_INTERVAL_S)
            now = time.monotonic()
            elapsed, last = now - last, now
            w, self._window = self._window, _Window()
            self.stream.emit(
                "workload", "sample", interval_s=elapsed, commits=w.commits,
                tps=w.commits / elapsed if elapsed > 0 else 0.0,
                p99_ms=p99(w.latencies_ms or []), journal_p99_ms=p99(w.journal_ms or []),
                errors=w.errors, indeterminate=w.indeterminate, drops=w.drops,
                reconnects=w.reconnects, connect_failures=w.connect_failures,
            )

    def _fail(self, worker_id: int, exc: BaseException) -> None:
        """Record the first failure, emit it, and stop the load: the evidence is incomplete."""
        if self.failure is None:
            self.failure = f"worker {worker_id}: {type(exc).__name__}: {exc}"
            self.stream.emit("workload", "fatal", worker=worker_id, error=self.failure)
        self._stop.set()

    async def _worker(self, worker_id: int) -> None:
        try:
            await self._work(worker_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 -- recorded as fatal; the orchestrator aborts
            self._fail(worker_id, exc)

    async def _work(self, worker_id: int) -> None:
        session: DatabaseSession | None = None
        ever_connected = False
        try:
            while not self._stop.is_set():
                if session is not None and session.is_closed:
                    # the server (or the network) closed a connection we were not using
                    self._count(drops=1)
                    session = None
                if session is None:
                    try:
                        session = await self.adapter.session(timeout_s=CONNECT_TIMEOUT_S)
                    except Exception:  # noqa: BLE001 -- engine-specific; the adapter owns the taxonomy
                        session = None
                        self._count(connect_failures=1)
                        await asyncio.sleep(RECONNECT_PAUSE_S)
                        continue
                    if ever_connected:
                        self._count(reconnects=1)
                    ever_connected = True
                session = await self._one_transaction(session)
        finally:
            await _discard(session)

    async def _one_transaction(self, session: DatabaseSession) -> DatabaseSession | None:
        """One marker transaction. Returns the session to reuse, or None if it was lost."""
        await self.limiter.acquire()
        seq, marker_id = self.journals.next_marker()
        # 1. journal + fdatasync BEFORE the commit is sent. Timed separately: if the driver
        # host's flush path is the bottleneck, the evidence must say so rather than leave a
        # low throughput unexplained.
        tj = time.monotonic()
        await self.journals.written(seq, marker_id, time.time())
        t0 = time.monotonic()
        journal_ms = (t0 - tj) * 1000
        try:
            async with asyncio.timeout(TXN_TIMEOUT_S):
                if self.churn:
                    key = self._churn_rng.randrange(1, self.churn_keys + 1)
                    # one in CHURN_REPLACE_EVERY replaces the row instead of updating it,
                    # so line pointers churn as well as tuples
                    replace = self._churn_rng.randrange(CHURN_REPLACE_EVERY) == 0
                    outcome = await session.commit_marker_with_churn(seq, marker_id, key, replace)
                else:
                    outcome = await session.commit_marker(seq, marker_id)
        except TimeoutError:
            outcome = TransactionOutcome.UNKNOWN
        except Exception:  # noqa: BLE001 -- anything the adapter did not classify is unknown
            outcome = TransactionOutcome.UNKNOWN
        if outcome is TransactionOutcome.DEFINITELY_ABORTED:
            self._count(errors=1)
            return session
        if outcome is TransactionOutcome.UNKNOWN:
            # stays in written - acked (indeterminate); the connection was lost or is no
            # longer trustworthy, so it is discarded -- a dropped connection for the client
            self._count(indeterminate=1, drops=1)
            await _discard(session)
            return None
        latency_ms = (time.monotonic() - t0) * 1000
        # 3. only on acknowledgement
        t2 = time.monotonic()
        await self.journals.acknowledged(marker_id, time.time())
        journal_ms += (time.monotonic() - t2) * 1000
        self._count(commits=1, latency_ms=latency_ms, journal_ms=journal_ms)
        return session

    def _count(self, *, commits: int = 0, errors: int = 0, indeterminate: int = 0, drops: int = 0,
               reconnects: int = 0, connect_failures: int = 0,
               latency_ms: float | None = None, journal_ms: float | None = None) -> None:
        for w in (self._window, self._measure):
            if w is None:
                continue
            w.commits += commits
            w.errors += errors
            w.indeterminate += indeterminate
            w.drops += drops
            w.reconnects += reconnects
            w.connect_failures += connect_failures
            if latency_ms is not None:
                assert w.latencies_ms is not None and w.journal_ms is not None
                w.latencies_ms.append(latency_ms)
                if journal_ms is not None:
                    w.journal_ms.append(journal_ms)


async def _discard(session: DatabaseSession | None) -> None:
    if session is None:
        return
    try:
        await session.close()
    except Exception:  # noqa: BLE001 -- best effort on a session already known to be broken
        pass
