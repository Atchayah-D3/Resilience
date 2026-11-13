"""The TPC-C engine in several worker processes (TPC-C build step 8; proposal 7.7).

One Python process runs Python code on one core. For more load, the coordinator starts N worker
processes; each runs the single-process engine (engine.py) for its share of the clerks and of
the rate, writes its own journals (marker.<n>.jrnl, acked.<n>.jrnl, aborted.<n>.jrnl), and sends
its raw numbers to the coordinator once per interval. The coordinator:

    - merges the raw numbers of all processes into one sample per interval (samples.merge_raws:
      percentiles from all latencies together, never averaged)
    - watches every process's CPU use and loop delay: if the harness is the limit for several
      seconds, the run is stopped as invalid (samples.DriverLoadWatch)
    - stops the run if a worker process dies
    - offers the orchestrator the same interface as the single-process engine

Clerks keep their global numbers, so their home warehouses, groups and random sequences are the
same whichever process runs them. The RPO and partial-commit checks read every journal file.
Worker processes are started with "spawn": each starts clean, without the coordinator's state.
"""

from __future__ import annotations

import asyncio
import multiprocessing
import queue
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

from catalog.schema import Workload
from resilience_tests.execution.workload.driver import MeasuredWindow
from resilience_tests.execution.workload.tpcc.samples import DriverLoadWatch, merge_raws, merge_window, split_workers
from resilience_tests.observability.event_stream import EventStream

SAMPLE_WAIT_S = 3.0          # a process's numbers for an interval may arrive this late before the sample is written
STOP_WAIT_S = 20.0           # worker processes get this long to finish their transactions and close their journals
POLL_S = 0.25


def _child_main(cfg: dict[str, Any], cmd_q: Any, out_q: Any) -> None:
    """Entry point of one worker process."""
    asyncio.run(_child(cfg, cmd_q, out_q))


async def _child(cfg: dict[str, Any], cmd_q: Any, out_q: Any) -> None:
    from resilience_tests.adapters import postgresql  # noqa: F401  (registers the adapter)
    from resilience_tests.adapters.base import adapter_for
    from resilience_tests.control.profile import Node
    from resilience_tests.execution.workload.markers import MarkerJournals
    from resilience_tests.execution.workload.tpcc.engine import TpccWorkloadDriver
    from resilience_tests.execution.workload.tpcc.inputs import RunConstants
    from resilience_tests.execution.workload.tpcc.profile import TpccProfile

    idx = cfg["index"]
    journals = None
    drv = None
    try:
        adapter = adapter_for(cfg["engine"], Node.model_validate(cfg["node"]))
        workload = Workload.model_validate(cfg["workload"])
        profile = TpccProfile(workload, seed=cfg["seed"])
        profile.constants = RunConstants(*cfg["constants"])
        journals = MarkerJournals(Path(cfg["run_dir"]), index=idx, processes=cfg["processes"])
        drv = TpccWorkloadDriver(adapter, workload, journals, None, profile, worker_ids=cfg["worker_ids"],
                                 rate_tps=cfg["rate_tps"], raw_sink=lambda raw: out_q.put(("raw", idx, raw)))
        await drv.start()
        loop = asyncio.get_running_loop()

        async def announce_ready() -> None:
            await drv.wait_until_ready()
            if drv.failure is None:
                out_q.put(("ready", idx, None))
        ready = asyncio.create_task(announce_ready())

        reported = False
        while True:
            try:
                cmd = await loop.run_in_executor(None, cmd_q.get, True, POLL_S)
            except queue.Empty:
                cmd = None
            if drv.failure and not reported:
                out_q.put(("failure", idx, drv.failure))
                reported = True
            if cmd == "stop":
                break
        ready.cancel()
    except Exception as exc:  # noqa: BLE001 -- reported to the coordinator, which stops the run
        out_q.put(("failure", idx, f"worker process {idx}: {type(exc).__name__}: {exc}"))
    finally:
        if drv is not None:
            await drv.stop()
        if journals is not None:
            journals.close()
        out_q.put(("stopped", idx, drv.failure if drv is not None else None))


class TpccCoordinator:
    """contract.WorkloadDriverContract over N worker processes."""

    def __init__(self, adapter: Any, workload: Workload, journals: Any, stream: EventStream, profile: Any,
                 processes: int) -> None:
        if processes < 2:
            raise ValueError("the coordinator is for 2 or more worker processes; use the engine for one")
        if profile.constants is None:
            raise RuntimeError("the run's NURand constants are not set: prepare() must run first")
        self.workload = workload
        self.stream = stream
        self.processes = processes
        self._ctx = multiprocessing.get_context("spawn")
        self._out_q = self._ctx.Queue()
        self._cmd_qs = [self._ctx.Queue() for _ in range(processes)]
        rate = workload.rate_tps
        consts = profile.constants
        self._configs = [{
            "index": i, "processes": processes, "engine": adapter.engine, "node": adapter.node.model_dump(mode="json"),
            "workload": workload.model_dump(mode="json"), "seed": profile.seed,
            "constants": (consts.c_last, consts.c_id, consts.ol_i_id), "run_dir": str(journals.run_dir),
            "worker_ids": ids, "rate_tps": (rate * len(ids) / workload.concurrency) if rate else None,
        } for i, ids in enumerate(split_workers(workload.concurrency, processes))]
        self._procs: list[Any] = []
        self._ready: set[int] = set()
        self._stopped: set[int] = set()
        self._pending: dict[int, list[dict[str, Any]]] = defaultdict(list)     # clock second -> raws
        self._first_seen: dict[int, float] = {}
        self._window_raws: list[dict[str, Any]] | None = None
        self._window_t0_ns = 0
        self._tasks: list[asyncio.Task[None]] = []
        self._stop = asyncio.Event()
        self.load_watch = DriverLoadWatch()
        self.window_t0_ns = 0
        self.failure: str | None = None

    @property
    def connected_workers(self) -> int:
        return sum(len(self._configs[i]["worker_ids"]) for i in self._ready)

    # --- the driver interface -------------------------------------------------------------

    async def start(self) -> None:
        self.stream.emit("workload", "start", profile=self.workload.profile, concurrency=self.workload.concurrency,
                         rate_tps=self.workload.rate_tps, warehouses=self.workload.tpcc.warehouses,
                         processes=self.processes,
                         clerks_per_process=[len(c["worker_ids"]) for c in self._configs])
        for cfg, cmd_q in zip(self._configs, self._cmd_qs):
            p = self._ctx.Process(target=_child_main, args=(cfg, cmd_q, self._out_q),
                                  name=f"tpcc-worker-{cfg['index']}", daemon=True)
            p.start()
            self._procs.append(p)
        self._tasks = [asyncio.create_task(self._reader(), name="tpcc-coordinator-reader"),
                       asyncio.create_task(self._sampler(), name="tpcc-coordinator-sampler")]

    async def wait_until_ready(self, poll_s: float = 0.1) -> None:
        while len(self._ready) < self.processes and self.failure is None:
            await asyncio.sleep(poll_s)

    def begin_window(self) -> None:
        self._window_raws = []
        self._window_t0_ns = time.monotonic_ns()
        self.window_t0_ns = self._window_t0_ns

    def end_window(self) -> MeasuredWindow:
        if self._window_raws is None:
            raise RuntimeError("end_window() without begin_window()")
        t1 = time.monotonic_ns()
        raws, self._window_raws = self._window_raws, None
        inside = [r for r in raws if self._window_t0_ns <= r["t_end_ns"] <= t1]
        w = merge_window(inside, (t1 - self._window_t0_ns) / 1e9)
        return MeasuredWindow(**w)

    async def stop(self) -> None:
        for q in self._cmd_qs:
            q.put("stop")
        deadline = time.monotonic() + STOP_WAIT_S
        while len(self._stopped) < self.processes and time.monotonic() < deadline:
            if all(not p.is_alive() for p in self._procs):
                break
            await asyncio.sleep(POLL_S)
        self._stop.set()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._drain()
        self._flush_samples(force=True)
        for p in self._procs:
            p.join(timeout=5)
            if p.is_alive():
                p.terminate()
        self.stream.emit("workload", "stop", processes_stopped_cleanly=len(self._stopped))

    # --- internals ---------------------------------------------------------------------------------

    def _fail(self, why: str, **detail: Any) -> None:
        if self.failure is None:
            self.failure = why
            self.stream.emit("workload", "fatal", error=why, **detail)

    def _handle(self, msg: tuple[str, int, Any]) -> None:
        kind, idx, payload = msg
        if kind == "raw":
            payload["process"] = idx
            self._pending[payload["second"]].append(payload)
            self._first_seen.setdefault(payload["second"], time.monotonic())
            if self._window_raws is not None:
                self._window_raws.append(payload)
        elif kind == "ready":
            self._ready.add(idx)
        elif kind == "failure":
            self._fail(str(payload), worker_process=idx)
        elif kind == "stopped":
            self._stopped.add(idx)
            if payload:
                self._fail(str(payload), worker_process=idx)

    def _drain(self) -> None:
        while True:
            try:
                self._handle(self._out_q.get_nowait())
            except queue.Empty:
                return

    async def _reader(self) -> None:
        loop = asyncio.get_running_loop()
        while not self._stop.is_set():
            try:
                msg = await loop.run_in_executor(None, self._out_q.get, True, POLL_S)
            except queue.Empty:
                continue
            self._handle(msg)

    async def _sampler(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(POLL_S)
            for i, p in enumerate(self._procs):
                if p.exitcode is not None and i not in self._stopped:
                    self._fail(f"worker process {i} exited unexpectedly (exit code {p.exitcode})", worker_process=i)
            self._flush_samples(force=False)

    def _flush_samples(self, force: bool) -> None:
        """Write the sample of each clock second for which every process has reported, or which
        has waited SAMPLE_WAIT_S (then with the processes that did report: a process that is
        late or stuck shows as missing in driver_load, never as invented numbers)."""
        now = time.monotonic()
        for second in sorted(self._pending):
            parts = self._pending[second]
            complete = len({r["process"] for r in parts}) >= self.processes
            if not (complete or force or now - self._first_seen.get(second, now) >= SAMPLE_WAIT_S):
                break
            del self._pending[second]
            self._first_seen.pop(second, None)
            self.stream.emit("workload", "sample", **merge_raws(parts))
            cpu = [round(r["cpu_share"], 3) for r in parts]
            lag = [round(r["loop_lag_s"], 3) for r in parts]
            self.stream.emit("workload", "driver_load", cpu_share=cpu, loop_lag_s=lag,
                             processes_reporting=len({r["process"] for r in parts}), second=second)
            limited = self.load_watch.update(cpu, lag)
            if limited:
                self._fail(limited, driver_limited=True)
