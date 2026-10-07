"""pgbench workload driver and process supervisor (Arch §6.1, contracts/workload-driver.md)."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import time
from typing import TYPE_CHECKING, Any

from catalog.schema import Workload
from resilience_tests.adapters.base import BaseDatabaseAdapter, Capability, DbEndpoint
from resilience_tests.execution.workload.driver import (
    MeasuredWindow,
    UnsupportedWorkload,
    p99,
    percentile,
)
from resilience_tests.execution.workload.history_writer import HistoryWriter
from resilience_tests.execution.workload.markers import MarkerJournals
from resilience_tests.execution.workload.pgbench_output import (
    PgbenchAbortLine,
    PgbenchParseError,
    PgbenchSummary,
    is_abort_line,
    parse_abort_line,
    parse_progress_line,
    parse_summary,
)
from resilience_tests.observability.event_stream import EventStream

if TYPE_CHECKING:
    from resilience_tests.execution.workload.record_channel import RecordChannel


def extract_major_version(ver_str: str | int) -> str:
    """Extract major version integer string from version text (e.g. '17.11.1.0' -> '17')."""
    if isinstance(ver_str, int):
        return str(ver_str)
    m = re.search(r"\b(\d+)(?:\.\d+)*\b", str(ver_str))
    if not m:
        raise ValueError(f"cannot parse major version from {ver_str!r}")
    return m.group(1)


def probe_pgbench(path: str, server_major: str | int) -> str:
    """Probe pgbench binary availability and verify its major version matches the target server.

    Raises UnsupportedWorkload naming the path and/or versions if unsupported (FR-004).
    """
    resolved = shutil.which(path) if not os.path.isabs(path) and not path.startswith(".") else path
    if not resolved or not os.path.exists(resolved) or not os.access(resolved, os.X_OK):
        raise UnsupportedWorkload(
            f"pgbench binary {path!r} is not found or not executable (check profile workload.pgbench_bin)"
        )

    try:
        res = subprocess.run([resolved, "--version"], capture_output=True, text=True, check=True)
    except Exception as exc:
        raise UnsupportedWorkload(f"failed to run pgbench --version on {path!r}: {exc}") from exc

    output = (res.stdout or res.stderr).strip()
    pg_major = extract_major_version(output)
    srv_major = extract_major_version(server_major)

    if pg_major != srv_major:
        raise UnsupportedWorkload(
            f"pgbench major version {pg_major!r} does not match target server major version {srv_major!r} "
            f"(pgbench output: {output!r}) (FR-004)"
        )
    return output


@dataclass
class _ClientState:
    client_id: int
    current_launch: int
    process: asyncio.subprocess.Process | None = None
    started_mono_ns: int = 0
    ended_mono_ns: int = 0
    argv: list[str] = field(default_factory=list)
    stdout_lines: list[str] = field(default_factory=list)
    stderr_lines: list[str] = field(default_factory=list)
    ended_as: str = "running"
    abort_message: str | None = None
    summary: PgbenchSummary | None = None


@dataclass
class _WindowAccumulator:
    commits: int = 0
    latencies_ms: list[float] = field(default_factory=list)
    errors: int = 0
    indeterminate: int = 0
    drops: int = 0
    reconnects: int = 0
    connect_failures: int = 0


class PgbenchWorkloadDriver:
    """Workload driver supervising one pgbench process per client."""

    generator: str = "pgbench"

    def __init__(
        self,
        adapter: BaseDatabaseAdapter,
        workload: Workload,
        journals: MarkerJournals,
        stream: EventStream,
        pgbench_bin: str = "pgbench",
        pgbench_version: str = "",
        channel: RecordChannel | None = None,
        history: HistoryWriter | None = None,
    ) -> None:
        self.adapter = adapter
        self.workload = workload
        self.journals = journals
        self.stream = stream
        self.pgbench_bin = pgbench_bin
        self.pgbench_version = pgbench_version
        self.channel = channel
        self.history = history

        self.run_id = self.journals.run_dir.name
        self.pgbench_dir = self.journals.run_dir / "pgbench"
        self.app_name = f"resilience-pgbench-{self.run_id}"

        self.connected_workers: int = 0
        self.failure: str | None = None
        self.window_t0_ns: int = 0
        self._measure_t0: float = 0.0

        self._launch_counter: int = 0
        self._clients: dict[int, _ClientState] = {}
        self._client_tasks: dict[int, asyncio.Task[None]] = {}
        self._stop = asyncio.Event()
        self._sampler_task: asyncio.Task[None] | None = None

        # Sample and window tracking
        self._sample_acc = _WindowAccumulator()
        self._measure_acc: _WindowAccumulator | None = None
        self._pending_pres: dict[str, float] = {}

        # Scheduling lag & overhead tracking
        self.scheduling_lags_us: list[float] = []
        self.recorded_summaries: list[PgbenchSummary] = []

    def _next_launch(self) -> int:
        n = self._launch_counter
        self._launch_counter += 1
        return n

    async def wait_until_ready(self, poll_s: float = 0.05) -> None:
        while self.connected_workers < self.workload.concurrency and self.failure is None:
            await asyncio.sleep(poll_s)

    def begin_window(self) -> None:
        self._measure_acc = _WindowAccumulator()
        self._measure_t0 = time.monotonic()
        self.window_t0_ns = time.monotonic_ns()

    def end_window(self) -> MeasuredWindow:
        if self._measure_acc is None:
            raise RuntimeError("end_window() called without begin_window()")
        duration = max(0.001, time.monotonic() - self._measure_t0)
        acc = self._measure_acc
        self._measure_acc = None

        lats = acc.latencies_ms
        return MeasuredWindow(
            duration_s=duration,
            commits=acc.commits,
            tps=acc.commits / duration if duration > 0 else 0.0,
            p99_ms=p99(lats) if lats else None,
            journal_p99_ms=p99(lats) if lats else None,
            errors=acc.errors,
            indeterminate=acc.indeterminate,
            drops=acc.drops,
            reconnects=acc.reconnects,
            connect_failures=acc.connect_failures,
            p50_ms=percentile(lats, 0.50) if lats else None,
            p95_ms=percentile(lats, 0.95) if lats else None,
        )

    def _derive_shape(self) -> str:
        if self.workload.profile not in ("oltp_write_heavy", "mixed") or not self.workload.transaction_markers:
            raise UnsupportedWorkload(
                f"workload profile {self.workload.profile!r} is not built yet"
            )
        churn = self.workload.profile == "mixed"
        list_append = self.workload.history == "list_append"
        if churn and list_append:
            raise UnsupportedWorkload("list-append history and the churn workload are not combined")
        if list_append:
            return "list_append"
        if churn:
            return "churn"
        return "marker"

    async def start(self) -> None:
        self.pgbench_dir.mkdir(parents=True, exist_ok=True)
        shape = self._derive_shape()

        self.stream.emit(
            "workload",
            "start",
            generator=self.generator,
            profile=self.workload.profile,
            concurrency=self.workload.concurrency,
            rate_tps=self.workload.rate_tps,
        )

        if self.channel is not None:
            await self.channel.open()

        # Launch one process per client
        concurrency = self.workload.concurrency
        rate_tps = self.workload.rate_tps
        # interval_us = 1e6 * concurrency / rate
        interval_us = (1e6 * concurrency) / rate_tps if rate_tps > 0 else 1000000.0

        for client_id in range(concurrency):
            task = asyncio.create_task(
                self._run_client_lifecycle(client_id, shape, interval_us),
                name=f"pgbench-client-{client_id}",
            )
            self._client_tasks[client_id] = task

        self._sampler_task = asyncio.create_task(self._sampler_loop(), name="pgbench-sampler")

    async def _run_client_lifecycle(self, client_id: int, shape: str, interval_us: float) -> None:
        """Manage process execution and automatic relaunch for a single client."""
        while not self._stop.is_set():
            launch_id = self._next_launch()
            spec = self.adapter.pgbench_launch(shape, launch=launch_id, client=client_id)

            script_file = self.pgbench_dir / f"script-c{client_id}-l{launch_id}.sql"
            script_file.write_text(spec.script)

            state = _ClientState(
                client_id=client_id,
                current_launch=launch_id,
                started_mono_ns=time.monotonic_ns(),
            )
            self._clients[client_id] = state

            # Assemble argv
            resolved_bin = (
                shutil.which(self.pgbench_bin)
                if not os.path.isabs(self.pgbench_bin) and not self.pgbench_bin.startswith(".")
                else self.pgbench_bin
            )
            argv = [
                resolved_bin,
                "-c", "1",
                "-j", "1",
                "-T", "86400",
                "-P", "1",
                "--report-per-command",
                "--failures-detailed",
                "--max-tries", "1",
                "-f", str(script_file),
                "-D", f"interval_us={int(interval_us)}",
            ]
            for k, v in spec.variables.items():
                argv.extend(["-D", f"{k}={v}"])

            # Connection parameters
            conn = spec.connection
            argv.extend(["-h", conn.host, "-p", str(conn.port), "-U", conn.user, "-d", conn.dbname])

            state.argv = argv

            self.stream.emit("workload", "launch", launch=launch_id, client=client_id, command=argv)

            env = os.environ.copy()
            env["PGAPPNAME"] = self.app_name

            # Spawn process in its own process group
            proc = await asyncio.create_subprocess_exec(
                *argv,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
                start_new_session=True,
            )
            state.process = proc

            if self.connected_workers < self.workload.concurrency:
                self.connected_workers += 1

            # Monitor process stdout and stderr concurrently
            stdout_task = asyncio.create_task(self._read_stdout(proc, state))
            stderr_task = asyncio.create_task(self._read_stderr(proc, state))

            await proc.wait()
            state.ended_mono_ns = time.monotonic_ns()
            await asyncio.gather(stdout_task, stderr_task, return_exceptions=True)

            # Classify exit
            aborted = False
            abort_msg = None
            for line in state.stderr_lines:
                if is_abort_line(line):
                    aborted = True
                    try:
                        abort_info = parse_abort_line(line)
                        abort_msg = abort_info.message
                    except PgbenchParseError:
                        abort_msg = line
                    break

            if self._stop.is_set():
                state.ended_as = "stopped"
            elif aborted:
                state.ended_as = "client_aborted"
                state.abort_message = abort_msg
                self._record_drop()
            elif proc.returncode == 0:
                state.ended_as = "stopped"
            else:
                # Unexpected failure (FR-014)
                state.ended_as = "failed"
                err_text = "".join(state.stderr_lines).strip()
                self.failure = f"client {client_id} exited unexpectedly with code {proc.returncode}: {err_text}"
                self.stream.emit("workload", "fatal", error=self.failure)
                self._write_launch_evidence(state)
                self.stream.emit(
                    "workload",
                    "launch_end",
                    launch=launch_id,
                    client=client_id,
                    ended_as=state.ended_as,
                    abort_message=state.abort_message,
                )
                break

            # Parse final summary if present
            full_stdout = "".join(state.stdout_lines)
            try:
                state.summary = parse_summary(full_stdout)
                self.recorded_summaries.append(state.summary)
            except PgbenchParseError:
                if not self._stop.is_set() and not aborted:
                    self.failure = f"client {client_id} produced unparseable summary"
                    self.stream.emit("workload", "fatal", error=self.failure)

            self._write_launch_evidence(state)
            self.stream.emit(
                "workload",
                "launch_end",
                launch=launch_id,
                client=client_id,
                ended_as=state.ended_as,
                abort_message=state.abort_message,
            )

            if state.ended_as == "client_aborted" and not self._stop.is_set():
                # Await target recovery before relaunching
                reconnected = await self._wait_for_database_recovery()
                if not reconnected or self._stop.is_set():
                    break
                self._record_reconnect()
            else:
                break

    async def _read_stdout(self, proc: asyncio.subprocess.Process, state: _ClientState) -> None:
        assert proc.stdout is not None
        while True:
            line_bytes = await proc.stdout.readline()
            if not line_bytes:
                break
            line = line_bytes.decode("utf-8", errors="replace")
            state.stdout_lines.append(line)
            if line.startswith("progress:"):
                try:
                    prog = parse_progress_line(line)
                    self._record_progress(prog)
                except PgbenchParseError:
                    pass

    async def _read_stderr(self, proc: asyncio.subprocess.Process, state: _ClientState) -> None:
        assert proc.stderr is not None
        while True:
            line_bytes = await proc.stderr.readline()
            if not line_bytes:
                break
            line = line_bytes.decode("utf-8", errors="replace")
            state.stderr_lines.append(line)

    def _record_drop(self) -> None:
        self._sample_acc.drops += 1
        self._sample_acc.indeterminate += 1
        if self._measure_acc is not None:
            self._measure_acc.drops += 1
            self._measure_acc.indeterminate += 1

    def _record_reconnect(self) -> None:
        self._sample_acc.reconnects += 1
        if self._measure_acc is not None:
            self._measure_acc.reconnects += 1

    def _record_connect_failure(self) -> None:
        self._sample_acc.connect_failures += 1
        if self._measure_acc is not None:
            self._measure_acc.connect_failures += 1

    def _record_progress(self, prog: Any) -> None:
        # Fallback when RecordChannel is not processing individual records
        if self.channel is None:
            commits = int(round(prog.tps * prog.interval_s))
            self._sample_acc.commits += commits
            self._sample_acc.latencies_ms.append(prog.lat_ms)
            if self._measure_acc is not None:
                self._measure_acc.commits += commits
                self._measure_acc.latencies_ms.append(prog.lat_ms)

    def _process_channel_records(self) -> None:
        if self.channel is None:
            return
        records = self.channel.drain()
        for rec in records:
            if rec.kind == "pre":
                self._pending_pres[rec.identity] = rec.t_wall
                if self.history is not None and isinstance(rec.value, dict):
                    proc = rec.value.get("process", 0)
                    invoked = rec.value.get("invoked")
                    if invoked:
                        self.history.record("invoke", proc, invoked)
            elif rec.kind == "ack":
                t_pre = self._pending_pres.pop(rec.identity, None)
                lat_ms = (rec.t_wall - t_pre) * 1000.0 if t_pre is not None else 0.0
                self._sample_acc.commits += 1
                self._sample_acc.latencies_ms.append(lat_ms)
                if self._measure_acc is not None:
                    self._measure_acc.commits += 1
                    self._measure_acc.latencies_ms.append(lat_ms)
                if self.history is not None and isinstance(rec.value, dict):
                    proc = rec.value.get("process", 0)
                    status = rec.value.get("status", "ok")
                    if status == "ok":
                        self.history.record("ok", proc, rec.value.get("executed", []))
                    elif status == "info":
                        self.history.record("info", proc, rec.value.get("invoked", []), rec.value.get("error", "indeterminate"))
                    elif status == "fail":
                        self.history.record("fail", proc, rec.value.get("invoked", []), rec.value.get("error", "aborted"))


    async def _wait_for_database_recovery(self, timeout_s: float = 60.0, retry_interval_s: float = 0.2) -> bool:
        t0 = time.monotonic()
        while not self._stop.is_set() and time.monotonic() - t0 < timeout_s:
            try:
                # Probe connection through adapter
                session = await self.adapter.session(timeout_s=1.0)
                try:
                    await session.ping()
                    return True
                finally:
                    await session.close()
            except Exception:
                self._record_connect_failure()
                await asyncio.sleep(retry_interval_s)
        return False

    def _write_launch_evidence(self, state: _ClientState) -> None:
        launch_file = self.pgbench_dir / f"launch-{state.current_launch}.txt"
        content = [
            f"client: {state.client_id}",
            f"launch: {state.current_launch}",
            f"ended_as: {state.ended_as}",
            f"command: {' '.join(state.argv)}",
            "",
            "--- stderr ---",
            "".join(state.stderr_lines),
            "--- stdout ---",
            "".join(state.stdout_lines),
        ]
        launch_file.write_text("\n".join(content))

    async def _sampler_loop(self) -> None:
        last = time.monotonic()
        while not self._stop.is_set():
            await asyncio.sleep(1.0)
            now = time.monotonic()
            elapsed, last = now - last, now

            self._process_channel_records()

            acc = self._sample_acc
            self._sample_acc = _WindowAccumulator()

            lats = acc.latencies_ms
            p50 = percentile(lats, 0.50) if lats else None
            p95 = percentile(lats, 0.95) if lats else None
            p99_val = p99(lats) if lats else None

            self.stream.emit(
                "workload",
                "sample",
                interval_s=elapsed,
                commits=acc.commits,
                tps=acc.commits / elapsed if elapsed > 0 else 0.0,
                p99_ms=p99_val,
                journal_p99_ms=p99_val,
                p50_ms=p50,
                p95_ms=p95,
                errors=acc.errors,
                indeterminate=acc.indeterminate,
                drops=acc.drops,
                reconnects=acc.reconnects,
                connect_failures=acc.connect_failures,
            )

    async def stop(self) -> None:
        self._stop.set()
        if self._sampler_task and not self._sampler_task.done():
            self._sampler_task.cancel()

        # Terminate all active pgbench process groups
        for client_id, state in self._clients.items():
            if state.process and state.process.returncode is None:
                try:
                    pgid = os.getpgid(state.process.pid)
                    os.killpg(pgid, signal.SIGTERM)
                except ProcessLookupError:
                    pass

        # Await task completions
        if self._client_tasks:
            await asyncio.gather(*self._client_tasks.values(), return_exceptions=True)

        # Force kill any lingering processes
        for client_id, state in self._clients.items():
            if state.process and state.process.returncode is None:
                try:
                    pgid = os.getpgid(state.process.pid)
                    os.killpg(pgid, signal.SIGKILL)
                except ProcessLookupError:
                    pass

        if self.channel is not None:
            await self.channel.close()

        # Confirm no target sessions remain (Arch §15, Constitution VI)
        active_sessions = await self.adapter.sessions_with_application_name(self.app_name)
        if active_sessions is not None and active_sessions > 0:
            raise RuntimeError(
                f"cleanup check failed: {active_sessions} active pgbench session(s) matching {self.app_name} remain"
            )

        self.stream.emit("workload", "stop")

    def recording_overhead_pct(self) -> float | None:
        """Calculate percentage of transaction latency contributed by record steps (R8, FR-011)."""
        if not self.recorded_summaries:
            return None
        total_cmd_lat = 0.0
        record_cmd_lat = 0.0
        for s in self.recorded_summaries:
            for cmd in s.command_latencies:
                total_cmd_lat += cmd.latency_ms
                if "seq" in cmd.command or "record" in cmd.command:
                    record_cmd_lat += cmd.latency_ms
        if total_cmd_lat <= 0:
            return None
        return (record_cmd_lat / total_cmd_lat) * 100.0
