"""pgbench workload driver (Arch §6.1, contracts/workload-driver.md; research R1 option B).

The harness's load is offered by pgbench, started and supervised here on the driver host:
one pgbench process per client (`-c 1`), so a client a fault disconnects is relaunched on its
own without touching the healthy ones (research R4).

The evidence is the built-in driver's, recorded by the same code: every transaction's marker
is journalled and flushed on the driver host before its COMMIT can be sent, and its
acknowledgement after the server confirmed it (Arch §6.2). pgbench cannot write files, so its
script calls the harness for both records -- see `shell_records.ShellRecordService`, which
also paces the load (the built-in driver's shared rate limiter), classifies outcomes and
writes the Elle history.

Behaviour shared with the built-in driver, so the orchestrator cannot tell them apart:
- a `sample` event every second (commits, tps, p50/p95/p99 over every attempt, errors,
  indeterminate, drops, reconnects, connect_failures), on the harness clock;
- a transaction in flight for more than TXN_TIMEOUT_S is abandoned as unknown (its client is
  killed and relaunched, as the built-in worker discards its session);
- a client that loses its connection is relaunched once the database accepts one again,
  retrying every RECONNECT_PAUSE_S for as long as the run lasts;
- `failure` is set, and the load stops, when the evidence cannot be kept: a record step that
  failed, a journal that could not be written, a pgbench that exited on its own.
"""

from __future__ import annotations

import asyncio
import collections
import os
import re
import shutil
import signal
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from catalog.schema import Workload
from resilience_tests.adapters.base import BaseDatabaseAdapter, Capability
from resilience_tests.execution.workload.driver import (
    CONNECT_TIMEOUT_S,
    RECONNECT_PAUSE_S,
    SAMPLE_INTERVAL_S,
    TXN_TIMEOUT_S,
    MeasuredWindow,
    UnsupportedWorkload,
    _Window,
    p99,
    percentile,
)
from resilience_tests.execution.workload.history_writer import HistoryWriter
from resilience_tests.execution.workload.markers import MarkerJournals
from resilience_tests.execution.workload.pgbench_output import (
    PgbenchParseError,
    is_abort_line,
    is_auth_failure,
    is_connect_failure,
    is_statement_error,
    parse_abort_line,
)
from resilience_tests.execution.workload.shell_records import (
    READ_CHUNK_CHARS,
    READ_CHUNKS,
    ShellRecordService,
    ack_steps,
    pre_steps,
)
from resilience_tests.observability.event_stream import EventStream

SCRIPT_FILE = "transaction.sql"
# A run is bounded by the orchestrator's phase timeouts; pgbench's own clock must never end it.
PGBENCH_DURATION_S = 7 * 24 * 3600
# Launches ended by a statement ERROR before the run's first commit: the script cannot work at
# all (a broken statement, a missing privilege), so the run stops and names the error.
STATEMENT_ERRORS_BEFORE_FIRST_COMMIT = 3
STOP_GRACE_S = 2.0


def extract_major_version(ver_str: str | int) -> str:
    """Extract major version integer string from version text (e.g. '17.11.1.0' -> '17')."""
    if isinstance(ver_str, int):
        return str(ver_str)
    m = re.search(r"\b(\d+)(?:\.\d+)*\b", str(ver_str))
    if not m:
        raise ValueError(f"cannot parse major version from {ver_str!r}")
    return m.group(1)


def _resolve(path: str) -> str | None:
    return shutil.which(path) if not os.path.isabs(path) and not path.startswith(".") else path


def probe_pgbench(path: str, server_major: str | int) -> str:
    """Probe pgbench binary availability and verify its major version matches the target server.

    Raises UnsupportedWorkload naming the path and/or versions if unsupported (FR-004).
    """
    resolved = _resolve(path)
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


def derive_shape(workload: Workload, adapter: BaseDatabaseAdapter, history: HistoryWriter | None) -> str:
    """The transaction shape, with exactly the built-in driver's refusals."""
    if workload.profile not in ("oltp_write_heavy", "mixed") or not workload.transaction_markers:
        raise UnsupportedWorkload(
            f"workload profile {workload.profile!r} (markers={workload.transaction_markers}) is not built yet"
        )
    if not adapter.has(Capability.TRANSACTIONAL_MARKERS):
        raise UnsupportedWorkload(
            f"engine {adapter.engine!r} cannot record transaction markers, so RPO cannot be measured")
    churn = workload.profile == "mixed"
    list_append = workload.history == "list_append"
    if churn and not adapter.has(Capability.WORKLOAD_CHURN):
        raise UnsupportedWorkload(
            f"engine {adapter.engine!r} cannot run the churn workload, so cumulative bloat cannot be measured")
    if list_append and not adapter.has(Capability.LIST_APPEND_HISTORY):
        raise UnsupportedWorkload(
            f"engine {adapter.engine!r} cannot run list-append transactions, so no history can be checked")
    if churn and list_append:
        raise UnsupportedWorkload("list-append history and the churn workload are not combined")
    if list_append and history is None:
        raise UnsupportedWorkload("list-append workload needs a history writer")
    if churn and adapter.churn_key_space < 1:
        raise UnsupportedWorkload(f"engine {adapter.engine!r} declares the churn capability but no key space")
    return "list_append" if list_append else "churn" if churn else "marker"


@dataclass
class _Launch:
    launch: int
    client: int
    argv: list[str]
    process: asyncio.subprocess.Process | None = None
    stderr: list[str] = field(default_factory=list)
    timed_out: bool = False
    ended_as: str = "running"


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
        history: HistoryWriter | None = None,
    ) -> None:
        self.shape = derive_shape(workload, adapter, history)
        self.adapter = adapter
        self.workload = workload
        self.journals = journals
        self.stream = stream
        self.history = history
        self.pgbench_bin = pgbench_bin
        self.pgbench_version = pgbench_version
        self.run_id = journals.run_dir.name
        self.pgbench_dir = journals.run_dir / "pgbench"
        self.app_name = ""
        self.failure: str | None = None
        self.connected_workers = 0
        self.window_t0_ns = 0
        self._window = _Window()
        self._measure: _Window | None = None
        self._measure_t0 = 0.0
        self._launch_counter = 0
        self._launches: dict[int, _Launch] = {}
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._gate = asyncio.Lock()
        self._gate_ok_at = float("-inf")
        self._setpriv = shutil.which("setpriv")
        self.statement_errors: collections.Counter[str] = collections.Counter()
        self.records = ShellRecordService(
            self.pgbench_dir, journals, self.shape, workload.rate_tps, workload.concurrency,
            marker_uuid=lambda seq: adapter.pgbench_marker_uuid(seq, self.run_id), count=self._count,
            on_connected=self._on_connected, on_fatal=self._fail, history=history,
            churn_keys=adapter.churn_key_space if self.shape == "churn" else 0,
        )

    # --- the interface the orchestrator uses (same as WorkloadDriver) -------------------

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
        return MeasuredWindow(
            duration_s=duration, commits=w.commits, tps=w.commits / duration if duration > 0 else 0.0,
            p99_ms=p99(w.latencies_ms or []), journal_p99_ms=p99(w.journal_ms or []),
            errors=w.errors, indeterminate=w.indeterminate, drops=w.drops,
            reconnects=w.reconnects, connect_failures=w.connect_failures,
            p50_ms=percentile(w.latencies_ms or [], 0.50), p95_ms=percentile(w.latencies_ms or [], 0.95),
        )

    async def start(self) -> None:
        self.pgbench_dir.mkdir(parents=True, exist_ok=True)
        if re.search(r"\s", str(self.pgbench_dir)):
            raise UnsupportedWorkload(f"the run directory {self.pgbench_dir} contains whitespace, which "
                                      "pgbench's shell commands cannot carry")
        spec = self.adapter.pgbench_launch(self.shape, self.run_id, read_chunks=READ_CHUNKS, chunk_chars=READ_CHUNK_CHARS)
        self.app_name = f"{spec.application_name}-{self.run_id}"
        self._spec = spec
        script = pre_steps(self.shape, self.records.token_base) + spec.script + ack_steps(self.shape)
        (self.pgbench_dir / SCRIPT_FILE).write_text(script)
        await self.records.open()
        self.stream.emit("workload", "start", generator=self.generator, profile=self.workload.profile,
                         concurrency=self.workload.concurrency, rate_tps=self.workload.rate_tps,
                         pgbench_version=self.pgbench_version, shape=self.shape)
        self._tasks = [asyncio.create_task(self._client(i), name=f"pgbench-client-{i}")
                       for i in range(self.workload.concurrency)]
        self._tasks.append(asyncio.create_task(self._sampler(), name="pgbench-sampler"))
        self._tasks.append(asyncio.create_task(self._watchdog(), name="pgbench-watchdog"))

    async def stop(self) -> None:
        self._stop.set()
        for st in list(self._launches.values()):
            self._signal(st, signal.SIGTERM)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + STOP_GRACE_S
        while any(st.process is not None and st.process.returncode is None for st in self._launches.values()):
            if loop.time() >= deadline:
                for st in self._launches.values():
                    self._signal(st, signal.SIGKILL)
                break
            await asyncio.sleep(0.05)
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.records.close()
        # No pgbench may outlive the run (FR-017), here or on the target (Arch §15).
        alive = [st.process.pid for st in self._launches.values()
                 if st.process is not None and st.process.returncode is None]
        if alive:
            raise RuntimeError(f"cleanup check failed: pgbench process(es) {alive} still running")
        if self.app_name:
            remaining = await self.adapter.sessions_with_application_name(self.app_name)
            if remaining:
                raise RuntimeError(f"cleanup check failed: {remaining} pgbench session(s) "
                                   f"({self.app_name}) still open on the target")
        self.stream.emit("workload", "stop", launches=self._launch_counter)

    def report_facts(self) -> dict[str, Any]:
        """What the report states about this generator (FR-011, FR-018, SC-006)."""
        jm = self.records.journal_ms
        return {
            "pgbench_launches": self._launch_counter,
            "pgbench_exit_with_harness": self._setpriv is not None,
            "pgbench_statement_errors": dict(self.statement_errors),
            "pgbench_record_journal_p50_ms": percentile(jm, 0.50),
            "pgbench_record_journal_p99_ms": p99(jm),
            "pgbench_record_mechanism": (
                "pgbench shell record steps to the harness journal service (research R1, option B): "
                "marker flushed before COMMIT, acknowledgement after; each step is one /bin/sh on the "
                "driver host, and the acknowledgement step's run time is inside the measured latency"),
        }

    # --- clients -----------------------------------------------------------------------

    def _next_launch(self) -> int:
        self._launch_counter += 1
        return self._launch_counter

    async def _client(self, client: int) -> None:
        try:
            while not self._stop.is_set() and self.failure is None:
                relaunch = await self._run_launch(client)
                if not relaunch or self._stop.is_set() or self.failure is not None:
                    return
                await self._database_accepting()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 -- recorded as fatal; the orchestrator aborts
            self._fail(f"client {client}: {type(exc).__name__}: {exc}")

    async def _run_launch(self, client: int) -> bool:
        """One pgbench process. Returns True if the client must be relaunched."""
        launch = self._next_launch()
        reply = self.records.new_launch(launch, client)
        conn = self._spec.connection
        argv = [_resolve(self.pgbench_bin) or self.pgbench_bin, "-n", "-c", "1", "-j", "1",
                "-T", str(PGBENCH_DURATION_S), "--max-tries=1", "-f", SCRIPT_FILE,
                "-D", f"launch={launch}", "-D", f"client={client}", "-D", f"reply={reply}",
                "-h", conn.host, "-p", str(conn.port), "-U", conn.user, conn.dbname]
        if self._setpriv:
            # a harness that dies takes its pgbench processes with it (FR-017)
            argv = [self._setpriv, "--pdeathsig", "KILL", "--"] + argv
        st = _Launch(launch, client, argv)
        self._launches[launch] = st
        env = dict(os.environ, PGAPPNAME=self.app_name, PGCONNECT_TIMEOUT=str(int(CONNECT_TIMEOUT_S)))
        self.stream.emit("workload", "launch", launch=launch, client=client)
        st.process = await asyncio.create_subprocess_exec(
            *argv, cwd=self.pgbench_dir, env=env, stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE, start_new_session=True)
        assert st.process.stderr is not None
        async for raw in st.process.stderr:
            st.stderr.append(raw.decode("utf-8", errors="replace"))
        await st.process.wait()
        relaunch = self._classify_exit(st)
        self._write_evidence(st)
        self.stream.emit("workload", "launch_end", launch=launch, client=client, ended_as=st.ended_as,
                         returncode=st.process.returncode)
        return relaunch

    def _classify_exit(self, st: _Launch) -> bool:
        if self._stop.is_set() or self.failure is not None:
            st.ended_as = "stopped"
            self.records.launch_ended(st.launch, "stopped")
            return False
        if st.timed_out:
            st.ended_as = "transaction_timeout"
            self.records.launch_ended(st.launch, "aborted")
            return True
        text = "".join(st.stderr)
        aborts = []
        for line in st.stderr:
            if is_abort_line(line):
                try:
                    aborts.append(parse_abort_line(line))
                except PgbenchParseError:
                    pass
        # only the server's words: a record step's shell error ("cannot open q: Permission
        # denied") is the instrument failing, never the database refusing us
        server_text = "".join(line for line in st.stderr
                              if "FATAL" in line or "ERROR" in line or "connection to server" in line)
        if is_auth_failure(server_text):
            # credentials or privileges: no relaunch can fix it
            st.ended_as = "refused"
            self.records.launch_ended(st.launch, "aborted")
            self._fail(f"pgbench was refused by the server for its credentials or privileges "
                       f"(check ~/.pgpass, its 0600 mode, the role and pg_hba.conf): {server_text.strip()[-300:]}")
            return False
        if aborts and all(a.kind == "sql" for a in aborts):
            st.ended_as = "client_aborted"
            errors = [a for a in aborts if is_statement_error(a)]
            if errors:
                # The server answered ERROR on a live connection. Counted as the built-in driver
                # counts an unclassified server error (unknown outcome + the session discarded),
                # so both generators give the same verdicts; reported apart from connection loss.
                st.ended_as = "statement_error"
                self.statement_errors[errors[0].message.split("\n")[0][:200]] += 1
            self.records.launch_ended(st.launch, "aborted")
            if errors and not self.records.journal_ms and \
                    sum(self.statement_errors.values()) >= STATEMENT_ERRORS_BEFORE_FIRST_COMMIT:
                self._fail(f"pgbench transactions fail with a statement error before any has committed, "
                           f"so the load cannot run: {errors[0].message}")
                return False
            return True
        if not aborts and st.process is not None and st.process.returncode == 1 and is_connect_failure(text):
            st.ended_as = "not_connected"
            self.records.launch_ended(st.launch, "not_connected")
            self._count(connect_failures=1)
            return True
        # a record step failed, or pgbench ended on its own: the instrument is broken
        st.ended_as = "failed"
        self.records.launch_ended(st.launch, "aborted")
        rc = st.process.returncode if st.process is not None else None
        self._fail(f"pgbench client {st.client} (launch {st.launch}) ended unexpectedly "
                   f"(exit {rc}): {text.strip()[-500:] or 'no output'}")
        return False

    async def _database_accepting(self) -> None:
        """Wait until the database accepts a connection. One probe serves every waiting client;
        a failed attempt counts as one connection failure, as a worker's attempt does."""
        async with self._gate:
            if time.monotonic() - self._gate_ok_at < 2 * RECONNECT_PAUSE_S:
                return
            while not self._stop.is_set() and self.failure is None:
                try:
                    session = await self.adapter.session(timeout_s=CONNECT_TIMEOUT_S)
                except Exception:  # noqa: BLE001 -- engine-specific; the adapter owns the taxonomy
                    self._count(connect_failures=1)
                    await asyncio.sleep(RECONNECT_PAUSE_S)
                    continue
                try:
                    if await session.ping():
                        self._gate_ok_at = time.monotonic()
                        return
                finally:
                    try:
                        await session.close()
                    except Exception:  # noqa: BLE001 -- best effort on a probe session
                        pass
                await asyncio.sleep(RECONNECT_PAUSE_S)

    async def _watchdog(self) -> None:
        """Abandon a transaction in flight longer than TXN_TIMEOUT_S, as the built-in driver
        does: its outcome is unknown, and the client starts over on a new connection."""
        while not self._stop.is_set():
            await asyncio.sleep(0.5)
            now = time.monotonic()
            for st in list(self._launches.values()):
                if st.process is None or st.process.returncode is not None or st.timed_out:
                    continue
                since = self.records.in_flight_since(st.launch)
                if since is not None and now - since > TXN_TIMEOUT_S:
                    st.timed_out = True
                    self._signal(st, signal.SIGKILL)

    def _signal(self, st: _Launch, sig: int) -> None:
        if st.process is None or st.process.returncode is not None:
            return
        try:
            os.killpg(st.process.pid, sig)   # its own session: pgbench and its shell steps
        except ProcessLookupError:
            pass

    def _write_evidence(self, st: _Launch) -> None:
        (self.pgbench_dir / f"launch-{st.launch}.txt").write_text(
            f"client: {st.client}\nlaunch: {st.launch}\nended_as: {st.ended_as}\n"
            f"returncode: {st.process.returncode if st.process else None}\n"
            f"command: {' '.join(st.argv)}\n\n--- stderr ---\n{''.join(st.stderr)}")

    # --- accounting (identical to WorkloadDriver) --------------------------------------

    def _on_connected(self, client: int, reconnect: bool) -> None:
        if reconnect:
            self._count(reconnects=1)
        else:
            self.connected_workers += 1

    def _fail(self, message: str) -> None:
        if self.failure is None:
            self.failure = message
            self.stream.emit("workload", "fatal", error=message)
        self._stop.set()
        for st in list(self._launches.values()):
            self._signal(st, signal.SIGTERM)

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
            assert w.latencies_ms is not None and w.journal_ms is not None
            if latency_ms is not None:
                w.latencies_ms.append(latency_ms)
            if journal_ms is not None:
                w.journal_ms.append(journal_ms)

    async def _sampler(self) -> None:
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
                p50_ms=percentile(w.latencies_ms or [], 0.50), p95_ms=percentile(w.latencies_ms or [], 0.95),
                errors=w.errors, indeterminate=w.indeterminate, drops=w.drops,
                reconnects=w.reconnects, connect_failures=w.connect_failures,
            )
