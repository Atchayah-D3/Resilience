"""A scenario attached to the running common workload (specs/007, attach story).

The common workload service runs the clerks for the whole resilience run; a scenario never starts
or stops it. This class stands where the scenario's own workload driver stood
(contract.WorkloadDriverContract), so the nine phases, the analysis, the watcher, the dashboards
and the report work unchanged:

    found        env profile + node -> the service's pointer on this harness host -> its run folder
    checked      healthy (status, fresh samples, real work) and the same target and workload the
                 scenario declares; refused otherwise, naming what differs (never a silent mismatch)
    copied       the workload's own events (1-second samples, driver load, outages) are copied into
                 the run's events.jsonl with their original timestamps, from attach to validate
    windows      any window's figures from those samples (service/windows.py: summed latency
                 histograms, so p99 is the upper edge of a 2% bucket -- never below the exact value)
    marked       run_start, t0, t1, run_end in the workload's folder, so the workload judges its own
                 baseline outside the scenarios
    snapshots    the markers (and the TPC-C order count) read in ONE database snapshot at init and
                 again at validate. Compared with each other they show what happened DURING the
                 scenario: a marker in the init snapshot but not the validate one is a commit
                 destroyed by the fault, however old. Only this scenario's journal lines (between
                 the two) are copied into the run folder, so the cost is the same for the first
                 scenario and the last; the existing lost-commit and TPC-C checks run on them

The workload's pid is read for messages only: no decision depends on it, and nothing here ever
signals the workload (stopping it is the runner's job, through the service's stop request).
"""

from __future__ import annotations

import asyncio
import gzip
import hashlib
import json
import time
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from resilience_tests.execution.workload.driver import MeasuredWindow
from resilience_tests.execution.workload.markers import (
    ABORTED_JOURNAL,
    ACKED_JOURNAL,
    MARKER_JOURNAL,
    journal_paths,
)
from resilience_tests.execution.workload.service import client
from resilience_tests.execution.workload.service import endchecks
from resilience_tests.execution.workload.service import state as st
from resilience_tests.execution.workload.tpcc.sizing import Layout
from resilience_tests.execution.workload.service import windows as win
from resilience_tests.observability.event_stream import Event, EventStream

POLL_S = 0.25                 # how often the workload's new events are copied in
SAMPLE_FRESH_S = 5.0          # attach / before a fault: the newest sample is at most this old
SAMPLE_STALE_S = 15.0         # while attached (a fault may be running): no sample this long = stopped working
STATUS_STALE_S = 20.0         # status.json is rewritten every few seconds; older = the service is stuck
HEALTH_WINDOW_S = 30.0        # the recent stretch the "real work" check looks at
MIN_RATE_SHARE = 0.75         # ...at least this share of the workload's rate
MAX_ERROR_SHARE = 0.01        # ...and at most this share of failed transactions (business rollbacks are not errors)
JOURNAL_SETTLE_S = 10.0       # after the validate snapshot: acknowledgements of earlier commits reach the journal
# A transaction in flight at the init snapshot wrote its marker line before it and commits after it:
# this scenario's marker lines start this long before the init snapshot (longer than any transaction
# runs -- the clerks abandon one after 10 s).
SPAN_MARGIN_S = 60.0
# The per-payment check reads the history rows written from this long before the init snapshot (the
# database's own clock, as H_DATE is): a Payment acknowledged after the snapshot was sent at most
# 10 s before its answer (the clerks abandon a transaction after 10 s); the rest is room for a busy
# harness. Not from the workload's start: that history grows with every hour and every scenario.
PAYMENT_SINCE_MARGIN_S = 60.0
DESTROYED_LIST_MAX = 100     # destroyed markers named in the facts (the count is exact)
SETTLED_S = 10.0
# A transaction whose marker line is this close before the validate cut-off may still have been
# running at it (the clerks abandon a transaction after 10 s): its answer may come after the cut-off.
INFLIGHT_S = 15.0              # before the baseline: this many clean seconds in a row (no drops, reconnects, unknowns)
TAIL_BYTES = 512 * 1024       # the end of metrics-1s.jsonl read for the recent stretch
# what is copied from the workload's events.jsonl into the run's stream
COPIED = {("workload", "sample"), ("workload", "driver_load"), ("workload", "fatal")}
COPIED_SERVICE_PREFIX = "outage"
# the journal lines' timestamps (markers.py): marker (before COMMIT), acknowledgement, rollback
JOURNAL_TIME = {MARKER_JOURNAL: "t_pre", ACKED_JOURNAL: "t_ack", ABORTED_JOURNAL: "t_abort"}


class AttachRefused(RuntimeError):
    """No usable common workload for this scenario: none running, not healthy, or not the one the
    scenario declares. The run stops before anything is applied."""


def find_folder(profile: Any, node: Any) -> Path:
    """The running workload's folder, from the service's pointer on this harness host."""
    pointer = st.read_pointer(profile, node)
    if pointer is None or not pointer.get("folder"):
        raise AttachRefused(
            f"no common workload is running for {profile.name}/{node.name} on this harness host (no pointer "
            f"{st.pointer_path(profile, node)}); start it first: python -m "
            f"resilience_tests.execution.workload.service.cli start --env {profile.name} --config <workload file>")
    folder = Path(pointer["folder"])
    if not (folder / st.STATUS_FILE).exists():
        raise AttachRefused(f"the pointer names {folder}, but it holds no workload status (an old pointer?)")
    return folder


def _norm(value: Any) -> str:
    return str(value).strip().lower()


@dataclass
class CutOff:
    """The database at one moment while the workload keeps writing: its markers (streamed to a
    sorted file, never held in memory) and the TPC-C order count, read in one snapshot."""
    file: Path                              # the markers, sorted as text, one per line, gzip
    count: int
    current_orders: int | None
    t_before: float                         # acknowledgements before this: committed before the snapshot
    t_after: float                          # the snapshot was taken by here: marker lines before this
    sha256: str = ""
    lines: dict[str, int] = field(default_factory=dict)
    read_s: float | None = None             # how long reading the markers took (after the snapshot)
    # this scenario's transactions in flight at the cut-off, answered after it (validate only):
    # raw journal lines, kept so the check can count them as answered, never as "unknown"
    late_acked: list[bytes] = field(default_factory=list)
    late_aborted: list[bytes] = field(default_factory=list)

    def facts(self) -> dict[str, Any]:
        return {"t_before_wall": round(self.t_before, 3), "t_after_wall": round(self.t_after, 3),
                "markers_read_s": None if self.read_s is None else round(self.read_s, 3),
                "markers_file": self.file.name, "markers_in_snapshot": self.count, "sha256": self.sha256,
                "orders_in_snapshot": self.current_orders, "journal_lines_copied": self.lines,
                "answered_after_cut_off": {"acked": len(self.late_acked), "aborted": len(self.late_aborted)}}


class AttachedWorkload:
    """The running common workload, as the scenario sees it (contract.WorkloadDriverContract)."""

    def __init__(self, folder: Path, stream: EventStream | None) -> None:
        """`stream`: the run's event stream the workload's events are copied into (None: health
        checks only, nothing is copied -- the runner between scenarios)."""
        self.folder = folder
        self.stream = stream
        self.info: dict[str, Any] = st.read_info(folder) or {}
        self.failure: str | None = None
        self.window_t0_ns = 0
        self.copied = 0
        self.mark_problems: list[str] = []
        self.last_window: dict[str, Any] | None = None
        self._samples: list[dict[str, Any]] = []
        self._offset = 0
        self._window_from: float | None = None
        self._task: asyncio.Task[None] | None = None
        self._copy_from: int | None = None        # events.jsonl offset of the moment of attach

    # --- what it runs ------------------------------------------------------------------------

    @property
    def used(self) -> dict[str, Any]:
        """The workload settings the service actually runs (sizes and rate as numbers)."""
        return ((self.info.get("settings") or {}).get("workload_used")) or {}

    @property
    def rate_tps(self) -> float | None:
        rate = self.used.get("rate_tps")
        return float(rate) if rate else None

    @property
    def tpcc_layout(self) -> dict[str, Any] | None:
        """The TPC-C layout the clerks run (tpcc/sizing.Layout.as_dict): warehouses loaded, the
        warehouses they use (1 .. active_warehouses; None: every one) and the extra warehouses they
        never touch. From the settings the service runs (sizes as numbers), with the memory cache
        it was calculated from when the service recorded it. None when the workload is not TPC-C."""
        used = self.used
        tpcc = used.get("tpcc") or {}
        if used.get("profile") != "tpcc" or not tpcc.get("warehouses"):
            return None
        active = tpcc.get("active_warehouses")
        recorded = ((self.info.get("start_facts") or {}).get("data_step") or {}).get("tpcc_layout") or {}
        return Layout(warehouses=int(tpcc["warehouses"]), active_warehouses=int(active) if active else None,
                      cache_bytes=recorded.get("cache_bytes"),
                      by_cache=tuple(recorded.get("by_cache") or ())).as_dict()

    @property
    def pid(self) -> int | None:
        """For messages only (never a decision)."""
        return (self.status() or {}).get("pid") or self.info.get("pid")

    def describe(self) -> dict[str, Any]:
        used, tpcc = self.used, (self.used.get("tpcc") or {})
        layout = self.tpcc_layout or {}
        return {"folder": str(self.folder), "workload_run_id": self.info.get("run_id"), "id": self.info.get("id"),
                "profile": used.get("profile"), "clerks": used.get("concurrency"), "rate_tps": self.rate_tps,
                "warehouses": tpcc.get("warehouses"), "active_warehouses": layout.get("active_warehouses"),
                "extra_warehouses": layout.get("extra_warehouses"), "session": used.get("session") or {},
                "clerks_started_wall": self.info.get("clerks_started_wall"),
                "application_name": self.info.get("application_name")}

    def match_problems(self, wanted: Any, profile_name: str, node_name: str) -> list[str]:
        """Why this workload is not the one the scenario declares (empty: it is). Must match: the
        target, the profile, transaction markers, the session settings, no Elle history and -- when
        the scenario keeps its clerks on part of the warehouses (workload.tpcc.active_warehouses) --
        a workload whose clerks leave warehouses untouched. Sizes and the rate are taken from the
        running workload and reported, not compared (a number of active warehouses the scenario
        states is compared: it is the scenario's own rule, not a size calculated from the target)."""
        problems: list[str] = []
        if not self.info:
            return ["the workload has not written service.json yet (its clerks are not running)"]
        if self.info.get("env") != profile_name or self.info.get("node") != node_name:
            problems.append(f"it runs against {self.info.get('env')}/{self.info.get('node')}, "
                            f"not {profile_name}/{node_name}")
        used = self.used
        if used.get("profile") != wanted.profile:
            problems.append(f"profile {used.get('profile')!r}, the scenario needs {wanted.profile!r}")
        if wanted.transaction_markers and not used.get("transaction_markers"):
            problems.append("it writes no transaction markers, so no lost commit could be found")
        if wanted.history != "none":
            problems.append(f"the scenario needs history {wanted.history!r}; the common workload records none")
        have = {k: _norm(v) for k, v in (used.get("session") or {}).items()}
        need = {k: _norm(v) for k, v in (wanted.session or {}).items()}
        if have != need:
            problems.append(f"session settings {have or 'none'}, the scenario needs {need or 'none'}")
        if wanted.groups:
            problems.append("the scenario declares clerk groups; the common workload has none")
        problems += self._layout_problems(wanted)
        return problems

    def _layout_problems(self, wanted: Any) -> list[str]:
        """A scenario that keeps its clerks on part of the warehouses (its other warehouses are read
        by the fault alone, e.g. NL-N-I01's index reports) needs a workload that does the same."""
        need = getattr(getattr(wanted, "tpcc", None), "active_warehouses", None)
        if wanted.profile != "tpcc" or need is None or self.used.get("profile") != "tpcc":
            return []
        layout = self.tpcc_layout or {}
        active, loaded = layout.get("active_warehouses"), layout.get("warehouses")
        if not layout.get("extra_warehouses"):
            return [f"the scenario keeps its clerks on part of the warehouses (active_warehouses: {need}), but the "
                    f"common workload's clerks use every one of its {loaded} warehouses; start it with "
                    "workload.tpcc.active_warehouses set (workloads/oltp-common.yaml)"]
        if isinstance(need, int) and need != active:
            return [f"the scenario's clerks use {need} warehouses (active_warehouses), the common workload's "
                    f"use {active}"]
        return []

    # --- health --------------------------------------------------------------------------------

    def status(self) -> dict[str, Any] | None:
        return st.read_status(self.folder)

    def _gone(self) -> str:
        pid = self.pid
        return f" (its process {pid} is gone)" if pid and not st.pid_alive(pid) else ""

    def alive_problems(self, newest_sample_wall: float | None, stale_s: float, now: float | None = None) -> list[str]:
        """Still working at all: not ended or failed, status kept up to date, samples arriving."""
        now = time.time() if now is None else now
        s = self.status()
        if s is None:
            return ["no workload status"]
        problems = []
        if s.get("state") in st.FINAL_STATES or s.get("state") in (st.STOPPING, st.CHECKING):
            problems.append(f"the workload is {s.get('state')}" + (f": {s['failure']}" if s.get("failure") else ""))
        elif s.get("failure"):
            problems.append(f"the workload reports a failure: {s['failure']}")
        updated = s.get("updated_wall")
        if updated is not None and now - float(updated) > STATUS_STALE_S:
            problems.append(f"its status was last written {now - float(updated):.0f} s ago")
        if newest_sample_wall is None:
            problems.append("it has written no sample")
        elif now - newest_sample_wall > stale_s:
            problems.append(f"no sample for {now - newest_sample_wall:.0f} s")
        if problems:
            problems[-1] += self._gone()
        return problems

    def health_problems(self, now: float | None = None) -> list[str]:
        """Healthy enough to measure a baseline on (attach, between scenarios): running past its
        warm-up, alive, and doing its work over the last HEALTH_WINDOW_S."""
        now = time.time() if now is None else now
        recent = self.recent_samples()
        newest = float(recent[-1]["t_wall"]) if recent else None
        problems = self.alive_problems(newest, SAMPLE_FRESH_S, now)
        s = self.status() or {}
        if s.get("state") == st.WARMING_UP:
            problems.append("it is still warming up")
        elif s.get("state") not in (st.RUNNING, None) and not problems:
            problems.append(f"it is {s.get('state')!r}, not running")
        if s.get("outage_now"):
            problems.append(f"an outage is open: {s['outage_now']}")
        stretch = [x for x in recent if float(x["t_wall"]) > now - HEALTH_WINDOW_S]
        seconds = sum(float(x.get("interval_s") or 0.0) for x in stretch)
        if seconds < HEALTH_WINDOW_S / 2:
            problems.append(f"only {seconds:.0f} s of samples in the last {HEALTH_WINDOW_S:.0f} s")
        else:
            commits = sum(int(x.get("commits", 0)) for x in stretch)
            errors = sum(int(x.get("errors", 0)) for x in stretch)
            tps = commits / seconds
            rate = self.rate_tps or s.get("rate_tps")
            if rate and tps < MIN_RATE_SHARE * float(rate):
                problems.append(f"{tps:.1f} TPS over the last {HEALTH_WINDOW_S:.0f} s, under "
                                f"{MIN_RATE_SHARE:.0%} of its rate {float(rate):g}")
            if errors > MAX_ERROR_SHARE * max(1, commits + errors):
                problems.append(f"{errors} failed transactions against {commits} commits in the last "
                                f"{HEALTH_WINDOW_S:.0f} s")
        return problems

    def recent_samples(self) -> list[dict[str, Any]]:
        """The last samples of metrics-1s.jsonl (whole lines only), without reading the whole file."""
        path = self.folder / st.METRICS_1S_FILE
        try:
            with path.open("rb") as fh:
                fh.seek(0, 2)
                size = fh.tell()
                fh.seek(max(0, size - TAIL_BYTES))
                data = fh.read()
        except FileNotFoundError:
            return []
        lines = data.split(b"\n")
        if size > TAIL_BYTES:
            lines = lines[1:]                     # the first line read may be cut
        return [json.loads(x) for x in lines[:-1] if x.strip()]

    # --- WorkloadDriverContract ----------------------------------------------------------------

    def copy_from_now(self) -> None:
        """Remember where the workload's events stand at attach: start() copies from here, so what
        the scenario's init does to the workload (a restart for server settings) is in the run too."""
        self._copy_from = self._complete_size(self.folder / st.EVENTS_FILE)

    async def start(self) -> None:
        """Copy the workload's events into the run from the moment of attach (or from now)."""
        self._offset = self._copy_from if self._copy_from is not None else self._complete_size(self.folder / st.EVENTS_FILE)
        self._task = asyncio.create_task(self._follow(), name="attached-workload")

    async def wait_until_ready(self, poll_s: float = 0.1) -> None:
        """Until a sample of this run has been copied (bounded by the phase), or the workload failed."""
        while self.failure is None and not self._samples:
            await asyncio.sleep(poll_s)

    def settled(self, seconds: float = SETTLED_S) -> bool:
        """The last `seconds` of copied samples committed every second and had no dropped
        connection, failed connection attempt or unknown outcome."""
        if not self._samples:
            return False
        newest = float(self._samples[-1]["t_wall"])
        recent = [s for s in self._samples if float(s["t_wall"]) > newest - seconds]
        span = sum(float(s.get("interval_s") or 0.0) for s in recent)
        return span >= seconds - 0.5 and all(
            int(s.get("commits", 0)) > 0 and not s.get("drops") and not s.get("connect_failures")
            and not s.get("indeterminate") for s in recent)

    async def wait_until_settled(self, seconds: float = SETTLED_S, poll_s: float = 0.5) -> float:
        """Until settled() (or the workload failed); returns how long it took."""
        t = time.monotonic()
        while self.failure is None and not self.settled(seconds):
            await asyncio.sleep(poll_s)
        return round(time.monotonic() - t, 1)

    def begin_window(self) -> None:
        self.poll()
        self._window_from = time.time()
        self.window_t0_ns = time.monotonic_ns()

    def end_window(self) -> MeasuredWindow:
        """The window from begin_window to now, over the samples that ended inside it."""
        assert self._window_from is not None, "end_window before begin_window"
        self.poll()
        try:
            w = win.window(self._samples, self._window_from, time.time())
        except win.NoSamples as exc:
            # nothing ended inside: no commit is counted and p99 is unknown -- every gate then fails
            self.last_window = {"error": str(exc)}
            return MeasuredWindow(duration_s=0.0, commits=0, tps=0.0, p99_ms=None, journal_p99_ms=None,
                                  errors=0, indeterminate=0)
        self.last_window = {k: w[k] for k in ("samples", "max_ms", "percentiles", "first_sample_start_wall",
                                              "last_sample_end_wall")}
        return MeasuredWindow(**{k: w[k] for k in win.MEASURED_WINDOW_FIELDS})

    async def stop(self) -> None:
        """Stop copying. The workload keeps running: only the runner stops it, after the last scenario."""
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.poll()

    # --- copying -------------------------------------------------------------------------------

    @staticmethod
    def _complete_size(path: Path) -> int:
        try:
            data = path.read_bytes()
        except FileNotFoundError:
            return 0
        return data.rfind(b"\n") + 1

    def poll(self) -> int:
        """Copy the workload's whole new lines into the run's stream; returns how many were copied."""
        assert self.stream is not None, "poll() needs the run's stream"
        path = self.folder / st.EVENTS_FILE
        try:
            with path.open("rb") as fh:
                fh.seek(self._offset)
                data = fh.read()
        except FileNotFoundError:
            return 0
        end = data.rfind(b"\n") + 1
        n = 0
        for raw in data[:end].split(b"\n"):
            if not raw.strip():
                continue
            e = Event.from_json(raw.decode())
            if (e.source, e.kind) in COPIED or (e.source == "service" and e.kind.startswith(COPIED_SERVICE_PREFIX)):
                self.stream.record(e)
                n += 1
                if e.source == "workload" and e.kind == "sample":
                    self._samples.append({"t_wall": e.t_wall, **e.data})
        self._offset += end
        self.copied += n
        return n

    async def _follow(self) -> None:
        while True:
            try:
                self.poll()
            except Exception as exc:  # noqa: BLE001 -- a line we cannot read stops the run, never skipped
                self.failure = f"the workload's events could not be read: {type(exc).__name__}: {exc}"
                return
            newest = float(self._samples[-1]["t_wall"]) if self._samples else None
            if newest is not None:
                problems = self.alive_problems(newest, SAMPLE_STALE_S)
                if problems:
                    self.failure = "the common workload stopped working: " + "; ".join(problems)
                    return
            await asyncio.sleep(POLL_S)

    # --- scenario marks ------------------------------------------------------------------------

    def mark(self, kind: str, *, run_id: str, scenario: str, t_wall: float | None = None) -> None:
        """A scenario mark in the workload's folder. Context for the workload's own baseline: a
        mark that cannot be written is recorded, never a reason to stop the scenario."""
        try:
            client.mark(self.folder, kind, run_id=run_id, scenario=scenario, t_wall=t_wall)
        except Exception as exc:  # noqa: BLE001
            self.mark_problems.append(f"{kind}: {type(exc).__name__}: {exc}")

    # --- the snapshots and this scenario's journal lines --------------------------------------

    async def cut_off(self, adapter: Any, path: Path, settle_s: float = JOURNAL_SETTLE_S) -> CutOff:
        """The database at one moment while the workload keeps writing: its markers streamed to
        `path` and the TPC-C order count, in one snapshot. Every commit acknowledged before
        `t_before` is in it; every commit in it wrote its marker line before `t_after`. Then a
        pause, so the acknowledgements of commits from before the snapshot reach the journals."""
        t_before = time.time()
        snap = await adapter.marker_snapshot(path)
        t_read = time.time()
        # the snapshot's own moment (its first query), not the end of reading every marker: a
        # transaction that starts while millions of markers are read is after the cut-off
        t_after = float(snap.get("snapshot_wall") or t_read)
        if settle_s:
            await asyncio.sleep(settle_s)
        return CutOff(Path(path), int(snap["count"]), snap.get("current_orders"), t_before, t_after,
                      sha256=snap.get("sha256", ""), read_s=t_read - t_after)

    def copy_span(self, init: CutOff, val: CutOff, run_dir: Path) -> None:
        """This scenario's journal lines, by time, into the run folder under their own names
        (one line at a time, never the whole file in memory):

            marker   t_pre   in [init.t_before - SPAN_MARGIN_S, val.t_after)
            acked    t_ack   in [init.t_before, val.t_before)
            aborted  t_abort in [init.t_before - SPAN_MARGIN_S, val.t_after)

        Transactions in flight at the init snapshot that were already in it are taken out in the
        database by the check (SpanCheckAdapter). A whole line that is not JSON is kept, so the
        check counts it as torn and refuses to trust the journals; a last line still being written
        is left out."""
        lower = init.t_before - SPAN_MARGIN_S
        window = {MARKER_JOURNAL: (lower, val.t_after), ACKED_JOURNAL: (init.t_before, val.t_before),
                  ABORTED_JOURNAL: (lower, val.t_after)}
        # in flight at the cut-off: a marker line from the last INFLIGHT_S before it; its answer,
        # if it came after the cut-off, is kept apart (late_acked / late_aborted)
        in_flight: set[str] = set()
        val.late_acked, val.late_aborted = [], []
        for name, key in JOURNAL_TIME.items():
            low, high = window[name]
            total = 0
            for path in journal_paths(self.folder, name):
                if path.stat().st_size == 0:
                    continue                                    # a journal the workload never wrote to
                with path.open("rb") as src, (run_dir / path.name).open("wb") as dst:
                    for raw in src:
                        if not raw.endswith(b"\n"):
                            break                               # the line still being written
                        try:
                            rec = json.loads(raw)
                            t = float(rec.get(key) or 0.0)
                        except (json.JSONDecodeError, UnicodeDecodeError, ValueError, TypeError, AttributeError):
                            dst.write(raw)
                            total += 1
                            continue
                        if low <= t < high:
                            dst.write(raw)
                            total += 1
                            if name == MARKER_JOURNAL and t >= val.t_before - INFLIGHT_S:
                                in_flight.add(rec.get("uuid"))
                        elif t >= high and rec.get("uuid") in in_flight:
                            (val.late_acked if name == ACKED_JOURNAL else val.late_aborted).append(raw.rstrip(b"\n"))
            if journal_paths(self.folder, name):
                val.lines[name] = total

    # --- the TPC-C checks on the cut-off --------------------------------------------------------

    def tpcc_hooks(self, db_clock_at_init: datetime | None = None) -> Any:
        """The TPC-C profile's checks, set up from the workload's start: its layout and its payment
        start (the order count before is set from the init snapshot by the caller). With the
        database's clock read at the init snapshot, the payment check starts there instead
        (payment_check_since)."""
        from catalog.schema import Workload
        from resilience_tests.execution.workload.hooks import hooks_for
        hooks = hooks_for(Workload.model_validate(self.used))
        tpcc = self.info.get("tpcc") or {}
        if hasattr(hooks, "orders_at_prepare"):
            hooks.orders_at_prepare = tpcc.get("orders_at_start")
            since = tpcc.get("history_since")
            start = datetime.fromisoformat(since) if since and since != "None" else None
            hooks.history_since = payment_check_since(start, db_clock_at_init)
        return hooks


def payment_check_since(workload_start: datetime | None, db_clock_at_init: datetime | None) -> datetime | None:
    """Where the per-payment check starts reading the history: PAYMENT_SINCE_MARGIN_S before the
    init snapshot by the database's clock, never before the workload's start. The scenario's own
    acknowledged Payments are the ones checked (copy_span), so reading less history can only find
    fewer rows -- a missing row is still missing, and an older row with the same values can no
    longer stand in for it. Without the clock (or when the two times cannot be compared), the
    workload's start, as before."""
    if workload_start is None or db_clock_at_init is None:
        return workload_start
    if (workload_start.tzinfo is None) != (db_clock_at_init.tzinfo is None):
        return workload_start
    return max(workload_start, db_clock_at_init - timedelta(seconds=PAYMENT_SINCE_MARGIN_S))


def _gz_lines(path: Path) -> Iterator[str]:
    with gzip.open(path, "rt") as fh:
        for line in fh:
            u = line.strip()
            if u:
                yield u


def compare_snapshots(init: Path, val: Path, new_file: Path, keep: int = DESTROYED_LIST_MAX) -> dict[str, Any]:
    """Both snapshots are sorted the same way, so one pass over each (constant memory) gives:
    the markers that appeared during the scenario -- written to `new_file` as markers_db.txt.gz,
    what the journal check compares this scenario's lines with -- and the markers that were in
    the database before the scenario and are gone now (destroyed by the fault, however old)."""
    new, destroyed, listed = 0, 0, []
    sha = hashlib.sha256()
    a_it, b_it = _gz_lines(init), _gz_lines(val)
    a, b = next(a_it, None), next(b_it, None)
    with gzip.open(new_file, "wb") as out:
        while a is not None or b is not None:
            if b is not None and (a is None or b < a):          # only at validate: new
                line = (b + "\n").encode()
                out.write(line)
                sha.update(line)
                new += 1
                b = next(b_it, None)
            elif a is not None and (b is None or a < b):        # only at init: destroyed
                destroyed += 1
                if len(listed) < keep:
                    listed.append(a)
                a = next(a_it, None)
            else:                                               # in both
                a, b = next(a_it, None), next(b_it, None)
    return {"new": new, "new_file": {"file": new_file.name, "count": new, "sha256": sha.hexdigest()},
            "destroyed": destroyed, "destroyed_listed": listed}


class SpanCheckAdapter:
    """The adapter, as the end checks (service/endchecks.run_end_checks) see it for one scenario:
      - the TPC-C order count is the validate snapshot's (the workload keeps creating orders);
      - before comparing, the journal lines of transactions already in the init snapshot (in flight
        when it was taken) are taken out, in the database, so they are neither "lost" nor counted
        as this scenario's orders -- whether they survived is the snapshots' comparison.
    Everything else is the real adapter (the consistency rules: each one SQL statement, so one
    consistent view of the data, which holds at every committed state)."""

    def __init__(self, adapter: Any, init: CutOff, val: CutOff) -> None:
        self._adapter, self._init, self._val = adapter, init, val
        self.excluded: dict[str, int] = {}
        self.in_flight_at_validate: dict[str, int] = {}

    def __getattr__(self, name: str) -> Any:
        return getattr(self._adapter, name)

    async def tpcc_data_status(self) -> dict[str, Any]:
        status = dict(await self._adapter.tpcc_data_status())
        status["current_orders"] = self._val.current_orders
        return status

    async def journal_check(self) -> Any:
        make = getattr(self._adapter, "journal_check", None)
        inner = await make() if make is not None else endchecks.InMemoryJournalCheck(self._adapter)
        return _SpanSession(inner, self._init.file, self.excluded, self._val, self.in_flight_at_validate)


class _SpanSession:
    """A journal-check session that, just before comparing:
      - removes the lines of uuids that were already in the init snapshot;
      - settles the transactions in flight at the validate cut-off that were answered just after
        it: one that is not in the snapshot committed (or rolled back) after the cut-off and is
        left out; one that is in it committed before the cut-off, and its acknowledgement (or
        rollback line) is counted. Neither is an "unknown outcome"; one never answered still is."""

    def __init__(self, inner: Any, init_file: Path, excluded: dict[str, int], val: CutOff | None = None,
                 in_flight: dict[str, int] | None = None) -> None:
        self._inner, self._init_file, self._excluded = inner, init_file, excluded
        self._val = val
        self._in_flight = in_flight if in_flight is not None else {}

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    async def prepare(self) -> None:
        conn = getattr(self._inner, "conn", None)
        if conn is not None:                                   # in the database (PgJournalCheck)
            await conn.execute("CREATE TEMP TABLE jc_init (uuid uuid NOT NULL)")
            batch: list[tuple[str]] = []
            for u in _gz_lines(self._init_file):
                batch.append((u,))
                if len(batch) >= endchecks.BATCH_ROWS:
                    await conn.copy_records_to_table("jc_init", records=batch, columns=["uuid"])
                    batch = []
            if batch:
                await conn.copy_records_to_table("jc_init", records=batch, columns=["uuid"])
            await conn.execute("CREATE INDEX ON jc_init (uuid); ANALYZE jc_init")
            for kind in ("marker", "acked", "aborted"):
                status = await conn.execute(f"DELETE FROM jc_{kind} j USING jc_init i WHERE j.uuid = i.uuid")
                self._excluded[kind] = int(status.split()[-1])
            await conn.execute("DROP TABLE jc_init")
            await self._settle_in_flight_db(conn)
        else:                                                  # in memory (engines without journal_check)
            gone = {"marker": 0, "acked": 0, "aborted": 0}
            for u in _gz_lines(self._init_file):
                gone["marker"] += self._inner.written.pop(u, None) is not None
                gone["acked"] += self._inner.acked.pop(u, None) is not None
                gone["aborted"] += self._inner.aborted.pop(u, None) is not None
            self._excluded.update(gone)
            self._settle_in_flight_memory()
        await self._inner.prepare()

    def _late_rows(self, kind: str) -> list[tuple[Any, ...]]:
        lines = (self._val.late_acked if kind == "acked" else self._val.late_aborted) if self._val else []
        out = []
        for n, raw in enumerate(lines, 1):
            try:
                out.append(endchecks._row(kind, 10_000, n, json.loads(raw), f"answered after the cut-off, line {n}"))
            except (ValueError, endchecks.JournalLineError):
                continue
        return out

    async def _settle_in_flight_db(self, conn: Any) -> None:
        acked, aborted = self._late_rows("acked"), self._late_rows("aborted")
        if not acked and not aborted:
            self._in_flight.update({"answered_after_cut_off_not_in_snapshot": 0, "answered_after_cut_off_in_snapshot": 0})
            return
        await conn.execute("CREATE TEMP TABLE jc_late_acked (LIKE jc_acked); CREATE TEMP TABLE jc_late_aborted (LIKE jc_aborted)")
        if acked:
            await conn.copy_records_to_table("jc_late_acked", records=acked)      # rows in the table's own column order
        if aborted:
            await conn.copy_records_to_table("jc_late_aborted", records=aborted)
        in_db = "EXISTS (SELECT 1 FROM jc_db d WHERE d.uuid = {a}.uuid)"
        gone = await conn.execute(f"""DELETE FROM jc_marker m WHERE NOT {in_db.format(a='m')} AND (
            EXISTS (SELECT 1 FROM jc_late_acked l WHERE l.uuid = m.uuid)
            OR EXISTS (SELECT 1 FROM jc_late_aborted l WHERE l.uuid = m.uuid))""")
        kept_a = await conn.execute(f"INSERT INTO jc_acked SELECT * FROM jc_late_acked l WHERE {in_db.format(a='l')}")
        kept_b = await conn.execute(f"INSERT INTO jc_aborted SELECT * FROM jc_late_aborted l WHERE {in_db.format(a='l')}")
        await conn.execute("DROP TABLE jc_late_acked; DROP TABLE jc_late_aborted")
        self._in_flight.update({"answered_after_cut_off_not_in_snapshot": int(gone.split()[-1]),
                                "answered_after_cut_off_in_snapshot": int(kept_a.split()[-1]) + int(kept_b.split()[-1])})

    def _settle_in_flight_memory(self) -> None:
        db = set(self._inner._db_rows or []) if getattr(self._inner, "_db_rows", None) is not None else None
        if db is None:
            return
        gone = kept = 0
        for kind, rows in (("acked", self._late_rows("acked")), ("aborted", self._late_rows("aborted"))):
            for r in rows:
                u = r[2]
                if u not in db:
                    gone += self._inner.written.pop(u, None) is not None
                elif kind == "acked":
                    keys = ("t_ack", "w", "d", "o_id", "ol_cnt", "c_w", "c_d", "c", "amt")
                    self._inner.acked[u] = {"uuid": u, **{k: v for k, v in zip(keys, r[3:]) if v is not None}}
                    kept += 1
                else:
                    self._inner.aborted[u] = {"uuid": u, "t_abort": r[3]}
                    kept += 1
        self._in_flight.update({"answered_after_cut_off_not_in_snapshot": gone, "answered_after_cut_off_in_snapshot": kept})



INIT_MARKERS_FILE = "markers_db_at_init.txt.gz"
VALIDATE_MARKERS_FILE = "markers_db_at_validate.txt.gz"


def wait_for_health(workload: AttachedWorkload, timeout_s: float, poll_s: float = 2.0) -> list[str]:
    """Until the workload is healthy (empty list) or `timeout_s` passed (the last problems)."""
    deadline = time.monotonic() + timeout_s
    while True:
        problems = workload.health_problems()
        if not problems or time.monotonic() >= deadline:
            return problems
        time.sleep(poll_s)
