"""The run's link to the environment's metrics stack (Arch §9, L4 Observability).

One observer per run, chosen by the env profile (`observability:`), the same for every scenario:

  start   check Prometheus and Grafana answer, then subscribe to the run's event stream
  events  the run's milestones (run start, each phase, warm-up, T0, hold, T1, ...) become Grafana
          annotations -- sent in the background, so no phase ever waits on Grafana
  finish  the whole run as one annotated region; the run window's metrics copied from Prometheus
          into the run folder (metrics.json, Arch §10.4 "metric snapshot"); the dashboard link
          the run report shows

Context only, never a pass/fail source (Arch §7.1): nothing here reads a measure or the verdict,
and a stack that does not answer is disclosed in the report -- the run is judged exactly as
without it. A profile with no `observability:` section gets NoObserver.
"""

from __future__ import annotations

import asyncio
import json
import math
import time
from abc import ABC, abstractmethod
from pathlib import Path
from typing import TYPE_CHECKING, Any

from resilience_tests.observability.event_stream import Event, EventStream
from resilience_tests.observability.grafana import GrafanaClient
from resilience_tests.observability.metrics import SNAPSHOT_QUERIES, render
from resilience_tests.observability.prometheus import PrometheusClient

if TYPE_CHECKING:
    from resilience_tests.control.profile import EnvProfile, Node, Observability

METRICS_FILE = "metrics.json"
CHECK_TIMEOUT_S = 5.0          # each of the start-up checks
DRAIN_TIMEOUT_S = 15.0         # pending annotations sent at the end of the run
SNAPSHOT_TIMEOUT_S = 90.0      # the whole metric snapshot
SNAPSHOT_MARGIN_S = 30.0       # metrics just before and after the run, for context
MAX_POINTS = 10_000            # Prometheus returns at most 11,000 points per series

# (source, kind) of the events marked on the dashboards, and their label
MARKED: dict[tuple[str, str], str] = {
    ("orchestrator", "run_start"): "run start",
    ("orchestrator", "phase_start"): "phase",
    ("orchestrator", "warmup"): "warm-up",
    ("injector", "t0"): "T0: fault injected",
    ("injector", "not_landed"): "fault did not land",
    ("orchestrator", "fault_milestone"): "fault milestone",     # RunContext.milestone: any fault's own moments
    ("orchestrator", "hold_start"): "hold start",
    ("orchestrator", "hold_end"): "hold end",
    ("orchestrator", "t1"): "T1: recovery measured from here",
    ("orchestrator", "cycle_recovered"): "cycle recovered",
    ("orchestrator", "cycle_not_recovered"): "cycle NOT recovered",
    ("orchestrator", "cleanup_incomplete"): "cleanup incomplete",
}


def label(event: Event) -> str:
    text = MARKED[(event.source, event.kind)]
    data = event.data
    if event.kind == "phase_start":
        return f"phase: {data.get('phase')}"
    if event.kind == "run_start":
        return f"run start: {data.get('scenario')} ({data.get('run_id')})"
    if event.kind == "warmup":
        return f"warm-up {data.get('seconds')} s (not measured)"
    if event.kind == "t1" and data.get("action"):
        return f"T1: {data.get('action')}"
    if event.kind == "not_landed" and data.get("reason"):
        return f"fault did not land: {str(data.get('reason'))[:120]}"
    if event.kind == "fault_milestone" and data.get("text"):
        return str(data["text"])[:200]
    return text


# An event may record when the moment it reports happened (t0_mono_ns, t1_mono_ns, at_mono_ns),
# which can be earlier than when it was emitted -- T1 is announced after the release that follows
# it. The marker goes where the moment happened, on the same harness clock as the event.
WHEN_KEYS = ("at_mono_ns", "t0_mono_ns", "t1_mono_ns")


def marker_time(event: Event) -> float:
    for key in WHEN_KEYS:
        when = event.data.get(key)
        if isinstance(when, int) and when > 0:
            return event.t_wall - (event.t_mono_ns - when) / 1e9
    return event.t_wall


class RunObserver(ABC):
    @abstractmethod
    async def start(self, stream: EventStream) -> list[str]:
        """Attach to the run; returns disclosures (what could not be connected). Never raises."""

    @abstractmethod
    async def finish(self, run_dir: Path, start_wall_s: float, end_wall_s: float, status: str) -> dict[str, Any]:
        """Close the run: region, metric snapshot, link. Returns the facts to record. Never raises."""

    async def close(self) -> None:
        """Interrupted run: stop sending, without a snapshot."""


class NoObserver(RunObserver):
    """No metrics stack in this environment (profile without `observability:`)."""

    async def start(self, stream: EventStream) -> list[str]:
        return []

    async def finish(self, run_dir: Path, start_wall_s: float, end_wall_s: float, status: str) -> dict[str, Any]:
        return {}


class StackObserver(RunObserver):
    """Prometheus + Grafana, as the env profile names them."""

    def __init__(self, config: Observability, *, env: str, node: Node, run_id: str, scenario: str,
                 prometheus: PrometheusClient | None = None, grafana: GrafanaClient | None = None) -> None:
        self.config, self.env, self.node, self.run_id, self.scenario = config, env, node, run_id, scenario
        self.prometheus = prometheus or PrometheusClient(config.prometheus_url)
        self._grafana = grafana
        self.problems: list[str] = []
        self.sent = 0
        self.failed = 0
        self.last_error: str | None = None
        self._queue: asyncio.Queue[Event] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._worker: asyncio.Task[None] | None = None
        self._prometheus_ok = False

    @property
    def tags(self) -> list[str]:
        return ["resilience", self.run_id, self.scenario, self.env, self.node.name]

    def _token(self) -> str:
        return Path(self.config.grafana_token_file).expanduser().read_text().strip()

    async def start(self, stream: EventStream) -> list[str]:
        try:
            if self._grafana is None:
                self._grafana = GrafanaClient(self.config.grafana_url, self._token())
            await asyncio.wait_for(self._grafana.health(), CHECK_TIMEOUT_S)
        except Exception as exc:  # noqa: BLE001 -- disclosed; the run goes on without markers
            self._grafana = None
            self.problems.append(f"Grafana ({self.config.grafana_url}) not used: {type(exc).__name__}: {exc}")
        try:
            await asyncio.wait_for(self.prometheus.ready(), CHECK_TIMEOUT_S)
            self._prometheus_ok = True
        except Exception as exc:  # noqa: BLE001
            self.problems.append(f"Prometheus ({self.config.prometheus_url}) not used: {type(exc).__name__}: {exc}")
        if self._grafana is not None:
            self._loop, self._queue = asyncio.get_running_loop(), asyncio.Queue()
            self._worker = asyncio.create_task(self._send(), name="grafana-annotations")
            stream.subscribe(self._on_event)
        return [self._disclosure()] if self.problems else []

    def _disclosure(self) -> str:
        return ("Observability (context only, never a pass/fail source): " + "; ".join(self.problems)
                + ". The run is judged exactly as without it.")

    def _on_event(self, event: Event) -> None:
        # called from EventStream.emit, possibly off the event loop's thread: hand over only
        if (event.source, event.kind) in MARKED and self._loop is not None and self._queue is not None:
            self._loop.call_soon_threadsafe(self._queue.put_nowait, event)

    async def _send(self) -> None:
        assert self._queue is not None and self._grafana is not None
        while True:
            event = await self._queue.get()
            try:
                await self._grafana.annotate(time_s=marker_time(event), text=label(event),
                                             tags=[*self.tags, event.kind])
                self.sent += 1
            except Exception as exc:  # noqa: BLE001 -- counted and disclosed, never raised
                self.failed += 1
                self.last_error = f"{type(exc).__name__}: {exc}"
            finally:
                self._queue.task_done()

    async def _stop_worker(self, drain: bool) -> None:
        if self._worker is None:
            return
        if drain and self._queue is not None:
            try:
                await asyncio.wait_for(self._queue.join(), DRAIN_TIMEOUT_S)
            except TimeoutError:
                self.problems.append(f"{self._queue.qsize()} run marker(s) could not be sent to Grafana in time")
        self._worker.cancel()
        await asyncio.gather(self._worker, return_exceptions=True)
        self._worker = None

    async def close(self) -> None:
        await self._stop_worker(drain=False)

    async def finish(self, run_dir: Path, start_wall_s: float, end_wall_s: float, status: str) -> dict[str, Any]:
        facts: dict[str, Any] = {"prometheus_url": self.config.prometheus_url,
                                 "grafana_url": self.config.grafana_url}
        try:
            await self._stop_worker(drain=True)
            if self._grafana is not None:
                try:
                    await self._grafana.annotate(time_s=start_wall_s, end_s=end_wall_s,
                                                 text=f"{self.scenario}: {status} ({self.run_id})",
                                                 tags=[*self.tags, "run"])
                    self.sent += 1
                except Exception as exc:  # noqa: BLE001
                    self.failed += 1
                    self.last_error = f"{type(exc).__name__}: {exc}"
                facts["grafana_link"] = self._grafana.dashboard_link(
                    self.config.grafana_dashboard_uid, start_wall_s - SNAPSHOT_MARGIN_S, end_wall_s + SNAPSHOT_MARGIN_S,
                    {"env": self.env, "node": self.node.name, "db": self.node.db.dbname})
                facts["annotations"] = {"sent": self.sent, "failed": self.failed}
                if self.failed:
                    self.problems.append(f"{self.failed} run marker(s) were not stored in Grafana ({self.last_error})")
            if self._prometheus_ok and self.config.save_metrics_snapshot:
                facts.update(await asyncio.wait_for(self._snapshot(run_dir, start_wall_s, end_wall_s),
                                                    SNAPSHOT_TIMEOUT_S))
        except Exception as exc:  # noqa: BLE001 -- never at the cost of the run's own evidence
            self.problems.append(f"metrics not recorded: {type(exc).__name__}: {exc}")
        if self.problems:
            facts["problems"] = list(self.problems)
            facts["disclosure"] = self._disclosure()
        return facts

    async def _snapshot(self, run_dir: Path, start_wall_s: float, end_wall_s: float) -> dict[str, Any]:
        # the margin after the run is capped at now: Prometheus has nothing later yet
        start, end = start_wall_s - SNAPSHOT_MARGIN_S, min(end_wall_s + SNAPSHOT_MARGIN_S, time.time())
        step = max(self.config.scrape_interval_s, math.ceil((end - start) / MAX_POINTS))
        series: dict[str, Any] = {}
        missing: list[str] = []
        for name, template in SNAPSHOT_QUERIES:
            query = render(template, env=self.env, node=self.node.name, db=self.node.db.dbname)
            try:
                result = await self.prometheus.query_range(query, start, end, step)
                series[name] = {"query": query, "result": result}
                if not result:
                    missing.append(name)
            except Exception as exc:  # noqa: BLE001 -- one failed query does not lose the others
                series[name] = {"query": query, "error": f"{type(exc).__name__}: {exc}"}
                missing.append(name)
        document = {"note": "context only (Arch §7.1): copied from Prometheus, never used in the verdict",
                    "prometheus_url": self.config.prometheus_url, "env": self.env, "node": self.node.name,
                    "start_wall_s": start, "end_wall_s": end, "step_s": step, "series": series}
        (run_dir / METRICS_FILE).write_text(json.dumps(document, separators=(",", ":")) + "\n")
        return {"metrics_file": METRICS_FILE, "metrics_series": len(series), "metrics_empty": missing}


def observer_for(profile: EnvProfile, node: Node, *, run_id: str, scenario: str) -> RunObserver:
    if profile.observability is None:
        return NoObserver()
    return StackObserver(profile.observability, env=profile.name, node=node, run_id=run_id, scenario=scenario)
