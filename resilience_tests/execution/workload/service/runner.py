"""The common workload's run, from its start to its end checks (specs/007-common-oltp-workload).

    start      the target checks (load.check_target, which also refuses if another common
               workload runs); refused while a scenario run holds the target; the workload lock
               on the target; then, ONCE for the whole run: the marker table emptied, the TPC-C
               data loaded or reset only if needed (TpccProfile.reset_data), the data checked and
               the rate fixed (TpccProfile.prepare). Nothing is reloaded or emptied after this.
    run        the TPC-C engine (the same clerks, journals and samples as in a scenario run) for
               `duration_s`, the first `warmup_s` not measured. Written while it runs:
                 metrics-1s.jsonl     every 1-second sample, with its time and part (warmup/measured)
                 intervals-<n>s.jsonl one line per interval: the engine's exact window figures
                                      plus errors by kind, business rollbacks, commits per type
                 events.jsonl         start, warm-up end, outages, failures, stop
                 status.json          the live state, rewritten every few seconds
               No scenario starts, stops or changes it. It ends at `duration_s`, on the stop
               command (stop.request), or when the workload itself fails:
                 a journal line that cannot be saved, a worker process that dies, the harness
                 being the limit (the engine's own rules), or the workload lock taken over.
               A database that is down does NOT end it: the clerks keep reconnecting; a long
               stretch without commits is reported (outage_long) and the run goes on.
    stop       the clerks finish their running transactions, the journals are closed
    end checks every acknowledged commit is in the database (no lost commit, no phantom), the
               TPC-C checks (partial commits, rolled-back transactions kept, consistency rules),
               then the baseline rules (baseline.py), summary.json (with the settings used) and
               summary.txt.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections.abc import Awaitable, Callable
from dataclasses import asdict
from pathlib import Path
from typing import Any

from resilience_tests.adapters.base import adapter_for
from resilience_tests.control.profile import EnvProfile, Node
from resilience_tests.execution.workload.hooks import hooks_for
from resilience_tests.execution.workload.markers import MarkerJournals, diff_from_journals, write_db_markers
from resilience_tests.execution.workload.profiles.base import WorkloadProfile
from resilience_tests.execution.workload.service import baseline as baseline_mod
from resilience_tests.execution.workload.service import state as st
from resilience_tests.execution.workload.service.config import ServiceConfig
from resilience_tests.execution.workload.service.metrics import (
    IntervalSums,
    OutageTracker,
    Second,
    period_summary,
)
from resilience_tests.execution.workload.tpcc.load import Refused, check_target
from resilience_tests.observability.event_stream import Event, EventStream

READY_TIMEOUT_S = 120.0       # every clerk connected, or the start fails
LOOP_S = 1.0
MEMORY_EVERY_S = 30.0
LOST_LIST_MAX = 100           # lost markers named in the summary (all are in the evidence files)


class ServiceRunner:
    def __init__(self, env: EnvProfile, node: Node, config: ServiceConfig, folder: Path, *,
                 adapter: Any = None, lock: Any = None,
                 target_check: Callable[..., Awaitable[str]] | None = None,
                 scenario_check: Callable[..., Awaitable[str | None]] | None = None,
                 clock: Callable[[], float] = time.monotonic) -> None:
        self.env, self.node, self.cfg, self.folder = env, node, config, folder
        self.run_id = folder.name
        self.adapter = adapter if adapter is not None else adapter_for(env.database.engine, node)
        self.lock = lock if lock is not None else st.workload_lock(node, run_id=self.run_id, workload_id=config.id,
                                                                   profile=env.name)
        self.target_check = target_check or check_target
        self.scenario_check = scenario_check or st.scenario_holding_target
        self.clock = clock
        # the workload's profile hooks, as a scenario run uses them (TPC-C: TpccProfile; the custom
        # workloads: nothing to load, nothing extra to check)
        self.profile: Any = hooks_for(config.workload)
        self.state = st.STARTING
        self.step = "checks"
        self.failure: str | None = None
        self.stop_by: str | None = None
        self._stop_flag = False
        self.facts: dict[str, Any] = {}
        self.disclosures: list[str] = []
        self.driver: Any = None
        self.journals: MarkerJournals | None = None
        self.stream: EventStream | None = None
        self._lock_held = False
        self._files: list[Any] = []
        # timing (monotonic) and the run's numbers
        self.started_wall = time.time()
        self.t0: float | None = None             # clerks started
        self.clerks_started_wall = 0.0
        self.measure_start_s: float | None = None
        self.ended_s: float | None = None
        self.seconds: list[Second] = []
        self.memory: list[tuple[float, int]] = []
        self.error_kinds: dict[str, int] = {}
        self.max_cpu_share = 0.0
        self.driver_limited = False
        self.interval = IntervalSums()
        self.outages = OutageTracker(config.workload.rate_tps, config.no_commit_alert_s)
        self.last_interval: dict[str, Any] | None = None
        self.alerts: list[str] = []

    # --- control from outside ----------------------------------------------------------------

    def request_stop(self, by: str = "signal") -> None:
        if not self._stop_flag:
            self._stop_flag, self.stop_by = True, by

    # --- the run ------------------------------------------------------------------------------

    async def run(self) -> dict[str, Any]:
        self.folder.mkdir(parents=True, exist_ok=True)
        self.stream = EventStream(self.folder / st.EVENTS_FILE, keep_in_memory=False)
        self._m1 = self._open(st.METRICS_1S_FILE)
        self._mn = self._open(st.metrics_interval_file(self.cfg.metrics_interval_s))
        self.stream.emit("service", "start", id=self.cfg.id, pid=os.getpid(), folder=str(self.folder),
                         config=self.cfg.as_dict())
        self._write_status()
        final = st.FAILED
        try:
            await self._start_up()
            final = await self._run_loop()
        except asyncio.CancelledError:
            self.failure = self.failure or "the service was cancelled"
        except Refused as exc:                  # the target checks (a SystemExit for the CLI tools)
            self.failure = self.failure or str(exc)
        except Exception as exc:  # noqa: BLE001 -- every failure is recorded; the summary says why
            self.failure = self.failure or f"{type(exc).__name__}: {exc}"
        if self.failure:
            final = st.FAILED
        try:
            await self._stop_clerks()
            # only when clerks ran: a start that failed earlier sent no transaction to check
            end = await self._end_checks() if self.t0 is not None else {}
            summary = self._summary(final, end)
        finally:
            await self._release()
        self.state = final
        self._write_status()
        self.stream.emit("service", "end", state=final, failure=self.failure, baseline_valid=summary["baseline_valid"])
        self.stream.close()
        for fh in self._files:
            fh.close()
        return summary

    def _open(self, name: str) -> Any:
        fh = (self.folder / name).open("a", buffering=1)
        self._files.append(fh)
        return fh

    async def _start_up(self) -> None:
        assert self.stream is not None
        self.facts["hostname"] = await self.target_check(self.env, self.node, self.adapter)
        holder = await self.scenario_check(self.env, self.node)
        if holder:
            raise st.TargetInUse(f"a scenario run holds {self.node.name} ({holder}); the common workload empties the "
                                 "marker table at its start and may not start under a running scenario")
        self.facts["workload_lock"] = await self.lock.acquire()
        self._lock_held = True
        self.lock.start_heartbeat()
        st.write_pointer(self.env, self.node, self.folder, os.getpid())

        # once for the whole run: nothing below is repeated while the customers run
        self._set_step("emptying the workload's tables (markers, churn)")
        await self.adapter.prepare_workload_state()
        self._set_step("TPC-C data (load or reset only if needed)")
        reset = await self.profile.reset_data(self.adapter, None, fresh=self.cfg.fresh_load)
        if reset:
            self.disclosures.extend(reset.pop("_disclosures", []))
            self.facts["data_step"] = reset
        self._set_step("checking the data, fixing the rate")
        prepared = await self.profile.prepare(self.adapter)
        if prepared:
            self.disclosures.extend(prepared.pop("_disclosures", []))
            self.facts["prepare"] = prepared
        # the rate is known only now when it is a share of the stored capacity
        self.outages = OutageTracker(self.profile_workload.rate_tps, self.cfg.no_commit_alert_s)
        self.stream.emit("service", "ready_to_start", rate_tps=self.profile_workload.rate_tps,
                         warehouses=self.profile_workload.tpcc.warehouses if self.profile_workload.tpcc else None)

        self._set_step("starting the clerks")
        self.journals = MarkerJournals(self.folder)
        self.driver = self._make_driver()
        self.stream.subscribe(self._on_event)
        self.t0 = self.clock()
        self.clerks_started_wall = time.time()
        await self.driver.start()
        try:
            await asyncio.wait_for(self.driver.wait_until_ready(), READY_TIMEOUT_S)
        except TimeoutError:
            raise RuntimeError(f"not every clerk could connect within {READY_TIMEOUT_S:.0f} s") from None
        if self.driver.failure:
            raise RuntimeError(f"the workload failed at its start: {self.driver.failure}")
        self.driver.begin_window()
        self._write_info()

    def _make_driver(self) -> Any:
        """The same engine a scenario run makes for this workload (hooks.make_workload_driver); the
        TPC-C engine's worker processes come from workload.tpcc.worker_processes, as in a scenario."""
        if isinstance(self.profile, WorkloadProfile):
            return self.profile.make_driver(self.adapter, self.journals, self.stream)
        from resilience_tests.execution.workload.driver import WorkloadDriver
        return WorkloadDriver(self.adapter, self.profile_workload, self.journals, self.stream, history=None)

    @property
    def profile_workload(self) -> Any:
        """The workload as it runs: the profile's (sizes and rate resolved) or the file's."""
        return getattr(self.profile, "workload", None) or self.cfg.workload

    async def _run_loop(self) -> str:
        assert self.stream is not None and self.t0 is not None
        self.state, self.step = st.WARMING_UP, "warm-up (not measured)"
        warm_end = self.t0 + self.cfg.warmup_s
        end = self.t0 + self.cfg.duration_s
        next_interval = self.clock() + self.cfg.metrics_interval_s
        next_status = 0.0
        next_memory = 0.0
        while True:
            now = self.clock()
            if self.state == st.WARMING_UP and now >= warm_end:
                self.measure_start_s = now - self.t0
                self.state, self.step = st.RUNNING, "measured"
                self.stream.emit("service", "measure_start", elapsed_s=round(self.measure_start_s, 3))
            if now >= next_interval:
                self._interval_line()
                next_interval += self.cfg.metrics_interval_s
            if now >= next_memory:
                rss = st.process_tree_rss_bytes(os.getpid())
                if rss:
                    self.memory.append((round(now - self.t0, 1), rss))
                next_memory = now + MEMORY_EVERY_S
            if self.driver.failure:
                self.failure = f"the workload failed: {self.driver.failure}"
                return st.FAILED
            if getattr(self.lock, "lost", None):
                self.failure = f"the workload lock on the target was lost: {self.lock.lost}"
                return st.FAILED
            request = st.stop_requested(self.folder)
            if request is not None:
                self.request_stop(str(request.get("by", "stop command")))
            if self._stop_flag:
                return st.STOPPED
            if now >= end:
                return st.FINISHED
            if now >= next_status:
                self._write_status()
                next_status = now + self.cfg.status_interval_s
            await asyncio.sleep(max(0.0, min(LOOP_S, end - now)))

    async def _stop_clerks(self) -> None:
        if self.driver is None or self.t0 is None:
            if self.journals is not None:
                self.journals.close()
            return
        self.state, self.step = st.STOPPING, "stopping the clerks"
        self._write_status()
        try:
            self._interval_line()
        except Exception:  # noqa: BLE001 -- the window may not be open after a failed start
            pass
        self.ended_s = self.clock() - self.t0
        try:
            await self.driver.stop()
        finally:
            if self.journals is not None:
                self.journals.close()
        assert self.stream is not None
        self.stream.emit("service", "stopped", state_before=self.stop_by or ("failed" if self.failure else "finished"),
                         ran_s=round(self.ended_s, 1))

    async def _end_checks(self) -> dict[str, Any]:
        """Every acknowledged commit in the database, no phantom, then the TPC-C checks. A part
        that cannot run is recorded and its measure stays unknown (the baseline then fails)."""
        self.state, self.step = st.CHECKING, "end checks"
        self._write_status()
        end: dict[str, Any] = {"_errors": []}
        try:
            db = await self.adapter.marker_ids()
            self.facts["markers_db_file"] = write_db_markers(self.folder, db)
            diff, torn = diff_from_journals(self.folder, db)
            trusted = not torn and not diff.unjournalled_ack
            end.update({"rpo_txn": diff.rpo_txn if trusted else None, "phantom": len(diff.phantom),
                        "markers": {"written": diff.written, "acked": diff.acked, "in_db": diff.in_db,
                                    "lost": sorted(diff.lost)[:LOST_LIST_MAX], "lost_count": len(diff.lost),
                                    "indeterminate": len(diff.indeterminate),
                                    "indeterminate_committed": len(diff.indeterminate_committed),
                                    "rolled_back": len(diff.rolled_back), "phantom": sorted(diff.phantom)[:LOST_LIST_MAX],
                                    "unjournalled_ack": len(diff.unjournalled_ack), "torn_journal_lines": torn}})
            if not trusted:
                end["_errors"].append("the journals disagree with themselves (torn lines or acknowledgements "
                                      "without a marker line): the lost-commit count is not trusted")
        except Exception as exc:  # noqa: BLE001
            end["_errors"].append(f"lost-commit check: {type(exc).__name__}: {exc}")
        try:
            checks = await self.profile.consistency_checks(self.adapter, self.folder)
            end["_not_measured"] = checks.pop("_not_measured", {})
            end["tpcc_facts"] = checks.pop("_facts", {})
            end.update(checks)
        except Exception as exc:  # noqa: BLE001
            end["_errors"].append(f"TPC-C checks: {type(exc).__name__}: {exc}")
        return end

    async def _release(self) -> None:
        if self._lock_held:
            try:
                self.facts["workload_lock_release"] = await self.lock.release()
            except Exception as exc:  # noqa: BLE001 -- a stale lock is taken over after its heartbeat stops
                self.facts["workload_lock_release"] = {"error": f"{type(exc).__name__}: {exc}"}
            self._lock_held = False
        st.remove_pointer(self.env, self.node, self.folder)

    # --- what the run writes while it runs ---------------------------------------------------

    def _on_event(self, event: Event) -> None:
        """Every event the engine writes (stream subscriber; must return at once)."""
        if event.source != "workload" or self.t0 is None:
            return
        d = event.data
        if event.kind == "sample":
            elapsed = event.t_mono_ns / 1e9 - self.t0
            part = "measured" if self.measure_start_s is not None and elapsed > self.measure_start_s else "warmup"
            self._m1.write(json.dumps({"t_wall": round(event.t_wall, 3), "elapsed_s": round(elapsed, 3),
                                       "part": part, **d}, separators=(",", ":"), default=str) + "\n")
            second = Second.from_sample(d, elapsed)
            self.seconds.append(second)
            self.interval.add(d)
            for kind, n in (d.get("error_kinds") or {}).items():
                self.error_kinds[kind] = self.error_kinds.get(kind, 0) + int(n)
            for kind, data in self.outages.update(second, event.t_wall):
                assert self.stream is not None
                self.stream.emit("service", kind, **data)
                if kind == "outage_long":
                    self.alerts.append(f"no commit for {data['seconds']:.0f} s (started "
                                       f"{self.outages.current['started_elapsed_s'] if self.outages.current else '?'} s "
                                       "into the run); the workload keeps trying")
        elif event.kind == "driver_load":
            self.max_cpu_share = max([self.max_cpu_share, *d.get("cpu_share", [])])
        elif event.kind == "fatal" and d.get("driver_limited"):
            self.driver_limited = True

    def _interval_line(self) -> None:
        if self.driver is None or self.t0 is None:
            return
        w = self.driver.end_window()
        self.driver.begin_window()
        now = self.clock()
        part = "measured" if self.measure_start_s is not None else "warmup"
        line = {"t_wall": round(time.time(), 3), "elapsed_s": round(now - self.t0, 3), "part": part,
                **asdict(w), **self.interval.take()}
        self.last_interval = line
        self._mn.write(json.dumps(line, separators=(",", ":"), default=str) + "\n")

    def _write_info(self) -> None:
        """service.json: what a scenario attaching to this workload needs, written once the clerks
        run (the settings used, the times, the facts the TPC-C checks compare against, the files)."""
        started = self.clerks_started_wall
        p = self.profile
        tpcc = None
        if self.cfg.workload.profile == "tpcc":
            consts = getattr(p, "constants", None)
            tpcc = {"orders_at_start": getattr(p, "orders_at_prepare", None),
                    "history_since": str(getattr(p, "history_since", None)),
                    "run_constants": ({"c_last": consts.c_last, "c_id": consts.c_id, "ol_i_id": consts.ol_i_id}
                                      if consts is not None else None)}
        st.write_info(self.folder, {
            "run_id": self.run_id, "id": self.cfg.id, "folder": str(self.folder), "pid": os.getpid(),
            "env": self.env.name, "node": self.node.name,
            "started_wall": round(self.started_wall, 3), "clerks_started_wall": round(started, 3),
            "warmup_s": self.cfg.warmup_s, "measure_start_planned_wall": round(started + self.cfg.warmup_s, 3),
            "duration_s": self.cfg.duration_s, "planned_end_wall": round(started + self.cfg.duration_s, 3),
            "settings": {"config": self.cfg.as_dict(), "workload_used": self.profile_workload.model_dump(mode="json"),
                         "layout": (p.layout.as_dict() if getattr(p, "layout", None) else None),
                         "rate": getattr(p, "rate", None),
                         "worker_processes": getattr(self.driver, "processes", 1)},
            "tpcc": tpcc,
            "files": {"status": st.STATUS_FILE, "metrics_1s": st.METRICS_1S_FILE,
                      "intervals": st.metrics_interval_file(self.cfg.metrics_interval_s), "events": st.EVENTS_FILE,
                      "journals": ["marker*.jrnl", "acked*.jrnl", "aborted*.jrnl"], "summary": st.SUMMARY_FILE},
            "workload_lock": (self.facts.get("workload_lock") or {}).get("path"),
        })

    def _set_step(self, step: str) -> None:
        self.step = step
        self._write_status()

    def _totals(self, seconds: list[Second]) -> dict[str, Any]:
        secs = sum(s.interval_s for s in seconds)
        commits = sum(s.commits for s in seconds)
        return {"seconds": round(secs, 1), "commits": commits, "tps": round(commits / secs, 1) if secs > 0 else None,
                "errors": sum(s.errors for s in seconds), "unknown": sum(s.indeterminate for s in seconds),
                "drops": sum(s.drops for s in seconds), "reconnects": sum(s.reconnects for s in seconds),
                "connect_failures": sum(s.connect_failures for s in seconds),
                "business_rollbacks": sum(s.business_rollbacks for s in seconds)}

    def _write_status(self) -> None:
        now = self.clock()
        elapsed = (now - self.t0) if self.t0 is not None else None
        st.write_status(self.folder, {
            "state": self.state, "step": self.step, "id": self.cfg.id, "folder": str(self.folder),
            "pid": os.getpid(), "env": self.env.name, "node": self.node.name,
            "started_wall": round(self.started_wall, 3), "updated_wall": round(time.time(), 3),
            "elapsed_s": round(elapsed, 1) if elapsed is not None else None,
            "duration_s": self.cfg.duration_s, "warmup_s": self.cfg.warmup_s,
            "remaining_s": round(max(0.0, self.cfg.duration_s - elapsed), 1) if elapsed is not None else None,
            "rate_tps": self.profile_workload.rate_tps,
            "clerks": self.cfg.workload.concurrency,
            "last_interval": self.last_interval,
            "so_far": self._totals(self.seconds) if self.seconds else None,
            "error_kinds_so_far": dict(sorted(self.error_kinds.items())),
            "outage_now": self.outages.open_outage(), "outages": len(self.outages.outages),
            "outage_detection": "on" if self.outages.enabled else
                                "off (rate below the minimum for a second without commits to mean an outage)",
            "alerts": self.alerts, "failure": self.failure, "stop_requested_by": self.stop_by,
        })

    # --- the end ----------------------------------------------------------------------------------

    def _summary(self, final: str, end: dict[str, Any]) -> dict[str, Any]:
        ran_s = self.ended_s or 0.0
        measure_start = self.measure_start_s if self.measure_start_s is not None else self.cfg.warmup_s
        full = final == st.FINISHED and ran_s >= self.cfg.duration_s - 2 * LOOP_S
        # judged up to the moment the stop began: the engine's last, part-second sample covers the
        # clerks finishing their transactions, not the workload's normal running
        judged = [x for x in self.seconds if x.elapsed_s <= ran_s + 1e-6] if ran_s else self.seconds
        checks = baseline_mod.evaluate(
            seconds=judged, measure_start_s=measure_start, rate_tps=self.profile_workload.rate_tps,
            rules=self.cfg.baseline, memory=self.memory, end=end, driver_limited=self.driver_limited,
            full_duration=full, ran_s=ran_s, duration_s=self.cfg.duration_s,
            tpcc=self.cfg.workload.profile == "tpcc")
        valid = baseline_mod.passed(checks) and final == st.FINISHED
        measured = [s for s in self.seconds if s.elapsed_s > measure_start]
        workload_used = self.profile_workload.model_dump(mode="json")
        summary = {
            "id": self.cfg.id, "run_id": self.run_id, "folder": str(self.folder), "state": final,
            "failure": self.failure, "stop_requested_by": self.stop_by,
            "baseline_valid": valid, "baseline_checks": checks,
            "baseline_note": "baseline thresholds are NEEDS SIGN-OFF until the lead agrees them",
            "times": {"started_wall": round(self.started_wall, 3), "ran_s": round(ran_s, 1),
                      "duration_s": self.cfg.duration_s, "warmup_s": self.cfg.warmup_s,
                      "measure_start_s": round(measure_start, 1), "ended_wall": round(time.time(), 3)},
            # the settings used: the file's settings, and the workload as it ran (sizes and rate as numbers)
            "settings": {"config": self.cfg.as_dict(), "workload_used": workload_used,
                         "layout": (self.profile.layout.as_dict() if getattr(self.profile, "layout", None) else None),
                         "rate": getattr(self.profile, "rate", None),
                         "worker_processes": getattr(self.driver, "processes", 1) if self.driver else None},
            "totals": {"whole_run": self._totals(self.seconds), "measured": self._totals(measured),
                       "error_kinds": dict(sorted(self.error_kinds.items()))},
            "periods": period_summary(self.seconds, 600.0, 0.0),
            "outages": self.outages.outages + ([{**self.outages.open_outage(), "open_at_end": True}]
                                               if self.outages.open_outage() else []),
            "harness": {"max_cpu_share": round(self.max_cpu_share, 3), "driver_limited": self.driver_limited,
                        "memory_readings": len(self.memory),
                        "memory_first_mb": round(self.memory[0][1] / 1e6) if self.memory else None,
                        "memory_last_mb": round(self.memory[-1][1] / 1e6) if self.memory else None,
                        "memory_max_mb": round(max(b for _, b in self.memory) / 1e6) if self.memory else None},
            "end_checks": end, "start": self.facts, "disclosures": self.disclosures,
        }
        (self.folder / st.SUMMARY_FILE).write_text(json.dumps(summary, indent=2, default=str) + "\n")
        (self.folder / st.SUMMARY_TEXT_FILE).write_text(summary_text(summary))
        return summary


def summary_text(s: dict[str, Any]) -> str:
    t, m = s["times"], s["totals"]["measured"]
    rate = (s["settings"]["workload_used"] or {}).get("rate_tps")
    lines = [
        f"common workload {s['id']}  ({s['run_id']})",
        f"state: {s['state'].upper()}" + (f"  -- {s['failure']}" if s.get("failure") else "")
        + (f"  (stopped by {s['stop_requested_by']})" if s.get("stop_requested_by") else ""),
        f"ran {t['ran_s']:.0f} s of {t['duration_s']:.0f} s (warm-up {t['warmup_s']:.0f} s, not measured)",
        f"rate {rate} TPS; measured: {m['tps']} TPS, {m['commits']} commits, errors {m['errors']}, "
        f"unknown {m['unknown']}, dropped connections {m['drops']}, reconnects {m['reconnects']}",
        f"outages (seconds without a commit): {len(s['outages'])}",
        "",
        f"baseline {'VALID' if s['baseline_valid'] else 'NOT VALID'} ({s['baseline_note']}):",
    ]
    for c in s["baseline_checks"]:
        lines.append(f"  {c['status']:<13} {c['name']}: {c['detail']}")
    errs = s["end_checks"].get("_errors") if isinstance(s.get("end_checks"), dict) else None
    if errs:
        lines += ["", "end-check problems:", *[f"  {e}" for e in errs]]
    if s["disclosures"]:
        lines += ["", "disclosures:", *[f"  {d}" for d in s["disclosures"]]]
    return "\n".join(lines) + "\n"
