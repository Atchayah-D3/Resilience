"""The eight-phase orchestrator (Arch §4.2; Framework §7.2, Fig. 9), with the reset phase
before init (Arch §4.2 addition).

    reset -> init -> baseline -> pre_fault -> fault_inject (T0) -> recovery (T1)
          -> validate -> report -> cleanup

Every phase is bounded by the profile's phase timeout (Arch §15). Any failure ends the run
and still produces a report and a cleanup; a run can end `passed`, `failed` (verdict),
`aborted` (safety / steady state / abort_if), `error` (harness or environment defect), or
`stopped_before_fault` (dry run of the pre-fault phases -- never a verdict).
"""

from __future__ import annotations

import asyncio
import errno
import fcntl
import socket
import os
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from catalog.schema import DEFAULT_DRIVER_SECTION, FAULT_DRIVER_SECTION
from resilience_tests.adapters import postgresql  # noqa: F401  (registers the adapter)
from resilience_tests.adapters.base import Capability, adapter_for
from resilience_tests.analysis import report as report_mod
from resilience_tests.analysis import threshold_eval
from resilience_tests.analysis.predicates import NOT_APPLICABLE, NOT_MEASURED
from resilience_tests.analysis.bloat import bloat_metrics
from resilience_tests.analysis.rto_decomposer import (
    Baseline,
    decompose,
    first_write_after,
    per_cycle_recovery,
    recovery_trend,
    write_recovery,
)
from resilience_tests.control.killswitch import ledger_for, revert_outstanding
from resilience_tests.control.ledger import InjectionLedger
from resilience_tests.control.matrix import RunPlanItem
from resilience_tests.control.profile import EnvProfile
from resilience_tests.control.reset import resolve_reset
from resilience_tests.control.safety import SafetyController, SafetyViolation
from resilience_tests.execution import injectors  # noqa: F401  (registers drivers)
from resilience_tests.execution.injectors.base import FaultInjector, resolve
from resilience_tests.execution.probes.probers import LogTailer, WriteProber, measure_clock_offset
from resilience_tests.execution.remote import run_once
from resilience_tests.execution.workload.driver import MeasuredWindow, WorkloadDriver
from resilience_tests.execution.workload.markers import MarkerJournals, diff_from_journals
from resilience_tests.observability.event_stream import EventStream

EVENTS_FILE = "events.jsonl"
INTEGRITY_FILE = "integrity.txt"
WORKLOAD_RAMP_S = 2.0  # connections established before the baseline window opens
RECOVERY_POLL_S = 1.0
ABORT_POLL_S = 1.0
RECOVERY_EXIT_MARGIN_S = 5.0  # leave the recovery loop before its phase timeout fires
# Faults that must interrupt writes. If the probes never see an outage for one of these, the
# availability gap is not measured rather than reported as ~0 s.
OUTAGE_FAULTS = frozenset({"process_kill", "service_restart", "host_power_loss"})
# Faults the service recovers from on its own; the ledger entry stays outstanding until
# cleanup's revert has confirmed the node is back in its pre-fault state.
UNATTENDED_FAULTS = frozenset({"process_kill", "service_restart", "config_reload"})


class TargetBusy(RuntimeError):
    """Another run holds this target. Two runs against one node corrupt each other's
    measurements (the second run's init truncates the first run's marker table), so the
    harness refuses rather than producing numbers nobody can trust."""


class PhaseAbort(RuntimeError):
    """The run cannot proceed for a reason that is a finding, not a harness defect."""


@dataclass(frozen=True)
class RunOptions:
    target_is_disposable: bool = False
    stop_before_fault: bool = False


@dataclass
class PhaseRecord:
    phase: str
    outcome: str = "running"
    t_wall_start: float = 0.0
    duration_s: float = 0.0
    error: str | None = None
    detail: dict[str, Any] = field(default_factory=dict)


class TestOrchestrator:
    __test__ = False  # not a pytest class

    RUN_PHASES = ("reset", "init", "baseline", "pre_fault", "fault_inject", "recovery", "validate")

    def __init__(self, item: RunPlanItem, profile: EnvProfile, options: RunOptions) -> None:
        self.item = item
        self.scenario = item.scenario
        self.node = item.node
        self.profile = profile
        self.options = options
        stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        self.run_id = f"{self.scenario.id}-{stamp}-{uuid.uuid4().hex[:6]}"
        self.run_dir = Path(profile.driver_host.run_dir) / self.run_id
        self.safety = SafetyController(profile, self.scenario, target_is_disposable=options.target_is_disposable)
        self.adapter = adapter_for(profile.database.engine, self.node)
        self._ledger: InjectionLedger | None = None
        self.stream: EventStream | None = None
        self.journals: MarkerJournals | None = None
        self.workload: WorkloadDriver | None = None
        self.write_prober: WriteProber | None = None
        self.log_tailer: LogTailer | None = None
        self.injector: FaultInjector | None = None
        self.abort_task: asyncio.Task[None] | None = None
        self.phases: list[PhaseRecord] = []
        self.disclosures: list[str] = list(profile.disclosures)
        self.baseline: MeasuredWindow | None = None
        self.t0_ns: int | None = None
        self.t1_ns: int | None = None
        self.measured: dict[str, Any] = {}
        self.verdict: threshold_eval.Verdict | None = None
        self.facts: dict[str, Any] = {}
        self.abort_checks: list[dict[str, Any]] = []
        # why a measure is NOT_MEASURED, by name -- reported with the failing predicate
        self.not_measured: dict[str, str] = {}
        self._abort_reason: str | None = None
        self._current_phase: asyncio.Task[Any] | None = None
        self._lock_fh: Any = None
        self._workload_failure: str | None = None
        self.cycle_t0s: list[int] = []          # one T0 per crash cycle (Framework NL-C-05)
        self.cycle_details: list[dict[str, Any]] = []
        self.footprints: list[dict[str, Any]] = []
        self.redo_at_t0: list[int | None] = []  # WAL left to replay, sampled before each kill

    @property
    def ledger(self) -> InjectionLedger:
        # created lazily: the driver-host check must report a clear error before anything
        # touches the driver host's filesystem.
        if self._ledger is None:
            self._ledger = ledger_for(self.profile)
        return self._ledger

    # ------------------------------------------------------------------ driver

    def _acquire_target_lock(self) -> None:
        """One run at a time per (profile, node). The lock lives on the driver host and is
        released by the OS if the harness dies, so a crash cannot block future runs."""
        path = Path(self.profile.driver_host.run_dir) / f".lock-{self.profile.name}-{self.node.name}"
        path.parent.mkdir(parents=True, exist_ok=True)
        self._lock_fh = path.open("a+")
        try:
            fcntl.flock(self._lock_fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno not in (errno.EACCES, errno.EAGAIN):
                raise
            self._lock_fh.seek(0)
            holder = self._lock_fh.read().strip() or "unknown run"
            self._lock_fh.close()
            self._lock_fh = None
            raise TargetBusy(f"{self.node.name} is already in use by {holder}; wait for it to finish")
        self._lock_fh.truncate(0)
        self._lock_fh.write(f"{self.run_id} pid={os.getpid()}\n")
        self._lock_fh.flush()

    def _release_target_lock(self) -> None:
        if self._lock_fh is not None:
            fcntl.flock(self._lock_fh.fileno(), fcntl.LOCK_UN)
            self._lock_fh.close()
            self._lock_fh = None

    async def run(self) -> dict[str, Any]:
        self._assert_on_driver_host()
        # before the lock: this refuses outright, and a held lock would then make every later
        # scenario on this node report the target as busy
        self._refuse_if_already_faulted()
        self._acquire_target_lock()
        self.run_dir.mkdir(parents=True, exist_ok=False)
        self.stream = EventStream(self.run_dir / EVENTS_FILE)
        self.stream.emit("orchestrator", "run_start", run_id=self.run_id, scenario=self.scenario.id,
                         profile=self.profile.name, node=self.node.name, options=asdict(self.options))
        status, error = "error", None
        try:
            for phase in self.RUN_PHASES:
                if phase == "fault_inject" and self.options.stop_before_fault:
                    status = "stopped_before_fault"
                    break
                await self._phase(phase)
            else:
                assert self.verdict is not None
                status = "passed" if self.verdict.passed else "failed"
        except (SafetyViolation, PhaseAbort) as exc:
            status, error = "aborted", str(exc)
        except Exception as exc:  # noqa: BLE001 -- recorded in the report, then cleanup runs
            status, error = "error", f"{type(exc).__name__}: {exc}"
        except BaseException:
            # Interrupted (Ctrl-C / cancellation): still never strand a node in a faulted state.
            await asyncio.shield(self._emergency_cleanup())
            self._release_target_lock()
            raise
        if self._abort_reason and status in ("error", "aborted"):
            status, error = "aborted", self._abort_reason
        results = self._results(status, error)
        await self._phase("report", results=results, swallow=True)
        await self._phase("cleanup", swallow=True)
        status, error = self._after_cleanup(status, error)
        results = self._results(status, error)  # final: includes the cleanup record
        report_mod.write_results(self.run_dir, results)
        self._close_stream()
        self._release_target_lock()
        return results

    async def _phase(self, name: str, *, swallow: bool = False, **kwargs: Any) -> None:
        assert self.stream is not None
        rec = PhaseRecord(name, t_wall_start=time.time())
        self.phases.append(rec)
        self.stream.emit("orchestrator", "phase_start", phase=name)
        t = time.monotonic()
        task = asyncio.create_task(getattr(self, f"_p_{name}")(**kwargs), name=f"phase:{name}")
        self._current_phase = task
        try:
            try:
                async with asyncio.timeout(self._phase_budget(name)):
                    detail = await task
            except asyncio.CancelledError:
                current = asyncio.current_task()
                if self._abort_reason and task.cancelled() and not (current and current.cancelling()):
                    raise PhaseAbort(self._abort_reason) from None  # abort_if fired: a finding, not a crash
                raise
            rec.outcome, rec.detail = "ok", detail or {}
        except PhaseAbort as exc:
            rec.outcome, rec.error = "aborted", str(exc)
            if not swallow:
                raise
        except TimeoutError:
            rec.outcome, rec.error = "timeout", f"phase exceeded {self._phase_budget(name)} s"
            if not swallow:
                raise PhaseAbort(f"{name}: {rec.error}") from None
        except Exception as exc:
            rec.outcome, rec.error = "failed", f"{type(exc).__name__}: {exc}"
            if not swallow:
                raise
        finally:
            rec.duration_s = time.monotonic() - t
            self.stream.emit("orchestrator", "phase_end", phase=name, outcome=rec.outcome, error=rec.error)

    def _refuse_if_already_faulted(self) -> None:
        """Refuse to start on a node that still carries an injection from an earlier run.

        The target lock stops two runs overlapping; it says nothing about a previous run that
        died leaving a fault applied. Measuring a baseline on a node whose service is stopped,
        or whose configuration the harness changed, produces numbers that describe neither the
        product nor the fault under test (Arch §15)."""
        stale = [e for e in self.ledger.outstanding() if e.node == self.node.name]
        if not stale:
            return
        detail = ", ".join(f"{e.fault_type} ({e.state}, run {e.run_id})" for e in stale)
        raise SafetyViolation(
            f"{self.node.name} still carries an injection from an earlier run: {detail}. "
            f"Revert it before measuring anything here: "
            f"python -m resilience_tests.control.killswitch --env {self.profile.name}"
        )

    def _phase_budget(self, name: str) -> float:
        """Every phase is bounded by the machine file. A repeated scenario runs its fault
        phase `cycles` times, so its bound is the profile's per-fault bound multiplied by the
        declared cycles plus the settle time between them -- still profile-derived, never a
        number chosen here."""
        base = self.profile.phase_timeouts_s[name]
        r = self.scenario.repeat
        if name == "fault_inject" and r is not None:
            return r.cycles * (base + r.interval_s)
        return base

    def _assert_on_driver_host(self) -> None:
        """Arch §6.2: journals must be on the driver host, never on a target. Binding the
        declared driver-host address succeeds only if it is local to this machine."""
        host = self.profile.driver_host.host
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            try:
                s.bind((host, 0))
            except OSError:
                raise SafetyViolation(
                    f"this machine is not the profile's driver host {host}; run the harness there (Arch §6.2)"
                ) from None

    # ------------------------------------------------------------------ phases

    async def _p_reset(self) -> dict[str, Any]:
        detail = await resolve_reset(self.profile).reset(self.node)
        if "disclosure" in detail:
            self.disclosures.append(detail["disclosure"])
        return detail

    async def _p_init(self) -> dict[str, Any]:
        assert self.stream is not None
        self.safety.check_static([self.node])
        hostname = (await run_once(self.node.ssh, "hostname", timeout_s=15)).stdout.strip()
        sentinel = await self.adapter.sentinel(self.profile.safety.sentinel_table, hostname)
        self.facts["fingerprint"] = self.safety.check_fingerprint(self.node, hostname, sentinel)

        settings = await self.adapter.durability_settings()
        self.facts["engine"] = self.adapter.engine
        self.facts["capabilities"] = sorted(c.value for c in self.adapter.capabilities)
        self.facts["settings"] = settings
        # Each engine decides what makes a result uncertifiable (e.g. page checksums off).
        blockers = await self.adapter.certification_blockers()
        if blockers:
            raise PhaseAbort("; ".join(blockers))

        self.facts["clock_offset_s"] = await measure_clock_offset(self.node, self.stream)

        section = FAULT_DRIVER_SECTION.get(self.scenario.fault.type, DEFAULT_DRIVER_SECTION)
        self.injector = resolve(self.scenario.fault, self.profile)
        if self.scenario.repeat is not None:
            self.injector.repeat_plan = (self.scenario.repeat.cycles, self.scenario.repeat.interval_s)
        self.facts["fault_driver"] = {"section": section, "driver": self.injector.driver_name}

        # Integrity counters are cumulative in most engines; the run is judged on its own delta.
        self.facts["integrity_baseline"] = await self.adapter.mark_integrity_baseline()
        await self.adapter.prepare_harness_state()
        self.journals = MarkerJournals(self.run_dir)
        self.workload = WorkloadDriver(self.adapter, self.scenario.workload, self.journals, self.stream)
        return {"hostname": hostname, "settings": settings}

    async def _p_baseline(self) -> dict[str, Any]:
        assert self.stream and self.workload
        self.write_prober = WriteProber(self.adapter, self.stream)
        self.write_prober.start()
        self.log_tailer = LogTailer(self.node, self.stream)
        self.log_tailer.start()
        await self.workload.start()
        self.abort_task = asyncio.create_task(self._abort_monitor(), name="abort-monitor")
        await asyncio.sleep(WORKLOAD_RAMP_S)
        self.workload.begin_window()
        await asyncio.sleep(self.scenario.steady_state.duration_s)
        self.baseline = self.workload.end_window()
        self.stream.emit("orchestrator", "baseline", **asdict(self.baseline))
        return asdict(self.baseline)

    async def _p_pre_fault(self) -> dict[str, Any]:
        assert self.baseline is not None
        ss = self.scenario.steady_state
        checks = {
            f"tps >= {ss.tps_min}": self.baseline.tps >= ss.tps_min,
            f"p99_ms <= {ss.p99_latency_ms_max}": self.baseline.p99_ms is not None and self.baseline.p99_ms <= ss.p99_latency_ms_max,
        }
        if ss.replication_lag_s_max is not None:
            if self.node.role != "standalone":
                raise NotImplementedError("replication lag steady-state check arrives with the Tier-2 probes")
            checks[f"replication_lag_s <= {ss.replication_lag_s_max}"] = "not_applicable (standalone)"
        failed = [k for k, v in checks.items() if v is False]
        if failed:
            # name the likely limiter: driver-side flush latency vs database latency
            jp99 = self.baseline.journal_p99_ms
            where = "driver journal flush" if jp99 is not None and self.baseline.p99_ms is not None and jp99 > self.baseline.p99_ms else "target database"
            raise PhaseAbort(
                f"steady state did not hold (tps={self.baseline.tps:.1f}, db p99={self.baseline.p99_ms:.1f} ms, "
                f"journal p99={jp99 if jp99 is None else round(jp99, 1)} ms; slower side: {where}): {failed}"
            )
        detail: dict[str, Any] = {"steady_state": checks}
        if self.options.stop_before_fault:
            detail["injector_preflight"] = "not run (stop_before_fault)"
        else:
            assert self.injector is not None
            detail["injector_preflight"] = await self.injector.preflight(self.node)
            # journalled with the injection, so a revert (possibly by the kill switch after a
            # crash) can restore what preflight observed rather than guess at it
            self.facts["injector_preflight"] = detail["injector_preflight"]
        return detail

    async def _p_fault_inject(self) -> dict[str, Any]:
        r = self.scenario.repeat
        if r is None:
            return await self._inject_once()
        self.footprints.append(await self._footprint("before cycle 1"))
        detail: dict[str, Any] = {"cycles": []}
        for cycle in range(1, r.cycles + 1):
            one = await self._inject_once(cycle=cycle)
            recovered = await self._await_cycle_recovery(cycle, last=cycle == r.cycles)
            one.update(recovered)
            quick_integrity = await self.adapter.quick_integrity_check()
            if quick_integrity:
                one["quick_integrity"] = quick_integrity
                if quick_integrity.get("checksum_failures", 0) > 0:
                    self.stream.emit("orchestrator", "inter_cycle_corruption", cycle=cycle, **quick_integrity)
            self.footprints.append(await self._footprint(f"after cycle {cycle}"))
            detail["cycles"].append(one)
            self.cycle_details.append(one)
            if cycle < r.cycles:
                # settle, so the next cycle starts from a comparable state rather than from
                # the tail of this recovery
                await asyncio.sleep(r.interval_s)
        return detail

    async def _inject_once(self, cycle: int | None = None) -> dict[str, Any]:
        assert self.injector and self.stream
        section = self.facts["fault_driver"]["section"]
        entry = self.ledger.intent(run_id=self.run_id, env_profile=self.profile.name, fault_type=self.scenario.fault.type,
                                   driver=self.injector.driver_name, node=self.node.name,
                                   detail={"section": section, "cycle": cycle,
                                           "preflight": self.facts.get("injector_preflight", {})})
        # Sampled immediately before the kill: the WAL a crash at this instant leaves to
        # replay. Without it, two cycles' recovery times are only comparable by assumption.
        redo_sample_mono_ns = time.monotonic_ns()
        redo = await self.adapter.redo_distance_bytes()
        detail = await self.injector.inject(self.node)
        t0 = detail.pop("t0_mono_ns", None) or time.monotonic_ns()
        sampling_delay_ms = round((t0 - redo_sample_mono_ns) / 1e6, 2)
        self.t0_ns = t0 if self.t0_ns is None else self.t0_ns   # T0 of the run is the first fault
        self.cycle_t0s.append(t0)
        self.ledger.transition(entry, "applied", inject=detail, cycle=cycle)
        self.stream.emit("injector", "t0", fault=self.scenario.fault.type, node=self.node.name,
                         t0_mono_ns=t0, cycle=cycle, redo_sampling_delay_ms=sampling_delay_ms, **detail)
        self.stream.sync()
        self.facts["injection_id"] = entry.injection_id
        self._cycle_entries = getattr(self, "_cycle_entries", {})
        self._cycle_entries[cycle] = entry
        self.redo_at_t0.append(redo)
        return {"cycle": cycle, "t0_mono_ns": t0, "redo_distance_bytes": redo,
                "redo_sampling_delay_ms": sampling_delay_ms, **detail}

    async def _await_cycle_recovery(self, cycle: int, last: bool = False) -> dict[str, Any]:
        """Wait for this cycle's service to come back, on the evidence of the write probe --
        never on a timer. The ledger entry is cleared only once a write has succeeded, so a
        cycle that never recovers stays outstanding for the kill switch.

        The LAST cycle's entry is deliberately left outstanding, exactly as a single-fault run
        leaves its one entry: cleanup's revert is what confirms the service is running and
        clears the service manager's failure counters after all those kills. Without it, ten
        restarts' worth of residue would be left for whoever runs next."""
        assert self.stream is not None
        t0 = self.cycle_t0s[-1]
        deadline = time.monotonic() + self.profile.phase_timeouts_s["fault_inject"]
        while time.monotonic() < deadline:
            r = write_recovery([e for e in self.stream.events() if e.t_mono_ns > t0], t0)
            if r.outage_observed and r.first_write_s is not None:
                entry = getattr(self, "_cycle_entries", {}).get(cycle)
                if entry is not None and not last:
                    self.ledger.transition(entry, "reverted", by="unattended restart",
                                           confirmed_by="write probe", recovery_s=r.first_write_s)
                self.stream.emit("orchestrator", "cycle_recovered", cycle=cycle, recovery_s=r.first_write_s)
                return {"recovery_s": r.first_write_s}
            await asyncio.sleep(RECOVERY_POLL_S)
        self.stream.emit("orchestrator", "cycle_not_recovered", cycle=cycle)
        raise PhaseAbort(f"cycle {cycle}: service did not accept a write again within "
                         f"{self.profile.phase_timeouts_s['fault_inject']} s")

    async def _footprint(self, when: str) -> dict[str, Any]:
        try:
            sample = await self.adapter.storage_footprint()
        except Exception as exc:  # noqa: BLE001 -- recorded; bloat then counts as not measured
            self.facts.setdefault("footprint_errors", []).append(f"{when}: {type(exc).__name__}: {exc}")
            return {}
        sample["when"] = when
        assert self.stream is not None
        self.stream.emit("orchestrator", "footprint", **sample)
        return sample

    async def _p_recovery(self) -> dict[str, Any]:
        """T1 = power restored. The harness only restores power; the database must start
        unattended (Framework §10.1). Then watch until service returns to SLO or the bound."""
        assert self.injector and self.stream and self.baseline and self.t0_ns is not None
        detail: dict[str, Any] = {}
        fault_type = self.scenario.fault.type
        if fault_type == "host_power_loss":
            # The harness restores power and nothing else; the database must start unattended.
            entry = next(e for e in self.ledger.outstanding() if e.injection_id == self.facts["injection_id"])
            detail["power_on"] = await self.injector.revert(self.node, entry.detail)
            self.t1_ns = time.monotonic_ns()
            self.ledger.transition(entry, "reverted", revert=detail["power_on"], by="orchestrator")
            self.stream.emit("orchestrator", "t1", t1_mono_ns=self.t1_ns, action="power restored")
        elif fault_type in UNATTENDED_FAULTS:
            # No harness action: the service comes back on its own (restart policy, or the
            # restart command itself). T1 = T0, so rto_first_write_s and "starts unattended"
            # are measured from the fault. The ledger entry is deliberately left outstanding:
            # cleanup's revert confirms the service is running (and, for a reload, restores the
            # parameter), and a harness crash before then leaves it to the kill switch.
            self.t1_ns = self.t0_ns
            self.stream.emit("orchestrator", "t1", t1_mono_ns=self.t1_ns, action="unattended recovery expected")
            detail["recovery"] = "unattended (no harness action)"
        else:
            raise NotImplementedError(f"recovery for {self.scenario.fault.type}/{self.scenario.fault.duration} not built yet")

        baseline = Baseline(self.baseline.tps, self.baseline.p99_ms or 0.0)
        slo_t0 = self.cycle_t0s[-1] if self.scenario.repeat else self.t0_ns
        self.facts["slo_t0_mono_ns"] = slo_t0
        deadline = time.monotonic() + self.profile.phase_timeouts_s["recovery"] - RECOVERY_EXIT_MARGIN_S
        while time.monotonic() < deadline:
            d = decompose(self.stream.events(), slo_t0, baseline, clustered=False,
                          detection_patterns=self.adapter.fault_detection_log_patterns(),
                          recovery_patterns=self.adapter.recovery_start_log_patterns(),
                          expect_outage=fault_type in OUTAGE_FAULTS)
            if d.rto_to_slo_s is not None:
                detail["slo_reached_s"] = d.rto_to_slo_s
                return detail
            await asyncio.sleep(RECOVERY_POLL_S)
        detail["slo_reached_s"] = None
        detail["note"] = "service did not return to SLO within the recovery bound"
        return detail

    async def _p_validate(self) -> dict[str, Any]:
        assert self.stream and self.baseline and self.t0_ns is not None
        await self._stop_load()
        if self._workload_failure:
            # the load generator is the measuring instrument; its evidence is incomplete
            raise PhaseAbort(f"workload driver failed: {self._workload_failure}")
        events = self.stream.events()
        m: dict[str, Any] = {}

        expect_outage = self.scenario.fault.type in OUTAGE_FAULTS
        slo_t0 = self.cycle_t0s[-1] if self.scenario.repeat else self.t0_ns
        self.facts["slo_t0_mono_ns"] = slo_t0
        d = decompose(events, slo_t0, Baseline(self.baseline.tps, self.baseline.p99_ms or 0.0),
                      clustered=False, detection_patterns=self.adapter.fault_detection_log_patterns(),
                      recovery_patterns=self.adapter.recovery_start_log_patterns(),
                      expect_outage=expect_outage)
        m.update(d.as_measured())
        self.facts["outage_observed"] = d.outage_observed
        if d.outage_observed and d.rto_first_write_s is None:
            # the probes saw the outage start and never saw it end
            why = ("service did not accept a write again before the recovery bound expired -- "
                   "as far as the harness could see it was still down when measurement stopped")
            m["rto_first_write_s"] = m["t_reconnect_s"] = NOT_MEASURED
            self.not_measured["rto_first_write_s"] = self.not_measured["t_reconnect_s"] = why
            self.facts["rto_note"] = why
        if expect_outage and not d.outage_observed:
            why = ("the write probes never saw this fault interrupt service: the outage was shorter "
                   "than the probe interval, or the fault did not take effect")
            self.facts["rto_note"] = why
            self.not_measured["rto_first_write_s"] = why
            self.not_measured["t_reconnect_s"] = why   # the same value under the Framework's name
        if d.components.get("mttd_s") is NOT_MEASURED:
            self.not_measured["mttd_s"] = (
                "this fault leaves the engine no chance to log that it noticed -- a killed process "
                "writes nothing. The replacement's own recovery-start line is reported separately "
                "as recovery_started_s, which is a restart time, not a detection time")
        if d.components.get("recovery_started_s") is NOT_MEASURED:
            self.not_measured["recovery_started_s"] = (
                "no recovery-start line from this engine reached the harness during the run")
        # Client-visible disturbance after the fault (Framework §10.7 NL-M-07).
        after = [e for e in events if e.kind == "sample" and e.source == "workload" and e.t_mono_ns > self.t0_ns]
        m["failed_transactions"] = sum(e.data.get("errors", 0) for e in after)
        m["dropped_connections"] = sum(e.data.get("drops", 0) for e in after)
        m["connect_failures"] = sum(e.data.get("connect_failures", 0) for e in after)
        self.facts["slo_window_end_s"] = d.slo_window_end_s
        # Framework §10.1: the database starts unattended -- a successful write started after
        # T1, with no harness action on the database. For a fault that must cause an outage,
        # that write must also come after the outage was observed.
        recovered = self.t1_ns is not None and first_write_after(events, self.t1_ns) is not None
        if expect_outage:
            recovered = recovered and isinstance(d.rto_first_write_s, (int, float))
        m["starts_unattended"] = recovered

        if self.scenario.repeat is not None:
            cycles = per_cycle_recovery(events, self.cycle_t0s, expect_outage=expect_outage)
            trend = recovery_trend(cycles)
            bloat = bloat_metrics(self.footprints)
            rows = [asdict(c) for c in cycles]
            # Join each cycle's recovery to the replay work it actually faced. A recovery time
            # on its own is not comparable across cycles; bytes replayed per second is.
            for i, (row, redo) in enumerate(zip(rows, self.redo_at_t0)):
                row["redo_distance_bytes"] = redo
                rec = row.get("recovery_s")
                row["replay_bytes_per_s"] = (
                    round(redo / rec, 1) if redo and isinstance(rec, (int, float)) and rec > 0 else None)
                if i < len(self.cycle_details):
                    row["redo_sampling_delay_ms"] = self.cycle_details[i].get("redo_sampling_delay_ms")
                    if "quick_integrity" in self.cycle_details[i]:
                        row["quick_integrity"] = self.cycle_details[i]["quick_integrity"]
                # Partition client-visible errors per cycle window
                c_start = self.cycle_t0s[i]
                c_end = self.cycle_t0s[i + 1] if i + 1 < len(self.cycle_t0s) else None
                c_samples = [e for e in events if e.kind == "sample" and e.source == "workload"
                             and e.t_mono_ns > c_start and (c_end is None or e.t_mono_ns <= c_end)]
                row["failed_transactions"] = sum(e.data.get("errors", 0) for e in c_samples)
                row["dropped_connections"] = sum(e.data.get("drops", 0) for e in c_samples)
                row["connect_failures"] = sum(e.data.get("connect_failures", 0) for e in c_samples)
            self.facts["cycles"] = rows
            m["failed_transactions_max_per_cycle"] = max((r.get("failed_transactions", 0) for r in rows), default=0)
            m["dropped_connections_max_per_cycle"] = max((r.get("dropped_connections", 0) for r in rows), default=0)
            replayed = [r["redo_distance_bytes"] for r in rows if r["redo_distance_bytes"] is not None]
            if replayed:
                m["wal_replayed_bytes_max"] = max(replayed)
                m["wal_replayed_bytes_median"] = sorted(replayed)[len(replayed) // 2]
            rates = [r["replay_bytes_per_s"] for r in rows if r["replay_bytes_per_s"] is not None]
            if rates:
                m["replay_bytes_per_s_min"] = min(rates)
            self.facts["recovery_trend"] = trend
            self.facts["bloat"] = bloat
            self.facts["footprints"] = self.footprints
            m["cycles_run"] = len(self.cycle_t0s)
            m["cycles_recovered"] = sum(1 for c in cycles if isinstance(c.recovery_s, (int, float)))
            for key, name in (("first_s", "recovery_s_first"), ("last_s", "recovery_s_last"),
                              ("median_s", "recovery_s_median"), ("max_s", "recovery_s_max"),
                              ("ratio_last_over_first", "recovery_ratio_last_over_first"),
                              ("slope_s_per_cycle", "recovery_slope_s_per_cycle")):
                if trend.get(key) is not None:
                    m[name] = trend[key]
            for key, name in (("bloat_ratio", "bloat_ratio"),
                              ("wal_ratio_of_max_wal_size", "wal_ratio_of_max_wal_size"),
                              ("bytes_unexplained_by_rows", "bytes_unexplained_by_rows")):
                if bloat.get(key) is not None:
                    m[name] = bloat[key]
            if m.get("cycles_recovered") != m.get("cycles_run"):
                self.not_measured["recovery_ratio_last_over_first"] = (
                    f"only {m.get('cycles_recovered')} of {m.get('cycles_run')} cycles were seen to recover")
            # A bloat figure the engine could not produce must say so by name. Left merely
            # absent it would still fail the predicate, but with "never measured" and no cause
            # -- and the cause here is usually one missing grant, not a misbehaving database.
            errors = "; ".join(self.facts.get("footprint_errors", []))
            if bloat.get("bloat_ratio") is None:
                m["bloat_ratio"] = NOT_MEASURED
                self.not_measured["bloat_ratio"] = (
                    f"on-disk footprint sampled {bloat.get('samples', 0)} time(s); two are needed"
                    + (f" ({errors})" if errors else ""))
            if bloat.get("wal_ratio_of_max_wal_size") is None:
                why_wal = next((s["wal_unavailable"] for s in reversed(self.footprints)
                                if s.get("wal_unavailable")), None)
                m["wal_ratio_of_max_wal_size"] = NOT_MEASURED
                self.not_measured["wal_ratio_of_max_wal_size"] = (
                    f"write-ahead log size not readable: {why_wal}" if why_wal else
                    # the footprint failed outright: name that cause rather than imply the
                    # engine simply does not report a WAL size
                    f"on-disk footprint could not be sampled ({errors})" if errors else
                    "write-ahead log size was not reported by this engine")

        journal_problem: str | None = None
        try:
            db_uuids = await self.adapter.marker_ids()
            diff, torn = diff_from_journals(self.run_dir, db_uuids)
            self.facts["markers"] = {
                "written": diff.written, "acked": diff.acked, "in_db": diff.in_db, "lost": sorted(diff.lost),
                "indeterminate": len(diff.indeterminate), "indeterminate_committed": len(diff.indeterminate_committed),
                "phantom": sorted(diff.phantom), "unjournalled_ack": sorted(diff.unjournalled_ack), "torn_journal_lines": torn,
            }
            if diff.unjournalled_ack or torn:
                # The RPO arithmetic is only as good as the journals; if they disagree with
                # themselves there is no RPO figure to evaluate.
                journal_problem = (f"marker journals inconsistent (unjournalled acks {len(diff.unjournalled_ack)}, "
                                   f"torn lines {torn}); RPO cannot be evaluated")
            else:
                m["rpo_txn"] = diff.rpo_txn
                m["indeterminate_txn"] = len(diff.indeterminate)
        except Exception as exc:  # noqa: BLE001 -- recorded; the measure then counts as missing
            self.facts["markers_error"] = f"{type(exc).__name__}: {exc}"

        try:
            integrity = await self.adapter.integrity_check(timeout_s=self.profile.phase_timeouts_s["validate"] / 2)
            (self.run_dir / INTEGRITY_FILE).write_text(integrity.raw_output)
            # A capability the engine lacks is NOT_APPLICABLE, never zero: a criterion that
            # depends on it then fails rather than passing on a measurement nobody took.
            structural = (integrity.structural_errors
                          if self.adapter.has(Capability.STRUCTURAL_INTEGRITY_CHECK) else NOT_APPLICABLE)
            m["structural_integrity_errors"] = structural
            m["amcheck_errors"] = structural   # the Framework's PostgreSQL wording, same value
            self.facts["checksum_failures"] = integrity.checksum_failures
            phantom = len(self.facts.get("markers", {}).get("phantom", [])) if "markers" in self.facts else None
            # corruption: structural findings + page checksum failures + rows nobody wrote. A
            # part the engine cannot answer for is NOT a zero -- the total is then not measured,
            # and any criterion over it fails rather than passing on a sum nobody could take.
            parts = {"structural_integrity": integrity.structural_errors,
                     "page_checksum_failures": integrity.checksum_failures,
                     "phantom_markers": phantom}
            self.facts["corruption_parts"] = parts
            unknown = sorted(k for k, v in parts.items() if v is None)
            if unknown:
                m["corruption_count"] = NOT_MEASURED
                self.not_measured["corruption_count"] = (
                    f"this engine produced no value for {', '.join(unknown)}, so the parts cannot be summed")
            else:
                m["corruption_count"] = sum(parts.values())
        except Exception as exc:  # noqa: BLE001 -- recorded; the measure then counts as missing
            self.facts["integrity_error"] = f"{type(exc).__name__}: {exc}"

        # Anything the scenario declared but the harness could not produce stays absent, and
        # the evaluator fails any predicate that needs it (never a default pass).
        self.facts["declared_not_produced"] = sorted(set(self.scenario.measure) - set(m))
        self.measured = m
        if journal_problem:
            # integrity output was still collected above, as evidence; no verdict is issued
            raise PhaseAbort(journal_problem)
        self.facts["not_measured"] = dict(self.not_measured)
        self.verdict = threshold_eval.evaluate(self.scenario.accept, m, self.not_measured)
        return {"verdict": "pass" if self.verdict.passed else "fail"}

    async def _p_report(self, results: dict[str, Any]) -> dict[str, Any]:
        path = report_mod.write_results(self.run_dir, results)
        return {"results": str(path)}

    async def _p_cleanup(self) -> dict[str, Any]:
        await self._stop_load()
        if self.log_tailer:
            await self.log_tailer.stop()
        reverted = await revert_outstanding(self.profile, self.ledger, run_id=self.run_id)
        return {"reverted": [(e.injection_id, outcome) for e, outcome in reverted]}

    def _after_cleanup(self, status: str, error: str | None, *,
                       expect_phase_record: bool = True) -> tuple[str, str | None]:
        """A run whose cleanup did not undo its own injections must never report `passed`.

        The verdict says whether the database behaved; it says nothing about whether the
        harness left the target as it found it. Cleanup is bounded and its failures are
        swallowed so that a report is always written -- so the check has to happen here,
        after it, or a stopped service (or a config change still in postgresql.auto.conf)
        would be reported as a pass and go green in CI (Arch §15: the harness must not be
        able to strand a node in a faulted state)."""
        problems: list[str] = []
        record = next((p for p in reversed(self.phases) if p.phase == "cleanup"), None)
        if record is None:
            if expect_phase_record:
                problems.append("cleanup did not run")
        elif record.outcome != "ok":
            problems.append(f"cleanup {record.outcome}: {record.error}")
        try:
            outstanding = [e for e in self.ledger.outstanding() if e.run_id == self.run_id]
        except Exception as exc:  # noqa: BLE001 -- an unreadable ledger is itself a problem
            outstanding = []
            problems.append(f"injection ledger could not be read: {type(exc).__name__}: {exc}")
        if outstanding:
            problems.append("injections not reverted: "
                            + ", ".join(f"{e.fault_type} on {e.node} ({e.state})" for e in outstanding))
        self.facts["cleanup_outstanding"] = [
            {"injection_id": e.injection_id, "fault_type": e.fault_type, "node": e.node, "state": e.state}
            for e in outstanding
        ]
        if not problems:
            return status, error
        message = ("the target may be left in a faulted state -- " + "; ".join(problems)
                   + f". Run: python -m resilience_tests.control.killswitch --env {self.profile.name}")
        self.stream and self.stream.emit("orchestrator", "cleanup_incomplete", detail=message)
        self.facts["cleanup_problem"] = message
        # never a pass; an existing failure/abort keeps its status and carries the note
        if status == "passed":
            return "error", message
        return status, f"{error}; {message}" if error else message

    def _close_stream(self) -> None:
        if self.stream:
            self.stream.emit("orchestrator", "run_end")
            self.stream.close()

    # ------------------------------------------------------------------ helpers

    async def _emergency_cleanup(self) -> None:
        """Interrupted (Ctrl-C / cancellation). Evidence is written BEFORE anything is awaited,
        because a second interrupt can cut this short, and the operator is always told what
        state the target is in -- an interrupted destructive run that says nothing is how a
        faulted node gets left behind at a customer site."""
        note = "harness interrupted"
        try:
            report_mod.write_results(self.run_dir, self._results("aborted", note))
        except BaseException as exc:  # noqa: BLE001 -- warn even if the evidence cannot be written
            note = f"{note}; results could not be written: {type(exc).__name__}: {exc}"
        try:
            async with asyncio.timeout(self.profile.phase_timeouts_s["cleanup"]):
                await self._p_cleanup()
        except BaseException as exc:  # noqa: BLE001 -- the ledger still lets the kill switch finish
            note = f"{note}; cleanup did not finish: {type(exc).__name__}: {exc}"
        status, error = self._after_cleanup("aborted", note, expect_phase_record=False)
        try:
            report_mod.write_results(self.run_dir, self._results(status, error))
        except BaseException:  # noqa: BLE001
            pass
        self._warn_operator(error or note)
        try:
            self._close_stream()
        except BaseException:  # noqa: BLE001
            pass

    def _warn_operator(self, message: str) -> None:
        """Straight to stderr: an interrupt unwinds as a traceback, and the one thing the
        operator needs -- whether a fault is still applied -- must not be inside it."""
        outstanding = self.facts.get("cleanup_outstanding") or []
        state = ("TARGET MAY BE FAULTED" if outstanding else "no injection left outstanding")
        print(f"\n!! run {self.run_id} interrupted -- {state}\n"
              f"   {message}\n"
              f"   evidence: {self.run_dir}", file=sys.stderr, flush=True)

    async def _stop_load(self) -> None:
        if self.abort_task:
            self.abort_task.cancel()
            await asyncio.gather(self.abort_task, return_exceptions=True)
            self.abort_task = None
        if self.workload:
            await self.workload.stop()
            self._workload_failure = self._workload_failure or self.workload.failure
            self.workload = None
        if self.write_prober:
            await self.write_prober.stop()
            self.write_prober = None
        if self.journals:
            self.journals.close()
            self.journals = None

    def _signals(self) -> dict[str, Any]:
        """Probe-stream signals for abort_if. The replication/HA signals come from the Tier-2
        probers; on a standalone target they do not exist and are marked not applicable."""
        if self.node.role == "standalone":
            return {"replication_lag_s": NOT_APPLICABLE, "secondary_node_unhealthy": NOT_APPLICABLE}
        raise NotImplementedError("abort signals for clustered targets arrive with the Tier-2 probes")

    async def _abort_monitor(self) -> None:
        first = True
        while True:
            if self.workload is not None and self.workload.failure:
                self._workload_failure = self.workload.failure
                self._abort_reason = f"workload driver failed: {self.workload.failure}"
                if self._current_phase is not None and not self._current_phase.done():
                    self._current_phase.cancel()
                return
            checks = self.safety.check_abort(self._signals())
            if first:
                self.abort_checks = [asdict(c) | {"t": time.time()} for c in checks]
                first = False
            fired = [c for c in checks if c.triggered]
            if fired:
                self._abort_reason = f"abort_if triggered: {[c.predicate for c in fired]}"
                self.abort_checks.extend(asdict(c) | {"t": time.time()} for c in fired)
                if self._current_phase is not None and not self._current_phase.done():
                    self._current_phase.cancel()
                return
            await asyncio.sleep(ABORT_POLL_S)

    def _results(self, status: str, error: str | None) -> dict[str, Any]:
        sc = self.scenario
        return {
            "run_id": self.run_id,
            "status": status,
            "error": error,
            "scenario": {"id": sc.id, "name": sc.name, "priority": sc.priority, "category": sc.category},
            "environment": {"profile": self.profile.name, "class": self.profile.env_class,
                            "environment": self.profile.environment, "storage_class": self.profile.storage_class},
            "target": {"node": self.node.name, "role": self.node.role, "topology_role": self.node.topology_role},
            "options": asdict(self.options),
            "phases": [asdict(p) for p in self.phases],
            "baseline": asdict(self.baseline) if self.baseline else None,
            "timing": {"t0_mono_ns": self.t0_ns, "t1_mono_ns": self.t1_ns},
            "measured": {k: (repr(v) if v is NOT_APPLICABLE or v is NOT_MEASURED else v)
                         for k, v in self.measured.items()},
            "verdict": None if self.verdict is None else {
                "passed": self.verdict.passed, "results": [asdict(r) for r in self.verdict.results]},
            "abort_checks": self.abort_checks,
            "facts": self.facts,
            "disclosures": self.disclosures,
            "evidence_dir": str(self.run_dir),
        }


async def run_item(item: RunPlanItem, profile: EnvProfile, options: RunOptions) -> dict[str, Any]:
    return await TestOrchestrator(item, profile, options).run()
