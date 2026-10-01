"""The phase machine end to end, against a fake engine and a fake fault driver.

The real orchestrator, workload driver, write prober, marker journals, injection ledger,
decomposer and threshold evaluator run; only the engine, the fault and SSH are fakes. These
cover what unit tests of the pure functions cannot: which phase records what, when the
ledger says an injection is undone, and which failures stop a verdict being issued.
"""

import asyncio
from pathlib import Path
import time
from typing import Any

import pytest

from catalog.schema import load_catalog
from resilience_tests.adapters.base import Capability, DatabaseSession, IntegrityResult, TransactionOutcome, register_adapter
from resilience_tests.control import killswitch
from resilience_tests.control import orchestrator as orch
from resilience_tests.control.killswitch import ledger_for
from resilience_tests.control.matrix import RunPlanItem
from resilience_tests.control.orchestrator import RunOptions, TestOrchestrator
from resilience_tests.analysis import rto_decomposer
from resilience_tests.control.profile import load_profile
from resilience_tests.control.safety import SafetyViolation
from resilience_tests.execution.injectors.base import FaultInjector
from resilience_tests.execution.remote import RemoteResult
from resilience_tests.execution.workload.markers import MarkerJournals
from tests.test_adapter_seam import FakeAdapter

CATALOG = load_catalog()
BASE_PROFILE = load_profile("e2-dedicated-vm")
OUTAGE_S = 2.0   # 10 write-probe intervals: the probes must see this outage even on a busy box


class Engine:
    """State shared by every session of the fake engine within one test."""

    down = False
    store: set[str] = set()
    churn_ops = 0          # update/delete traffic the mixed profile asked for


class OutageSession(DatabaseSession):
    def __init__(self) -> None:
        self._closed = False

    async def commit_marker(self, seq: int, marker_id: str) -> TransactionOutcome:
        if Engine.down or self._closed:
            self._closed = True
            return TransactionOutcome.UNKNOWN
        Engine.store.add(marker_id)
        return TransactionOutcome.COMMITTED

    async def commit_marker_with_churn(self, seq: int, marker_id: str, churn_key: int,
                                       replace: bool) -> TransactionOutcome:
        outcome = await self.commit_marker(seq, marker_id)
        if outcome is TransactionOutcome.COMMITTED:
            Engine.churn_ops += 1
        return outcome

    async def try_write(self) -> bool:
        if Engine.down:
            self._closed = True
            return False
        return True

    async def ping(self) -> bool:
        return not Engine.down

    async def close(self) -> None:
        self._closed = True

    @property
    def is_closed(self) -> bool:
        return self._closed


@register_adapter
class OutageAdapter(FakeAdapter):
    engine = "orch-fake"
    capabilities = frozenset({Capability.TRANSACTIONAL_MARKERS, Capability.WORKLOAD_CHURN,
                              Capability.STRUCTURAL_INTEGRITY_CHECK, Capability.DURABILITY_SETTINGS})
    churn_key_space = 1000

    async def session(self, endpoint=None, timeout_s: float = 5.0) -> DatabaseSession:
        if Engine.down:
            raise ConnectionRefusedError("the database system is starting up")
        return OutageSession()

    async def prepare_harness_state(self) -> None:
        Engine.store.clear()

    async def marker_ids(self) -> set[str]:
        return set(Engine.store)

    async def integrity_check(self, timeout_s: float) -> IntegrityResult:
        return IntegrityResult(structural_errors=0, checksum_failures=0)

    async def trigger_checkpoint_and_await_active(self, timeout_s: float = 10.0) -> dict[str, Any]:
        """Simulate a successfully synchronized checkpoint for NL-C-02 tests."""
        self._checkpoint_baseline = {"checkpoint_lsn": 1000000, "redo_lsn": 1000000,
                                     "checkpoint_time": "2026-01-01 00:00:00"}
        return {
            "checkpointer_active": True,
            "checkpointer_pid": 9999,
            "wait_event_type": "Timeout",
            "wait_event": "CheckpointWriteDelay",
            "prior_checkpoint": self._checkpoint_baseline,
            "t_active_mono_ns": time.monotonic_ns(),
        }

    async def verify_checkpoint_aborted(self) -> dict[str, Any]:
        """Simulate verified checkpoint abort with LSN comparison."""
        baseline = getattr(self, "_checkpoint_baseline", {"checkpoint_lsn": 1000000, "redo_lsn": 1000000})
        return {
            "checkpoint_aborted": True,
            "current_checkpoint": {"checkpoint_lsn": 2000000, "redo_lsn": 1500000,
                                   "checkpoint_time": "2026-01-01 00:00:05"},
            "prior_checkpoint": baseline,
            "redo_advanced": True,
        }

    async def inject_idle_transaction(self) -> dict[str, Any]:
        """Simulate idle transaction injection for NL-M-05."""
        return {
            "supported": True,
            "pid": 8888,
            "backend_xmin": "5000",
            "state": "idle in transaction",
            "xact_age_s": 0.1,
        }

    async def check_idle_transaction(self, pid: int | None = None) -> dict[str, Any]:
        return {
            "pid": pid or 8888,
            "terminated_by_timeout": True,
            "still_idle": False,
        }

    async def evaluate_vacuum_bloat(self) -> dict[str, Any]:
        return {
            "dead_tuple_ratio": 0.05,
            "unvacuumed_dead_tuples": 10,
            "live_tuples": 200,
            "oldest_transaction_age_s": 0.0,
            "bloat_alert_fired": False,
        }

    async def exhaust_connections(self, hold_duration_s: float = 2.0) -> dict[str, Any]:
        return {
            "action": "connection_exhaustion",
            "t0_mono_ns": time.monotonic_ns(),
            "max_connections": 100,
            "superuser_reserved": 3,
            "held_connections": 97,
            "rejections_explicit": True,
            "rejection_error": "FATAL: remaining connection slots are reserved for roles with the SUPERUSER attribute",
            "superuser_slot_honoured": True,
        }

    async def revert_exhaust_connections(self) -> dict[str, Any]:
        return {"action": "connection_exhaustion_drained", "state": "active"}


class FakeFault(FaultInjector):
    """`process_kill` takes the engine down for OUTAGE_S; `config_reload` disturbs nothing.
    `lands=False` models a kill that never took effect."""

    fault_types = frozenset({"process_kill", "config_reload", "connection_exhaustion", "idle_in_transaction"})
    driver_name = "os_ssh"
    lands = True
    reverts: list[dict[str, Any]] = []

    async def preflight(self, node):
        return {"auto_conf_prior": "1s"} if self.fault_type == "config_reload" else {"ok": True}

    async def inject(self, node):
        if self.fault_type == "process_kill" and self.lands:
            Engine.down = True
            asyncio.get_running_loop().call_later(OUTAGE_S, lambda: setattr(Engine, "down", False))
        if self.fault_type == "connection_exhaustion":
            return {
                "action": "connection_exhaustion",
                "t0_mono_ns": time.monotonic_ns(),
                "max_connections": 100,
                "superuser_reserved": 3,
                "held_connections": 97,
                "rejections_explicit": True,
                "rejection_error": "FATAL: remaining connection slots are reserved",
                "superuser_slot_honoured": True,
            }
        return {"action": self.fault_type}

    async def revert(self, node, detail=None):
        FakeFault.reverts.append({"fault_type": self.fault_type, "detail": dict(detail or {})})
        return {"action": "revert"}


@pytest.fixture
def env(tmp_path, monkeypatch):
    Engine.down, FakeFault.lands, FakeFault.reverts = False, True, []
    Engine.store, Engine.churn_ops = set(), 0
    # The recovery loop must outlast the fake outage, or nothing ever records service
    # returning -- which the harness correctly refuses to score, and which then reads as a
    # flaky test rather than as the timing mistake it is.
    timeouts = dict(BASE_PROFILE.phase_timeouts_s, recovery=12.0)   # loop runs ~7 s
    monkeypatch.setattr(rto_decomposer, "SLO_SUSTAIN_S", 2.0)       # reachable: exercises the early exit
    profile = BASE_PROFILE.model_copy(update={
        "database": BASE_PROFILE.database.model_copy(update={"engine": "orch-fake"}),
        "driver_host": BASE_PROFILE.driver_host.model_copy(update={"host": "127.0.0.1", "run_dir": str(tmp_path)}),
        "phase_timeouts_s": timeouts,
    })

    async def hostname(endpoint, command, *, timeout_s, check=True):
        return RemoteResult(0, "BDB-QA-U22-30\n", "")

    async def no_offset(node, stream, samples=5):
        return 0.0

    class NoTail:
        def __init__(self, *a, **k):
            pass

        def start(self):
            pass

        async def stop(self):
            pass

    monkeypatch.setattr(orch, "run_once", hostname)
    monkeypatch.setattr(orch, "measure_clock_offset", no_offset)
    monkeypatch.setattr(orch, "LogTailer", NoTail)
    monkeypatch.setattr(orch, "WORKLOAD_RAMP_S", 0.3)
    monkeypatch.setattr(orch, "resolve", lambda fault, prof: FakeFault(prof, fault.type))
    monkeypatch.setattr(killswitch, "resolve_by_name", lambda section, driver, prof, fault_type="": FakeFault(prof, fault_type))
    return profile


def scenario(sid):
    sc = CATALOG.scenarios[sid]
    fast = sc.steady_state.model_copy(update={"duration_s": 1, "tps_min": 1})
    return sc.model_copy(update={"steady_state": fast})


def run(profile, sid):
    item = RunPlanItem(scenario=scenario(sid), env_class=profile.env_class, role="standalone", node=profile.nodes[0])
    return asyncio.run(TestOrchestrator(item, profile, RunOptions()).run())


def outcomes(results):
    return {r["predicate"]: r["outcome"] for r in results["verdict"]["results"]}


def why(results):
    """Everything needed to tell a real failure from a slow machine, in the assertion text."""
    verdict = results.get("verdict") or {}
    lines = [f"status={results['status']} error={results.get('error')}"]
    for r in verdict.get("results", []):
        lines.append(f"  [{r['outcome']}] {r['predicate']}  values={r['values']}  reason={r.get('reason')}")
    facts = results.get("facts", {})
    lines.append(f"  outage_observed={facts.get('outage_observed')} markers={facts.get('markers')}")
    lines.append(f"  measured={results.get('measured')}")
    lines.append(f"  phases={[(p['phase'], p['outcome'], round(p.get('duration_s', 0), 1)) for p in results.get('phases', [])]}")
    return "\n".join(lines)


def test_kill_run_measures_the_real_outage_and_passes(env):
    results = run(env, "NL-C-01")
    assert results["status"] == "passed", why(results)
    rto = results["measured"]["rto_first_write_s"]
    assert OUTAGE_S - 0.05 <= rto <= OUTAGE_S + 1.0
    assert results["facts"]["outage_observed"] is True and results["measured"]["starts_unattended"] is True


def test_checkpoint_crash_nlc02_passes_verdict(env):
    results = run(env, "NL-C-02")
    assert results["status"] == "passed", why(results)
    assert results["measured"]["corruption_count"] == 0
    assert results["measured"]["structural_integrity_errors"] == 0
    assert results["measured"]["starts_unattended"] is True
    assert results["measured"]["rpo_txn"] == 0


def test_large_transaction_crash_nlc03_passes_verdict(env):
    results = run(env, "NL-C-03")
    assert results["status"] == "passed", why(results)
    assert results["measured"]["corruption_count"] == 0
    assert results["measured"]["structural_integrity_errors"] == 0
    assert results["measured"]["starts_unattended"] is True
    assert results["measured"]["rpo_txn"] == 0
    assert "elle" in results["facts"]
    assert results["facts"]["elle"]["valid"] is True
    hist_file = Path(results["evidence_dir"]) / "history.edn"
    assert hist_file.stat().st_size > 0
    summary_file = Path(results["evidence_dir"]) / "summary.txt"
    print(f"\n[LOCAL TEST NL-C-03 RUN: {results['run_id']}]")
    print(f"history.edn lines: {len(hist_file.read_text().splitlines())}")
    print(f"elle facts: {results['facts']['elle']}")
    print(f"\n--- summary.txt ---\n{summary_file.read_text()}")


def test_concurrent_index_crash_nlc06_passes_verdict(env):
    results = run(env, "NL-C-06")
    assert results["status"] == "passed", why(results)
    assert results["measured"]["corruption_count"] == 0
    assert results["measured"]["structural_integrity_errors"] == 0
    assert results["measured"]["starts_unattended"] is True
    assert results["measured"]["rpo_txn"] == 0
    assert "concurrent_index" in results["facts"]
    assert results["facts"]["concurrent_index"]["cleanup_or_rebuild_succeeded"] is True




def test_kill_that_interrupted_nothing_cannot_pass(env):
    """Was: rto_first_write_s was the first write after T0 -- ~0.2 s -- and the run passed."""
    FakeFault.lands = False
    results = run(env, "NL-C-01")
    assert results["status"] == "failed", why(results)
    assert outcomes(results)["rto_first_write_s <= 60"] == "not_measured", why(results)
    assert results["measured"]["starts_unattended"] is False


def test_unattended_fault_stays_outstanding_until_cleanup_reverts_it(env):
    """Was: the ledger entry was marked reverted at the start of recovery with no action, so
    the kill switch had nothing to act on if the harness died mid-recovery."""
    results = run(env, "NL-C-01")
    entries = ledger_for(env).entries()
    assert [e.state for e in entries] == ["intent", "applied", "reverted"]
    assert entries[-1].detail["by"] == "killswitch"   # cleanup, via the ledger
    assert [r["fault_type"] for r in FakeFault.reverts] == ["process_kill"]
    assert results["phases"][-1]["phase"] == "cleanup"


def test_config_reload_is_reverted_with_the_preflight_value(env):
    """Was: never reverted -- the ALTER SYSTEM change stayed on the target."""
    results = run(env, "NL-M-07")
    assert results["status"] == "passed", why(results)
    (revert,) = FakeFault.reverts
    assert revert["fault_type"] == "config_reload"
    assert revert["detail"]["preflight"] == {"auto_conf_prior": "1s"}
    assert not ledger_for(env).outstanding()
    assert results["measured"]["dropped_connections"] == 0 and results["measured"]["failed_transactions"] == 0


def test_inconsistent_journals_abort_instead_of_passing(env, monkeypatch):
    """Was: the PhaseAbort was caught by its own `except Exception`; RPO was evaluated anyway."""
    real = orch.diff_from_journals
    monkeypatch.setattr(orch, "diff_from_journals", lambda run_dir, ids: (real(run_dir, ids)[0], 1))
    results = run(env, "NL-C-01")
    assert results["status"] == "aborted" and "journals inconsistent" in results["error"]
    assert results["verdict"] is None and "rpo_txn" not in results["measured"]
    assert not ledger_for(env).outstanding()   # cleanup still ran


def test_workload_journal_failure_aborts_the_run(env, monkeypatch):
    """Was: the worker died silently and the run went on with a crippled instrument."""
    class DiskFull(MarkerJournals):
        async def written(self, seq, uuid, t_pre):
            if seq > 30:
                raise OSError(28, "No space left on device")
            await super().written(seq, uuid, t_pre)

    monkeypatch.setattr(orch, "MarkerJournals", DiskFull)
    results = run(env, "NL-C-01")
    assert results["status"] == "aborted" and "No space left on device" in results["error"]
    assert results["verdict"] is None


def test_corruption_count_is_not_measured_when_a_part_is_unknown(env, monkeypatch):
    """Was: `(structural or 0) + (checksums or 0) + phantom` reported a real-looking count
    built partly from a measurement the engine never took."""
    from resilience_tests.adapters.base import IntegrityResult

    async def no_checkers(self, timeout_s):
        return IntegrityResult(structural_errors=None, checksum_failures=None)

    monkeypatch.setattr(OutageAdapter, "integrity_check", no_checkers)
    results = run(env, "NL-C-01")
    assert results["measured"]["corruption_count"] == "NOT_MEASURED"
    assert results["facts"]["corruption_parts"]["structural_integrity"] is None
    assert "cannot be summed" in results["facts"]["not_measured"]["corruption_count"]


def test_not_measured_verdict_says_why(env):
    """Was: every NOT_MEASURED predicate read 'no signal source was configured', even when the
    real cause was that the fault never interrupted service."""
    FakeFault.lands = False
    results = run(env, "NL-C-01")
    (rto,) = [r for r in results["verdict"]["results"] if r["predicate"].startswith("rto_first_write_s")]
    assert rto["outcome"] == "not_measured"
    assert "never saw this fault interrupt service" in rto["reason"]
    assert "no signal source was configured" not in rto["reason"]


def failing_revert(monkeypatch):
    async def boom(self, node, detail=None):
        raise RuntimeError("systemctl start failed: unit entered failed state")
    monkeypatch.setattr(FakeFault, "revert", boom)


def test_a_run_whose_cleanup_left_the_fault_applied_cannot_pass(env, monkeypatch):
    """Was: status was computed before cleanup and never revisited, so a revert that failed
    still reported STATUS: PASSED with the service left stopped -- green CI, faulted target."""
    failing_revert(monkeypatch)
    results = run(env, "NL-C-01")
    assert results["status"] != "passed"
    assert results["verdict"]["passed"] is True          # the database did behave; cleanup did not
    assert "may be left in a faulted state" in results["error"] and "killswitch" in results["error"]
    assert [e["fault_type"] for e in results["facts"]["cleanup_outstanding"]] == ["process_kill"]
    assert [e.state for e in ledger_for(env).outstanding()] == ["revert_failed"]


def test_cleanup_problem_does_not_hide_an_existing_failure(env, monkeypatch):
    """A verdict failure keeps its status; the cleanup problem is appended, not swapped in."""
    FakeFault.lands = False                              # the kill interrupted nothing -> failed
    failing_revert(monkeypatch)
    results = run(env, "NL-C-01")
    assert results["status"] == "failed"
    assert "may be left in a faulted state" in results["error"]


def test_summary_shows_the_cleanup_problem(env, monkeypatch):
    from resilience_tests.analysis.report import render_summary

    failing_revert(monkeypatch)
    summary = render_summary(run(env, "NL-C-01"))
    assert "STATUS: ERROR" in summary and "killswitch" in summary


def test_a_node_still_carrying_an_earlier_injection_is_refused(env):
    """Was: the target lock stops two runs overlapping, but nothing checked whether a previous
    run had died leaving a fault applied -- the new run measured a baseline on a faulted node."""
    ledger = ledger_for(env)
    stale = ledger.intent(run_id="NL-C-01-earlier", env_profile=env.name, fault_type="process_kill",
                          driver="os_ssh", node=env.nodes[0].name, detail={"section": "os_ssh"})
    ledger.transition(stale, "applied")
    # refused outright, like the other preconditions: no run directory, nothing measured
    with pytest.raises(SafetyViolation, match="still carries an injection from an earlier run"):
        run(env, "NL-C-01")
    # and the refusal must not leave the target lock held for the rest of the session
    ledger.transition(stale, "reverted")
    results = run(env, "NL-C-01")
    assert results["status"] == "passed", why(results)


def test_nl_m_05_execution(env):
    """NL-M-05: idle-in-transaction blocking vacuum execution through orchestrator."""
    results = run(env, "NL-M-05")
    assert results["status"] == "passed", why(results)
    m = results["measured"]
    assert m["rpo_txn"] == 0
    assert m["structural_integrity_errors"] == 0
    assert m["corruption_count"] == 0
    assert m["idle_in_transaction_session_timeout_enforced"] is True or m["bloat_alert_fired"] is True
    assert "idle_transaction" in results["facts"]

