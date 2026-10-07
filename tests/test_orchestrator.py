"""The phase machine end to end, against a fake engine and a fake fault driver.

The real orchestrator, workload driver, write prober, marker journals, injection ledger,
decomposer and threshold evaluator run; only the engine, the fault and SSH are fakes. These
cover what unit tests of the pure functions cannot: which phase records what, when the
ledger says an injection is undone, and which failures stop a verdict being issued.
"""

import asyncio
import re
from pathlib import Path
import time
from typing import Any

import pytest

from catalog.schema import load_catalog
from resilience_tests.analysis.elle_checker import ElleResult
from resilience_tests.adapters.base import Capability, DatabaseSession, IntegrityResult, TransactionOutcome, register_adapter
from resilience_tests.control import killswitch
from resilience_tests.control import orchestrator as orch
from resilience_tests.control.killswitch import ledger_for
from resilience_tests.control.matrix import RunPlanItem
from resilience_tests.control.orchestrator import RunOptions, TestOrchestrator
from resilience_tests.analysis import rto_decomposer
from resilience_tests.control.profile import load_profile
from resilience_tests.control.safety import SafetyViolation
from resilience_tests.execution.injectors.base import FaultInjector, FaultNotLanded
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
    lists: dict[int, list[int]] = {}   # Elle's list-append objects
    # what the interrupted `during` operation leaves behind after the fake crash
    during_in_progress = True
    after_during: dict[str, Any] = {}
    targeted_misses = 0     # NL-M-03: upcoming targeted kills that find the worker already gone


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

    async def commit_marker_list_append(self, seq, marker_id, read_key, append_key):
        outcome = await self.commit_marker(seq, marker_id)
        if outcome is not TransactionOutcome.COMMITTED:
            return outcome, []
        first = Engine.lists.get(read_key)
        Engine.lists.setdefault(append_key, []).append(seq)
        return outcome, [("r", read_key, None if first is None else list(first)),
                         ("append", append_key, seq), ("r", append_key, list(Engine.lists[append_key]))]

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
                              Capability.STRUCTURAL_INTEGRITY_CHECK, Capability.DURABILITY_SETTINGS,
                              Capability.LIST_APPEND_HISTORY})
    churn_key_space = 1000

    async def start_large_transaction(self):
        return {"in_progress": Engine.during_in_progress, "pid": 7777, "note": "fake"}

    async def verify_large_transaction(self):
        return {"rows_visible": 0, "parent_rows_visible": 0, "fk_violations": 0, **Engine.after_during}

    async def start_concurrent_index_build(self):
        return {"in_progress": Engine.during_in_progress, "pid": 7778, "phase": "building index: scanning table"}

    async def verify_concurrent_index(self):
        return {"index_left_invalid": True, "table_readable": True, "rebuild_succeeds": True, **Engine.after_during}

    async def start_autovacuum_worker(self):
        if not Engine.during_in_progress:
            return {"in_progress": False, "note": "no autovacuum worker vacuumed the table within 150 s"}
        return {"in_progress": True, "kill_target": {"pid": 9001, "title_marker": "autovacuum worker",
                                                     "relation": "resilience.avac_target"}}

    async def verify_autovacuum_resumed(self):
        return {"autovacuum_worker_respawned": True, "relations_eligible": 2,
                "relations_left_unvacuumed": 0, **Engine.after_during}

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

    async def checkpoint_in_flight_at_kill(self, log_lines) -> dict[str, Any]:
        """Simulate recovery that started from the pre-CHECKPOINT redo point."""
        return {"in_flight": True, "prior_redo_lsn": "0/F4240", "recovery_redo_start_lsn": "0/F4240",
                "note": "recovery started from the redo point that preceded the CHECKPOINT"}

    async def inject_idle_transaction(self) -> dict[str, Any]:
        """Simulate an idle transaction confirmed by the server for NL-M-05."""
        return {
            "supported": True,
            "pid": 8888,
            "state": "idle in transaction",
            "backend_xid": 5000,
            "backend_xmin": "5000",
            "xact_age_s": 0.1,
        }

    idle_timeout_sqlstates = ("25P03",)

    async def check_idle_transaction(self, pid: int | None = None) -> dict[str, Any]:
        """Path A: the session was ended by the timeout, and said so."""
        return {
            "pid": pid or 8888,
            "terminated_by_timeout": True,
            "still_idle": False,
            "termination_sqlstate": "25P03",
        }

    async def evaluate_vacuum_bloat(self) -> dict[str, Any]:
        return {
            "dead_tuple_ratio": 0.05,
            "unvacuumed_dead_tuples": 10,
            "live_tuples": 200,
            "tuple_bloat_ratio": 1.05,
            "oldest_transaction_age_s": 0.0,
        }

    async def probe_vacuum_horizon(self) -> dict[str, Any]:
        return {"supported": True, "dead_not_removable": 4200, "removable_cutoff": 5000}

    def idle_session_timeout_s(self, observed: dict[str, str]) -> float:
        from resilience_tests.adapters.postgresql.adapter import parse_pg_interval_s
        return parse_pg_interval_s(observed.get("idle_in_transaction_session_timeout"))

    async def exhaust_connections(self, hold_s: float) -> dict[str, Any]:
        return {
            "action": "connection_exhaustion",
            "t0_mono_ns": time.monotonic_ns(),
            "max_connections": 100,
            "superuser_reserved": 3,
            "held_connections": 97,
            "rejected_explicit": 53,
            "rejected_other": 0,
            "rejections_explicit": True,
            "superuser_slot_honoured": True,
            "hold_s": hold_s,
            "connections_recover_after_release": True,
        }

    async def revert_exhaust_connections(self) -> dict[str, Any]:
        return {"action": "flood sessions terminated", "remaining": 0}


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
        if self.fault_type == "idle_in_transaction":
            # as the real driver: applied through the run's adapter, confirmed or not landed
            detail = dict(await self.adapter.inject_idle_transaction())
            if not detail.get("supported"):
                raise FaultNotLanded(f"idle session not established: {detail.get('error')}", detail)
            return detail | {"action": "idle_in_transaction", "t0_mono_ns": time.monotonic_ns()}
        if self.fault_type == "process_kill" and self.kill_target is not None and Engine.targeted_misses:
            Engine.targeted_misses -= 1      # the worker ended first: nothing was touched
            raise FaultNotLanded("pid 9001 was not killed (NOPROC)", {"changed_nothing": True, "reason": "NOPROC"})
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
                "superuser_slot_honoured": True,
                "connections_recover_after_release": True,
            }
        if self.kill_target is not None:
            return {"action": "kill -9 9001", "target_pid": 9001, "landed": self.lands,
                    "t0_mono_ns": time.monotonic_ns()}
        return {"action": self.fault_type}

    async def confirm(self, node, detail):
        return {"fault_confirmed": self.lands}

    async def revert(self, node, detail=None):
        FakeFault.reverts.append({"fault_type": self.fault_type, "detail": dict(detail or {}),
                                  "adapter": self.adapter})
        if self.fault_type == "idle_in_transaction" and self.adapter is not None:
            await self.adapter.close_idle_transaction()
        return {"action": "revert"}


@pytest.fixture(params=["builtin"])
def env(request, tmp_path, monkeypatch):
    Engine.down, FakeFault.lands, FakeFault.reverts = False, True, []
    Engine.store, Engine.churn_ops, Engine.lists = set(), 0, {}
    Engine.during_in_progress, Engine.after_during, Engine.targeted_misses = True, {}, 0
    # The recovery loop must outlast the fake outage, or nothing ever records service
    # returning -- which the harness correctly refuses to score, and which then reads as a
    # flaky test rather than as the timing mistake it is.
    timeouts = dict(BASE_PROFILE.phase_timeouts_s, recovery=12.0)   # loop runs ~7 s
    monkeypatch.setattr(rto_decomposer, "SLO_SUSTAIN_S", 2.0)       # reachable: exercises the early exit
    monkeypatch.setattr(orch, "IDLE_TRANSACTION_MIN_SOAK_S", 2.0)
    generator = request.param
    profile = BASE_PROFILE.model_copy(update={
        "database": BASE_PROFILE.database.model_copy(update={"engine": "orch-fake"}),
        "driver_host": BASE_PROFILE.driver_host.model_copy(update={"host": "127.0.0.1", "run_dir": str(tmp_path)}),
        "phase_timeouts_s": timeouts,
        "workload": BASE_PROFILE.workload.model_copy(update={"generator": generator}),
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
    monkeypatch.setattr(orch, "DiskUsageProber", NoTail)
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


def elle_clean(monkeypatch):
    """Elle itself is not run in unit tests; its verdict is stubbed as clean."""
    def check(history_path, out_dir, **kw):
        return ElleResult(valid=True, anomalies_count=0, operations=1)
    monkeypatch.setattr(orch.ElleChecker, "check", staticmethod(check))


def test_large_transaction_crash_nlc03_passes_verdict(env, monkeypatch):
    elle_clean(monkeypatch)
    results = run(env, "NL-C-03")
    assert results["status"] == "passed", why(results)
    m = results["measured"]
    assert m["operation_in_progress_at_fault"] is True
    assert m["large_txn_rows_visible"] == 0 and m["large_txn_parent_rows_visible"] == 0
    assert m["fk_violations"] == 0 and m["elle_anomalies_count"] == 0
    assert results["facts"]["during"]["operation"] == "large_transaction"


def test_nlc03_history_records_what_the_database_returned(env, monkeypatch):
    """Was: every :ok claimed a read of [seq] -- the transaction's own value, never read."""
    elle_clean(monkeypatch)
    results = run(env, "NL-C-03")
    lines = (Path(results["evidence_dir"]) / "history.edn").read_text().splitlines()
    oks = [ln for ln in lines if ":type :ok" in ln]
    assert oks, "no committed transaction recorded"
    # some list was read back holding more than the transaction's own append
    assert any(re.search(r"\[:r \d+ \[\d+ \d+", ln) for ln in oks)
    # a worker whose transaction ended :info never issues another under that process id
    info_procs = {re.search(r":process (\d+)", ln).group(1) for ln in lines if ":type :info" in ln}
    for proc in info_procs:
        after = lines[next(i for i, ln in enumerate(lines) if ":type :info" in ln and f":process {proc}," in ln) + 1:]
        assert not any(f":process {proc}," in ln for ln in after), proc


def test_nlc03_without_elle_is_not_measured_and_fails(env, monkeypatch, tmp_path):
    real = orch.ElleChecker.check
    monkeypatch.setattr(orch.ElleChecker, "check",
                        staticmethod(lambda h, o, **kw: real(h, o, jar=tmp_path / "missing.jar")))
    results = run(env, "NL-C-03")
    assert results["status"] == "failed", why(results)
    assert outcomes(results)["elle_anomalies_count == 0"] == "not_measured"
    assert results["facts"]["elle"]["valid"] is None


def test_nlc03_partial_rows_after_recovery_fail(env, monkeypatch):
    elle_clean(monkeypatch)
    Engine.after_during = {"rows_visible": 1_000, "fk_violations": 3}
    results = run(env, "NL-C-03")
    assert results["status"] == "failed", why(results)
    o = outcomes(results)
    assert o["large_txn_rows_visible == 0"] == "fail" and o["fk_violations == 0"] == "fail"


def test_fault_is_not_injected_when_the_operation_is_not_running(env, monkeypatch):
    """Was: the kill was sent after a fixed 50 ms sleep, running or not."""
    elle_clean(monkeypatch)
    Engine.during_in_progress = False
    results = run(env, "NL-C-03")
    assert results["status"] == "aborted", why(results)
    assert "not in progress" in results["error"]
    assert results["timing"]["t0_mono_ns"] is None    # no fault was ever injected


def test_concurrent_index_crash_nlc06_passes_verdict(env):
    results = run(env, "NL-C-06")
    assert results["status"] == "passed", why(results)
    m = results["measured"]
    assert m["operation_in_progress_at_fault"] is True
    assert m["index_left_invalid"] is True and m["table_readable"] is True and m["rebuild_succeeds"] is True
    assert "scenario_objects_cleanup_error" not in results["facts"]


def test_nlc06_index_found_valid_means_the_kill_missed(env):
    """An index that finished building before the kill is VALID -- that run tested nothing."""
    Engine.after_during = {"index_left_invalid": False}
    results = run(env, "NL-C-06")
    assert results["status"] == "failed", why(results)
    assert outcomes(results)["index_left_invalid == true"] == "fail"


def run_nlm03(profile):
    """NL-M-03 with its five cycles, settling 0.1 s instead of 10 s between them."""
    sc = scenario("NL-M-03")
    sc = sc.model_copy(update={"repeat": sc.repeat.model_copy(update={"interval_s": 0.1})})
    item = RunPlanItem(scenario=sc, env_class=profile.env_class, role="standalone", node=profile.nodes[0])
    return asyncio.run(TestOrchestrator(item, profile, RunOptions()).run())


def failing(results):
    return {p for p, o in outcomes(results).items() if o != "pass"}


def test_autovacuum_kill_nlm03_passes_verdict(env):
    results = run_nlm03(env)
    assert results["status"] == "passed", why(results)
    m = results["measured"]
    assert m["kills_landed"] == 5 and m["cycles_recovered"] == m["cycles_run"] == 5
    assert m["autovacuum_worker_respawned"] is True and m["relations_left_unvacuumed"] == 0
    assert all(c["target_pid"] == 9001 for c in results["facts"]["cycles"])


def test_nlm03_unvacuumed_relation_fails(env):
    Engine.after_during = {"relations_left_unvacuumed": 1, "unvacuumed": ["resilience.churn"]}
    results = run_nlm03(env)
    assert results["status"] == "failed", why(results)
    assert failing(results) == {"relations_left_unvacuumed == 0"}, why(results)


def test_nlm03_no_respawn_fails(env):
    Engine.after_during = {"autovacuum_worker_respawned": False}
    results = run_nlm03(env)
    assert failing(results) == {"autovacuum_worker_respawned == true"}, why(results)


def test_nlm03_zero_eligible_is_not_measured(env):
    """A vacuous '0 of 0 relations left unvacuumed' must not pass."""
    Engine.after_during = {"relations_eligible": 0, "relations_left_unvacuumed": None,
                           "relations_left_unvacuumed_why": "no relation became eligible for autovacuum during verification"}
    results = run_nlm03(env)
    assert outcomes(results)["relations_left_unvacuumed == 0"] == "not_measured", why(results)
    assert "no relation became eligible" in results["facts"]["not_measured"]["relations_left_unvacuumed"]


def test_nlm03_lost_commit_fails(env, monkeypatch):
    async def lose_one(self):
        ids = set(Engine.store)
        ids.discard(next(iter(ids)))       # an acknowledged marker missing after recovery
        return ids
    monkeypatch.setattr(OutageAdapter, "marker_ids", lose_one)
    results = run_nlm03(env)
    assert "rpo_txn == 0" in failing(results), why(results)


def test_nlm03_corruption_fails(env, monkeypatch):
    async def corrupt(self, timeout_s):
        return IntegrityResult(structural_errors=1, checksum_failures=0)
    monkeypatch.setattr(OutageAdapter, "integrity_check", corrupt)
    results = run_nlm03(env)
    assert failing(results) == {"corruption_count == 0", "structural_integrity_errors == 0"}, why(results)


def test_nlm03_no_worker_aborts(env):
    Engine.during_in_progress = False
    results = run_nlm03(env)
    assert results["status"] == "aborted", why(results)
    assert "no autovacuum worker" in results["error"]
    assert results["timing"]["t0_mono_ns"] is None          # nothing was killed


def test_nlm03_miss_is_retried_not_counted(env):
    Engine.targeted_misses = 2           # first cycle: two misses, then a kill lands
    results = run_nlm03(env)
    assert results["status"] == "passed", why(results)
    assert results["measured"]["kills_landed"] == 5          # misses never counted
    assert len(results["facts"]["cycles"][0]["missed_attempts"]) == 2


def test_nlm03_three_misses_abort(env):
    Engine.targeted_misses = 3
    results = run_nlm03(env)
    assert results["status"] == "aborted", why(results)
    assert "after 3 attempts" in results["error"]
    assert len(results["facts"]["missed_attempts"]) == 3


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
    assert m["idle_in_transaction_session_timeout_enforced"] is True
    assert str(m["bloat_alert_fired"]) == "NOT_MEASURED"   # no alert source is connected
    assert "idle_transaction" in results["facts"]


def test_orchestrator_runs_with_pgbench_generator(env, monkeypatch):
    """T048: Orchestrator executes cleanly with generator=pgbench when channel double is registered."""
    from pathlib import Path
    from resilience_tests.execution.workload.interface import register_record_channel_factory
    from resilience_tests.execution.workload.record_channel import InMemoryRecordChannel

    fake_pg = str(Path(__file__).parent / "fakes" / "fake_pgbench.py")
    profile = env.model_copy(update={
        "workload": env.workload.model_copy(update={
            "generator": "pgbench",
            "pgbench_bin": fake_pg,
        })
    })

    channel = InMemoryRecordChannel()
    register_record_channel_factory(lambda: channel, tests_passed=True)

    try:
        from resilience_tests.adapters.base import Capability, PgbenchLaunchSpec
        monkeypatch.setattr(OutageAdapter, "capabilities", frozenset({
            Capability.TRANSACTIONAL_MARKERS,
            Capability.WORKLOAD_CHURN,
            Capability.STRUCTURAL_INTEGRITY_CHECK,
            Capability.DURABILITY_SETTINGS,
            Capability.LIST_APPEND_HISTORY,
            Capability.PGBENCH_WORKLOAD,
        }))
        monkeypatch.setattr(
            OutageAdapter,
            "pgbench_launch",
            lambda self, shape, launch, client: PgbenchLaunchSpec(
                script="SELECT 1;\n",
                variables={},
                connection=self.node.client,
                application_name=f"resilience-pgbench-test-{launch}",
            ),
        )

        async def fake_server_version(self):
            return "PostgreSQL 17.11.1.0 on x86_64"

        async def fake_sessions(self, name):
            return 0

        monkeypatch.setattr(OutageAdapter, "server_version", fake_server_version)
        monkeypatch.setattr(OutageAdapter, "sessions_with_application_name", fake_sessions)


        item = RunPlanItem(scenario=scenario("NL-C-01"), env_class=profile.env_class, role="standalone", node=profile.nodes[0])
        orch_inst = TestOrchestrator(item, profile, RunOptions(stop_before_fault=True))
        results = asyncio.run(orch_inst.run())
        assert results["facts"]["workload_generator"] == "pgbench"
        assert "17.11" in results["facts"]["pgbench_version"]
    finally:
        register_record_channel_factory(None, tests_passed=False)


