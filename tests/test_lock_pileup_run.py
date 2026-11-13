"""NL-N-I03 through the real orchestrator: all nine phases, the real lock pile-up driver and
behaviour, the real workload and probes, against a fake engine that freezes its contended table
the way a schema change waiting for its lock does.

The plans are shortened (the read is held 1 s, the lock timeout is 2 s, no warm-up) so a run
takes seconds; the catalog's own thresholds are judged unchanged, except the others' throughput
budget: a 10% loss over a minute is a 6 s pause, and the shortened run observes the others for
only a few seconds after the schema change, so its budget is scaled to the window it can observe.

Runs 3a and 3b are catalogued on the TPC-C workload and its stock table. The fake engine has no
TPC-C, so here they run on the marker workload and the harness's contended table (run 3c's
workload), with their own rules -- less the two that only TPC-C produces (partial commits and
its consistency rules), which the TPC-C tests cover. Latency is still timed from the intended
start, as the catalog says.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from resilience_tests.adapters.base import Capability, DatabaseSession, TransactionOutcome, register_adapter
from resilience_tests.analysis.predicates import NOT_MEASURED
from resilience_tests.control import killswitch
from resilience_tests.control import orchestrator as orch
from resilience_tests.control.killswitch import ledger_for
from resilience_tests.control.matrix import RunPlanItem
from resilience_tests.control.orchestrator import RunOptions, TestOrchestrator
from resilience_tests.execution.injectors import lock_pileup as lock_driver_mod
from resilience_tests.execution.injectors.lock_pileup import OsSshLockPileupDriver
from resilience_tests.execution.remote import RemoteResult
from tests.test_orchestrator import Engine, OutageAdapter, OutageSession, outcomes, scenario, why
from tests.test_orchestrator import env  # noqa: F401  (env is a fixture)

READ_HELD_S = 1.0       # ddl_after_s: the read alone, before the schema change
LOCK_TIMEOUT_S = 2.0    # ddl_lock_timeout_s of the shortened runs 3a and 3c
HOLD_S = 8.0            # fault.duration: the hold's upper bound
BASELINE_S = 6          # longer than the fixture's 2 s SLO sustain, so rto_to_slo_s is measurable
OTHERS_BUDGET = "other_tables_tps_lost_pct <= 10"
TPCC_ONLY = {"partial_commits == 0", "tpcc_consistency_violations == 0", "aborted_markers_present == 0"}
SHORT_OTHERS_BUDGET = "other_tables_tps_lost_pct <= 75"   # ~2.6 s paused in a few seconds observed
QUEUE_DRAIN_S = 0.6     # a queued table user finishes this long after the queue clears (longer than
                        # the behaviour's state poll, so a window closed at the clear misses it)


class Table:
    """The fake engine's contended table and the pile-up on it, shared by every session."""

    unlocked: asyncio.Event | None = None
    # how the fake schema change ends: "lock_timeout" when its timeout fires, "completed" once
    # the read is released, or after `rewrite_s` with no blocker
    ends: str = "lock_timeout"
    chain_seen: bool = True
    state: dict[str, Any] = {}
    calls: list[str] = []
    probe_column: bool = False
    keep_column: bool = False        # a revert that cannot drop the probe column


def reset_table() -> None:
    Table.unlocked, Table.ends, Table.chain_seen = None, "lock_timeout", True
    Table.state, Table.calls, Table.probe_column, Table.keep_column = {}, [], False, False


class ContendedSession(OutageSession):
    async def commit_marker_with_contended_row(self, seq: int, marker_id: str, key: int) -> TransactionOutcome:
        if Table.unlocked is not None and not Table.unlocked.is_set():
            await Table.unlocked.wait()      # queued behind the schema change
            await asyncio.sleep(QUEUE_DRAIN_S)
        return await self.commit_marker(seq, marker_id)


@register_adapter
class PileupEngine(OutageAdapter):
    engine = "orch-lock-fake"
    capabilities = OutageAdapter.capabilities | {Capability.CONTENDED_TABLE}
    contended_key_space = 100

    async def session(self, endpoint=None, timeout_s: float = 5.0) -> DatabaseSession:
        if Engine.down:
            raise ConnectionRefusedError("the database system is starting up")
        return ContendedSession()

    async def prepare_contended_table(self) -> dict[str, Any]:
        Table.calls.append("prepare")
        return {"table": "fake.lock_target", "rows": self.contended_key_space}

    async def observe_lock_settings(self) -> dict[str, str]:
        return {"lock_timeout": "0", "log_lock_waits": "off"}

    def crash_restart_log_patterns(self) -> tuple[str, ...]:
        return (r"terminating any other active server processes",)

    async def start_lock_pileup(self, *, blocker, ddl, ddl_lock_timeout_s, ddl_after_s,
                                table="contended_table") -> dict[str, Any]:
        Table.calls.append("start")
        Table.unlocked = asyncio.Event()
        Table.unlocked.set()                 # a read alone blocks no table user
        Table.state = {"started": True, "blocker": blocker, "ddl": ddl, "ddl_lock_timeout_s": ddl_lock_timeout_s,
                       "reader_pid": 11 if blocker == "long_read" else None, "ddl_pid": 22,
                       "ddl_issued_mono_ns": None, "ddl_ended_mono_ns": None, "ddl_outcome": None,
                       "ddl_sqlstate": None, "chain_confirmed": None, "chain": None, "queue_max": 0}
        self._ddl = asyncio.create_task(self._schema_change(ddl_after_s, ddl_lock_timeout_s))
        await asyncio.sleep(0)
        return {"supported": True, "t0_mono_ns": time.monotonic_ns(), "table": "fake.lock_target"}

    async def _schema_change(self, after_s: float, timeout_s: float) -> None:
        st = Table.state
        await asyncio.sleep(after_s)
        st["ddl_issued_mono_ns"] = time.monotonic_ns()
        Table.unlocked.clear()               # the table users now queue behind it
        await asyncio.sleep(0.3)
        if Table.chain_seen:
            st.update(chain_confirmed=True, queue_max=5,
                      chain={"ddl": {"pid": 22, "blocked_by": [st["reader_pid"]] if st["reader_pid"] else None},
                             "blocker": {"pid": st["reader_pid"]} if st["reader_pid"] else None})
        else:
            st["chain_confirmed"] = False
        if Table.ends == "completed":
            return                           # waits for the read: ends at release
        await asyncio.sleep(max(0.0, timeout_s - 0.3))
        self._end_ddl("lock_timeout" if Table.ends == "lock_timeout" else "completed")

    def _end_ddl(self, outcome: str) -> None:
        st = Table.state
        if st.get("ddl_outcome") is None and st.get("ddl_issued_mono_ns") is not None:
            st.update(ddl_outcome=outcome, ddl_ended_mono_ns=time.monotonic_ns(),
                      ddl_sqlstate="55P03" if outcome == "lock_timeout" else None)
            Table.probe_column = outcome == "completed"
        Table.unlocked.set()

    async def lock_pileup_state(self) -> dict[str, Any]:
        return dict(Table.state) if Table.state else {"started": False, "ddl_outcome": None}

    async def end_lock_pileup(self) -> dict[str, Any]:
        Table.calls.append("end")
        if Table.state:
            self._end_ddl("completed")       # the read is committed: a waiting schema change finishes
        return {"read_committed": True}

    async def revert_lock_pileup(self) -> dict[str, Any]:
        Table.calls.append("revert")
        if not Table.keep_column:
            Table.probe_column = False
        return {"sessions_remaining": 0, "probe_columns_remaining": int(Table.probe_column)}

    async def lock_pileup_leftovers(self) -> dict[str, Any]:
        return {"sessions": 0, "probe_columns": ["fake.lock_target.resilience_probe_col"] if Table.probe_column else []}


class ActiveService:
    def __init__(self, *a, **k):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return None

    async def run(self, command, *, timeout_s, check=True):
        return RemoteResult(0, "active", "")


class QuietTail:
    """The engine's log reaches the harness, and says nothing about a crash."""

    def __init__(self, node, stream) -> None:
        self.stream = stream

    def start(self) -> None:
        self.stream.emit("log_tailer", "log_line", line="LOG:  checkpoint starting: time")

    async def stop(self) -> None:
        self.stream.emit("log_tailer", "log_line", line="LOG:  checkpoint complete")


@pytest.fixture
def lock_env(env, monkeypatch):
    reset_table()
    profile = env.model_copy(update={"database": env.database.model_copy(update={"engine": "orch-lock-fake"})})
    monkeypatch.setattr(lock_driver_mod, "RemoteHost", ActiveService)
    monkeypatch.setattr(orch, "LogTailer", QuietTail)
    monkeypatch.setattr(orch, "resolve", lambda fault, prof: OsSshLockPileupDriver(prof, fault.type))
    monkeypatch.setattr(killswitch, "resolve_by_name",
                        lambda section, driver, prof, fault_type="": OsSshLockPileupDriver(prof, fault_type))
    return profile


def short(sid: str, **plan):
    sc = scenario(sid)
    update: dict[str, Any] = {}
    if sc.fault.lock_pileup.table == "tpcc_stock":      # see the module docstring
        marker = scenario("NL-N-I03c").workload.model_copy(update={"latency_from": sc.workload.latency_from})
        plan = {"table": "contended_table", **plan}
        update = {"workload": marker,
                  "requires": ["contended_table" if r == "tpcc_workload" else r for r in sc.requires]}
    pileup = sc.fault.lock_pileup.model_copy(update=plan)
    accept = [SHORT_OTHERS_BUDGET if a == OTHERS_BUDGET else a for a in sc.accept if a not in TPCC_ONLY]
    return sc.model_copy(update={"fault": sc.fault.model_copy(update={"duration": HOLD_S, "lock_pileup": pileup}),
                                 "steady_state": sc.steady_state.model_copy(update={"duration_s": BASELINE_S,
                                                                                    "warmup_s": 0.0}),
                                 "accept": accept, **update})


def run(profile, sc):
    item = RunPlanItem(scenario=sc, env_class=profile.env_class, role="standalone", node=profile.nodes[0])
    return asyncio.run(TestOrchestrator(item, profile, RunOptions()).run())


def run_3a(profile):
    return run(profile, short("NL-N-I03a", ddl_after_s=READ_HELD_S, ddl_lock_timeout_s=LOCK_TIMEOUT_S))


def test_run_3a_passes_when_the_lock_timeout_clears_the_queue(lock_env):
    results = run_3a(lock_env)
    assert results["status"] == "passed", why(results)
    m = results["measured"]
    assert m["fault_confirmed"] is True and m["ddl_lock_timeout_fired"] is True and m["ddl_completed"] is False
    assert LOCK_TIMEOUT_S <= m["queue_freeze_s"] < LOCK_TIMEOUT_S + 1.0
    assert m["blocker_visible"] is True and m["queue_max_sessions"] == 5
    assert m["server_crash_restarts"] == 0 and m["rpo_txn"] == 0
    # the table users waited out the freeze; the others kept committing
    assert m["longest_table_wait_s"] >= 1.0
    assert isinstance(m["other_tables_tps_lost_pct"], float)
    assert isinstance(m["writer_max_stall_s"], float)

    # the hold ended when the queue cleared, not at fault.duration; T1 is the schema change's end
    hold = results["facts"]["hold"]
    assert (hold["end_mono_ns"] - hold["start_mono_ns"]) / 1e9 < HOLD_S - 2.0
    assert results["facts"]["lock_pileup_state"]["ddl_outcome"] == "lock_timeout"
    # released in the hold, reverted by cleanup -- after the workload stopped -- and journalled
    assert Table.calls[:2] == ["prepare", "start"] and Table.calls.count("revert") >= 1
    assert Table.calls.index("end") < Table.calls.index("revert")
    assert not Table.probe_column
    assert not [e for e in ledger_for(lock_env).outstanding() if e.run_id == results["run_id"]]
    assert not results["facts"].get("cleanup_outstanding")
    with open(f"{results['evidence_dir']}/summary.txt") as summary:
        text = summary.read()
    assert "lock pile-up behind a schema change (NL-N-I03)" in text and "its lock timeout: 2 s" in text


def test_the_hold_counts_the_waits_of_the_transactions_queued_behind_the_schema_change(lock_env):
    """Was: the load window closed as soon as the schema change ended, before the table users
    queued behind it had finished -- their waits fell after the hold, and 3a's hold p99 came
    out below the baseline's (oltp_p99_ratio 0.73 on the lab target)."""
    results = run_3a(lock_env)
    assert results["status"] == "passed", why(results)
    load = results["facts"]["hold"]["load"]
    assert load["p99_ms"] >= 1000.0, load      # the queued waits (~2 s here) are in the hold
    assert results["measured"]["oltp_p99_ratio"] > 1.0


def test_run_3a_fails_when_the_schema_change_waited_for_its_blocker(lock_env):
    Table.ends = "completed"     # as if no lock timeout reached the server: it waits for the read
    results = run_3a(lock_env)
    assert results["status"] == "failed", why(results)
    o = outcomes(results)
    assert o["ddl_lock_timeout_fired == true"] == "fail"
    assert o["queue_freeze_s <= 6.0"] == "fail"       # frozen until the hold ran out
    assert results["measured"]["ddl_completed"] is True


def test_a_pile_up_whose_chain_was_never_seen_cannot_pass(lock_env):
    Table.chain_seen = False
    results = run_3a(lock_env)
    assert results["status"] == "failed", why(results)
    assert outcomes(results)["fault_confirmed == true"] == "fail"
    assert results["measured"]["blocker_visible"] is False


def test_a_crash_restart_in_the_engine_log_fails_run_3a(lock_env, monkeypatch):
    class CrashTail(QuietTail):
        def start(self) -> None:
            super().start()
            # during the hold: the tailer starts at baseline, the fault lands ~1.5 s after it ends
            asyncio.get_running_loop().call_later(BASELINE_S + 3.0, lambda: self.stream.emit(
                "log_tailer", "log_line", line="LOG:  terminating any other active server processes"))
    monkeypatch.setattr(orch, "LogTailer", CrashTail)
    results = run_3a(lock_env)
    assert results["status"] == "failed", why(results)
    assert results["measured"]["server_crash_restarts"] == 1
    assert outcomes(results)["server_crash_restarts == 0"] == "fail"


def test_without_the_engine_log_a_crash_restart_is_not_measured(lock_env, monkeypatch):
    class NoTail(QuietTail):
        def start(self) -> None:
            pass

        async def stop(self) -> None:
            pass
    monkeypatch.setattr(orch, "LogTailer", NoTail)
    results = run_3a(lock_env)
    assert results["status"] == "failed", why(results)
    assert str(results["measured"]["server_crash_restarts"]) == str(NOT_MEASURED)


def test_run_3b_is_a_finding_the_freeze_lasts_until_the_hold_releases_the_read(lock_env):
    Table.ends = "completed"
    results = run(lock_env, short("NL-N-I03b", ddl_after_s=READ_HELD_S))
    assert results["status"] == "passed", why(results)
    m = results["measured"]
    assert m["ddl_completed"] is True and m["ddl_lock_timeout_fired"] is False
    assert m["queue_freeze_s"] >= HOLD_S - READ_HELD_S - 1.0      # reported, not judged
    with open(f"{results['evidence_dir']}/summary.txt") as summary:
        assert "its lock timeout: none" in summary.read()          # 0 means none, not "0.0 s"
    assert m["longest_table_wait_s"] >= 2.0


def test_a_setting_changed_during_the_run_fails_the_cleanup_check(lock_env, monkeypatch):
    reads = iter([{"lock_timeout": "0", "log_lock_waits": "off"}, {"lock_timeout": "5s", "log_lock_waits": "off"}])

    async def observed(self) -> dict[str, str]:
        return next(reads)
    monkeypatch.setattr(PileupEngine, "observe_lock_settings", observed)
    results = run_3a(lock_env)
    assert results["status"] == "failed", why(results)
    assert outcomes(results)["settings_unchanged == true"] == "fail"
    assert results["facts"]["settings_changed"] == {"lock_timeout": {"before": "0", "after": "5s"}}


def test_a_probe_column_the_revert_could_not_drop_fails_the_run(lock_env):
    Table.ends, Table.keep_column = "completed", True
    results = run(lock_env, short("NL-N-I03b", ddl_after_s=READ_HELD_S))
    assert results["status"] == "failed", why(results)
    assert outcomes(results)["probe_column_absent == true"] == "fail"


def test_run_3c_is_a_finding_a_rewrite_holds_the_lock_and_its_timeout_never_fires(lock_env):
    Table.ends = "rewrite"
    results = run(lock_env, short("NL-N-I03c", ddl_lock_timeout_s=LOCK_TIMEOUT_S))
    assert results["status"] == "passed", why(results)
    m = results["measured"]
    assert m["fault_confirmed"] is True and m["ddl_completed"] is True and m["ddl_lock_timeout_fired"] is False
    assert m["blocker_visible"] is True      # no blocker to name: the schema change itself holds the lock
