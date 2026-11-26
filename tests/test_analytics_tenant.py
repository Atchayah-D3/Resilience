"""NL-N-I01b, the misbehaving tenant (specs/008): the report sessions' own setting changes and their
answers, the hard-limit attempt, the memory guard, and the rules that must still hold. Every rule
here has a test that fails for the right reason; 1a (NL-N-I01a) produces none of these measures."""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import asyncpg
import pytest
from pydantic import ValidationError

from catalog.schema import AnalyticsLoad, MemoryGuard as GuardConfig, load_scenario
from resilience_tests.adapters.postgresql.analytics import AnalyticsMixin, attempt_session
from resilience_tests.analysis.predicates import NOT_MEASURED
from resilience_tests.control.faults.analytics import (AnalyticsLoadFault, PhaseAbort, overrides_summary,
                                                       same_setting, tenant_lines)
from resilience_tests.execution.probes.memory_guard import (GuardRules, MemoryGuard, Reading, command, parse_reading,
                                                            trip_reason)
from tests.test_analytics_fault import window_cache
from tests.test_analytics_run import SETTINGS

CATALOG = Path(__file__).resolve().parents[1] / "catalog" / "NL"
OVERRIDES = {"max_parallel_workers_per_gather": "8", "work_mem": "1GB", "statement_timeout": "0"}
TENANT_MEASURES = ["overrides_recorded", "temp_limit_override_refused", "role_settings_unchanged",
                   "server_crash_restarts", "writer_max_gap_s", "report_helpers_total_max", "temp_gb_written",
                   "limit_test_2_ran_s", "reports_ran_s", "memory_guard_tripped", "available_memory_min_mb",
                   "swapped_out_max_mb"]


class Stream:
    def __init__(self) -> None:
        self.events: list[tuple[str, str, dict[str, Any]]] = []

    def emit(self, source: str, kind: str, **data: Any) -> None:
        self.events.append((source, kind, data))


# --- the catalog -----------------------------------------------------------------------------------

def test_session_overrides_need_a_memory_guard():
    with pytest.raises(ValidationError, match="needs a memory_guard"):
        AnalyticsLoad(session_overrides={"work_mem": "1GB"})


def test_yaml_numbers_become_setting_text_and_names_must_be_identifiers():
    plan = AnalyticsLoad(session_overrides={"statement_timeout": 0, "max_parallel_workers_per_gather": 8},
                         hard_limit_attempts={"temp_file_limit": -1}, memory_guard=GuardConfig())
    assert plan.session_overrides == {"statement_timeout": "0", "max_parallel_workers_per_gather": "8"}
    assert plan.hard_limit_attempts == {"temp_file_limit": "-1"}
    with pytest.raises(ValidationError, match="snake_case identifiers"):
        AnalyticsLoad(session_overrides={"work_mem; drop": "1GB"}, memory_guard=GuardConfig())


def test_a_guard_must_wait_longer_for_a_reading_than_it_reads():
    with pytest.raises(ValidationError, match="stale_s must be longer"):
        GuardConfig(interval_s=1.0, stale_s=1.0)


def test_1b_judges_only_what_must_still_hold_and_1a_is_unchanged():
    b = load_scenario(CATALOG / "NL-N-I01b.yaml")
    a = load_scenario(CATALOG / "NL-N-I01a.yaml")
    assert b.fault.analytics_load.session_overrides == OVERRIDES
    assert b.fault.analytics_load.hard_limit_attempts == {"temp_file_limit": "-1"}
    for must in ("overrides_recorded == true", "temp_limit_override_refused == true", "server_crash_restarts == 0",
                 "role_settings_unchanged == true", "temp_limit_enforced == true", "customer_limit_errors == 0"):
        assert must in b.accept
    judged = " ".join(b.accept)
    for finding in ("oltp_tps_ratio", "oltp_p99_ratio", "customer_p90_max_s", "helpers_over_limit",
                    "statement_timeout_enforced", "rto_to_slo_s", "customer_cache_hit_ratio_vs_baseline"):
        assert finding not in judged            # recorded only, never a pass rule in 1b
    assert a.fault.analytics_load.session_overrides == {} and a.fault.analytics_load.memory_guard is None
    assert not set(TENANT_MEASURES) & set(a.measure)


# --- the memory guard ------------------------------------------------------------------------------

RULES = GuardRules(available_min_mb=2048, swap_out_max_mb=512, interval_s=0.5, stale_s=2.0)


def reading(available_mb: float, pswpout: int | None = 1000, t: int | None = None) -> Reading:
    return Reading(time.monotonic_ns() if t is None else t, int(available_mb * 1024), 1_000_000, pswpout)


def test_a_reading_needs_available_memory_never_a_zero():
    lines = ["MemAvailable:   13312000 kB", "SwapFree:  1900000 kB", "pswpout 4242"]
    r = parse_reading(lines, 5)
    assert (r.available_kb, r.swap_free_kb, r.pswpout) == (13312000, 1900000, 4242)
    assert parse_reading(["SwapFree: 1 kB", "pswpout 1"], 5) is None
    assert "MemAvailable" in command(0.5) and "pswpout" in command(0.5) and "sleep 0.5" in command(0.5)


def test_low_memory_large_swap_out_or_no_swap_reading_trips_the_guard():
    assert trip_reason(reading(4000, 1000), 1000, 4096, RULES) is None
    assert "below 2048 MB" in trip_reason(reading(2000, 1000), 1000, 4096, RULES)
    # 140,000 pages of 4 KiB = 547 MB swapped out since arming
    assert "swapped out" in trip_reason(reading(4000, 141_000), 1000, 4096, RULES)
    assert "cannot be read" in trip_reason(reading(4000, None), 1000, 4096, RULES)


def guard(tripped: list[str]) -> MemoryGuard:
    async def stop(why: str) -> dict[str, Any]:
        tripped.append(why)
        return {"stopped": True}
    return MemoryGuard(SimpleNamespace(ssh=None), Stream(), RULES, on_trigger=stop)


def test_the_guard_is_never_armed_blind():
    g = guard([])
    with pytest.raises(RuntimeError, match="no reading"):
        g.arm()
    g.last = reading(4000, t=time.monotonic_ns() - int(5e9))     # 5 s old
    with pytest.raises(RuntimeError, match="no reading"):
        g.arm()
    g.last = reading(4000, pswpout=None)
    with pytest.raises(RuntimeError, match="swap-out cannot be read"):
        g.arm()


def test_the_guard_judges_only_while_armed_and_ends_the_reports_once():
    async def scenario() -> tuple[MemoryGuard, list[str]]:
        calls: list[str] = []
        g = guard(calls)
        g.observe(reading(1000))                  # not armed yet: recorded, not judged
        assert g.tripped is None
        g.observe(reading(4000, 1000))
        g.arm()
        g.observe(reading(3000, 1100))
        g.observe(reading(1500, 1200))            # below 2 GB
        g.observe(reading(1000, 1300))            # already tripped: no second stop
        await asyncio.sleep(0)
        await asyncio.gather(g._trigger_task)
        return g, calls
    g, calls = asyncio.run(scenario())
    assert len(calls) == 1 and "below 2048 MB" in calls[0]
    state = g.state()
    assert state["tripped"]["action"] == {"stopped": True}
    assert state["min_available_mb"] == 1000.0       # lowest while armed, after the trip too
    g.disarm()
    g.tripped = None
    g.observe(reading(500))                           # disarmed: never judged again
    assert g.tripped is None


def test_no_reading_while_armed_ends_the_reports():
    async def scenario() -> list[str]:
        calls: list[str] = []
        g = guard(calls)
        g.rules = GuardRules(available_min_mb=2048, swap_out_max_mb=512, interval_s=0.01, stale_s=0.05)
        g.observe(reading(4000))
        g.arm()
        watch = asyncio.create_task(g._watch_loop())
        await asyncio.sleep(0.2)                     # no reading arrives
        watch.cancel()
        await asyncio.gather(watch, return_exceptions=True)
        if g._trigger_task:
            await g._trigger_task
        return calls
    calls = asyncio.run(scenario())
    assert len(calls) == 1 and "no memory reading" in calls[0]


# --- the sessions' answers -------------------------------------------------------------------------

def rec(session: int, setting: str, answer: str | None, read_back: str | None = None, **extra: Any) -> dict[str, Any]:
    return {"session": session, "setting": setting, "asked": OVERRIDES.get(setting, "-1"), "answer": answer,
            "read_back": read_back, "before": "2", **extra}


def all_accepted(sessions: int = 2) -> list[dict[str, Any]]:
    return [rec(s, name, "accepted", value) for s in range(1, sessions + 1) for name, value in OVERRIDES.items()]


def test_settings_read_back_as_asked_are_the_same_setting():
    assert same_setting("1GB", "1gb") and same_setting("1024MB", "1GB") and same_setting("0", "0")
    assert not same_setting("16MB", "1GB") and not same_setting(None, "1GB")


def test_every_session_answered_so_the_overrides_are_recorded():
    recorded, why, per = overrides_summary(all_accepted(), 2, OVERRIDES)
    assert recorded is True and why is None
    assert per["work_mem"]["overridden_sessions"] == 2 and per["work_mem"]["enforced_sessions"] == 0


def test_a_refusal_is_a_finding_never_a_failure():
    records = all_accepted()
    records[0] = rec(1, "max_parallel_workers_per_gather", "refused", sqlstate="42501", error="permission denied")
    records[1] = rec(1, "work_mem", "accepted", "16MB")          # accepted but read back unchanged
    recorded, _, per = overrides_summary(records, 2, OVERRIDES)
    assert recorded is True
    assert per["max_parallel_workers_per_gather"]["enforced_sessions"] == 1
    assert per["max_parallel_workers_per_gather"]["refused"] == ["42501: permission denied"]
    assert per["work_mem"]["enforced_sessions"] == 1 and per["work_mem"]["overridden_sessions"] == 1


def test_a_missing_answer_leaves_the_overrides_unmeasured():
    records = all_accepted()
    records[2] = rec(1, "statement_timeout", None, error="ConnectionDoesNotExistError: closed")
    recorded, why, _ = overrides_summary(records, 2, OVERRIDES)
    assert recorded is None and "5 of 6" in why and "closed" in why
    recorded, why, _ = overrides_summary(all_accepted(1), 2, OVERRIDES)     # one session never answered
    assert recorded is None and "3 of 6" in why
    assert overrides_summary([], 0, OVERRIDES)[0] is None


@pytest.mark.parametrize("plan, limit_tests, expected", [
    (["full", "full", "full", "index"], True, 3),      # the index session: its reads write no temp files
    (["full", "full"], True, 1),                       # never limit test 1's session
    (["full"], True, None),
    (["full"], False, 0),
])
def test_the_hard_limits_are_tried_away_from_limit_test_1(plan, limit_tests, expected):
    assert attempt_session(plan, limit_tests) == expected


class Conn:
    """set_config / current_setting as a non-superuser login sees them."""

    def __init__(self, refuse: frozenset[str] = frozenset({"temp_file_limit"}), broken: str | None = None,
                 granted: bool = False) -> None:
        self.values = {"max_parallel_workers_per_gather": "2", "work_mem": "16MB", "statement_timeout": "2min",
                       "temp_file_limit": "4GB"}
        self.refuse, self.broken, self.granted = refuse, broken, granted

    async def fetchval(self, sql: str, *args: Any) -> Any:
        name = args[0]
        if name == self.broken:
            raise ConnectionResetError("connection lost")
        if sql.startswith("SELECT set_config"):
            if name in self.refuse:
                raise asyncpg.exceptions.InsufficientPrivilegeError(f'permission denied to set parameter "{name}"')
            self.values[name] = args[1].upper() if args[1].endswith(("gb", "GB")) else args[1]
            return self.values[name]
        if sql.startswith("SELECT has_parameter_privilege"):
            return self.granted
        return self.values[name]


def test_each_change_is_answered_accepted_refused_or_not_at_all():
    adapter = AnalyticsMixin()
    out = asyncio.run(adapter._set_own(2, Conn(broken="statement_timeout"),
                                       {**OVERRIDES, "temp_file_limit": "-1"}, privilege=True))
    by = {r["setting"]: r for r in out}
    assert by["work_mem"]["answer"] == "accepted" and by["work_mem"]["read_back"] == "1GB"
    assert by["work_mem"]["before"] == "16MB" and by["work_mem"]["granted_set"] is False
    assert by["temp_file_limit"]["answer"] == "refused" and by["temp_file_limit"]["sqlstate"] == "42501"
    assert by["statement_timeout"]["answer"] is None and "connection lost" in by["statement_timeout"]["error"]


# --- the fault behaviour ---------------------------------------------------------------------------

def tenant_plan(**extra: Any) -> AnalyticsLoad:
    return AnalyticsLoad(session_overrides=OVERRIDES, hard_limit_attempts={"temp_file_limit": "-1"},
                         memory_guard=GuardConfig(), **extra)


def tenant_ctx(tmp_path, plan: AnalyticsLoad | None = None, measure: list[str] | None = None,
               settings: dict[str, Any] | None = None, after: dict[str, Any] | None = None, **extra: Any) -> Any:
    class Adapter:
        calls = 0

        async def observe_analytics_settings(self):
            Adapter.calls += 1
            return dict((after if Adapter.calls > 1 and after is not None else settings) or SETTINGS)

        def crash_restart_log_patterns(self):
            return (r"server process \(PID \d+\) was terminated by signal", r"all server processes terminated")

        async def stop_analytics(self):
            return {"stopped": True}
    fault = SimpleNamespace(analytics_load=plan or tenant_plan(), duration=300.0, type="analytics_load")
    scenario = SimpleNamespace(fault=fault, workload=SimpleNamespace(rate_tps=900.0), repeat=None,
                               measure=TENANT_MEASURES if measure is None else measure)
    base = dict(scenario=scenario, facts={}, not_measured={}, disclosures=[], run_dir=tmp_path,
                baseline=SimpleNamespace(tps=900.0), adapter=Adapter(), phase_timeout_s=lambda phase: 120.0,
                attached_rate_tps=None, t0_ns=1_000, node=SimpleNamespace(ssh=None), stream=Stream())
    return SimpleNamespace(**{**base, **extra})


def tenant_fault(tmp_path, **kw: Any) -> AnalyticsLoadFault:
    f = AnalyticsLoadFault(tenant_ctx(tmp_path, **kw))
    asyncio.run(f.observe_settings())
    return f


@pytest.mark.parametrize("timeout", [None, "290s", "0"])
def test_1b_is_not_refused_for_a_time_limit_the_sessions_turn_off_themselves(tmp_path, timeout):
    f = tenant_fault(tmp_path, settings={**SETTINGS, "role_statement_timeout": timeout})
    assert "statement_timeout = '0'" in f.ctx.facts["limit_test_2_note"]


def test_1b_still_needs_the_temp_file_limit_it_judges(tmp_path):
    with pytest.raises(PhaseAbort, match="no temp_file_limit"):
        tenant_fault(tmp_path, settings={**SETTINGS, "role_temp_file_limit": None})


def test_1b_never_starts_without_a_memory_reading(tmp_path):
    f = tenant_fault(tmp_path)
    f._cache = window_cache("baseline", 995, 5)
    f.samplers()                                    # the guard, as the run's probes start it (no reading yet)
    with pytest.raises(PhaseAbort, match="memory guard cannot protect the target: no reading"):
        asyncio.run(f.before_inject())


def test_1b_arms_the_guard_last_before_the_reports(tmp_path):
    f = tenant_fault(tmp_path)
    f._cache = window_cache("baseline", 995, 5)
    f.samplers()
    f._guard.observe(reading(8000))
    asyncio.run(f.before_inject())
    assert f._guard.armed


@pytest.mark.parametrize("attempt, expected", [
    ({"answer": "refused", "sqlstate": "42501"}, True),
    ({"answer": "refused", "sqlstate": "22023"}, False),          # refused, but not for lack of privilege
    ({"answer": "accepted", "read_back": "-1", "granted_set": True}, False),
])
def test_the_temp_file_limit_must_be_refused_for_lack_of_privilege(tmp_path, attempt, expected):
    f = tenant_fault(tmp_path)
    assert f._override_refused([{"setting": "temp_file_limit", **attempt}], "temp_file_limit", "x") == {"x": expected}


def test_no_attempt_or_no_answer_leaves_the_refusal_unmeasured(tmp_path):
    f = tenant_fault(tmp_path)
    assert f._override_refused([], "temp_file_limit", "x") == {"x": NOT_MEASURED}
    assert f._override_refused([{"setting": "temp_file_limit", "answer": None, "error": "lost"}],
                               "temp_file_limit", "x") == {"x": NOT_MEASURED}
    assert "lost" in f.ctx.not_measured["x"]


def ev(kind: str, t: int, **data: Any) -> Any:
    return SimpleNamespace(kind=kind, t_mono_ns=t, data=data, source="x")


def state(**extra: Any) -> dict[str, Any]:
    base = {"sessions": [{"session": 1}, {"session": 2}], "overrides": all_accepted(2),
            "attempts": [{"session": 2, "setting": "temp_file_limit", "asked": "-1", "answer": "refused",
                          "sqlstate": "42501", "error": "permission denied"}], "attempt_session": 2,
            "samples": [{"helpers_total": 6, "temp_bytes": 0, "temp_files": 0},
                        {"helpers_total": 8, "temp_bytes": 6 * 1024 ** 3, "temp_files": 30}],
            "limit_tests": {"limit_timeout": {"outcome": "cancelled", "seconds": 299.4}},
            "started_mono_ns": 0, "stopped_mono_ns": int(300.5e9)}
    return {**base, **extra}


def test_the_1b_measures_and_findings(tmp_path):
    f = tenant_fault(tmp_path)
    f.ctx.facts["hold"] = {"start_mono_ns": int(10e9), "end_mono_ns": int(310e9)}
    events = [ev("log_line", 500, line="server process (PID 4) was terminated by signal 9: Killed"),   # before T0
              ev("log_line", 2_000, line="checkpoint complete"),
              *[ev("write_probe", int(t * 1e9), ok=True) for t in (9, 10, 11, 14.5, 15)]]
    m = asyncio.run(f._tenant_measures(state(), events))
    assert m["overrides_recorded"] is True and m["temp_limit_override_refused"] is True
    assert m["role_settings_unchanged"] is True and m["server_crash_restarts"] == 0
    assert m["writer_max_gap_s"] == 3.5 and m["report_helpers_total_max"] == 8 and m["temp_gb_written"] == 6.0
    assert m["limit_test_2_ran_s"] == 299.4 and m["reports_ran_s"] == 300.5
    assert f.ctx.facts["temp_files_written"] == 30
    assert f.ctx.facts["analytics_tenant"]["overrides"]["statement_timeout"]["overridden_sessions"] == 2


def test_a_crash_restart_after_the_fault_started_fails_the_crash_check(tmp_path):
    f = tenant_fault(tmp_path)
    events = [ev("log_line", 5_000, line="server process (PID 4711) was terminated by signal 9: Killed"),
              ev("log_line", 5_001, line="all server processes terminated; reinitializing")]
    assert f._crash_restarts(events) == {"server_crash_restarts": 2}
    assert "terminated by signal" in f.ctx.facts["crash_restart_lines"][0]
    assert f._crash_restarts([]) == {"server_crash_restarts": NOT_MEASURED}      # no log line reached us


def test_a_role_changed_during_the_run_fails(tmp_path):
    f = tenant_fault(tmp_path, after={**SETTINGS, "role_work_mem": "1GB"})
    assert asyncio.run(f._role_unchanged()) == {"role_settings_unchanged": False}
    assert f.ctx.facts["role_settings_changed"] == {"role_work_mem": {"before": "16MB", "after": "1GB"}}


def test_1a_produces_none_of_the_1b_measures(tmp_path):
    f = tenant_fault(tmp_path, plan=AnalyticsLoad(), measure=[])
    assert asyncio.run(f._tenant_measures(state(), [])) == {}
    assert "analytics_tenant" not in f.ctx.facts


def test_the_findings_block_names_what_was_overridden_and_what_was_refused(tmp_path):
    f = tenant_fault(tmp_path)
    f.ctx.facts["hold"] = {"start_mono_ns": int(10e9), "end_mono_ns": int(310e9)}
    records = all_accepted(2)
    records[0] = rec(1, "max_parallel_workers_per_gather", "refused", sqlstate="42501", error="permission denied")
    m = asyncio.run(f._tenant_measures(state(overrides=records), []))
    lines = "\n".join(tenant_lines(f.ctx.facts, m))
    assert "1b findings" in lines
    assert "work_mem: asked 1GB" in lines and "overridden: 2 session(s)" in lines
    assert "max_parallel_workers_per_gather: asked 8" in lines and "mixed" in lines and "42501: permission denied" in lines
    assert "tried temp_file_limit = -1: refused 42501" in lines
    accepted = [{"session": 4, "setting": "temp_file_limit", "asked": "-1", "answer": "accepted", "read_back": "-1",
                 "granted_set": True}]
    lines = "\n".join(tenant_lines({"analytics_tenant": {"attempts": accepted}}, {}))
    assert "ACCEPTED" in lines and "granted SET on it by the operator" in lines
    assert tenant_lines({}, {}) == []                 # 1a: no block


# --- the whole run, through the real orchestrator (tests/test_analytics_run.py's fake engine) ------

from resilience_tests.adapters.base import register_adapter  # noqa: E402
from resilience_tests.control import orchestrator as orch  # noqa: E402
from resilience_tests.control.matrix import RunPlanItem  # noqa: E402
from resilience_tests.control.orchestrator import RunOptions, TestOrchestrator  # noqa: E402
from resilience_tests.execution.probes import memory_guard as guard_mod  # noqa: E402
from tests.test_analytics_run import (BASELINE_S, HOLD_S, NOT_ON_MARKER_WORKLOAD, AnalyticsEngine,  # noqa: E402
                                      Reports, reports_env)  # noqa: F401  (reports_env is a fixture)
from tests.test_lock_pileup_run import QuietTail  # noqa: E402
from tests.test_orchestrator import env, outcomes, scenario, why  # noqa: E402,F401  (env is a fixture)


class Tenant:
    asked: tuple[Any, Any] = (None, None)
    refuse_override: str | None = None
    memory_mb: list[float] = []          # available memory per reading; the last one repeats
    stopped_ns: int | None = None


@register_adapter
class TenantEngine(AnalyticsEngine):
    engine = "orch-analytics-tenant-fake"

    def crash_restart_log_patterns(self) -> tuple[str, ...]:
        return (r"terminating any other active server processes",)

    async def start_analytics(self, *, full_table_sessions, index_sessions, limit_tests, extra_warehouses,
                              session_overrides=None, hard_limit_attempts=None):
        Tenant.asked = (session_overrides, hard_limit_attempts)
        detail = await super().start_analytics(full_table_sessions=full_table_sessions, index_sessions=index_sessions,
                                               limit_tests=limit_tests, extra_warehouses=extra_warehouses)
        n = len(Reports.sessions)
        detail["attempt_session"] = n
        return detail

    async def analytics_state(self) -> dict[str, Any]:
        st = await super().analytics_state()
        if not st.get("started"):
            return st
        overrides, attempts = Tenant.asked
        n = len(Reports.sessions)
        st["overrides"] = [
            {"session": s, "setting": k, "asked": v, "before": "2", "answer": "refused", "sqlstate": "42501",
             "error": "permission denied"} if k == Tenant.refuse_override else
            {"session": s, "setting": k, "asked": v, "before": "2", "answer": "accepted", "read_back": v}
            for s in range(1, n + 1) for k, v in (overrides or {}).items()]
        st["attempts"] = [{"session": n, "setting": k, "asked": v, "answer": "refused", "sqlstate": "42501",
                           "error": "permission denied"} for k, v in (attempts or {}).items()]
        st["attempt_session"] = n
        st["stopped_mono_ns"] = Tenant.stopped_ns
        return st

    async def stop_analytics(self) -> dict[str, Any]:
        if Reports.running:
            Tenant.stopped_ns = time.monotonic_ns()
        return await super().stop_analytics()

    async def revert_analytics(self) -> dict[str, Any]:
        if Reports.running:
            Tenant.stopped_ns = time.monotonic_ns()
        return await super().revert_analytics()


async def fake_readings(self: MemoryGuard) -> None:
    """The target's memory as the scripted list says, every 0.1 s (no SSH)."""
    i = 0
    while True:
        values = Tenant.memory_mb or [8000.0]
        self.observe(Reading(time.monotonic_ns(), int(values[min(i, len(values) - 1)] * 1024), 1_000_000, 1000))
        i += 1
        await asyncio.sleep(0.1)


@pytest.fixture
def tenant_env(reports_env, monkeypatch):
    Tenant.asked, Tenant.refuse_override, Tenant.memory_mb, Tenant.stopped_ns = (None, None), None, [], None
    monkeypatch.setattr(guard_mod.MemoryGuard, "_read_loop", fake_readings)
    monkeypatch.setattr(orch, "LogTailer", QuietTail)
    return reports_env.model_copy(update={"database": reports_env.database.model_copy(
        update={"engine": "orch-analytics-tenant-fake"})})


def run_1b(profile) -> dict[str, Any]:
    sc = scenario("NL-N-I01b")
    marker = scenario("NL-C-01").workload.model_copy(update={"latency_from": "intended_start"})
    sc = sc.model_copy(update={
        "workload": marker, "accept": [a for a in sc.accept if a not in NOT_ON_MARKER_WORKLOAD],
        "requires": [r for r in sc.requires if r != "tpcc_workload"],
        "fault": sc.fault.model_copy(update={"duration": HOLD_S}),
        "steady_state": sc.steady_state.model_copy(update={"duration_s": BASELINE_S, "warmup_s": 0.0})})
    item = RunPlanItem(scenario=sc, env_class=profile.env_class, role="standalone", node=profile.nodes[0])
    return asyncio.run(TestOrchestrator(item, profile, RunOptions()).run())


def test_a_1b_run_records_the_tenants_changes_and_passes_on_what_must_hold(tenant_env):
    Reports.helpers_max = 7                 # more than the role's default 2: a finding, not a failure
    Reports.settings["role_statement_timeout"] = "10min"     # would refuse 1a; 1b sets its own
    results = run_1b(tenant_env)
    assert results["status"] == "passed", why(results)
    assert Tenant.asked == (OVERRIDES, {"temp_file_limit": "-1"})
    m = results["measured"]
    assert m["overrides_recorded"] is True and m["temp_limit_override_refused"] is True
    assert m["server_crash_restarts"] == 0 and m["role_settings_unchanged"] is True
    assert m["memory_guard_tripped"] is False and m["available_memory_min_mb"] == 8000.0
    assert m["report_helpers_max"] == 7 and m["helpers_over_limit"] > 0
    with open(f"{results['evidence_dir']}/summary.txt") as summary:
        text = summary.read()
    assert "noisy analytics (NL-N-I01b):" in text and "1b findings" in text
    assert "work_mem: asked 1GB" in text and "tried temp_file_limit = -1: refused 42501" in text
    assert "guard: did not trigger" in text


def test_a_refused_override_is_recorded_and_the_run_still_passes(tenant_env):
    Tenant.refuse_override = "max_parallel_workers_per_gather"
    results = run_1b(tenant_env)
    assert results["status"] == "passed", why(results)
    per = results["facts"]["analytics_tenant"]["overrides"]["max_parallel_workers_per_gather"]
    assert per["overridden_sessions"] == 0 and per["enforced_sessions"] == 4


def test_low_memory_ends_the_reports_early_and_is_recorded(tenant_env):
    Tenant.memory_mb = [8000.0] * (BASELINE_S * 10 + 30) + [1500.0]     # short of memory a few seconds into the hold
    results = run_1b(tenant_env)
    m = results["measured"]
    assert m["memory_guard_tripped"] is True and m["available_memory_min_mb"] == 1500.0
    assert m["reports_ran_s"] < HOLD_S
    assert "below 2048 MB" in results["facts"]["memory_guard"]["tripped"]["reason"]
    assert results["status"] == "passed", why(results)       # judged on what must still hold
