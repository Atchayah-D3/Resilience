"""NL-N-I01 through the real orchestrator: all nine phases, the real analytics driver and behaviour,
the real workload and probes, against a fake engine whose report sessions, samples and cache
counters are scripted.

Shortened (no warm-up, a 6 s baseline, an 8 s hold, "minute 1" at 2 s, a 2 s statement_timeout
with a 1 s margin) so a run takes seconds. NL-N-I01 is catalogued on the TPC-C workload; the fake
engine has no TPC-C, so here it runs on the marker workload with the catalog's own rules -- less
those only TPC-C produces (the TPC-C gates, the per-type p90, the customers' limit errors, read from
TPC-C's rolled-back journal), which tests/test_analytics_fault.py covers.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from resilience_tests.adapters.base import Capability, register_adapter
from resilience_tests.control import killswitch
from resilience_tests.control import orchestrator as orch
from resilience_tests.control.faults import analytics as behaviour_mod
from resilience_tests.control.killswitch import ledger_for
from resilience_tests.control.matrix import RunPlanItem
from resilience_tests.control.orchestrator import RunOptions, TestOrchestrator
from resilience_tests.control.profile import DbEndpoint
from resilience_tests.execution.injectors import analytics as driver_mod
from resilience_tests.execution.injectors.analytics import OsSshAnalyticsDriver
from resilience_tests.execution.workload import contract
from tests.test_lock_pileup_run import ActiveService
from tests.test_orchestrator import OutageAdapter, outcomes, scenario, why
from tests.test_orchestrator import env  # noqa: F401  (env is a fixture)

HOLD_S = 8.0
BASELINE_S = 6
TIMEOUT_S = 2.0
NOT_ON_MARKER_WORKLOAD = {"partial_commits == 0", "tpcc_consistency_violations == 0", "aborted_markers_present == 0",
                          "customer_p90_max_s <= 5.0", "customer_limit_errors == 0"}
SETTINGS = {"analytics_login": "analytics_role", "shared_buffers": "1GB", "max_parallel_workers": "8",
            "max_parallel_workers_per_gather": "2", "max_worker_processes": "8", "max_connections": "100",
            "role_exists": "true", "role_can_login": "true", "role_connection_limit": "4",
            "role_max_parallel_workers_per_gather": "2", "role_work_mem": "16MB", "role_temp_file_limit": "4GB",
            "role_statement_timeout": f"{TIMEOUT_S:g}s", "cpu_cores": "8",
            "pg_stat_statements": "1.11", "pg_stat_statements_track": "top"}


def clerk_snapshot(hit: int, read: int, **extra: Any) -> dict[str, Any]:
    """pg_stat_statements' counts of the clerks' five calls (all pages on New-Order, for short)."""
    by_type = {t: {"entries": 1, "calls": 1, "hit": 0, "read": 0} for t in contract.TPCC_TYPES}
    by_type[contract.NEW_ORDER].update(hit=hit, read=read)
    return {"login": "harness", "hit": hit, "read": read, "calls": 5, "by_type": by_type, "dealloc": 0,
            "stats_reset": "2026-11-24T00:00:00+00:00", **extra}


class Reports:
    """The fake engine's report sessions and cache counters, shared by every adapter instance."""

    settings: dict[str, Any] = {}
    running = False
    started_ns = 0
    sessions: list[dict[str, Any]] = []
    samples: list[dict[str, Any]] = []
    limit_tests: dict[str, Any] = {}
    temp_outcome: tuple[str, str | None] = ("error", "53400")
    timeout_outcome: tuple[str, str | None] = ("error", "57014")
    helpers_max = 2
    active = True
    cache_calls = 0
    hit_ratios = {"baseline": 0.995, "hold": 0.96}
    counters = {"customers": {"hit": 0, "read": 0}, "reports": {"hit": 0, "read": 0}}
    calls: list[str] = []
    readable = True


def reset_reports() -> None:
    Reports.settings, Reports.running, Reports.started_ns = dict(SETTINGS), False, 0
    Reports.sessions, Reports.samples, Reports.limit_tests, Reports.calls = [], [], {}, []
    Reports.temp_outcome, Reports.timeout_outcome = ("error", "53400"), ("error", "57014")
    Reports.helpers_max, Reports.active, Reports.cache_calls, Reports.readable = 2, True, 0, True
    Reports.hit_ratios = {"baseline": 0.995, "hold": 0.96}
    Reports.counters = {"customers": {"hit": 0, "read": 0}, "reports": {"hit": 0, "read": 0}}


@register_adapter
class AnalyticsEngine(OutageAdapter):
    engine = "orch-analytics-fake"
    capabilities = OutageAdapter.capabilities | {Capability.ANALYTICS_SESSIONS}

    def tpcc_layout(self) -> dict[str, Any] | None:
        return {"warehouses": 10, "active_warehouses": 4, "extra_warehouses": (5, 10)}

    async def observe_analytics_settings(self) -> dict[str, Any]:
        return dict(Reports.settings)

    async def analytics_read_access(self) -> dict[str, Any]:
        ok = Reports.readable
        return {"login": "analytics_role", "schema": ok, "tables": {"order_line": ok, "orders": ok}}

    async def start_analytics(self, *, full_table_sessions, index_sessions, limit_tests, extra_warehouses):
        Reports.calls.append("start")
        Reports.running, Reports.started_ns = True, time.monotonic_ns()
        kinds = ["full"] * full_table_sessions + ["index"] * index_sessions
        Reports.sessions = [{"session": i + 1, "kind": k, "pid": 100 + i} for i, k in enumerate(kinds)]
        if limit_tests:
            asyncio.get_running_loop().call_later(0.2, self._ended, "limit_temp", Reports.temp_outcome, 0.2)
            asyncio.get_running_loop().call_later(TIMEOUT_S, self._ended, "limit_timeout", Reports.timeout_outcome,
                                                  TIMEOUT_S + 0.05)
        return {"supported": True, "t0_mono_ns": Reports.started_ns, "sessions": Reports.sessions,
                "application_name": "resilience-analytics", "login": "analytics_role",
                "extra_warehouses": list(extra_warehouses)}

    @staticmethod
    def _ended(query: str, outcome: tuple[str, str | None], seconds: float) -> None:
        if Reports.running:
            Reports.limit_tests[query] = {"query": query, "outcome": outcome[0], "sqlstate": outcome[1],
                                          "seconds": seconds}

    async def analytics_sample(self) -> dict[str, Any]:
        n = len(Reports.sessions) if Reports.running else 0
        sample = {"sessions_open": n, "sessions_active": n if Reports.active else 0,
                  "helpers_max": Reports.helpers_max if n else 0, "helpers_total": Reports.helpers_max * 3 if n else 0,
                  "helpers_all": 0, "temp_files": 0, "temp_bytes": 0, "t_mono_ns": time.monotonic_ns()}
        if Reports.running:
            Reports.samples.append(dict(sample))
        return sample

    async def analytics_state(self) -> dict[str, Any]:
        if not Reports.started_ns:
            return {"started": False}
        return {"started": True, "stopped": not Reports.running, "started_mono_ns": Reports.started_ns,
                "sessions": Reports.sessions, "samples": list(Reports.samples),
                "limit_tests": dict(Reports.limit_tests), "by_kind": {"q1": {"count": 3, "completed": 3}}}

    async def stop_analytics(self) -> dict[str, Any]:
        Reports.calls.append("stop")
        Reports.running = False
        return {"stopped": True}

    async def revert_analytics(self) -> dict[str, Any]:
        Reports.calls.append("revert")
        Reports.running = False
        return {"sessions_remaining": 0}

    async def analytics_leftovers(self) -> dict[str, Any]:
        return {"sessions": 0, "temp_files": 0}

    async def customer_cache_counters(self) -> dict[str, Any]:
        """Called at baseline start and end, then hold start and end: the pages between the 1st and
        2nd call are the baseline's, between the 3rd and 4th the hold's."""
        Reports.cache_calls += 1
        period = {2: "baseline", 4: "hold"}.get(Reports.cache_calls)
        if period:
            pages = 100_000
            hit = round(pages * Reports.hit_ratios[period])
            Reports.counters["customers"]["hit"] += hit
            Reports.counters["customers"]["read"] += pages - hit
        clerks = clerk_snapshot(Reports.counters["customers"]["hit"], Reports.counters["customers"]["read"])
        return {"clerks": clerks, "clerks_unavailable": None, "customers": dict(Reports.counters["customers"]),
                "reports": dict(Reports.counters["reports"])}


@pytest.fixture
def reports_env(env, monkeypatch):
    reset_reports()
    node = env.nodes[0].model_copy(update={"analytics": DbEndpoint(host="127.0.0.1", port=5432, dbname="x",
                                                                       user="analytics_role")})
    profile = env.model_copy(update={"database": env.database.model_copy(update={"engine": "orch-analytics-fake"}),
                                     "infra": ["analytics_login"], "nodes": [node]})
    monkeypatch.setattr(driver_mod, "RemoteHost", ActiveService)
    monkeypatch.setattr(driver_mod, "CONFIRM_AT_S", 2.0)
    monkeypatch.setattr(behaviour_mod, "TIMEOUT_MARGIN_S", 1.0)
    monkeypatch.setattr(orch, "resolve", lambda fault, prof: OsSshAnalyticsDriver(prof, fault.type))
    monkeypatch.setattr(killswitch, "resolve_by_name",
                        lambda section, driver, prof, fault_type="": OsSshAnalyticsDriver(prof, fault_type))
    return profile


def short() -> Any:
    sc = scenario("NL-N-I01a")
    marker = scenario("NL-C-01").workload.model_copy(update={"latency_from": "intended_start"})
    accept = [a for a in sc.accept if a not in NOT_ON_MARKER_WORKLOAD]
    return sc.model_copy(update={
        "workload": marker, "accept": accept,
        "requires": [r for r in sc.requires if r != "tpcc_workload"],
        "fault": sc.fault.model_copy(update={"duration": HOLD_S}),
        "steady_state": sc.steady_state.model_copy(update={"duration_s": BASELINE_S, "warmup_s": 0.0})})


def run(profile) -> dict[str, Any]:
    item = RunPlanItem(scenario=short(), env_class=profile.env_class, role="standalone", node=profile.nodes[0])
    return asyncio.run(TestOrchestrator(item, profile, RunOptions()).run())


def test_the_reports_run_their_limits_trip_and_the_customers_keep_their_service(reports_env):
    results = run(reports_env)
    assert results["status"] == "passed", why(results)
    m = results["measured"]
    assert m["fault_confirmed"] is True
    assert m["temp_limit_enforced"] is True and m["statement_timeout_enforced"] is True
    assert m["helpers_over_limit"] == 0 and m["report_helpers_max"] == 2
    assert m["customer_cache_hit_ratio_baseline"] == 0.995 and m["customer_cache_hit_ratio_hold"] == 0.96
    assert m["customer_cache_hit_ratio_vs_baseline"] == pytest.approx(0.96 / 0.995, abs=1e-4)
    assert m["analytics_sessions_left"] == 0 and m["temp_files_left"] == 0
    assert m["analytics_active_share"] == 1.0
    # the plan followed the target: ceil(8 cores / 3) = 3 full-table sessions + 1 index session
    assert results["facts"]["analytics_validity"]["plan"]["total"] == 4
    # started in the hold, stopped by its revert, reverted again before the leftovers are read
    assert Reports.calls[0] == "start" and Reports.calls.count("revert") >= 1
    assert not [e for e in ledger_for(reports_env).outstanding() if e.run_id == results["run_id"]]
    with open(f"{results['evidence_dir']}/summary.txt") as summary:
        text = summary.read()
    assert "noisy analytics (NL-N-I01a):" in text and "limit test 1 (temp_file_limit): error 53400" in text


def test_a_limit_test_the_role_did_not_stop_fails_the_run(reports_env):
    Reports.temp_outcome = ("completed", None)
    results = run(reports_env)
    assert results["status"] == "failed", why(results)
    assert outcomes(results)["temp_limit_enforced == true"] == "fail"


def test_a_report_with_more_helpers_than_allowed_fails_the_run(reports_env):
    Reports.helpers_max = 3
    results = run(reports_env)
    assert results["status"] == "failed", why(results)
    assert outcomes(results)["helpers_over_limit == 0"] == "fail"


def test_reports_not_seen_running_at_minute_1_cannot_pass(reports_env):
    Reports.active = False
    results = run(reports_env)
    assert results["status"] == "failed", why(results)
    assert outcomes(results)["fault_confirmed == true"] == "fail"


def test_a_cache_loss_beyond_the_framework_limit_fails_the_run(reports_env):
    Reports.hit_ratios = {"baseline": 0.995, "hold": 0.70}
    results = run(reports_env)
    assert results["status"] == "failed", why(results)
    assert outcomes(results)["customer_cache_hit_ratio_vs_baseline >= 0.80"] == "fail"


def test_a_baseline_out_of_the_cache_makes_the_run_invalid_before_the_fault(reports_env):
    Reports.hit_ratios = {"baseline": 0.95, "hold": 0.95}
    results = run(reports_env)
    assert results["status"] == "aborted", why(results)
    assert "baseline cache hit ratio is 0.9500" in str(results["error"])
    assert "start" not in Reports.calls          # refused before anything was applied


def test_a_login_with_too_few_sessions_makes_the_run_invalid_at_init(reports_env):
    Reports.settings["role_connection_limit"] = "3"
    results = run(reports_env)
    assert results["status"] == "aborted", why(results)
    assert "allows 3 sessions" in str(results["error"]) and "needs 4 report sessions" in str(results["error"])


def test_a_statement_timeout_longer_than_the_hold_makes_the_run_invalid_at_init(reports_env):
    Reports.settings["role_statement_timeout"] = "10min"
    results = run(reports_env)
    assert results["status"] == "aborted", why(results)
    assert "does not end inside" in str(results["error"])


def test_a_role_setting_changed_during_the_run_fails_the_cleanup_check(reports_env, monkeypatch):
    async def observed(self) -> dict[str, Any]:
        # read at init and by the driver's preflight as set; changed once the reports have stopped
        changed = Reports.started_ns and not Reports.running
        return {**SETTINGS, "role_work_mem": "64MB"} if changed else dict(SETTINGS)
    monkeypatch.setattr(AnalyticsEngine, "observe_analytics_settings", observed)
    results = run(reports_env)
    assert results["status"] == "failed", why(results)
    assert outcomes(results)["settings_unchanged == true"] == "fail"
    assert results["facts"]["settings_changed"] == {"role_work_mem": {"before": "16MB", "after": "64MB"}}


def test_a_login_that_may_not_read_the_data_makes_the_run_invalid_at_init(reports_env):
    """Was (lab, 16 November): the report login had lost its grants in a reload; the run found out
    only when every report failed, after the warm-up and the baseline."""
    Reports.readable = False
    results = run(reports_env)
    assert results["status"] == "aborted", why(results)
    assert "lacks USAGE on schema resilience_tpcc, SELECT on order_line, SELECT on orders" in str(results["error"])
    phases = {p["phase"]: p["outcome"] for p in results["phases"]}
    assert phases["init"] == "aborted" and "baseline" not in phases     # before the warm-up and the baseline
