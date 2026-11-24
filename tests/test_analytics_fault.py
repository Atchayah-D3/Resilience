"""NL-N-I01 measures and rules that a run on the marker workload cannot reach (tests/test_analytics_run.py
covers the whole run): the session plan, the minute-1 confirmation, the worst p90 per type, the
customers' limit errors, the limit tests' timing, the validity rules, and the rate taken from the
stored maximum."""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace
from typing import Any

import pytest

from catalog.schema import AnalyticsLoad, Workload
from resilience_tests.analysis.predicates import NOT_MEASURED
from resilience_tests.control.faults.analytics import (AnalyticsLoadFault, PhaseAbort, clerk_hit_ratio, duration_s,
                                                       hit_ratio, size_bytes)
from resilience_tests.execution.injectors.analytics import minute_one, session_plan
from resilience_tests.execution.injectors.base import DriverNotAvailable
from resilience_tests.execution.workload import contract
from resilience_tests.execution.workload.markers import MarkerJournals
from resilience_tests.execution.workload.tpcc.profile import TpccDataError, TpccProfile
from tests.test_analytics_run import SETTINGS, clerk_snapshot


# --- the session plan and the minute-1 confirmation ----------------------------------------------

@pytest.mark.parametrize("cores, limit_tests, full", [(8, True, 3), (9, True, 3), (16, True, 6), (2, True, 2), (2, False, 1)])
def test_one_full_table_session_per_three_cores_and_one_per_limit_test(cores, limit_tests, full):
    plan = session_plan(AnalyticsLoad(limit_tests=limit_tests), cores)
    assert plan.full_table == full and plan.index == 1 and plan.total == full + 1
    assert plan.report_processes(2) == full * 3 + 1


def test_a_fixed_number_of_sessions_needs_no_cores_but_by_cores_does():
    assert session_plan(AnalyticsLoad(full_table_sessions=5), None).full_table == 5
    with pytest.raises(DriverNotAvailable, match="CPU cores could not be read"):
        session_plan(AnalyticsLoad(), None)


def test_minute_1_is_judged_on_the_first_sample_at_or_after_60_s():
    t0 = 1_000_000_000
    def at(s, active, helpers):
        return {"t_mono_ns": t0 + int(s * 1e9), "sessions_active": active, "helpers_total": helpers}
    samples = [at(30, 4, 6), at(59.9, 4, 6), at(60.4, 3, 6), at(61, 4, 6)]
    assert minute_one(samples, t0, 4) == (False, samples[2])       # one session idle at minute 1
    assert minute_one([at(60.2, 4, 0)], t0, 4)[0] is False          # no helper working
    assert minute_one([at(60.2, 4, 2)], t0, 4)[0] is True
    assert minute_one([at(30, 4, 6)], t0, 4) == (None, None)        # no sample that late: not measured


# --- setting values ------------------------------------------------------------------------------

def test_postgresql_units_are_read_as_postgresql_does():
    assert duration_s("2min") == 120 and duration_s("120000") == 120 and duration_s("1h") == 3600
    assert duration_s("0") is None and duration_s("") is None and duration_s("fast") is None
    assert size_bytes("4GB") == 4 * 1024 ** 3 and size_bytes("1024") == 1024 * 1024 and size_bytes("-1") is None


def test_the_hit_ratio_is_taken_between_two_snapshots():
    a = {"customers": {"hit": 100, "read": 0}}
    b = {"customers": {"hit": 1090, "read": 10}}
    assert hit_ratio(a, b) == 0.99
    assert hit_ratio(a, None) is None and hit_ratio(a, a) is None   # nothing read: no ratio, never 1.0


def snap(hit: int, read: int, **extra: Any) -> dict[str, Any]:
    """One cache snapshot: the clerks' calls in pg_stat_statements (judged) and the untouched tables (evidence)."""
    return {"clerks": clerk_snapshot(hit, read, **extra), "customers": {"hit": 10_000, "read": 0},
            "reports": {"hit": 0, "read": 0}}


def window_cache(window: str, hit: int, read: int) -> dict[str, Any]:
    return {f"{window}_start": snap(0, 0), f"{window}_end": snap(hit, read)}


# --- the behaviour, with a fake run context ------------------------------------------------------

def ctx(tmp_path, **extra: Any) -> Any:
    fault = SimpleNamespace(analytics_load=AnalyticsLoad(), duration=300.0, type="analytics_load")
    scenario = SimpleNamespace(fault=fault, workload=SimpleNamespace(rate_tps=900.0), repeat=None)
    base = dict(scenario=scenario, facts={}, not_measured={}, disclosures=[], run_dir=tmp_path,
                baseline=SimpleNamespace(tps=900.0), adapter=None, phase_timeout_s=lambda phase: 120.0,
                attached_rate_tps=None)
    return SimpleNamespace(**{**base, **extra})


def fault(tmp_path, settings=None, **extra) -> AnalyticsLoadFault:
    class Adapter:
        async def observe_analytics_settings(self):
            return dict(settings or SETTINGS)
    f = AnalyticsLoadFault(ctx(tmp_path, adapter=Adapter(), **extra))
    asyncio.run(f.observe_settings())
    return f


@pytest.mark.parametrize("change, message", [
    ({"role_exists": "false"}, "does not exist on the target or cannot log in"),
    ({"role_can_login": "false"}, "cannot log in"),
    ({"role_temp_file_limit": None}, "no temp_file_limit"),
    ({"role_statement_timeout": None}, "no statement_timeout"),
    ({"role_statement_timeout": "290s"}, "does not end inside the 300 s hold"),
    ({"cpu_cores": None}, "CPU cores could not be read"),
])
def test_a_target_where_the_test_cannot_be_valid_is_refused_at_init(tmp_path, change, message):
    with pytest.raises(PhaseAbort, match=message):
        fault(tmp_path, settings={**SETTINGS, **change})


def test_an_unlimited_login_is_accepted(tmp_path):
    assert fault(tmp_path, settings={**SETTINGS, "role_connection_limit": "-1"})._plan.total == 4


def test_a_baseline_below_the_planned_rate_is_invalid(tmp_path):
    f = fault(tmp_path, baseline=SimpleNamespace(tps=800.0))
    f._cache = window_cache("baseline", 995, 5)
    with pytest.raises(PhaseAbort, match="below 95% of the planned 900 TPS"):
        asyncio.run(f.before_inject())


def test_attached_the_planned_rate_is_the_running_workloads_and_a_baseline_below_it_is_invalid(tmp_path):
    # attached to the common workload: the scenario's own rate (here 900) and prepare's are not used
    f = fault(tmp_path, baseline=SimpleNamespace(tps=1300.0), attached_rate_tps=1400.0)
    assert f.planned_rate() == 1400.0
    f._cache = window_cache("baseline", 995, 5)
    with pytest.raises(PhaseAbort, match="1300.0 TPS, below 95% of the planned 1400 TPS"):
        asyncio.run(f.before_inject())


def test_an_unknown_planned_rate_is_invalid_never_skipped(tmp_path):
    # rate_fraction_of_capacity with no rate recorded at prepare and no running workload: nothing to check against
    f = fault(tmp_path, scenario=SimpleNamespace(fault=SimpleNamespace(analytics_load=AnalyticsLoad(), duration=300.0,
                                                                       type="analytics_load"),
                                                 workload=SimpleNamespace(rate_tps=None), repeat=None))
    f._cache = window_cache("baseline", 995, 5)
    with pytest.raises(PhaseAbort, match="planned rate is not known"):
        asyncio.run(f.before_inject())


def test_reports_that_cannot_occupy_every_core_are_invalid(tmp_path):
    f = fault(tmp_path, settings={**SETTINGS, "role_max_parallel_workers_per_gather": "0", "cpu_cores": "16",
                                  "role_connection_limit": "-1"})
    f._cache = window_cache("baseline", 995, 5)
    with pytest.raises(PhaseAbort, match="only 7 processes busy"):     # 6 full-table x 1 + 1 index < 16
        asyncio.run(f.before_inject())


def sample_event(t_ns: int, p90: dict[str, float]) -> Any:
    return SimpleNamespace(kind=contract.SAMPLE_KIND, source=contract.SAMPLE_SOURCE, t_mono_ns=t_ns,
                           data={contract.BY_TYPE: {t: {"p90_ms": v} for t, v in p90.items()}})


def test_the_worst_one_second_p90_of_the_three_types_during_the_hold_is_judged(tmp_path):
    f = fault(tmp_path)
    f.ctx.facts["hold"] = {"start_mono_ns": 1_000, "end_mono_ns": 10_000}
    events = [sample_event(500, {"new_order": 9000.0, "payment": 1.0, "order_status": 1.0}),      # before the hold
              sample_event(2_000, {"new_order": 40.0, "payment": 30.0, "order_status": 20.0, "delivery": 9000.0}),
              sample_event(3_000, {"new_order": 4200.0, "payment": 35.0, "order_status": 25.0})]
    assert f._p90(events) == {"customer_p90_max_s": 4.2}      # Delivery and the second before the hold left out
    assert f.ctx.facts["customer_p90_max_ms"]["new_order"] == 4200.0


def test_a_type_with_no_transaction_in_the_hold_leaves_the_p90_not_measured(tmp_path):
    f = fault(tmp_path)
    f.ctx.facts["hold"] = {"start_mono_ns": 1_000, "end_mono_ns": 10_000}
    out = f._p90([sample_event(2_000, {"new_order": 40.0, "payment": 30.0})])
    assert str(out["customer_p90_max_s"]) == str(NOT_MEASURED)
    assert "order_status" in f.ctx.not_measured["customer_p90_max_s"]


def test_customers_stopped_by_a_limit_are_counted_from_the_rolled_back_journal(tmp_path):
    journals = MarkerJournals(tmp_path)
    for uuid, code in (("a", "57014"), ("b", "40P01"), ("c", "53400"), ("d", "TP001")):
        asyncio.run(journals.aborted(uuid, time.time(), type="new_order", sqlstate=code))
    journals.close()
    f = fault(tmp_path)
    assert f._customer_limit_errors() == {"customer_limit_errors": 2}       # a deadlock is not a limit
    assert f.ctx.facts["customer_limit_errors"] == {"57014": 1, "53400": 1}


def test_without_a_rolled_back_journal_the_customers_limit_errors_are_not_measured(tmp_path):
    f = fault(tmp_path)
    assert str(f._customer_limit_errors()["customer_limit_errors"]) == str(NOT_MEASURED)


@pytest.mark.parametrize("record, enforced", [
    ({"outcome": "error", "sqlstate": "57014", "seconds": 2.6}, True),      # within 1 s of the 2 s limit
    ({"outcome": "error", "sqlstate": "57014", "seconds": 3.5}, False),     # stopped, but 1.5 s late
    ({"outcome": "error", "sqlstate": "53400", "seconds": 2.1}, False),     # stopped by another limit
    ({"outcome": "cancelled", "sqlstate": None, "seconds": 8.0}, False),    # ran until the hold ended
])
def test_limit_test_2_must_be_stopped_by_the_timeout_and_in_time(tmp_path, record, enforced):
    f = fault(tmp_path, settings={**SETTINGS, "role_statement_timeout": "2s"})
    f.ctx.scenario.fault.duration = 60.0
    out = f._limit_tests({"limit_tests": {"limit_temp": {"outcome": "error", "sqlstate": "53400"},
                                          "limit_timeout": record}})
    assert out["statement_timeout_enforced"] is enforced and out["temp_limit_enforced"] is True


def test_limit_tests_that_never_ended_are_not_measured(tmp_path):
    out = fault(tmp_path)._limit_tests({"limit_tests": {}})
    assert str(out["temp_limit_enforced"]) == str(NOT_MEASURED)
    assert str(out["statement_timeout_enforced"]) == str(NOT_MEASURED)


# --- the rate taken from the stored maximum (tpcc/profile.py) -------------------------------------

class CapacityAdapter:
    def __init__(self, records):
        self.records = records

    async def tpcc_capacity_records(self):
        return self.records


def resolved_profile() -> TpccProfile:
    w = Workload(profile="tpcc", concurrency=64, transaction_markers=True, rate_fraction_of_capacity=0.6,
                 tpcc={"warehouses": 283, "active_warehouses": 4})
    return TpccProfile(w)


def test_the_rate_is_the_share_of_the_newest_maximum_of_the_same_layout():
    records = [{"clerks": 64, "warehouses": 283, "active_warehouses": 4, "tps": 1400.0, "measured_at": "2026-11-14"},
               {"clerks": 64, "warehouses": 283, "active_warehouses": 4, "tps": 1500.0, "measured_at": "2026-11-15"},
               {"clerks": 64, "warehouses": 283, "active_warehouses": None, "tps": 3000.0, "measured_at": "2026-11-16"},
               {"clerks": 32, "warehouses": 283, "active_warehouses": 4, "tps": 2000.0, "measured_at": "2026-11-16"}]
    p = resolved_profile()
    rate = asyncio.run(p.resolve_rate(CapacityAdapter(records)))
    assert rate == {"fraction": 0.6, "capacity_tps": 1500.0, "measured_at": "2026-11-15", "rate_tps": 900.0}
    assert p.workload.rate_tps == 900.0 and p.workload.rate_fraction_of_capacity is None
    Workload.model_validate(p.workload.model_dump(mode="json"))     # what a worker process does with it


def test_without_a_maximum_of_the_same_layout_the_run_is_refused_with_the_command():
    records = [{"clerks": 64, "warehouses": 283, "active_warehouses": None, "tps": 3000.0, "measured_at": "x"}]
    with pytest.raises(TpccDataError, match="--clerks 64 --active-warehouses 4"):
        asyncio.run(resolved_profile().resolve_rate(CapacityAdapter(records)))


# --- the customers apart from the harness's main program (workload.tpcc.worker_processes) --------

def test_worker_processes_put_the_clerks_in_programs_of_their_own_even_at_a_low_rate(monkeypatch):
    from resilience_tests.execution.workload.tpcc import profile as profile_mod
    from resilience_tests.execution.workload.tpcc.coordinator import TpccCoordinator
    monkeypatch.setattr(profile_mod.os, "cpu_count", lambda: 4)
    w = Workload(profile="tpcc", concurrency=8, transaction_markers=True, rate_tps=100,
                 tpcc={"warehouses": 4, "worker_processes": 2})
    p = TpccProfile(w)
    p.set_run_constants(100)
    adapter = SimpleNamespace(engine="postgresql", node=SimpleNamespace(model_dump=lambda mode=None: {}))
    driver = p.make_driver(adapter, SimpleNamespace(run_dir="/tmp"), None)
    assert isinstance(driver, TpccCoordinator) and driver.processes == 2       # 100 TPS alone would be 1


def test_more_worker_processes_than_the_harness_machine_can_give_are_refused(monkeypatch):
    from resilience_tests.execution.workload.tpcc import profile as profile_mod
    monkeypatch.setattr(profile_mod.os, "cpu_count", lambda: 2)                # one core stays free
    w = Workload(profile="tpcc", concurrency=8, transaction_markers=True, rate_tps=100,
                 tpcc={"warehouses": 4, "worker_processes": 2})
    p = TpccProfile(w)
    p.set_run_constants(100)
    with pytest.raises(TpccDataError, match="worker_processes 2: this harness machine has 2 CPU core"):
        p.make_driver(SimpleNamespace(), SimpleNamespace(run_dir="/tmp"), None)


def test_worker_processes_below_two_are_not_a_setting():
    with pytest.raises(ValueError):
        Workload(profile="tpcc", concurrency=8, transaction_markers=True, rate_tps=100,
                 tpcc={"warehouses": 4, "worker_processes": 1})


# --- back to normal: judged over the 60 s window, as scenario 3 ----------------------------------

def test_back_to_normal_is_judged_over_the_60_s_window_so_a_lone_checkpoint_stall_does_not_rule_it_out(tmp_path):
    from resilience_tests.analysis import rto_decomposer
    from resilience_tests.execution.workload.histogram import latency_histogram
    from resilience_tests.observability.event_stream import Event

    f = fault(tmp_path)
    assert f.slo_tps_window_s() == rto_decomposer.SLO_SUSTAIN_S and f.slo_tps_fraction() == 0.95
    s, t0 = 1_000_000_000, 1_000 * 1_000_000_000
    baseline = rto_decomposer.Baseline(2000.0, 30.0)

    def sample(i: int, slow_ms: float | None = None) -> Event:
        lat = [5.0] * 1990 + [slow_ms or 20.0] * 10        # 10 of 2,000 slow: 0.5% of the second
        return Event(t0 + (i + 1) * s, 0.0, "workload", "sample",
                     {"interval_s": 1.0, "commits": 2000, "tps": 2000.0, "p99_ms": slow_ms or 20.0,
                      "latency_hist": latency_histogram(lat)})

    # a checkpoint stall every 30 s: that second's p99 is 300 ms (10 x the baseline's), the rest is normal
    samples = [sample(i, 300.0 if i % 30 == 15 else None) for i in range(120)]
    per_second = rto_decomposer.slo_recovery(samples, t0, baseline, tps_fraction=f.slo_tps_fraction())
    windowed = rto_decomposer.slo_recovery(samples, t0, baseline, tps_fraction=f.slo_tps_fraction(),
                                           tps_window_s=f.slo_tps_window_s())
    assert per_second == (None, None)          # never 60 s without a stall second
    assert windowed == (0.0, 60.0)             # over the window, 0.5% of a few seconds is not the p99
    # a service still slow for the whole window is not back to normal, windowed or not
    slow = [sample(i, 300.0) for i in range(120)]
    lat_slow = [Event(e.t_mono_ns, e.t_wall, e.source, e.kind,
                      {**e.data, "latency_hist": latency_histogram([5.0] * 1960 + [300.0] * 40)}) for e in slow]
    assert rto_decomposer.slo_recovery(lat_slow, t0, baseline, tps_fraction=0.95, tps_window_s=60.0) == (None, None)


# --- the customers' cache hit ratio: their own calls on every table (pg_stat_statements) ---------

def test_the_customers_ratio_counts_their_calls_on_every_table_not_only_the_tables_the_reports_never_read(tmp_path):
    # the 21 November case: the untouched tables stay at 1.0 while the customers' own pages (most on
    # order_line and orders, the tables the reports flood) fall to 70% hits -- now judged, and it fails
    f = fault(tmp_path)
    f._cache = {"baseline_start": snap(0, 0), "baseline_end": snap(99_500, 500),
                "hold_start": snap(99_500, 500), "hold_end": snap(169_500, 30_500)}
    for c in f._cache.values():
        c["customers"] = {"hit": 10_000, "read": 0}
    f._cache["baseline_end"]["customers"] = f._cache["hold_start"]["customers"] = {"hit": 20_000, "read": 0}
    f._cache["hold_end"]["customers"] = {"hit": 30_000, "read": 0}
    m = f._cache_measures()
    assert m["customer_cache_hit_ratio_baseline"] == 0.995 and m["customer_cache_hit_ratio_hold"] == 0.7
    assert m["customer_cache_hit_ratio_vs_baseline"] == pytest.approx(0.7 / 0.995, abs=1e-4)     # < 0.80: fails
    facts = f.ctx.facts["analytics_cache_ratios"]
    assert facts["untouched_tables_hold"] == 1.0                     # the old measure would have passed
    assert facts["method"].startswith("pg_stat_statements")


@pytest.mark.parametrize("change, why", [
    ({"stats_reset": "2026-11-24T01:00:00+00:00"}, "was reset inside the window"),
    ({"dealloc": 3}, "discarded entries"),
])
def test_counts_that_may_have_started_again_give_no_ratio(change, why):
    ratio, reason = clerk_hit_ratio(snap(100, 0), snap(1_000, 10, **change))
    assert ratio is None and why in reason


def test_a_call_without_an_entry_or_with_a_count_that_went_down_gives_no_ratio():
    start, end = snap(100, 0), snap(1_000, 10)
    end["clerks"]["by_type"][contract.DELIVERY]["entries"] = 0
    assert "no entry for the clerks' delivery" in clerk_hit_ratio(start, end)[1]
    start, end = snap(100, 0), snap(1_000, 10)
    start["clerks"]["by_type"][contract.PAYMENT]["calls"] = 50       # end has 1: the entry started again
    assert "went down" in clerk_hit_ratio(start, end)[1]
    assert clerk_hit_ratio(snap(100, 0), snap(100, 0)) == (None, "the clerks' calls read no page inside the window")
    assert clerk_hit_ratio(snap(0, 0), snap(990, 10)) == (0.99, None)


def test_unreadable_statements_give_no_ratio_and_say_why():
    gone = {"clerks": None, "clerks_unavailable": "pg_stat_statements could not be read: X"}
    assert clerk_hit_ratio(snap(0, 0), gone) == (None, "pg_stat_statements could not be read: X")
    assert clerk_hit_ratio(None, snap(1, 1))[0] is None


def test_a_baseline_that_cannot_be_measured_is_invalid_and_names_the_reason(tmp_path):
    f = fault(tmp_path)
    f._cache = {"baseline_start": snap(0, 0), "baseline_end": snap(995, 5, dealloc=1)}
    with pytest.raises(PhaseAbort, match="could not be measured: pg_stat_statements discarded entries"):
        asyncio.run(f.before_inject())


def test_the_99_percent_validity_rule_uses_the_customers_own_calls(tmp_path):
    # untouched tables all hits, but the customers' own pages only 98%: their data is not in the cache
    f = fault(tmp_path)
    f._cache = window_cache("baseline", 980, 20)
    with pytest.raises(PhaseAbort, match="baseline cache hit ratio is 0.9800, below 0.99"):
        asyncio.run(f.before_inject())


@pytest.mark.parametrize("change, message", [
    ({"pg_stat_statements": None}, "pg_stat_statements is not installed"),
    ({"pg_stat_statements_track": None}, "library is not loaded"),
    ({"pg_stat_statements_track": "none"}, "pg_stat_statements.track is 'none'"),
])
def test_a_target_without_pg_stat_statements_is_refused_at_init(tmp_path, change, message):
    with pytest.raises(PhaseAbort, match=message):
        fault(tmp_path, settings={**SETTINGS, **change})
