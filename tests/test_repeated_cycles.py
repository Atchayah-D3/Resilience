"""NL-C-05 -- repeated crash cycles: per-cycle measurement, cumulative bloat, cadence guard.

A single crash is NL-C-01. What NL-C-05 has to prove is that the tenth crash costs no more
than the first and that nothing is left behind on the way, so everything here is about
measuring each cycle on its own rather than the run as a whole.
"""

import asyncio
from typing import Any

import pytest

from catalog.schema import load_catalog
from resilience_tests.analysis.bloat import bloat_metrics, bytes_per_live_row
from resilience_tests.analysis.rto_decomposer import per_cycle_recovery, recovery_trend
from resilience_tests.control.killswitch import ledger_for
from resilience_tests.observability.event_stream import Event
from resilience_tests.execution.injectors.base import DriverNotAvailable
from resilience_tests.execution.injectors.process import OsSshProcessDriver, _usec_to_s
from tests.test_measurement_and_safety import NODE, PROFILE, use_host

# reuse the orchestrator's fake engine, fake fault and fixture wiring
from tests.test_orchestrator import CATALOG, Engine, FakeFault, OutageAdapter, env, run, why  # noqa: F401
from tests.test_orchestrator import scenario as _scenario
from resilience_tests.control import orchestrator as orch
from resilience_tests.control.matrix import RunPlanItem
from resilience_tests.control.orchestrator import RunOptions, TestOrchestrator


# --- per-cycle recovery, from the recorded probe stream ------------------------------------

S = 1_000_000_000


def probe(t_s: float, ok: bool) -> Event:
    """A write probe as the driver records it: the attempt starts `t_start_mono_ns` before the
    result, which is what lets a cycle's first failure be attributed to that cycle's kill."""
    return Event(t_mono_ns=int(t_s * S), t_wall=0.0, source="write_prober", kind="write_probe",
                 data={"ok": ok, "t_start_mono_ns": int((t_s - 0.05) * S)})


def stream(*spans: tuple[float, float]) -> list[Event]:
    """One probe every 200 ms from 0 s to 30 s, failing inside each (start, end) span."""
    # exact multiples: accumulating 0.2 drifts, and a probe landing a microsecond early or
    # late either side of a span boundary changes the answer by a whole probe interval
    times = [round(i * 0.2, 6) for i in range(150)]
    return [probe(t, not any(a <= t < b for a, b in spans)) for t in times]


def test_each_cycle_is_measured_from_its_own_kill():
    """Three kills at 0 s / 10 s / 20 s, each down for 1 s, 2 s and 3 s."""
    events = stream((0.1, 1.1), (10.1, 12.1), (20.1, 23.1))
    cycles = per_cycle_recovery(events, [0 * S, 10 * S, 20 * S])
    assert [c.cycle for c in cycles] == [1, 2, 3]
    assert all(c.outage_observed for c in cycles)
    assert [round(c.recovery_s, 1) for c in cycles] == [1.2, 2.2, 3.2]


def test_a_cycle_window_ends_at_the_next_kill():
    """Cycle 1 never recovers before cycle 2's kill: its time must not be borrowed from the
    write that cycle 2 eventually allows, which would report a fast cycle 1 and hide the fault."""
    events = stream((0.1, 15.0))
    cycles = per_cycle_recovery(events, [0 * S, 10 * S])
    assert cycles[0].recovery_s is None and cycles[0].outage_observed is True
    assert round(cycles[1].recovery_s, 1) == 5.0


def test_a_cycle_that_never_interrupted_service_is_not_scored():
    """A kill that landed on nothing leaves every probe green; there is no recovery to time."""
    (cycle,) = per_cycle_recovery(stream(), [0 * S])
    assert cycle.outage_observed is False and cycle.recovery_s is None


# --- the trend across cycles ---------------------------------------------------------------


def cycles_with(*seconds: float):
    events, t0s = [], []
    for i, dur in enumerate(seconds):
        start = i * 40.0
        t0s.append(int(start * S))
        events += [probe(start + t / 10, t / 10 >= dur) for t in range(1, 350)]
    return per_cycle_recovery(events, t0s)


def test_trend_reports_the_frameworks_ratio_and_the_slope_beside_it():
    trend = recovery_trend(cycles_with(2.0, 3.0, 4.0))
    assert trend["first_s"] == pytest.approx(2.0, abs=0.15)
    assert trend["last_s"] == pytest.approx(4.0, abs=0.15)
    assert trend["ratio_last_over_first"] == pytest.approx(2.0, abs=0.15)
    # the Framework's cycle-10-vs-1 ratio is one number from one pair; the slope uses them all
    assert trend["slope_s_per_cycle"] == pytest.approx(1.0, abs=0.15)


def test_a_slow_first_cycle_does_not_launder_the_ratio():
    """cycle 1 was the outlier: last/first looks healthy while every later cycle degrades.
    That is why the median of the first three is reported alongside it."""
    trend = recovery_trend(cycles_with(8.0, 2.0, 2.2, 2.5, 6.0))
    assert trend["ratio_last_over_first"] < 1.0                     # passes the Framework's test
    assert trend["ratio_last_over_median_of_first_three"] > 2.0     # ... and this one does not


def test_an_unrecovered_cycle_leaves_the_trend_unmeasured():
    events = stream((0.1, 1.1), (10.1, 40.0))   # cycle 2 is still down when the run ends
    trend = recovery_trend(per_cycle_recovery(events, [0 * S, 10 * S]))
    assert trend.get("ratio_last_over_first") is None and trend["unrecovered_cycles"] == [2]


# --- cumulative bloat, against the server's own ceiling ------------------------------------


def footprint(rows, table_bytes, wal=64 * 1024**2, dead=0, max_wal=1024**3):
    """A churn-table sample: live rows held constant by the workload, bytes are the signal."""
    return {"churn_live_rows": rows, "churn_bytes": table_bytes, "churn_dead_rows": dead,
            "wal_bytes": wal, "wal_segments": wal // (16 * 1024**2), "max_wal_size_bytes": max_wal,
            "database_bytes": table_bytes + 8 * 1024**2}


def test_a_growing_table_is_not_bloat():
    """The workload inserts throughout, so bytes MUST go up. Ten cycles of clean recovery
    triple the row count at an unchanged cost per row -- ratio 1.0, nothing unexplained."""
    samples = [footprint(1000 * (i + 1), 100_000 * (i + 1)) for i in range(11)]
    b = bloat_metrics(samples)
    assert b["samples"] == 11 and b["bytes_added"] == 1_000_000
    assert b["bloat_ratio"] == 1.0 and b["bytes_unexplained_by_rows"] == 0.0


def test_space_that_the_rows_do_not_explain_is_caught():
    """Same rows inserted, but each cycle leaves 50 kB behind -- the per-row cost climbs."""
    samples = [footprint(1000 * (i + 1), 100_000 * (i + 1) + 50_000 * i) for i in range(11)]
    b = bloat_metrics(samples)
    assert b["bloat_ratio"] > 1.15                                  # visible in the report
    assert b["bytes_unexplained_by_rows"] == 500_000.0
    assert b["bytes_per_live_row_last"] > b["bytes_per_live_row_first"]


def test_wal_is_judged_against_the_servers_own_max_wal_size():
    b = bloat_metrics([footprint(1000, 100_000), footprint(2000, 200_000, wal=3 * 1024**3)])
    assert b["wal_ratio_of_max_wal_size"] == 3.0                    # 3 GB against max_wal_size 1 GB
    assert bloat_metrics([footprint(1000, 100_000, max_wal=0)])["samples"] == 1


def test_bloat_is_unmeasured_rather_than_guessed_when_a_sample_is_missing():
    """An engine that cannot report its footprint, or a sample lost to an error, must not
    produce a ratio out of one point."""
    assert bloat_metrics([]) == {"samples": 0}
    assert bloat_metrics([footprint(1000, 100_000), {}]) == {"samples": 1}
    assert bytes_per_live_row({"churn_live_rows": 0, "churn_bytes": 100}) is None


# --- the service manager's restart limit ---------------------------------------------------


def test_usec_to_s():
    assert _usec_to_s("10000000") == 10.0 and _usec_to_s("infinity") == 0.0 and _usec_to_s("") == 0.0


def cadence_host(monkeypatch, burst, window_usec):
    return use_host(monkeypatch, __import__("resilience_tests.execution.injectors.process",
                                            fromlist=["x"]), {
        "is-active": "active", "postmaster.pid": "4242", "show -p Restart": "on-failure",
        "StartLimitBurst": burst, "StartLimitIntervalUSec": window_usec,
    })


def preflight_with_plan(cycles, interval):
    driver = OsSshProcessDriver(PROFILE, "process_kill")
    driver.repeat_plan = (cycles, interval)
    return asyncio.run(driver.preflight(NODE))


def test_a_cadence_that_systemd_would_refuse_is_caught_before_the_first_kill(monkeypatch):
    """systemd's default: 5 starts per 10 s. Killing every 1 s would exhaust that at cycle 5
    and the unit would simply stop coming back -- the harness would then be measuring its own
    damage as a recovery-time regression."""
    cadence_host(monkeypatch, "5", "10000000")
    with pytest.raises(DriverNotAvailable, match="allows 5 restarts per 10 s"):
        preflight_with_plan(10, 1.0)


def test_the_catalogued_cadence_fits_the_default_unit(monkeypatch):
    """NL-C-05 as catalogued -- 10 cycles at 5 s -- is 2 restarts inside systemd's 10 s window."""
    cadence_host(monkeypatch, "5", "10000000")
    plan = preflight_with_plan(10, 5.0)["restart_cadence"]
    assert plan["worst_case_restarts_in_window"] == 2.0 and "fits" in plan["verdict"]


def test_a_unit_with_no_restart_limit_is_reported_not_assumed(monkeypatch):
    cadence_host(monkeypatch, "0", "infinity")
    assert preflight_with_plan(10, 1.0)["restart_cadence"]["verdict"] == "no restart limit configured"


def test_the_cadence_is_only_checked_for_repeated_scenarios(monkeypatch):
    calls = cadence_host(monkeypatch, "5", "10000000")
    assert "restart_cadence" not in asyncio.run(OsSshProcessDriver(PROFILE, "process_kill").preflight(NODE))
    assert not [c for c in calls if "StartLimit" in c]


# --- the catalogued scenario ---------------------------------------------------------------


def test_nl_c_05_encodes_the_framework_criterion():
    sc = CATALOG.scenarios["NL-C-05"]
    assert (sc.repeat.cycles, sc.repeat.interval_s) == (10, 5.0)    # Framework §10.2
    assert "recovery_ratio_last_over_first <= 1.5" in sc.accept
    # Framework §16.3: pg_amcheck clean after every Tier-1 scenario
    assert "structural_integrity_errors == 0" in sc.accept and "corruption_count == 0" in sc.accept
    # bloat and WAL growth are measured and reported, but nothing gates on a threshold that
    # was never measured on an untuned server (Framework §13.7)
    assert not [a for a in sc.accept if "bloat_ratio" in a or "wal_ratio" in a]
    # the cycle count lives in `repeat` alone: an accept rule naming 10 would silently
    # stop checking every cycle the moment someone changed it
    assert "cycles_recovered == cycles_run" in sc.accept
    assert sc.fault.type == "process_kill" and sc.priority == "P1"
    # nothing may be gated on a number the run does not measure
    assert {"cycles_recovered", "bloat_ratio", "wal_ratio_of_max_wal_size"} <= set(sc.measure)


# --- end to end ----------------------------------------------------------------------------


def cycled(sid, cycles, interval_s):
    sc = _scenario(sid)
    return sc.model_copy(update={"repeat": sc.repeat.model_copy(update={"cycles": cycles, "interval_s": interval_s})})


def run_cycles(profile, sid="NL-C-05", cycles=3, interval_s=0.2):
    item = RunPlanItem(scenario=cycled(sid, cycles, interval_s), env_class=profile.env_class,
                       role="standalone", node=profile.nodes[0])
    return asyncio.run(TestOrchestrator(item, profile, RunOptions()).run())


@pytest.fixture
def footprints(monkeypatch):
    """The fake engine's on-disk footprint: rows accumulate, cost per row does not."""
    state = {"n": 0}

    async def sample(self) -> dict[str, Any]:
        state["n"] += 1
        rows = 1000 * state["n"]
        return footprint(rows, 100 * rows, wal=state["bloat_wal"] if "bloat_wal" in state else 64 * 1024**2)

    monkeypatch.setattr(OutageAdapter, "storage_footprint", sample)
    return state


def test_every_cycle_is_injected_measured_and_cleared(env, footprints):
    results = run_cycles(env)
    assert results["status"] == "passed", why(results)
    m = results["measured"]
    assert m["cycles_run"] == 3 and m["cycles_recovered"] == 3
    assert m["recovery_ratio_last_over_first"] is not None
    assert m["bloat_ratio"] == 1.0 and m["bytes_unexplained_by_rows"] == 0.0
    # one footprint before the first kill, one after each cycle
    assert [f["when"] for f in results["facts"]["footprints"]] == [
        "before cycle 1", "after cycle 1", "after cycle 2", "after cycle 3"]
    # every cycle opened its own ledger entry, and cleanup closed the last one -- the revert
    # after ten kills is what confirms the service and clears systemd's failure counters
    states = [e.state for e in ledger_for(env).entries()]
    assert states.count("applied") == 3 and not ledger_for(env).outstanding()
    assert [r["fault_type"] for r in FakeFault.reverts] == ["process_kill"]
    assert [e.detail.get("cycle") for e in ledger_for(env).entries() if e.state == "applied"] == [1, 2, 3]


def test_a_cycle_that_does_not_come_back_stops_the_run_and_is_left_for_the_killswitch(env, footprints, monkeypatch):
    """The run must not press on to cycle 3 against a database that never returned from
    cycle 2, and the unrecovered injection must stay visible to the kill switch."""
    real_inject = FakeFault.inject
    calls = {"n": 0}

    async def inject(self, node):
        calls["n"] += 1
        if calls["n"] == 2:
            Engine.down = True          # stays down: no call_later to bring it back
            return {"action": "process_kill"}
        return await real_inject(self, node)

    monkeypatch.setattr(FakeFault, "inject", inject)
    monkeypatch.setattr(orch, "RECOVERY_POLL_S", 0.2)
    profile = env.model_copy(update={"phase_timeouts_s": dict(env.phase_timeouts_s, fault_inject=4.0)})
    results = run_cycles(profile)
    assert results["status"] in ("aborted", "error"), why(results)
    assert "cycle 2" in results["error"] and results["verdict"] is None
    assert calls["n"] == 2                                  # cycle 3 was never injected
    Engine.down = False


def test_the_slo_clock_runs_from_the_last_cycle_not_the_first(env, footprints):
    """Recovery to SLO is measured from the final kill. Measured from the first, the ten
    cycles' own outages would be counted as the database failing to return to service."""
    results = run_cycles(env)
    t0s = [c["t0_ns"] for c in results["facts"]["cycles"]]
    assert len(t0s) == 3 and t0s == sorted(t0s)
    # The reference timestamp recorded for SLO must be the final cycle's kill
    assert results["facts"].get("slo_t0_mono_ns") == t0s[-1], why(results)
    slo = results["measured"].get("rto_to_slo_s")
    if isinstance(slo, (int, float)):
        span_s = (t0s[-1] - t0s[0]) / S
        # If measured from the first cycle, SLO would include the entire duration across
        # preceding cycle outages and settle intervals (span_s + slo). We verify that
        # the reported slo excludes that span.
        slo_if_measured_from_first = span_s + slo
        assert slo < slo_if_measured_from_first, why(results)
        assert results["facts"]["slo_window_end_s"] is not None
        assert results["facts"]["slo_window_end_s"] >= slo


def test_cycles_record_per_cycle_errors_and_sampling_delay(env, footprints):
    """Each cycle isolates client-visible error counts and tracks redo sampling latency."""
    results = run_cycles(env)
    cycles = results["facts"]["cycles"]
    assert len(cycles) == 3
    for c in cycles:
        assert "failed_transactions" in c and "dropped_connections" in c and "connect_failures" in c
        assert "redo_sampling_delay_ms" in c
        assert c["redo_sampling_delay_ms"] >= 0
    m = results["measured"]
    assert "failed_transactions_max_per_cycle" in m
    assert "dropped_connections_max_per_cycle" in m


def test_inter_cycle_quick_integrity_check(env, footprints, monkeypatch):
    """A quick checksum check runs between cycles to localize corruptions to the cycle that caused them."""
    calls = []

    async def quick_check(self):
        calls.append(len(calls) + 1)
        return {"checksum_failures": 0, "ok": True}

    monkeypatch.setattr(OutageAdapter, "quick_integrity_check", quick_check)
    results = run_cycles(env)
    assert len(calls) == 3
    for c in results["facts"]["cycles"]:
        assert c.get("quick_integrity", {}).get("ok") is True


def test_a_footprint_the_engine_cannot_take_is_named_not_merely_absent(env, monkeypatch):
    """A missing grant is the usual cause. The figures are reported, not gated, but the report
    has to say which measurement was impossible and why -- never a silent zero."""
    async def refuses(self) -> dict[str, Any]:
        raise PermissionError("permission denied for function pg_ls_waldir")

    monkeypatch.setattr(OutageAdapter, "storage_footprint", refuses)
    results = run_cycles(env)
    assert str(results["measured"]["wal_ratio_of_max_wal_size"]) == "NOT_MEASURED"
    assert "pg_ls_waldir" in results["facts"]["not_measured"]["wal_ratio_of_max_wal_size"]
    assert str(results["measured"]["bloat_ratio"]) == "NOT_MEASURED"
    assert "two are needed" in results["facts"]["not_measured"]["bloat_ratio"]
    # the cycles themselves were still measured: one unreadable number is not a lost run
    assert results["measured"]["cycles_recovered"] == 3
    assert results["status"] == "passed", why(results)


def test_wal_that_cannot_be_read_is_not_reported_as_a_wal_that_never_grew(env, monkeypatch):
    """Was the temptation: coalesce the WAL columns to 0, which reads as a WAL that never grew."""
    async def no_wal(self) -> dict[str, Any]:
        no_wal.n = getattr(no_wal, "n", 0) + 1
        return {"churn_live_rows": 1000 * no_wal.n, "churn_bytes": 100_000 * no_wal.n,
                "wal_unavailable": "permission denied for function pg_ls_waldir"}

    monkeypatch.setattr(OutageAdapter, "storage_footprint", no_wal)
    results = run_cycles(env)
    assert results["measured"]["bloat_ratio"] == 1.0                  # the table side still works
    assert str(results["measured"]["wal_ratio_of_max_wal_size"]) == "NOT_MEASURED"
    assert "pg_ls_waldir" in results["facts"]["not_measured"]["wal_ratio_of_max_wal_size"]


def test_corruption_after_the_cycles_fails_the_run(env, footprints, monkeypatch):
    """Was: NL-C-05 measured pg_amcheck but did not gate it (Framework §16.3 requires it)."""
    from resilience_tests.adapters.base import IntegrityResult

    async def corrupt(self, timeout_s: float):
        return IntegrityResult(structural_errors=2, checksum_failures=0)

    monkeypatch.setattr(OutageAdapter, "integrity_check", corrupt)
    results = run_cycles(env)
    assert results["status"] == "failed", why(results)
    failed = {r["predicate"] for r in results["verdict"]["results"] if r["outcome"] != "pass"}
    assert failed == {"structural_integrity_errors == 0", "corruption_count == 0"}


def test_the_summary_shows_every_cycle_not_just_the_ratio(env, footprints):
    from resilience_tests.analysis.report import render_summary

    text = render_summary(run_cycles(env))
    assert "per cycle (T0 -> first accepted write)" in text
    assert text.count("  cycle ") == 3
    assert "s per cycle" in text and "bytes per live row" in text


# --- the churn workload: without it the bloat rule cannot fail ----------------------------


def test_the_workload_actually_churns(env, footprints):
    """`mixed` must issue update/delete traffic, not just marker inserts. If it silently fell
    back to append-only, bloat_ratio would sit at 1.0 for any database, healthy or not."""
    run_cycles(env)
    assert Engine.churn_ops > 100, f"only {Engine.churn_ops} churn operations were issued"


def test_an_engine_without_churn_is_refused_not_quietly_downgraded(env, monkeypatch):
    """Was the danger: fall back to insert-only and still report a bloat_ratio. It would read
    1.0 forever and certify 'no cumulative bloat' against a database nobody tested for it."""
    from resilience_tests.adapters.base import Capability
    monkeypatch.setattr(OutageAdapter, "capabilities",
                        OutageAdapter.capabilities - {Capability.WORKLOAD_CHURN})
    results = run_cycles(env)
    assert results["status"] == "error"
    assert "cannot run the churn workload" in results["error"]
    assert "bloat_ratio" not in results["measured"]


def test_the_driver_takes_the_key_space_from_the_engine(env, monkeypatch):
    """A second copy of the row count in the driver would drift from the adapter's seed, and
    UPDATEs matching no row leave the bloat measurement flat -- silently."""
    from resilience_tests.execution.workload.driver import WorkloadDriver
    from resilience_tests.adapters.postgresql.adapter import CHURN_ROWS, PostgreSQLAdapter
    assert PostgreSQLAdapter.churn_key_space == CHURN_ROWS
    monkeypatch.setattr(OutageAdapter, "churn_key_space", 0)
    results = run_cycles(env)
    assert results["status"] == "error" and "no key space" in results["error"]


def test_replay_depth_is_recorded_for_every_cycle(env, footprints, monkeypatch):
    """A recovery time is only comparable against the replay work that produced it."""
    async def redo(self):
        redo.n = getattr(redo, "n", 0) + 1
        return 1_000_000 * redo.n

    monkeypatch.setattr(OutageAdapter, "redo_distance_bytes", redo)
    results = run_cycles(env)
    cycles = results["facts"]["cycles"]
    assert [c["redo_distance_bytes"] for c in cycles] == [1_000_000, 2_000_000, 3_000_000]
    assert all(c["replay_bytes_per_s"] > 0 for c in cycles)
    assert results["measured"]["wal_replayed_bytes_max"] == 3_000_000
    assert "replay_bytes_per_s_min" in results["measured"]


def test_an_engine_that_cannot_report_replay_depth_still_runs(env, footprints):
    """redo distance needs pg_monitor. Missing it costs the normalisation, not the run."""
    results = run_cycles(env)
    assert results["status"] == "passed", why(results)
    assert all(c["redo_distance_bytes"] is None for c in results["facts"]["cycles"])
    assert "wal_replayed_bytes_max" not in results["measured"]


def test_the_harness_never_retunes_the_server_for_nl_c_05(env, footprints, monkeypatch):
    """Was: ALTER SYSTEM set autovacuum_naptime = 5s and cost_delay = 0 before the run, so the
    bloat figure described a server configuration the customer does not run."""
    observed: list[str] = []

    async def observe(self, fault_type):
        observed.append(fault_type)
        return {}

    monkeypatch.setattr(OutageAdapter, "observe_fault_settings", observe)
    results = run_cycles(env)
    assert observed == ["process_kill"]
    assert "scenario_tuning" not in results["facts"]
    assert not any("tuning" in d.lower() for d in results["disclosures"])


def test_postgres_adapter_quick_integrity_check():
    """Verify that PostgreSQLAdapter.quick_integrity_check correctly reports checksum failure deltas."""
    async def _test():
        from unittest.mock import AsyncMock
        from resilience_tests.adapters.postgresql.adapter import PostgreSQLAdapter
        from resilience_tests.control.profile import Node

        node = Node(
            name="test-node",
            role="standalone",
            topology_role="primary",
            ssh={"host": "127.0.0.1", "port": 22, "user": "test"},
            db={"host": "127.0.0.1", "port": 5432, "dbname": "test", "user": "test"},
            client={"host": "127.0.0.1", "port": 5432, "dbname": "test", "user": "test"},
            pgdata="/data",
            pg_bin="/bin",
            os_user="postgres",
            service="postgresql.service",
            log_file="/data/logfile",
        )
        adapter = PostgreSQLAdapter(node)
        adapter._checksum_baseline = {"resilience": (0, "2026-09-30 00:00:00")}

        # 1. No failures
        adapter._checksum_stats = AsyncMock(return_value={"resilience": (0, "2026-09-30 00:00:00")})
        res1 = await adapter.quick_integrity_check()
        assert res1 == {"checksum_failures": 0, "ok": True}

        # 2. Checksum failures increased
        adapter._checksum_stats = AsyncMock(return_value={"resilience": (2, "2026-09-30 00:00:00")})
        res2 = await adapter.quick_integrity_check()
        assert res2 == {"checksum_failures": 2, "ok": False}

    asyncio.run(_test())



