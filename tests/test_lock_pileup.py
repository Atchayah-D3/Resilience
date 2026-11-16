"""Tests for NL-N-I03 (lock pile-up behind a schema change): the internal-scenario id and its
catalog fields, the contended-table workload, the fault's measures and its driver.

No database: the workload runs against the in-memory engine of test_adapter_seam, the measures
are computed from hand-built events, and the driver acts on a fake adapter.
"""

from __future__ import annotations

import asyncio
import json
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from catalog.schema import CATALOG_ROOT, SCENARIO_REF_RE, Scenario, load_catalog, load_reference, run_checks
from resilience_tests.adapters.base import Capability, TransactionOutcome
from resilience_tests.analysis import activity, rto_decomposer
from resilience_tests.execution.probes.activity import parse_vmstat_line
from resilience_tests.analysis.predicates import NOT_MEASURED
from resilience_tests.control.faults import base as faults_base
from resilience_tests.control.faults.lock_pileup import LockPileupFault
from resilience_tests.control.orchestrator import RECOVERY_EXIT_MARGIN_S
from resilience_tests.control.profile import load_profile
from resilience_tests.execution.injectors.base import FaultNotLanded, resolve
from resilience_tests.execution.injectors.lock_pileup import OsSshLockPileupDriver
from resilience_tests.execution.injectors.process import OsSshProcessDriver
from resilience_tests.execution.workload.driver import (
    CONTENDED_GROUP,
    OTHER_GROUP,
    RateLimiter,
    UnsupportedWorkload,
    WorkloadDriver,
)
from resilience_tests.execution.workload.markers import MarkerJournals, diff_from_journals
from resilience_tests.observability.event_stream import Event, EventStream
from tests.test_adapter_seam import NODE, FakeAdapter, FakeSession

CATALOG = load_catalog()
IDS = ("NL-N-I03a", "NL-N-I03b", "NL-N-I03c")
PROFILE = load_profile("e2-dedicated-vm")
S = 1_000_000_000   # ns per second


def raw(sid: str = "NL-N-I03a") -> dict[str, Any]:
    return yaml.safe_load((CATALOG_ROOT / "NL" / f"{sid}.yaml").read_text())


def validate(d: dict[str, Any]) -> Scenario:
    return Scenario.model_validate(d)


# --- catalog ---------------------------------------------------------------------------------

def test_the_three_runs_are_internal_scenarios_of_category_nl_n():
    for sid in IDS:
        sc = CATALOG.scenarios[sid]
        assert sc.internal and sc.category == "NL-N" and sc.number == 3, sid
        assert sc.fault.type == "lock_pileup" and sc.fault.section == "os_ssh"
        assert "structural_integrity_errors == 0" in sc.accept   # Framework §16.3, every run
        assert "fault_confirmed == true" in sc.accept            # the pile-up must be shown to have formed
    assert not CATALOG.scenarios["NL-R-04"].internal


def test_only_run_3a_is_judged_on_the_lock_timeout():
    judged = {sid for sid in IDS if any("ddl_lock_timeout_fired" in a for a in CATALOG.scenarios[sid].accept)}
    assert judged == {"NL-N-I03a"}


def test_runs_3a_and_3b_judge_the_checks_of_the_scenario_3_document():
    a, b = CATALOG.scenarios["NL-N-I03a"], CATALOG.scenarios["NL-N-I03b"]
    for check in ("longest_table_wait_s <= 6.0", "other_tables_tps_lost_pct <= 10", "rto_to_slo_s <= 60",
                  "blocker_visible == true", "rpo_txn == 0", "corruption_count == 0"):
        assert check in a.accept, check
    assert "ddl_completed == true" in b.accept               # 3b: the schema change completes once the read ends
    for sc in (a, b):     # the document: TPC-C's stock, cleanup checks, no damaged data, intended-start latency
        assert sc.workload.profile == "tpcc" and sc.fault.lock_pileup.table == "tpcc_stock"
        assert sc.workload.latency_from == "intended_start" and sc.steady_state.warmup_s == 300
        for check in ("probe_column_absent == true", "harness_sessions_left == 0", "settings_unchanged == true",
                      "partial_commits == 0", "tpcc_consistency_violations == 0"):
            assert check in sc.accept, (sc.id, check)
    assert a.priority == b.priority == "P0"
    assert a.fault.lock_pileup.ddl_after_s == b.fault.lock_pileup.ddl_after_s == 60.0   # minute 1


def test_internal_scenarios_stay_out_of_the_framework_reconciliation():
    # NL-N declares 8 Framework rows, none authored: the three internal runs must not count
    results = run_checks(CATALOG, load_reference(), complete=False)
    assert all(r.passed for r in results), [(r.number, r.detail) for r in results if not r.passed]


def test_a_lettered_variant_is_only_for_an_internal_id():
    d = raw()
    d["id"] = "NL-N-03a"
    with pytest.raises(ValidationError, match="lettered variants"):
        validate(d)
    assert SCENARIO_REF_RE.findall("see NL-N-I03b and NL-R-04, not NL-R-04a") == ["NL-N-I03b", "NL-R-04"]


def test_an_internal_scenario_is_not_environment_sensitive():
    d = raw()
    d["environment"]["env_sensitive"] = True
    with pytest.raises(ValidationError, match="environment-sensitive"):
        validate(d)


@pytest.mark.parametrize("sid, change, message", [
    ("NL-N-I03a", lambda d: d["fault"].pop("lock_pileup"), "lock_pileup"),
    ("NL-N-I03a", lambda d: d["fault"]["lock_pileup"].update(ddl_after_s=0.0), "ddl_after_s"),
    ("NL-N-I03a", lambda d: d["fault"]["lock_pileup"].update(blocker="none"), "ddl_after_s"),
    ("NL-N-I03a", lambda d: d["fault"]["lock_pileup"].update(ddl_after_s=d["fault"]["duration"]), "ddl_after_s"),
    ("NL-N-I03a", lambda d: d["fault"].update(duration="permanent"), "duration"),
    # 3a is on TPC-C's stock; the harness's contended table needs its marker workload share
    ("NL-N-I03a", lambda d: d["fault"]["lock_pileup"].update(table="contended_table"), "contended_share"),
    ("NL-N-I03c", lambda d: d["fault"]["lock_pileup"].update(table="tpcc_stock"), "tpcc_stock"),
    ("NL-N-I03c", lambda d: d["workload"].pop("contended_share"), "contended_share"),
    ("NL-N-I03c", lambda d: d["requires"].remove("contended_table"), "contended_table"),
    ("NL-N-I03c", lambda d: d["workload"].update(contended_share=1.0), "contended_share"),
])
def test_a_malformed_lock_pileup_scenario_is_refused(sid, change, message):
    d = raw(sid)
    change(d)
    with pytest.raises(ValidationError, match=message):
        validate(d)


def test_a_lock_pileup_block_needs_the_lock_pileup_fault():
    d = raw("NL-R-04")
    d["fault"]["lock_pileup"] = raw()["fault"]["lock_pileup"]
    with pytest.raises(ValidationError, match="lock_pileup"):
        validate(d)


def test_the_new_driver_serves_the_os_ssh_section_without_changing_the_process_driver():
    assert "lock_pileup" not in OsSshProcessDriver.fault_types
    for sid in IDS:
        injector = resolve(CATALOG.scenarios[sid].fault, PROFILE)
        assert isinstance(injector, OsSshLockPileupDriver) and injector.fault_type == "lock_pileup"
    assert faults_base.has_fault_behaviour("lock_pileup") and faults_base._FAULTS["lock_pileup"] is LockPileupFault


# --- the contended-table workload -----------------------------------------------------------

class ContendedSession(FakeSession):
    def __init__(self, store: set[str], keys: list[int]) -> None:
        super().__init__(store)
        self.keys = keys

    async def commit_marker_with_contended_row(self, seq: int, marker_id: str, key: int) -> TransactionOutcome:
        self.keys.append(key)
        self.store.add(marker_id)
        return TransactionOutcome.COMMITTED


class ContendedAdapter(FakeAdapter):
    capabilities = FakeAdapter.capabilities | {Capability.CONTENDED_TABLE}
    contended_key_space = 10

    def __init__(self, node) -> None:
        super().__init__(node)
        self.keys: list[int] = []
        self.prepared = 0

    async def session(self, endpoint=None, timeout_s: float = 5.0):
        return ContendedSession(self.store, self.keys)

    async def prepare_contended_table(self) -> dict[str, Any]:
        self.prepared += 1
        return {"table": "fake.lock_target", "rows": self.contended_key_space}


def run(adapter, workload, seconds: float = 2.2) -> tuple[list[dict[str, Any]], Any]:
    async def go():
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            stream = EventStream(d / "events.jsonl")
            journals = MarkerJournals(d)
            driver = WorkloadDriver(adapter, workload, journals, stream)
            await driver.start()
            await asyncio.sleep(seconds)
            await driver.stop()
            journals.close()
            stream.close()
            events = [json.loads(line) for line in (d / "events.jsonl").read_text().splitlines()]
            diff, _ = diff_from_journals(d, await adapter.marker_ids())
            return events, diff
    return asyncio.run(go())


def test_a_share_of_transactions_uses_the_contended_table_and_each_group_is_sampled():
    adapter = ContendedAdapter(NODE)
    events, diff = run(adapter, CATALOG.scenarios["NL-N-I03c"].workload)
    assert adapter.prepared == 1
    assert any(e["kind"] == "contended_table" for e in events)
    assert diff.rpo_txn == 0 and diff.acked > 0
    assert adapter.keys and all(1 <= k <= 10 for k in adapter.keys)
    # half of the acknowledged transactions, give or take: never all, never none
    assert 0.2 < len(adapter.keys) / diff.acked < 0.8
    groups = [e["data"] for e in events if e["kind"] == "group_sample"]
    assert {g["group"] for g in groups} == {CONTENDED_GROUP, OTHER_GROUP}
    contended = sum(g["commits"] for g in groups if g["group"] == CONTENDED_GROUP)
    assert 0 < contended <= len(adapter.keys)


def test_without_a_contended_share_the_workload_is_unchanged():
    adapter = ContendedAdapter(NODE)
    events, _ = run(adapter, CATALOG.scenarios["NL-C-01"].workload, seconds=1.2)
    assert adapter.prepared == 0 and not adapter.keys
    assert not any(e["kind"] in ("group_sample", "contended_table") for e in events)


def test_an_engine_without_a_contended_table_refuses_the_workload():
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        with pytest.raises(UnsupportedWorkload, match="contended table"):
            WorkloadDriver(FakeAdapter(NODE), CATALOG.scenarios["NL-N-I03c"].workload,
                           MarkerJournals(d), EventStream(d / "e.jsonl"))


# --- the fault's measures ---------------------------------------------------------------------

T0 = 1_000 * S          # the read took its lock
ISSUED = T0 + 5 * S     # the schema change was issued


def ev(t_ns: int, source: str, kind: str, **data) -> Event:
    return Event(t_ns, 0.0, source, kind, data)


class StateAdapter:
    def __init__(self, state: dict[str, Any]) -> None:
        self.state = state

    async def lock_pileup_state(self) -> dict[str, Any]:
        return self.state

    def crash_restart_log_patterns(self) -> tuple[str, ...]:
        return (r"server process \(PID \d+\) was terminated by signal",)

    def lock_wait_log_patterns(self) -> tuple[str, ...]:
        return (r"process \d+ still waiting for \w+ on relation",)


def state(**over) -> dict[str, Any]:
    s = {"started": True, "blocker": "long_read", "ddl": "add_column", "ddl_lock_timeout_s": 5.0,
         "reader_pid": 11, "ddl_pid": 22, "ddl_issued_mono_ns": ISSUED, "ddl_ended_mono_ns": ISSUED + 5 * S,
         "ddl_outcome": "lock_timeout", "ddl_sqlstate": "55P03", "chain_confirmed": True,
         "chain": {"ddl": {"pid": 22, "blocked_by": [11]}, "blocker": {"pid": 11}, "queued_sessions": 4},
         "queue_max": 7}
    s.update(over)
    return s


def behaviour(st: dict[str, Any], sid: str = "NL-N-I03c", **facts) -> LockPileupFault:
    """The fault's behaviour on the harness's contended table (run 3c's workload) by default;
    sid NL-N-I03a for TPC-C's stock table."""
    ctx = SimpleNamespace(
        scenario=CATALOG.scenarios[sid], adapter=StateAdapter(st), facts={"lock_pileup_state": st, **facts},
        not_measured={}, disclosures=[], t0_ns=T0, baseline=SimpleNamespace(duration_s=10.0, tps=100.0, p99_ms=5.0),
        phase_timeout_s=lambda phase: 120.0)
    return LockPileupFault(ctx)


def group_samples(group: str, start_ns: int, seconds: int, commits: int, max_ms: float = 4.0) -> list[Event]:
    return [ev(start_ns + (i + 1) * S, "workload", "group_sample", group=group, interval_s=1.0,
               commits=commits, max_ms=max_ms) for i in range(seconds)]


def probes(start_ns: int, end_ns: int, step_ns: int) -> list[Event]:
    return [ev(t, "write_prober", "write_probe", ok=True) for t in range(start_ns, end_ns, step_ns)]


def measure(st: dict[str, Any], events: list[Event], sid: str = "NL-N-I03c") -> tuple[dict[str, Any], LockPileupFault]:
    b = behaviour(st, sid)
    return asyncio.run(b.measure(events)), b


def test_a_lock_timeout_that_fires_is_measured_from_the_server_state():
    events = (group_samples(OTHER_GROUP, T0 - 10 * S, 10, commits=100)       # baseline
              + group_samples(OTHER_GROUP, ISSUED, 60, commits=50)           # first minute after
              + [ev(ISSUED + 3 * S, "workload", "group_sample", group=CONTENDED_GROUP, interval_s=1.0,
                    commits=0, max_ms=4900.0)]
              + probes(ISSUED - 3 * S, ISSUED + 8 * S, S // 5)
              + [ev(ISSUED, "log_tailer", "log_line", line="LOG:  checkpoint starting: time")])
    m, b = measure(state(), events)
    assert m["ddl_lock_timeout_fired"] is True and m["ddl_completed"] is False
    assert m["queue_freeze_s"] == 5.0 and m["queue_max_sessions"] == 7
    assert m["blocker_visible"] is True
    assert m["longest_table_wait_s"] == 4.9
    assert m["other_tables_tps_lost_pct"] == 50.0
    assert m["writer_max_stall_s"] == pytest.approx(0.2)
    assert m["server_crash_restarts"] == 0


def test_a_schema_change_that_completed_did_not_time_out():
    m, _ = measure(state(ddl_outcome="completed", ddl_sqlstate=None), [])
    assert m["ddl_completed"] is True and m["ddl_lock_timeout_fired"] is False


def test_a_pile_up_that_never_formed_is_not_measured_never_a_pass():
    m, b = measure(state(ddl_issued_mono_ns=None, ddl_ended_mono_ns=None, ddl_outcome=None,
                         chain_confirmed=None, chain=None), [])
    for name in ("ddl_lock_timeout_fired", "ddl_completed", "queue_freeze_s", "queue_max_sessions",
                 "blocker_visible", "longest_table_wait_s", "other_tables_tps_lost_pct", "writer_max_stall_s"):
        assert m[name] is NOT_MEASURED, name
        assert b.ctx.not_measured[name], name


def test_a_chain_not_seen_and_a_blocker_not_named_are_told_apart():
    m, _ = measure(state(chain_confirmed=None, chain=None), [])
    assert m["blocker_visible"] is NOT_MEASURED
    m, _ = measure(state(chain={"ddl": {"pid": 22, "blocked_by": [99]}}), [])
    assert m["blocker_visible"] is False


def test_a_freeze_that_had_not_ended_is_not_measured():
    m, b = measure(state(ddl_ended_mono_ns=None, ddl_outcome=None), [])
    assert m["queue_freeze_s"] is NOT_MEASURED and "had not ended" in b.ctx.not_measured["queue_freeze_s"]


def test_a_write_stall_across_the_freeze_is_measured_whole():
    events = probes(ISSUED - 2 * S, ISSUED, S // 5) + probes(ISSUED + 4 * S, ISSUED + 6 * S, S // 5)
    m, _ = measure(state(), events)
    assert m["writer_max_stall_s"] == pytest.approx(4.2)


def test_a_crash_restart_in_the_log_is_counted_and_no_log_is_not_measured():
    line = "LOG:  server process (PID 4711) was terminated by signal 9: Killed"
    m, b = measure(state(), [ev(ISSUED + S, "log_tailer", "log_line", line=line),
                             ev(T0 - S, "log_tailer", "log_line", line=line)])   # before the fault: not ours
    assert m["server_crash_restarts"] == 1 and b.ctx.facts["crash_restart_lines"] == [line]
    m, b = measure(state(), [])
    assert m["server_crash_restarts"] is NOT_MEASURED


def _bursty(t0: int, n: int, counts: list[int], p99_ms: float = 5.0) -> list[Event]:
    """n consecutive 1 s workload samples after t0 whose commit counts cycle through `counts`."""
    return [ev(t0 + (i + 1) * S, "workload", "sample", interval_s=1.0, commits=counts[i % len(counts)],
               tps=float(counts[i % len(counts)]), p99_ms=p99_ms) for i in range(n)]


def test_back_to_normal_judges_tps_over_the_60_s_window_not_each_second():
    b = behaviour(state(), "NL-N-I03a")
    window = b.slo_tps_window_s()
    assert window == rto_decomposer.SLO_SUSTAIN_S
    baseline = rto_decomposer.Baseline(60.0, 100.0)
    # 60 TPS delivered in bursts (30 / 90 per second): every other second is under 95%, the
    # per-second form never finds a window; over 60 s the service is at 100% of its baseline
    samples = _bursty(T0, 70, [30, 90])
    assert rto_decomposer.slo_recovery(samples, T0, baseline, tps_fraction=0.95) == (None, None)
    assert rto_decomposer.slo_recovery(samples, T0, baseline, tps_fraction=0.95,
                                       tps_window_s=window) == (0.0, 60.0)


def test_the_60_s_window_still_fails_a_service_below_95_percent():
    baseline = rto_decomposer.Baseline(60.0, 100.0)
    # 56 TPS on average (93%): steady or bursty, it is not back to normal
    for counts in ([56], [26, 86]):
        samples = _bursty(T0, 90, counts)
        assert rto_decomposer.slo_recovery(samples, T0, baseline, tps_fraction=0.95, tps_window_s=60.0) == (None, None)


def test_the_60_s_window_holds_p99_in_every_second_and_breaks_on_gaps_and_starved_samples():
    baseline = rto_decomposer.Baseline(60.0, 100.0)
    samples = _bursty(T0, 90, [60])
    # one second with p99 above 1.5x: the window can only start after it
    slow = [ev(e.t_mono_ns, "workload", "sample", **{**e.data, "p99_ms": 151.0}) if i == 9 else e
            for i, e in enumerate(samples)]
    assert rto_decomposer.slo_recovery(slow, T0, baseline, tps_fraction=0.95, tps_window_s=60.0)[0] == 10.0
    # a missing sample breaks the window (what happened in it is unknown)
    gapped = samples[:30] + samples[31:]
    assert rto_decomposer.slo_recovery(gapped, T0, baseline, tps_fraction=0.95, tps_window_s=60.0) == (None, None)
    # a starved sampler (5 s interval) cannot certify the window it averages over
    starved = [ev(e.t_mono_ns, "workload", "sample", **{**e.data, "interval_s": 5.0}) if i == 20 else e
               for i, e in enumerate(samples)]
    assert rto_decomposer.slo_recovery(starved, T0, baseline, tps_fraction=0.95, tps_window_s=60.0)[0] == 21.0


def test_the_baseline_check_uses_the_same_window_rule():
    baseline = rto_decomposer.Baseline(60.0, 100.0)
    samples = _bursty(T0, 119, [30, 90])
    per_second = rto_decomposer.baseline_slo_check(samples, T0, T0 + 120 * S, baseline, 0.95)
    windowed = rto_decomposer.baseline_slo_check(samples, T0, T0 + 120 * S, baseline, 0.95, tps_window_s=60.0)
    assert not per_second.sustained_window_found and windowed.sustained_window_found
    assert windowed.compliant == per_second.compliant      # per-second evidence is unchanged


def test_scenarios_other_than_lock_pileup_keep_the_per_second_rule():
    assert faults_base.FaultBehaviour.slo_tps_window_s(object()) is None


def test_back_to_normal_is_the_documents_95_percent_watched_long_enough_to_judge_60_s():
    b = behaviour(state(), "NL-N-I03a")
    assert b.slo_tps_fraction() == 0.95
    # a return at 90% of the baseline TPS is back to the Framework's SLO but not to normal here
    samples = [ev(T0 + (i + 1) * S, "workload", "sample", interval_s=1.0, tps=90.0, p99_ms=5.0)
               for i in range(int(rto_decomposer.SLO_SUSTAIN_S) + 2)]
    baseline = rto_decomposer.Baseline(100.0, 5.0)
    assert rto_decomposer.slo_recovery(samples, T0, baseline)[0] == 0.0
    assert rto_decomposer.slo_recovery(samples, T0, baseline, tps_fraction=b.slo_tps_fraction()) == (None, None)
    # a return starting at the 60 s limit is still watched for its full sustain period
    watched_s = 120.0 + b.phase_extension_s("recovery") - RECOVERY_EXIT_MARGIN_S
    assert watched_s > 60.0 + rto_decomposer.SLO_SUSTAIN_S
    assert b.phase_extension_s("fault_inject") == 120.0      # the hold, unchanged


def test_the_servers_lock_wait_lines_are_counted_and_without_log_lock_waits_not_measured():
    wait = "LOG:  process 4711 still waiting for AccessExclusiveLock on relation 16384 of database 5 after 1000.1 ms"
    lines = [ev(ISSUED + 2 * S, "log_tailer", "log_line", line=wait),
             ev(ISSUED - S, "log_tailer", "log_line", line=wait)]         # before the schema change: not this one
    m = asyncio.run(behaviour(state(), scenario_observed={"log_lock_waits": "on"}).measure(lines))
    assert m["lock_waits_logged"] == 1
    b = behaviour(state(), scenario_observed={"log_lock_waits": "off"})
    m = asyncio.run(b.measure(lines))
    assert m["lock_waits_logged"] is NOT_MEASURED and "log_lock_waits" in b.ctx.not_measured["lock_waits_logged"]


def tpcc_samples(start_ns: int, seconds: int, users: int, others: int, users_max_ms: float = 4.0) -> list[Event]:
    """TPC-C samples: per-type commits, the table users' commits split over New-Order and Stock-Level."""
    def types(u: int, o: int, peak: float) -> dict[str, Any]:
        return {"new_order": {"commits": u - u // 2, "max_ms": peak}, "stock_level": {"commits": u // 2, "max_ms": 3.0},
                "payment": {"commits": o, "max_ms": 9.0}, "order_status": {"commits": 0, "max_ms": None},
                "delivery": {"commits": 0, "max_ms": None}}
    return [ev(start_ns + (i + 1) * S, "workload", "sample", interval_s=1.0, by_type=types(users, others, users_max_ms))
            for i in range(seconds)]


def test_on_tpcc_stock_the_table_users_are_new_order_and_stock_level():
    events = (tpcc_samples(T0 - 10 * S, 10, users=40, others=100)                   # baseline
              + tpcc_samples(ISSUED, 60, users=10, others=90, users_max_ms=5200.0))  # the minute after
    m, _ = measure(state(), events, "NL-N-I03a")
    assert m["longest_table_wait_s"] == 5.2               # New-Order's peak, not Payment's
    assert m["other_tables_tps_lost_pct"] == 10.0         # Payment + Order-Status + Delivery: 100 -> 90


class LeftoversAdapter(StateAdapter):
    def __init__(self, st: dict[str, Any], columns: list[str], fail: bool = False) -> None:
        super().__init__(st)
        self.columns, self.fail, self.reverted = columns, fail, 0

    async def revert_lock_pileup(self) -> dict[str, Any]:
        self.reverted += 1
        return {}

    async def lock_pileup_leftovers(self) -> dict[str, Any]:
        if self.fail:
            raise ConnectionRefusedError("down")
        return {"sessions": 0, "probe_columns": self.columns}


def test_the_probe_column_and_sessions_left_are_read_back_after_the_revert():
    b = behaviour(state())
    b.ctx.adapter = LeftoversAdapter(state(), ["resilience_tpcc.stock.resilience_probe_col"])
    m = asyncio.run(b.measure([]))
    assert b.ctx.adapter.reverted == 1
    assert m["probe_column_absent"] is False and m["harness_sessions_left"] == 0
    b.ctx.adapter = LeftoversAdapter(state(), [], fail=True)
    m = asyncio.run(b.measure([]))
    assert m["probe_column_absent"] is NOT_MEASURED and m["harness_sessions_left"] is NOT_MEASURED


def test_latency_from_the_intended_start_counts_the_time_a_stall_kept_a_transaction_waiting():
    async def go(keep: bool) -> float:
        limiter = RateLimiter(100.0, keep_schedule=keep)
        await limiter.acquire()
        await asyncio.sleep(0.3)                  # a stall: 30 slots go by with no worker free
        before = time.monotonic()
        return before - await limiter.acquire()   # how long ago this transaction was due
    assert asyncio.run(go(False)) <= 0.01         # schedule restarts from now: the stall is hidden
    assert asyncio.run(go(True)) >= 0.25          # schedule kept: the transaction was due ~0.29 s ago


def test_activity_is_summarised_per_phase_and_a_counter_reset_is_never_negative():
    def db(t: int, wal: int, timed: int, waits: dict[str, int]) -> Event:
        return ev(t * S, "db_activity", "sample", client_connections=10 + t, max_connections=100, lock_waiters=t,
                  wal_lsn_bytes=wal, checkpoints={"timed": timed, "requested": 0}, wait_events=waits,
                  database={"blks_hit": 90 * t, "blks_read": 10 * t, "temp_files": 0, "deadlocks": 0})
    events = [db(1, 1000, 5, {"Lock:relation": 3}), db(2, 3000, 6, {"Lock:relation": 2, "IO:DataFileRead": 1}),
              db(3, 500, 0, {}),                                          # statistics reset (a crash)
              ev(2 * S, "host_stats", "sample", us=30, sy=10, wa=20, id=40, st=0, r=3, b=1, free=500, si=0, so=0,
                 bi=5, bo=50),
              ev(4 * S, "db_activity", "unavailable", error="ConnectionRefusedError")]
    out = activity.summarize(events, {"fault": (1 * S, 3 * S), "empty": (10 * S, 11 * S)})
    d = out["fault"]["database"]
    assert d["samples"] == 3 and d["client_connections_max"] == 13 and d["lock_waiters_max"] == 3
    assert d["wal_bytes"] == 2000.0 and d["checkpoints_timed"] == 1.0      # the reset step is not subtracted
    assert d["wait_events_top"] == {"Lock:relation": 5, "IO:DataFileRead": 1}
    assert out["fault"]["host"]["cpu_busy_pct_max"] == 40 and out["fault"]["host"]["iowait_pct_max"] == 20
    assert out["empty"]["database"] == {"samples": 0}                      # never a figure it did not measure
    assert out["unavailable"]["db_activity"]["count"] == 1
    header = "r b swpd free buff cache si so bi bo in cs us sy id wa st gu".split()
    assert parse_vmstat_line(header, " 2 0 0 81 2 3 0 0 4 9 100 200 12 3 80 5 0 0")["wa"] == 5
    assert parse_vmstat_line(header, "procs ---memory---") is None


def test_others_without_baseline_samples_are_not_measured():
    m, b = measure(state(), group_samples(OTHER_GROUP, ISSUED, 10, commits=50))
    assert m["other_tables_tps_lost_pct"] is NOT_MEASURED


# --- the driver -------------------------------------------------------------------------------

class PileupAdapter:
    def __init__(self, started: dict[str, Any], st: dict[str, Any] | None = None) -> None:
        self.started, self.st, self.calls = started, st or {}, []

    async def start_lock_pileup(self, **plan) -> dict[str, Any]:
        self.calls.append(("start", plan))
        return self.started

    async def lock_pileup_state(self) -> dict[str, Any]:
        return self.st

    async def revert_lock_pileup(self) -> dict[str, Any]:
        self.calls.append(("revert", None))
        return {"sessions_remaining": 0, "probe_columns_remaining": 0}


def driver(adapter: PileupAdapter, sid: str = "NL-N-I03a") -> OsSshLockPileupDriver:
    d = OsSshLockPileupDriver(PROFILE, "lock_pileup")
    d.configure(CATALOG.scenarios[sid].fault, None, adapter)
    return d


def test_the_driver_starts_the_catalogued_plan_through_the_runs_adapter():
    adapter = PileupAdapter({"supported": True, "t0_mono_ns": 123})
    detail = asyncio.run(driver(adapter, "NL-N-I03c").inject(NODE))
    assert detail["t0_mono_ns"] == 123 and detail["action"] == "lock_pileup"
    assert adapter.calls == [("start", {"blocker": "none", "ddl": "add_column_rewrite",
                                        "ddl_lock_timeout_s": 5.0, "ddl_after_s": 0.0,
                                        "table": "contended_table"})]


def test_a_pile_up_the_engine_did_not_start_has_not_landed():
    adapter = PileupAdapter({"supported": False, "error": "read did not take its lock"})
    with pytest.raises(FaultNotLanded, match="read did not take its lock"):
        asyncio.run(driver(adapter).inject(NODE))


def test_confirmation_is_the_chain_the_engine_showed():
    assert asyncio.run(driver(PileupAdapter({}, state())).confirm(NODE, {}))["fault_confirmed"] is True
    assert asyncio.run(driver(PileupAdapter({}, state(chain_confirmed=False))).confirm(NODE, {}))[
        "fault_confirmed"] is False
    unseen = asyncio.run(driver(PileupAdapter({}, state(chain_confirmed=None, ddl_issued_mono_ns=None)))
                         .confirm(NODE, {}))
    assert unseen["fault_confirmed"] is None and "never seen" in unseen["note"]


def test_revert_goes_through_the_adapter():
    adapter = PileupAdapter({})
    assert asyncio.run(driver(adapter).revert(NODE))["sessions_remaining"] == 0
    assert adapter.calls == [("revert", None)]
