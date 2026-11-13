"""TPC-C build step 6: the single-process TPC-C engine, without a database.

A scripted fake session returns every kind of outcome, so each path of the engine is checked:
what is journaled, what is counted, and that read-only transactions write nothing. The real
database is exercised by tests/test_tpcc_functions_db.py.
"""

from __future__ import annotations

import asyncio
import json
import tempfile
from collections import Counter
from pathlib import Path

import pytest

from catalog.schema import Workload
from resilience_tests.adapters.base import DatabaseSession
from resilience_tests.control.orchestrator import RunOptions, TestOrchestrator
from resilience_tests.control.matrix import RunPlanItem
from resilience_tests.execution.workload import contract as c
from resilience_tests.execution.workload import driver as driver_mod
from resilience_tests.execution.workload.markers import MarkerJournals, read_journal
from resilience_tests.execution.workload.tpcc import engine as engine_mod
from resilience_tests.execution.workload.tpcc.engine import TpccWorkloadDriver
from resilience_tests.execution.workload.tpcc.profile import TpccProfile
from resilience_tests.observability.event_stream import EventStream
from tests.test_adapter_seam import NODE, FakeAdapter
from tests.test_orchestrator import env, scenario  # noqa: F401  (pytest fixture)


class ScriptedSession(DatabaseSession):
    """New-Order: every 20th a business rollback. Payment: every 15th unknown (connection lost).
    Delivery: every 3rd a deadlock (sure rollback). Read-only types: always committed."""

    log: list = []
    counts: Counter = Counter()

    def __init__(self) -> None:
        self._closed = False

    async def commit_marker(self, seq, marker_id):
        raise AssertionError("the TPC-C engine never uses the custom marker transaction")

    async def tpcc_execute(self, txn_type, inputs, marker):
        await asyncio.sleep(0.001)
        ScriptedSession.counts[txn_type] += 1
        n = ScriptedSession.counts[txn_type]
        if txn_type == c.NEW_ORDER and n % 20 == 0:
            result = ("business_rollback", {}, "TP001")
        elif txn_type == c.PAYMENT and n % 15 == 0:
            self._closed = True
            result = ("unknown", {}, None)
        elif txn_type == c.DELIVERY and n % 3 == 0:
            result = ("definitely_aborted", {}, "40P01")
        elif txn_type == c.NEW_ORDER:
            result = ("committed", {"w": inputs["w_id"], "d": inputs["d_id"], "o_id": 3000 + n,
                                    "ol_cnt": len(inputs["items"])}, None)
        else:
            result = ("committed", {}, None)
        ScriptedSession.log.append((txn_type, marker, result[0]))
        return result

    async def try_write(self):
        return True

    async def ping(self):
        return True

    async def close(self):
        self._closed = True

    @property
    def is_closed(self):
        return self._closed


class ScriptedAdapter(FakeAdapter):
    def __init__(self, node):
        super().__init__(node)
        self.session_kwargs: list = []

    async def session(self, endpoint=None, timeout_s=5.0, **kwargs):
        self.session_kwargs.append(kwargs)
        return ScriptedSession()


def _workload(**extra):
    return Workload(profile="tpcc", concurrency=4, transaction_markers=True, rate_tps=400,
                    tpcc={"warehouses": 8}, **extra)


@pytest.fixture
def ran(monkeypatch):
    ScriptedSession.log, ScriptedSession.counts = [], Counter()
    monkeypatch.setattr(engine_mod, "SAMPLE_INTERVAL_S", 0.2)
    profile = TpccProfile(_workload(session={"lock_timeout": "5s"}))
    profile.set_run_constants(100)
    adapter = ScriptedAdapter(NODE)

    async def go():
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            stream, journals = EventStream(d / "events.jsonl"), MarkerJournals(d)
            drv = profile.make_driver(adapter, journals, stream)
            assert isinstance(drv, TpccWorkloadDriver) and isinstance(drv, c.WorkloadDriverContract)
            await drv.start()
            await asyncio.wait_for(drv.wait_until_ready(), 5)
            drv.begin_window()
            await asyncio.sleep(1.5)
            window = drv.end_window()
            await drv.stop()
            journals.close()
            stream.close()
            files = {name: read_journal(d / name).records for name in ("marker.jrnl", "acked.jrnl", "aborted.jrnl")
                     if (d / name).exists()}
            samples = [e.data for e in stream.events() if e.kind == "sample"]
            return drv, window, files, samples
    drv, window, files, samples = asyncio.run(go())
    return {"driver": drv, "window": window, "files": files, "samples": samples, "adapter": adapter}


def test_every_clerk_connects_with_its_session_settings(ran):
    assert ran["driver"].failure is None
    assert all(k == {"settings": {"lock_timeout": "5s"}} for k in ran["adapter"].session_kwargs)
    assert len(ran["adapter"].session_kwargs) >= 4                    # 4 clerks, plus reconnects


def test_only_write_transactions_are_journaled_and_each_line_names_its_type(ran):
    written = ran["files"]["marker.jrnl"]
    assert written and {r["type"] for r in written} <= {c.NEW_ORDER, c.PAYMENT, c.DELIVERY}
    calls = [x for x in ScriptedSession.log if x[0] in (c.ORDER_STATUS, c.STOCK_LEVEL)]
    assert calls and all(marker is None for _, marker, _ in calls)    # read-only: no marker at all


def test_each_outcome_lands_in_the_right_journal(ran):
    acked = {r["uuid"]: r for r in ran["files"]["acked.jrnl"]}
    aborted = {r["uuid"]: r for r in ran["files"]["aborted.jrnl"]}
    seen = Counter()
    for txn_type, marker, outcome in ScriptedSession.log:
        if marker is None:
            continue
        uid = marker[1]
        if outcome == "committed" and uid in acked:          # the very last ones may be cut off by stop()
            seen["committed"] += 1
            assert uid not in aborted
            if txn_type == c.NEW_ORDER:
                assert {"w", "d", "o_id", "ol_cnt"} <= set(acked[uid])   # for the per-order check
        elif outcome == "business_rollback" and uid in aborted:
            seen["business"] += 1
            assert aborted[uid]["business_rollback"] is True and aborted[uid]["sqlstate"] == "TP001"
        elif outcome == "definitely_aborted" and uid in aborted:
            seen["aborted"] += 1
            assert aborted[uid]["business_rollback"] is False and aborted[uid]["sqlstate"] == "40P01"
        elif outcome == "unknown":
            seen["unknown"] += 1
            assert uid not in acked and uid not in aborted               # unknown: never guessed
    assert seen["committed"] and seen["business"] and seen["aborted"] and seen["unknown"]


def test_samples_report_five_types_and_the_totals(ran):
    samples = ran["samples"]
    assert samples
    for s in samples:
        assert set(s) == set(c.SAMPLE_FIELDS) | set(c.TPCC_EXTRA_SAMPLE_FIELDS) | {c.BY_TYPE, c.BY_GROUP}
        assert set(s[c.BY_TYPE]) == set(c.TPCC_TYPES) and set(s[c.BY_GROUP]) == {"all"}
        assert sum(t["commits"] for t in s[c.BY_TYPE].values()) == s["commits"] == s[c.BY_GROUP]["all"]["commits"]
        assert sum(t["errors"] for t in s[c.BY_TYPE].values()) == s["errors"]
        for t in s[c.BY_TYPE].values():
            assert set(t) == set(c.TypeStats.__annotations__)
    total = lambda key: sum(s[key] for s in samples)
    business = sum(s[c.BY_TYPE][c.NEW_ORDER]["business_rollbacks"] for s in samples)
    assert business > 0 and total("errors") > 0 and total("indeterminate") > 0 and total("reconnects") > 0
    # a business rollback is never an error, and only New-Order has them
    assert all(s[c.BY_TYPE][t]["business_rollbacks"] == 0 for s in samples for t in c.TPCC_TYPES if t != c.NEW_ORDER)
    assert sum(s[c.BY_TYPE][c.DELIVERY]["errors"] for s in samples) == total("errors")


def test_measured_window_covers_all_types(ran):
    w = ran["window"]
    assert w.commits > 0 and w.tps > 0 and w.p99_ms is not None and w.duration_s > 1.0


def test_the_engine_needs_prepare_first():
    profile = TpccProfile(_workload())
    with pytest.raises(RuntimeError, match="prepare"):
        profile.make_driver(ScriptedAdapter(NODE), None, None)


def test_the_custom_workload_keeps_its_own_driver():
    from resilience_tests.execution.workload.hooks import NoProfileHooks, make_workload_driver
    from tests.test_adapter_seam import WORKLOAD
    with tempfile.TemporaryDirectory() as tmp:
        journals = MarkerJournals(Path(tmp))
        drv = make_workload_driver(FakeAdapter(NODE), WORKLOAD, journals, EventStream(Path(tmp) / "e.jsonl"),
                                   history=None, hooks=NoProfileHooks())
        journals.close()
    assert isinstance(drv, driver_mod.WorkloadDriver)


def test_a_fault_run_with_a_profile_not_ready_for_faults_is_refused_before_anything_is_applied(env, monkeypatch):  # noqa: F811
    """A profile whose checks after a fault are not built may only be dry-run (tpcc was, until step 7)."""
    from resilience_tests.execution.workload.profiles import base as profiles_base

    class DryOnly(TpccProfile):
        ready_for_faults = False
    monkeypatch.setitem(profiles_base._PROFILES, "tpcc", DryOnly)
    sc = scenario("NL-C-01")
    sc = sc.model_copy(update={"workload": _workload()})
    item = RunPlanItem(scenario=sc, env_class=env.env_class, role="standalone", node=env.nodes[0])
    results = asyncio.run(TestOrchestrator(item, env, RunOptions()).run())
    assert results["status"] == "aborted" and "only dry runs" in (results["error"] or "")
    phases = {p["phase"]: p["outcome"] for p in results["phases"]}
    assert phases.get("init") == "aborted" and "fault_inject" not in phases


# --- the trial command's checks (pure) -------------------------------------------------------------

from resilience_tests.execution.workload.tpcc import trial  # noqa: E402


def _sample(no=99, p=99, os_=10, d=10, sl=10, no_business=1, errors=0, indeterminate=0):
    # default: 100 New-Order attempts per sample, 1 of them a business rollback (1%)
    types = {c.NEW_ORDER: (no, no_business), c.PAYMENT: (p, 0), c.ORDER_STATUS: (os_, 0),
             c.DELIVERY: (d, 0), c.STOCK_LEVEL: (sl, 0)}
    by_type = {t: {"commits": n, "tps": n, "p90_ms": 1, "p99_ms": 2, "errors": 0, "business_rollbacks": b}
               for t, (n, b) in types.items()}
    by_type[c.PAYMENT]["errors"] = errors
    return {"commits": no + p + os_ + d + sl, "indeterminate": indeterminate, "drops": indeterminate, c.BY_TYPE: by_type}


def _checked(**changes):
    samples = [_sample(**changes) for _ in range(60)]
    s = trial.summarize(samples, 60.0, 228.0)
    s.update({"rpo_txn": 0, "phantom": 0, "aborted_kept": 0, "orders_checked": s["commits"]["new_order"],
              "payments_checked": s["commits"]["payment"], "payments_missing": 0,
              "orders_incomplete": 0, "rules": {1: 0, 4: 0}})
    return {name: ok for name, ok, _ in trial.verdicts(s)}, s


def test_trial_passes_a_clean_run():
    checks, s = _checked()
    assert all(checks.values()), checks
    assert s["tps"] == pytest.approx(228.0) and s["mix_ok"] and s["rollback_share"] == pytest.approx(0.01)


def test_trial_fails_for_each_reason():
    assert not _checked(errors=1)[0]["no errors, no unknown outcomes"]
    assert not _checked(indeterminate=1)[0]["no errors, no unknown outcomes"]
    assert not _checked(os_=0, d=0, sl=0)[0]["mix meets TPC-C minimums"]
    slow = trial.summarize([_sample() for _ in range(30)], 60.0, 228.0)      # half the rate
    assert not slow["rate_kept"]
    s = _checked()[1]
    for key, bad in (("rpo_txn", 1), ("phantom", 2), ("aborted_kept", 1), ("orders_incomplete", 3), ("rpo_txn", None)):
        broken = {name: ok for name, ok, _ in trial.verdicts({**s, key: bad})}
        assert not all(broken.values()), key
    assert not {n: ok for n, ok, _ in trial.verdicts({**s, "rules": {4: 2}})}["consistency rules 1-10, 12 hold"]
    assert not {n: ok for n, ok, _ in trial.verdicts({**s, "rules": {}})}["consistency rules 1-10, 12 hold"]


def test_trial_fails_when_fewer_orders_were_checked_than_new_orders_committed():
    # Step 8: with several worker processes the orders are in acked.<n>.jrnl; a trial that read
    # only acked.jrnl would check 0 orders and find 0 incomplete -- that must fail, not pass.
    s = _checked()[1]
    for checked in (0, s["commits"]["new_order"] - 1):
        assert not {n: ok for n, ok, _ in trial.verdicts({**s, "orders_checked": checked})}[
            "every acknowledged New-Order complete"], checked


def _verdicts(samples, seconds=10.0, rate=1.0):
    s = trial.summarize(samples, seconds, rate)
    s.update({"rpo_txn": 0, "phantom": 0, "aborted_kept": 0, "orders_checked": s["commits"]["new_order"],
              "payments_checked": s["commits"]["payment"], "payments_missing": 0,
              "orders_incomplete": 0, "rules": {1: 0}})
    return {n: ok for n, ok, _ in trial.verdicts(s)}


def test_business_rollback_share_is_checked_only_with_enough_new_orders():
    assert "business rollbacks about 1%" not in _verdicts([_sample(no_business=0) for _ in range(4)])   # 396: too few
    assert _verdicts([_sample() for _ in range(10)])["business rollbacks about 1%"]                      # 1,000 at 1%
    assert not _verdicts([_sample(no_business=0) for _ in range(10)])["business rollbacks about 1%"]     # 1,000, none
    assert not _verdicts([_sample(no=90, no_business=10) for _ in range(10)])["business rollbacks about 1%"]  # 10%


def test_trial_fails_when_a_payment_lost_its_history_row_or_was_not_checked():
    """Fix 2: the trial checks every acknowledged Payment one by one."""
    s = _checked()[1]
    name = "every acknowledged Payment saved"
    assert {n: ok for n, ok, _ in trial.verdicts(s)}[name]
    assert not {n: ok for n, ok, _ in trial.verdicts({**s, "payments_missing": 1})}[name]
    assert not {n: ok for n, ok, _ in trial.verdicts({**s, "payments_checked": 0})}[name]


# --- issue 5 (review, 13 November): planned starts, missed starts, the corrected p99 -----------------

def test_the_pacer_plans_starts_and_counts_the_ones_a_gap_skipped():
    async def go():
        p = engine_mod.PlannedPacer(100)                 # one start planned every 10 ms
        first = await p.acquire()
        second = await p.acquire()
        assert first[1] == 0 and second[1] == 0          # on schedule: nothing missed
        assert abs((second[0] - first[0]) - 0.010) < 0.002
        await asyncio.sleep(0.05)                        # 50 ms late: jitter, below the 0.1 s tolerance
        jitter = await p.acquire()
        assert jitter[1] == 0
        second = jitter
        await asyncio.sleep(0.3)                         # a gap: the database stuck or down
        planned, missed = await p.acquire()
        assert 27 <= missed <= 31                        # about 30 planned starts were never made
        assert abs(planned - (second[0] + 0.010)) < 0.002   # this start was planned right after the last
        t = asyncio.get_running_loop().time()
        after = await p.acquire()                        # no burst to catch up: the pace goes on from now
        assert after[1] == 0 and asyncio.get_running_loop().time() - t >= 0.008
        assert (await engine_mod.PlannedPacer(None).acquire())[1] == 0   # no rate: nothing is planned
    asyncio.run(go())


class OutageAdapter(ScriptedAdapter):
    """The database goes away for `down_s` once: open connections break at once and new ones are
    refused until it is back -- as after kill -9 (a crash), not a freeze."""

    def __init__(self, node, down_at, down_s):
        super().__init__(node)
        self.down_at, self.down_s = down_at, down_s
        self.t0 = None

    def down(self):
        import time as _t
        if self.t0 is None:
            return False
        e = _t.monotonic() - self.t0
        return self.down_at <= e < self.down_at + self.down_s

    async def session(self, endpoint=None, timeout_s=5.0, **kwargs):
        if self.down():
            raise ConnectionRefusedError("the database system is starting up")
        adapter = self

        class S(ScriptedSession):
            async def tpcc_execute(self, txn_type, inputs, marker):
                if adapter.down():
                    self._closed = True
                    return ("unknown", {}, None)
                return await super().tpcc_execute(txn_type, inputs, marker)
        return S()


def _run_with(adapter, monkeypatch, seconds=2.4):
    import time as _t
    ScriptedSession.log, ScriptedSession.counts = [], Counter()
    monkeypatch.setattr(engine_mod, "SAMPLE_INTERVAL_S", 0.2)
    monkeypatch.setattr(engine_mod, "RECONNECT_PAUSE_S", 0.05)
    profile = TpccProfile(_workload())
    profile.set_run_constants(100)

    async def go():
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            stream, journals = EventStream(d / "events.jsonl"), MarkerJournals(d)
            drv = profile.make_driver(adapter, journals, stream)
            await drv.start()
            await asyncio.wait_for(drv.wait_until_ready(), 5)
            adapter.t0 = _t.monotonic()
            drv.begin_window()
            await asyncio.sleep(seconds)
            window = drv.end_window()
            await drv.stop()
            journals.close()
            stream.close()
            return window, [e.data for e in stream.events() if e.kind == "sample"]
    return asyncio.run(go())


def test_a_crash_outage_now_shows_in_the_corrected_p99_and_as_missed_starts(monkeypatch):
    """Was (review): in a crash the connections break at once and the clerks reconnect, so no
    slow answer exists and p99_corrected_ms equalled p99_ms; the skipped work appeared nowhere."""
    window, samples = _run_with(OutageAdapter(NODE, down_at=0.6, down_s=0.8), monkeypatch)
    missed = sum(s["missed"] for s in samples)
    assert missed >= 0.8 * 400 * 0.6                     # most of the ~320 starts planned during the outage
    assert window.p99_corrected_ms > 500 > window.p99_ms  # the 0.8 s outage is in the corrected value only
    assert any((s["p99_corrected_ms"] or 0) > 500 for s in samples)   # seconds with no transaction have none


def test_without_a_gap_nothing_is_missed_and_both_p99_agree(monkeypatch):
    window, samples = _run_with(OutageAdapter(NODE, down_at=99, down_s=0), monkeypatch, seconds=1.2)
    assert sum(s["missed"] for s in samples) == 0
    assert window.p99_corrected_ms < 50 and abs(window.p99_corrected_ms - window.p99_ms) < 20


class FreezeAdapter(OutageAdapter):
    """The database stays connected but answers nothing for `down_s` once (a lock, a stall)."""

    async def session(self, endpoint=None, timeout_s=5.0, **kwargs):
        adapter = self

        class S(ScriptedSession):
            async def tpcc_execute(self, txn_type, inputs, marker):
                while adapter.down():
                    await asyncio.sleep(0.01)
                return await super().tpcc_execute(txn_type, inputs, marker)
        return S()


def test_a_freeze_shows_in_the_corrected_p99_and_as_missed_starts(monkeypatch):
    window, samples = _run_with(FreezeAdapter(NODE, down_at=0.6, down_s=0.8), monkeypatch)
    assert sum(s["missed"] for s in samples) >= 0.8 * 400 * 0.6
    # only one call per clerk was frozen (4 of several hundred, under 1%): the measured p99 does not
    # show the freeze at all -- exactly the coordinated-omission problem -- the corrected p99 does
    assert window.p99_corrected_ms > 500 > window.p99_ms
