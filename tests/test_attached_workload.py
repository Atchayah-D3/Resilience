"""A scenario attached to the running common workload (specs/007, attach story), without a database.

The attach checks (found, healthy, the right workload), the copying of the workload's events into a
run, windows over them, the cut-off of the journals, and -- against the real service running the
TPC-C engine on a scripted database -- the whole path a scenario takes while the workload keeps
writing.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path

import pytest

from catalog.schema import Workload
from resilience_tests.execution.workload import attached as att
from resilience_tests.execution.workload.attached import (
    AttachedWorkload,
    AttachRefused,
    CutOff,
    SpanCheckAdapter,
    compare_snapshots,
)
from resilience_tests.execution.workload.histogram import latency_histogram
from resilience_tests.execution.workload.markers import DB_MARKERS_FILE
from resilience_tests.execution.workload.service import endchecks
from resilience_tests.execution.workload.service import state as st
from resilience_tests.observability.event_stream import Event, EventStream
from tests.test_orchestrator import env  # noqa: F401  (pytest fixture)
from tests.test_workload_service import Shop, _config, _env, service

SESSION = {"lock_timeout": "0", "statement_timeout": "0", "transaction_timeout": "0"}


def wanted(**kw):
    args = dict(profile="tpcc", concurrency=64, transaction_markers=True, rate_tps=60, tpcc={"warehouses": 64},
                session=SESSION)
    args.update(kw)
    return Workload(**args)


def sample(t_wall, commits=60, errors=0, latencies=(5.0, 7.0, 9.0)):
    return {"t_wall": t_wall, "interval_s": 1.0, "commits": commits, "tps": float(commits), "errors": errors,
            "indeterminate": 0, "drops": 0, "reconnects": 0, "connect_failures": 0,
            "p99_ms": max(latencies), "max_ms": max(latencies), "latency_hist": latency_histogram(latencies),
            "journal_hist": latency_histogram([0.5]), "by_type": {}, "by_group": {}}


def folder_with(tmp_path, *, state=st.RUNNING, samples=(), used=None, env="e2-u22-3", node="shaktidb-standalone",
                updated=None, failure=None, outage=None):
    folder = tmp_path / "workload-oltp-common-1"
    folder.mkdir(exist_ok=True)
    now = time.time()
    st.write_status(folder, {"state": state, "pid": os.getpid(), "updated_wall": now if updated is None else updated,
                             "rate_tps": 60.0, "failure": failure, "outage_now": outage})
    st.write_info(folder, {"run_id": "workload-run-1", "id": "oltp-common", "folder": str(folder), "pid": os.getpid(),
                           "env": env, "node": node, "warmup_s": 300, "clerks_started_wall": now - 600,
                           "application_name": "resilience-workload",
                           "settings": {"workload_used": used or wanted().model_dump(mode="json")},
                           "tpcc": {"orders_at_start": 1000, "history_since": "2026-11-19 10:00:00.000000+05:30"}})
    with (folder / st.METRICS_1S_FILE).open("w") as fh:
        for s in samples:
            fh.write(json.dumps(s) + "\n")
    return folder


def steady(n=30, end=None, **kw):
    end = time.time() if end is None else end
    return [sample(end - n + i + 1, **kw) for i in range(n)]


# --- found -----------------------------------------------------------------------------------------


def test_no_running_workload_is_refused_with_how_to_start_one(tmp_path):
    env = _env(tmp_path)
    with pytest.raises(AttachRefused, match="no common workload is running.*service.cli start"):
        att.find_folder(env, env.nodes[0])


def test_the_pointer_leads_to_the_folder_without_any_decision_on_the_pid(tmp_path):
    env = _env(tmp_path)
    folder = folder_with(tmp_path)
    st.write_pointer(env, env.nodes[0], folder, 2**22 + 12345)       # a pid that is gone: read, never decisive
    assert att.find_folder(env, env.nodes[0]) == folder
    st.write_pointer(env, env.nodes[0], tmp_path / "nowhere", os.getpid())
    with pytest.raises(AttachRefused, match="old pointer"):
        att.find_folder(env, env.nodes[0])


# --- the right workload ----------------------------------------------------------------------------


def test_the_running_workload_matches_when_target_profile_markers_and_session_agree(tmp_path):
    w = AttachedWorkload(folder_with(tmp_path), None)
    assert w.match_problems(wanted(), "e2-u22-3", "shaktidb-standalone") == []
    # sizes and the rate are the running workload's, never compared
    assert w.match_problems(wanted(rate_tps=500, concurrency=8, tpcc={"warehouses": 2}),
                            "e2-u22-3", "shaktidb-standalone") == []


@pytest.mark.parametrize("change, env, message", [
    ({}, "e2-dedicated-vm", "runs against e2-u22-3"),
    ({"session": {"lock_timeout": "2s"}}, "e2-u22-3", "session settings"),
    ({"profile": "oltp_write_heavy", "tpcc": None}, "e2-u22-3", "profile 'tpcc'"),
    ({"history": "list_append"}, "e2-u22-3", "records none"),
])
def test_a_workload_that_is_not_the_one_the_scenario_needs_is_refused_naming_the_difference(tmp_path, change, env,
                                                                                               message):
    w = AttachedWorkload(folder_with(tmp_path), None)
    problems = w.match_problems(wanted(**change), env, "shaktidb-standalone")
    assert any(message in p for p in problems), problems


# --- healthy ---------------------------------------------------------------------------------------


def test_a_running_workload_doing_its_work_is_healthy(tmp_path):
    assert AttachedWorkload(folder_with(tmp_path, samples=steady()), None).health_problems() == []


@pytest.mark.parametrize("kw, samples, message", [
    ({"state": st.WARMING_UP}, None, "warming up"),
    ({"state": st.FAILED, "failure": "a worker process died"}, None, "a worker process died"),
    ({"updated": time.time() - 120}, None, "status was last written"),
    ({}, "stale", "no sample for"),
    ({}, "slow", "under 75% of its rate"),
    ({}, "errors", "failed transactions"),
    ({"outage": {"started_elapsed_s": 10}}, None, "outage is open"),
    ({}, "none", "written no sample"),
])
def test_a_workload_that_is_alive_but_not_working_is_not_healthy(tmp_path, kw, samples, message):
    rows = {"stale": steady(end=time.time() - 60), "slow": steady(commits=30), "errors": steady(errors=5),
            "none": []}.get(samples, steady())
    problems = AttachedWorkload(folder_with(tmp_path, samples=rows, **kw), None).health_problems()
    assert any(message in p for p in problems), problems


def test_the_pid_only_explains_a_failure_it_does_not_decide_it(tmp_path):
    folder = folder_with(tmp_path, samples=steady(end=time.time() - 60))
    status = st.read_status(folder)
    st.write_status(folder, {**status, "pid": 2**22 + 12345})
    problems = AttachedWorkload(folder, None).health_problems()
    assert any("no sample for" in p and "process 4206649 is gone" in p for p in problems), problems
    # a live pid does not make a workload without samples healthy
    st.write_status(folder, {**status, "pid": os.getpid()})
    assert any("no sample for" in p for p in AttachedWorkload(folder, None).health_problems())


# --- copied, and windows over the copy --------------------------------------------------------------


def test_the_workloads_events_are_copied_with_their_own_times_and_windows_come_from_them(tmp_path):
    folder = folder_with(tmp_path)
    src = EventStream(folder / st.EVENTS_FILE, keep_in_memory=False)
    run = EventStream(tmp_path / "run" / "events.jsonl")
    w = AttachedWorkload(folder, run)
    src.emit("workload", "sample", **{k: v for k, v in sample(time.time()).items() if k != "t_wall"})  # before attach

    async def go():
        await w.start()
        t0 = time.time()
        w.begin_window()
        for i in range(3):
            src.emit("workload", "sample", **{k: v for k, v in sample(0, latencies=(10.0, 20.0, 40.0 + i)).items()
                                               if k != "t_wall"})
        src.emit("service", "outage_start", started_elapsed_s=12)
        src.emit("service", "measure_start", elapsed_s=300)                 # not copied
        await asyncio.sleep(0.6)
        window = w.end_window()
        await w.stop()
        return t0, window
    t0, window = asyncio.run(go())
    copied = run.events()
    kinds = [(e.source, e.kind) for e in copied]
    assert kinds == [("workload", "sample")] * 3 + [("service", "outage_start")]   # only from attach on
    original = [Event.from_json(x) for x in (folder / st.EVENTS_FILE).read_text().splitlines()]
    assert [e.t_mono_ns for e in copied] == [e.t_mono_ns for e in original[1:5]]
    assert window.commits == 180 and window.tps == pytest.approx(60.0)
    assert 42.0 <= window.p99_ms <= 42.0 * 1.02                               # histogram: never below the exact value
    assert w.window_t0_ns > 0 and w.last_window["samples"] == 3


def test_a_window_with_no_sample_counts_nothing_and_fails_every_gate(tmp_path):
    w = AttachedWorkload(folder_with(tmp_path), EventStream(tmp_path / "run.jsonl"))
    w.begin_window()
    window = w.end_window()
    assert window.tps == 0 and window.p99_ms is None and "no 1-second sample" in w.last_window["error"]


def test_the_workload_stopping_while_attached_is_a_failure_the_run_aborts_on(tmp_path, monkeypatch):
    monkeypatch.setattr(att, "SAMPLE_STALE_S", 0.5)
    monkeypatch.setattr(att, "POLL_S", 0.05)
    folder = folder_with(tmp_path)
    src = EventStream(folder / st.EVENTS_FILE, keep_in_memory=False)
    w = AttachedWorkload(folder, EventStream(tmp_path / "run.jsonl"))

    async def go():
        await w.start()
        src.emit("workload", "sample", **{k: v for k, v in sample(0).items() if k != "t_wall"})
        await w.wait_until_ready()
        await asyncio.sleep(1.2)                                               # no further sample
        failure = w.failure
        await w.stop()
        return failure
    assert "stopped working" in asyncio.run(go())


# --- the cut-off ----------------------------------------------------------------------------------------


def journals(folder, rows):
    """rows: (uuid, t_pre, t_ack or None)"""
    with (folder / "marker.1.jrnl").open("w") as m, (folder / "acked.1.jrnl").open("w") as a:
        for u, t_pre, t_ack in rows:
            m.write(json.dumps({"uuid": u, "t_pre": t_pre, "type": "new_order"}) + "\n")
            if t_ack is not None:
                a.write(json.dumps({"uuid": u, "t_ack": t_ack}) + "\n")
        m.write('{"uuid": "half-writ')                                         # still being written: left out


# times: the init snapshot at 1000, the validate snapshot at 2000
INIT = dict(t_before=1000.0, t_after=1000.5)
VAL = dict(t_before=2000.0, t_after=2000.5)
ROWS = [("old", 10, 11),               # long before the scenario: in both snapshots
        ("gone", 20, 21),              # before the scenario, destroyed during it
        ("lost-earlier", 30, 31),      # an earlier scenario's loss: missing already at init
        ("straddle", 990, 1001),       # in flight at the init snapshot, committed after it
        ("new", 1500, 1501),           # this scenario's commit
        ("late-ok", 1999, 2001),       # committed before the validate snapshot, acknowledged after
        ("after", 2001, 2002)]         # after the validate snapshot


def write_snapshot(path, uuids, orders=None):
    """A snapshot file as marker_snapshot writes it: sorted as text, one per line, gzip."""
    import gzip
    import hashlib
    text = "".join(f"{u}\n" for u in sorted(uuids)).encode()
    with gzip.open(path, "wb") as fh:
        fh.write(text)
    return {"file": str(path), "count": len(uuids), "sha256": hashlib.sha256(text).hexdigest(),
            "current_orders": orders}


class NoJournalCheckEngine:
    """An engine without journal_check: the end checks compare in memory, with the same rules."""


def check_span(tmp_path, s_init, s_val):
    """The validate path on fixed journals: copy this scenario's lines, compare the snapshots,
    run the workload service's end checks on them."""
    folder = folder_with(tmp_path)
    journals(folder, ROWS)
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    write_snapshot(run_dir / "init.gz", s_init)
    write_snapshot(run_dir / "val.gz", s_val)
    init = CutOff(run_dir / "init.gz", len(s_init), None, **INIT)
    val = CutOff(run_dir / "val.gz", len(s_val), None, **VAL)
    AttachedWorkload(folder, None).copy_span(init, val, run_dir)
    snaps = compare_snapshots(init.file, val.file, run_dir / DB_MARKERS_FILE)
    span = SpanCheckAdapter(NoJournalCheckEngine(), init, val)
    result = asyncio.run(endchecks.run_end_checks(span, run_dir, tpcc=None, markers_from_file=True))
    return result.end, snaps, val.lines, span.excluded


def test_only_this_scenarios_journal_lines_are_checked_and_nothing_is_lost(tmp_path):
    end, snaps, lines, excluded = check_span(tmp_path, {"old", "gone"}, {"old", "gone", "straddle", "new", "late-ok"})
    # the old commits are judged by the two snapshots; only straddle, new, late-ok come from the journals
    assert lines == {"marker.jrnl": 3, "acked.jrnl": 2}
    assert end["rpo_txn"] == 0 and end["phantom"] == 0 and snaps["destroyed"] == 0 and snaps["new"] == 3
    # late-ok committed before the snapshot and was acknowledged just after it: answered, not unknown
    assert end["markers"]["indeterminate"] == 0 and end["markers"]["indeterminate_committed"] == 0


def test_a_commit_destroyed_during_the_scenario_is_lost_however_old(tmp_path):
    end, snaps, *_ = check_span(tmp_path, {"old", "gone"}, {"old", "straddle", "new", "late-ok"})
    assert snaps["destroyed"] == 1 and snaps["destroyed_listed"] == ["gone"] and end["rpo_txn"] == 0
    # the orchestrator's rpo_txn = the span's lost + destroyed = 1


def test_this_scenarios_acknowledged_commit_missing_is_lost(tmp_path):
    end, snaps, *_ = check_span(tmp_path, {"old", "gone"}, {"old", "gone", "straddle", "late-ok"})
    assert end["rpo_txn"] == 1 and end["markers"]["lost"] == ["new"] and snaps["destroyed"] == 0


def test_an_earlier_scenarios_loss_is_not_charged_again(tmp_path):
    # "lost-earlier" was acknowledged long before and is missing from both snapshots
    end, snaps, *_ = check_span(tmp_path, {"old", "gone"}, {"old", "gone", "straddle", "new", "late-ok"})
    assert end["rpo_txn"] == 0 and snaps["destroyed"] == 0


def test_a_transaction_already_in_the_init_snapshot_is_judged_by_the_snapshots_only(tmp_path):
    # "straddle" committed before the init snapshot (its acknowledgement came just after it)
    end, snaps, lines, excluded = check_span(tmp_path, {"old", "gone", "straddle"},
                                             {"old", "gone", "straddle", "new", "late-ok"})
    assert excluded == {"marker": 1, "acked": 1, "aborted": 0}
    assert end["rpo_txn"] == 0 and end["phantom"] == 0 and snaps["new"] == 2


def test_a_marker_that_appeared_from_nowhere_is_a_phantom(tmp_path):
    end, *_ = check_span(tmp_path, {"old"}, {"old", "gone", "straddle", "new", "late-ok", "ghost"})
    assert end["phantom"] >= 1 and "ghost" in end["markers"]["phantom"]


def test_the_snapshots_are_compared_in_one_pass_whatever_their_size(tmp_path):
    init = {f"{i:08d}" for i in range(0, 200_000)}
    val = (init - {"00000007", "00123456"}) | {f"{i:08d}" for i in range(200_000, 260_000)}
    write_snapshot(tmp_path / "i.gz", init)
    write_snapshot(tmp_path / "v.gz", val)
    out = compare_snapshots(tmp_path / "i.gz", tmp_path / "v.gz", tmp_path / "new.gz")
    assert out["new"] == 60_000 and out["destroyed"] == 2 and out["destroyed_listed"] == ["00000007", "00123456"]


def test_the_tpcc_checks_use_the_order_count_of_the_validate_snapshot(tmp_path):
    class Real:
        async def tpcc_data_status(self):
            return {"loaded": True, "current_orders": 99999}

        def other(self):
            return "real"
    val = CutOff(tmp_path / "v.gz", 0, 1003, 1.0, 1.5)
    span = SpanCheckAdapter(Real(), CutOff(tmp_path / "i.gz", 0, 1000, 0.0, 0.5), val)
    assert asyncio.run(span.tpcc_data_status()) == {"loaded": True, "current_orders": 1003}
    assert span.other() == "real"


def test_the_tpcc_checks_start_from_the_workloads_own_start(tmp_path):
    hooks = AttachedWorkload(folder_with(tmp_path), None).tpcc_hooks()
    assert hooks.orders_at_prepare == 1000 and hooks.history_since.year == 2026
    assert hooks.workload.tpcc.warehouses == 64


# --- against the real service, while it keeps writing ---------------------------------------------------


class SnapshotShop:
    """The service's scripted database, read as the attached scenario reads it."""

    async def marker_snapshot(self, path):
        return write_snapshot(path, set(Shop.committed))


def test_a_scenario_attaches_to_the_running_service_and_cuts_off_while_it_keeps_writing(tmp_path, monkeypatch):
    r, lock, env = service(tmp_path, monkeypatch, cfg=_config(duration_s=60.0))
    folder = tmp_path / "workload-test"
    monkeypatch.setattr(att, "POLL_S", 0.05)
    run_dir = tmp_path / "scenario-run"
    run_dir.mkdir()

    async def go():
        task = asyncio.create_task(r.run())
        while (st.read_status(folder) or {}).get("state") != st.RUNNING:
            await asyncio.sleep(0.1)
        found = att.find_folder(env, env.nodes[0])
        w = AttachedWorkload(found, EventStream(run_dir / "events.jsonl"))
        info_ok = w.match_problems(Workload(**{**w.used, "session": {}}), env.name, env.nodes[0].name)
        await w.start()
        await w.wait_until_ready()
        init = await w.cut_off(SnapshotShop(), run_dir / "init.gz", settle_s=0)
        w.mark("run_start", run_id="NL-X-run", scenario="NL-X")
        w.begin_window()
        await asyncio.sleep(2.2)
        window = w.end_window()
        cut = await w.cut_off(SnapshotShop(), run_dir / "val.gz", settle_s=0.5)
        w.copy_span(init, cut, run_dir)
        snaps = compare_snapshots(init.file, cut.file, run_dir / DB_MARKERS_FILE)
        result = await endchecks.run_end_checks(SpanCheckAdapter(NoJournalCheckEngine(), init, cut), run_dir,
                                                tpcc=None, markers_from_file=True)
        await w.stop()
        w.mark("run_end", run_id="NL-X-run", scenario="NL-X")
        state_after = st.read_status(folder)["state"]
        st.request_stop(folder, "test")
        await task
        return w, window, snaps, result.end, info_ok, state_after
    w, window, snaps, end, info_ok, state_after = asyncio.run(go())
    assert info_ok == [] and state_after == st.RUNNING                     # attaching and detaching never stop it
    assert window.commits > 0 and window.p99_ms is not None and w.copied > 0
    assert end["rpo_txn"] == 0 and end["phantom"] == 0 and end["markers"]["acked"] > 0, end
    assert snaps["destroyed"] == 0 and snaps["new"] > 0
    assert [m["kind"] for m in st.read_marks(folder)] == ["run_start", "run_end"]
    assert not w.mark_problems and w.failure is None


# --- a whole scenario run, attached to the running service ---------------------------------------------


def test_a_kill_scenario_runs_all_nine_phases_attached_and_the_workload_keeps_running(env, tmp_path, monkeypatch):
    """NL-C-01 (process kill, fake engine) against the real service running the scenario's marker
    workload in its own thread: attached, judged, marked -- and the workload outlives it."""
    import threading

    from resilience_tests.control import orchestrator as orch
    from resilience_tests.control.matrix import RunPlanItem
    from resilience_tests.execution.workload.hooks import hooks_for
    from resilience_tests.execution.workload.service import runner as runner_mod
    from resilience_tests.execution.workload.service.runner import ServiceRunner
    from tests.test_orchestrator import Engine, OutageAdapter, scenario, why
    from tests.test_workload_service import FakeLock

    original_session = OutageAdapter.session

    async def session(self, endpoint=None, timeout_s=5.0, **kwargs):     # the service passes its settings
        return await original_session(self, endpoint, timeout_s)

    async def marker_snapshot(self, path):
        return write_snapshot(path, set(Engine.store))

    async def prepare_scenario_state(self):
        pass                                                              # never the workload's tables
    monkeypatch.setattr(OutageAdapter, "session", session)
    monkeypatch.setattr(OutageAdapter, "marker_snapshot", marker_snapshot)
    monkeypatch.setattr(OutageAdapter, "prepare_scenario_state", prepare_scenario_state)
    monkeypatch.setattr(runner_mod, "LOOP_S", 0.05)
    monkeypatch.setattr(att, "HEALTH_WINDOW_S", 2.0)
    monkeypatch.setattr(att, "JOURNAL_SETTLE_S", 0.5)
    monkeypatch.setattr(att, "POLL_S", 0.05)

    sc = scenario("NL-C-01")
    sc = sc.model_copy(update={"workload": sc.workload.model_copy(update={"concurrency": 4, "rate_tps": 100})})
    node = env.nodes[0]
    cfg = _config(workload=sc.workload, duration_s=120.0, warmup_s=1.0)

    async def target_check(e, n, a):
        return "BDB-QA-U22-30"

    async def scenario_check(e, n):
        return None
    folder = Path(env.driver_host.run_dir) / "workload-test"
    r = ServiceRunner(env, node, cfg, folder, adapter=OutageAdapter(node), lock=FakeLock(),
                      target_check=target_check, scenario_check=scenario_check)
    r.profile = hooks_for(sc.workload)
    thread = threading.Thread(target=lambda: asyncio.run(r.run()), daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 30
        while (st.read_status(folder) or {}).get("state") != st.RUNNING or time.monotonic() < deadline - 26:
            assert time.monotonic() < deadline, st.read_status(folder)
            time.sleep(0.1)
        item = RunPlanItem(scenario=sc, env_class=env.env_class, role="standalone", node=node)
        results = asyncio.run(orch.TestOrchestrator(item, env, orch.RunOptions(attach_workload=True)).run())
        state_after = st.read_status(folder)["state"]
    finally:
        st.request_stop(folder, "test")
        thread.join(timeout=60)
    assert results["status"] == "passed", why(results)
    assert state_after == st.RUNNING                                      # the scenario never stopped it
    facts = results["facts"]
    assert facts["common_workload"]["profile"] == "oltp_write_heavy" and "init_snapshot" in facts
    assert facts["validate_snapshot"]["journal_lines_copied"]["acked.jrnl"] > 0
    assert facts["markers"]["destroyed_older_count"] == 0
    assert results["measured"]["rpo_txn"] == 0 and results["measured"]["rto_first_write_s"] is not None
    assert [m["kind"] for m in st.read_marks(folder)] == ["run_start", "t0", "t1", "run_end"]
    assert "settled_before_baseline_s" in facts
    run_dir = Path(results["evidence_dir"])
    copied = [json.loads(x) for x in (run_dir / "events.jsonl").read_text().splitlines()]
    assert sum(1 for e in copied if e["source"] == "workload" and e["kind"] == "sample") > 3
    assert any("Attached to the running common workload" in d for d in results["disclosures"])


def test_the_baseline_waits_until_the_workload_has_settled_after_a_restart(tmp_path):
    w = AttachedWorkload(folder_with(tmp_path), EventStream(tmp_path / "run.jsonl"))
    now = time.time()
    rough = [sample(now - 12 + i) for i in range(12)]
    rough[4] = {**rough[4], "commits": 0, "drops": 57, "connect_failures": 210, "indeterminate": 57}
    w._samples = rough
    assert not w.settled()                       # the reconnects are inside the last 10 s
    w._samples = rough + [sample(now + i + 1) for i in range(10)]
    assert w.settled()


# --- back to normal (rto_to_slo_s), judged like for like with the baseline -----------------------------


def _samples(p99s, t0_ns=10**12, per_second=60, base_ms=12.0):
    """One 1 s sample per entry; each has `per_second` transactions at base_ms and, if the entry
    is larger, one transaction at that latency (a single slow one) -- or, for a negative entry,
    every transaction of the second at -entry ms (a stall)."""
    from resilience_tests.execution.workload.histogram import latency_histogram
    out = []
    for i, p in enumerate(p99s):
        if p < 0:
            lat = [-p] * per_second
        else:
            lat = [base_ms] * (per_second - 1) + [max(base_ms, p)]
        out.append(Event(t0_ns + (i + 1) * 10**9, 1000.0 + i, "workload", "sample",
                         {"interval_s": 1.0, "commits": per_second, "tps": float(per_second), "p99_ms": max(lat),
                          "latency_hist": latency_histogram(lat)}))
    return out


def test_a_few_single_slow_transactions_do_not_hide_recovery_but_a_stall_does_not_pass():
    from resilience_tests.analysis.rto_decomposer import Baseline, slo_recovery
    base = Baseline(tps=60.0, p99_ms=14.3)
    t0 = 10**12
    # as measured on e2-u22-3 with no fault: single seconds at 33, 205, 24, 207, 84 ms
    healthy = [0] * 120
    for i, p in zip((10, 31, 55, 77, 101), (33, 205, 24, 207, 84)):
        healthy[i] = p
    start, end = slo_recovery(_samples(healthy, t0), t0, base, tps_fraction=0.95, tps_window_s=60.0)
    assert start == 0.0 and end == 60.0                    # judged over the window: normal
    # held per second (no histograms), the same healthy minute never counts as normal
    plain = [Event(e.t_mono_ns, e.t_wall, e.source, e.kind, {k: v for k, v in e.data.items() if k != "latency_hist"})
             for e in _samples(healthy, t0)]
    assert slo_recovery(plain, t0, base, tps_fraction=0.95, tps_window_s=60.0) == (None, None)
    # a real stall -- every transaction of 2 seconds waited 3 s -- fails every window that holds it
    stalled = [0] * 59 + [-3000, -3000] + [0] * 65
    start, _ = slo_recovery(_samples(stalled, t0), t0, base, tps_fraction=0.95, tps_window_s=60.0)
    assert start == 61.0                                   # normal only from the second after the stall


def test_a_slow_snapshot_read_and_the_transactions_in_flight_at_the_cut_off_are_not_unknown(tmp_path):
    """As found on a long run (NL-N-I01, 2.1 million markers, an 18.9 s read): the snapshot is
    taken by its first query, so transactions that start while the markers are read are after the
    cut-off; those in flight at it are settled by their answer, which comes just after."""
    folder = folder_with(tmp_path)
    tb, snap = 2000.0, 2000.005                      # t_before; the snapshot's own moment
    rows = [("new", 1500, 1501),                     # this scenario's commit: in the snapshot
            ("inflight-in", 1999.99, 2000.02),       # committed just before the snapshot, acknowledged after
            ("inflight-out", 1999.99, 2000.03),      # committed just after the snapshot
            ("never", 1999.99, None),                # in flight, never answered: truly unknown
            ("during-read", 2010.0, 2010.01)]        # started while the markers were read
    journals(folder, rows)
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    write_snapshot(run_dir / "init.gz", set())
    write_snapshot(run_dir / "val.gz", {"new", "inflight-in"})
    init = CutOff(run_dir / "init.gz", 0, None, 1000.0, 1000.5)
    val = CutOff(run_dir / "val.gz", 2, None, tb, snap, read_s=18.9)
    AttachedWorkload(folder, None).copy_span(init, val, run_dir)
    assert val.lines["marker.jrnl"] == 4                       # "during-read" is after the cut-off
    assert len(val.late_acked) == 2                            # the two answered after it
    compare_snapshots(init.file, val.file, run_dir / DB_MARKERS_FILE)
    span = SpanCheckAdapter(NoJournalCheckEngine(), init, val)
    end = asyncio.run(endchecks.run_end_checks(span, run_dir, tpcc=None, markers_from_file=True)).end
    assert end["rpo_txn"] == 0 and end["phantom"] == 0, end
    assert end["markers"]["indeterminate"] == 1                # only "never"
    assert span.in_flight_at_validate == {"answered_after_cut_off_not_in_snapshot": 1,
                                          "answered_after_cut_off_in_snapshot": 1}


def test_the_cut_off_is_the_moment_the_snapshot_was_taken_not_the_end_of_reading_it(tmp_path):
    class SlowRead:
        async def marker_snapshot(self, path):
            snap = time.time()
            await asyncio.sleep(0.3)                           # reading millions of markers
            return {**write_snapshot(path, {"a"}), "snapshot_wall": snap}
    w = AttachedWorkload(folder_with(tmp_path), None)
    cut = asyncio.run(w.cut_off(SlowRead(), tmp_path / "v.gz", settle_s=0))
    assert cut.t_after - cut.t_before < 0.05 and cut.read_s >= 0.29
