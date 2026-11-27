"""Back to normal before the next scenario's baseline (resilience_tests/execution/workload/recovered.py).

The case it exists for (e2-u22-3, 26 Nov 2026): NL-N-I01a's baseline p99 was 11 ms; 61 s after it
ended, NL-N-I01b took its baseline at 40 ms and its ratios came out too small. The health check
between scenarios looked only at the rate and the errors.
"""

from __future__ import annotations

import time
from types import SimpleNamespace

import pytest

from resilience_tests.control.orchestrator import PhaseAbort, TestOrchestrator
from resilience_tests.execution.workload import attached as att
from resilience_tests.execution.workload import recovered as rec
from resilience_tests.execution.workload.attached import AttachedWorkload, wait_for_health
from resilience_tests.execution.workload.histogram import latency_histogram
from resilience_tests.execution.workload.service import state as st
from tests.test_attached_workload import folder_with

NORMAL = (2.0, 4.0, 6.0, 8.0, 10.0)          # p99 of a second: 10 ms
SLOW = (20.0, 30.0, 40.0, 40.0, 40.0)        # 40 ms: the NL-N-I01b baseline after NL-N-I01a


def s(t_wall, latencies=NORMAL, commits=1500, part="measured", interval=1.0):
    return {"t_wall": t_wall, "interval_s": interval, "part": part, "commits": commits, "tps": commits / interval,
            "errors": 0, "indeterminate": 0, "drops": 0, "reconnects": 0, "connect_failures": 0,
            "p99_ms": max(latencies), "latency_hist": latency_histogram(latencies), "by_type": {}, "by_group": {}}


def run(start, n, **kw):
    return [s(start + i + 1, **kw) for i in range(n)]


def mark(kind, t, run_id="NL-N-I01a-1"):
    return {"kind": kind, "run_id": run_id, "scenario": run_id.rsplit("-", 1)[0], "t_wall": t}


# 1a: attaches at 1000 (start-up to 1005), baseline 1005..1605, fault 1605..1905, run_end 2400
FIRST_RUN = [mark("run_start", 1000), mark("baseline", 1005), mark("t0", 1605), mark("t1", 1905), mark("run_end", 2400)]


# --- the reference ----------------------------------------------------------------------------------------


def test_the_reference_is_the_last_ten_undisturbed_minutes_before_the_first_fault():
    samples = (run(700, 300, part="warmup", latencies=SLOW)          # warm-up: never counted
               + run(1000, 5, latencies=(900.0,))                    # 1a's start-up (a restart, say): left out
               + run(1005, 600)                                       # 1a's baseline: the reference
               + run(1605, 300, latencies=SLOW))                      # the fault: after the first T0
    ref = rec.reference(samples, FIRST_RUN)
    assert ref.seconds == 600 and ref.samples == 600 and ref.first_fault_wall == 1605
    assert ref.p99_ms == pytest.approx(10.0, rel=0.02) and ref.tps == pytest.approx(1500.0)
    assert (ref.from_wall, ref.to_wall) == (1005, 1605)


def test_no_reference_before_any_fault_or_from_too_few_undisturbed_seconds():
    with pytest.raises(rec.NoReference, match="no fault has run"):
        rec.reference(run(1000, 900), [])
    short = [mark("run_start", 1000), mark("baseline", 1005), mark("t0", 1125)]   # a 2-minute baseline first
    with pytest.raises(rec.NoReference, match="only 120 s of undisturbed measured samples"):
        rec.reference(run(1005, 300), short)
    with pytest.raises(rec.NoReference, match="no latency histograms"):
        rec.reference([{k: v for k, v in x.items() if k != "latency_hist"} for x in run(1005, 600)], FIRST_RUN)


def test_a_run_without_a_baseline_mark_is_left_out_whole():
    """Written by a harness from before the baseline mark: its whole span is start-up, never normal."""
    old = [mark("run_start", 1000), mark("t0", 1605)]
    samples = run(400, 600) + run(1000, 605, latencies=SLOW)
    ref = rec.reference(samples, old)
    assert ref.to_wall == 1000 and ref.p99_ms == pytest.approx(10.0, rel=0.02)


# --- the check --------------------------------------------------------------------------------------------


REF = rec.Reference(p99_ms=10.0, tps=1500.0, seconds=600, samples=600, from_wall=1005, to_wall=1605,
                    first_fault_wall=1605)


def test_back_to_normal_when_the_last_minute_is_within_the_framework_rule():
    result = rec.check(run(3000, 60), REF, now=3060)
    assert result["ok"] is True and result["problems"] == []
    assert result["p99_bound_ms"] == 15.0 and result["tps_min"] == 1200.0


def test_the_slow_baseline_of_nl_n_i01b_is_not_back_to_normal():
    """Fails for the right reason: the delay, not the rate (the rate was back, 1470-1490 TPS)."""
    result = rec.check(run(3000, 60, latencies=SLOW, commits=1480), REF, now=3060)
    assert result["ok"] is False and len(result["problems"]) == 1
    assert "delay is not back to normal: p99 40.7 ms" in result["problems"][0]
    assert "above 1.5 x the workload's reference 10.0 ms = 15.0 ms (Framework §6.2)" in result["problems"][0]


def test_one_slow_second_does_not_fail_a_normal_minute_but_a_slow_rate_does():
    """The p99 of every transaction in the minute (summed histograms), not of any one second."""
    minute = run(3000, 59) + [s(3060, latencies=(800.0,), commits=10)]
    assert rec.check(minute, REF, now=3060)["ok"] is True
    slow_rate = rec.check(run(3000, 60, commits=1100), REF, now=3060)
    assert slow_rate["ok"] is False and "rate is not back to normal: 1100 TPS" in slow_rate["problems"][0]


def test_a_gap_or_a_starved_sampler_is_never_taken_as_normal():
    gap = run(3000, 30) + run(3045, 15)
    assert "only 45 s of samples" in rec.check(gap, REF, now=3060)["problems"][0]
    starved = run(3000, 55) + [s(3060, interval=5.0)]
    assert any("a sample covers 5.0 s" in p for p in rec.check(starved, REF, now=3060)["problems"])


# --- in the health check between scenarios ---------------------------------------------------------------


def workload(tmp_path, recent, marks=FIRST_RUN, before=None):
    """A workload folder: its reference period (1a's baseline) and, at now, the last minute `recent`."""
    now = time.time()
    shift = now - 4000                       # the marks and the reference, moved to real time
    history = [dict(x, t_wall=x["t_wall"] + shift) for x in (before or run(1005, 600))]
    last = [dict(x, t_wall=now - 60 + i + 1) for i, x in enumerate(recent)]
    tmp_path.mkdir(parents=True, exist_ok=True)
    folder = folder_with(tmp_path, samples=history + last)
    for m in marks:
        st.write_mark(folder, m["kind"], run_id=m["run_id"], scenario=m["scenario"], t_wall=m["t_wall"] + shift)
    return folder, now


def test_the_health_check_waits_for_the_delay_once_a_fault_has_run(tmp_path):
    folder, now = workload(tmp_path, run(0, 60, latencies=SLOW, commits=1480))
    w = AttachedWorkload(folder, None)
    problems = w.health_problems(now=now)
    assert len(problems) == 1 and "delay is not back to normal" in problems[0]
    assert w.health_problems(now=now, check_recovery=False) == []      # the old check alone lets it through
    folder, now = workload(tmp_path / "ok", run(0, 60))
    assert AttachedWorkload(folder, None).health_problems(now=now) == []


def test_before_any_fault_the_delay_is_not_checked(tmp_path):
    folder, now = workload(tmp_path, run(0, 60, latencies=SLOW, commits=1480), marks=[])
    w = AttachedWorkload(folder, None)
    assert w.health_problems(now=now) == []
    assert w.recovery(now)["needed"] is False


def test_a_workload_without_a_reference_is_not_healthy_after_a_fault(tmp_path):
    folder, now = workload(tmp_path, run(0, 60), before=run(1005, 600, part="warmup"))
    problems = AttachedWorkload(folder, None).health_problems(now=now)
    assert problems == ["back to normal cannot be judged: only 0 s of undisturbed measured samples before the "
                        "first fault, at least 300 s needed"]


def test_waiting_gives_up_with_the_reason_when_the_delay_never_comes_back(tmp_path, monkeypatch):
    folder, now = workload(tmp_path, run(0, 60, latencies=SLOW, commits=1480))
    monkeypatch.setattr(att.time, "time", lambda: now)
    problems = wait_for_health(AttachedWorkload(folder, None), timeout_s=0.3, poll_s=0.1)
    assert problems and "delay is not back to normal" in problems[0]


# --- at the scenario's baseline (orchestrator) -----------------------------------------------------------


class FakeAttached:
    def __init__(self, result):
        self.result, self.marks = result, []

    def recovery(self):
        return self.result

    def mark(self, kind, **kw):
        self.marks.append(kind)


def orchestrator_with(result):
    events = []
    return SimpleNamespace(attached=FakeAttached(result), facts={}, disclosures=[], run_id="NL-N-I01b-1",
                           scenario=SimpleNamespace(id="NL-N-I01b"),
                           stream=SimpleNamespace(emit=lambda *a, **k: events.append((a, k))))


def test_the_baseline_is_measured_only_on_a_workload_back_to_normal():
    ok = {"needed": True, "ok": True, **rec.check(run(3000, 60), REF, now=3060)}
    o = orchestrator_with(ok)
    TestOrchestrator._check_back_to_normal(o)
    assert o.facts["back_to_normal_before_baseline"]["ok"] is True and o.attached.marks == ["baseline"]
    assert "back to normal." in o.disclosures[0] and "reference p99 10.0 ms" in o.disclosures[0]

    slow = {"needed": True, **rec.check(run(3000, 60, latencies=SLOW), REF, now=3060)}
    o = orchestrator_with(slow)
    with pytest.raises(PhaseAbort, match="the baseline would not be normal: the customers' delay is not back"):
        TestOrchestrator._check_back_to_normal(o)
    assert o.facts["back_to_normal_before_baseline"]["ok"] is False and o.attached.marks == []
    assert "NOT back to normal" in o.disclosures[0]

    o = orchestrator_with({"needed": True, "error": "the samples carry no latency histograms"})
    with pytest.raises(PhaseAbort, match="no latency histograms"):
        TestOrchestrator._check_back_to_normal(o)

    o = orchestrator_with({"needed": False, "why": "no fault has run on the common workload yet"})
    TestOrchestrator._check_back_to_normal(o)
    assert o.attached.marks == ["baseline"] and "not checked, no fault has run" in o.disclosures[0]


def test_the_baseline_mark_is_a_mark_the_workload_accepts(tmp_path):
    folder = folder_with(tmp_path)
    rec_ = st.write_mark(folder, "baseline", run_id="r1", scenario="NL-N-I01a")
    assert rec_["kind"] == "baseline" and st.read_marks(folder)[-1]["kind"] == "baseline"


def test_the_samples_are_read_only_up_to_the_first_fault(tmp_path):
    folder = folder_with(tmp_path, samples=run(1000, 50))
    with (folder / st.METRICS_1S_FILE).open("a") as fh:
        fh.write('{"t_wall": 2000')                  # a line still being written
    got = rec.read_samples_until(folder / st.METRICS_1S_FILE, 1020)
    assert [x["t_wall"] for x in got] == list(range(1001, 1021))
    assert len(rec.read_samples_until(folder / st.METRICS_1S_FILE, 9999)) == 50


def test_a_reference_once_read_is_kept(tmp_path, monkeypatch):
    folder, now = workload(tmp_path, run(0, 60))
    w = AttachedWorkload(folder, None)
    assert w.recovery(now)["ok"] is True
    monkeypatch.setattr(rec, "read_samples_until", lambda *a: pytest.fail("read again"))
    assert w.recovery(now)["ok"] is True

