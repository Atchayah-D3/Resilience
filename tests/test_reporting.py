"""Reports (Arch §10.4; spec specs/002-test-reports-html-junit): JUnit XML, the P0 gate and the
HTML pages. Every rule about honest reporting has a test that fails for the reason under test
(Constitution III): a run without `passed` is never a pass, NOT_MEASURED is never a value."""

from __future__ import annotations

import datetime as dt
import json
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from resilience_tests.reporting import gate
from resilience_tests.reporting.cases import ReportCase, case_from_run, case_from_skip
from resilience_tests.reporting.evidence import load_run
from resilience_tests.reporting.html import render_index, render_run, render_unit_summary
from resilience_tests.reporting.junit import build_junit, write_junit
from resilience_tests.reporting.view import MAX_POINTS, build_run_view, classify

NOW = dt.datetime(2026, 10, 7, 9, 0, tzinfo=dt.UTC)
S = 1_000_000_000            # ns per second
BASE = 50 * S


def write_run(tmp: Path, *, status="passed", priority="P0", rules=None, measured=None, error=None,
              not_measured=None, events=None, phases=None, disclosures=None, cycles=None, sid="NL-C-01",
              name="run") -> Path:
    d = tmp / name
    d.mkdir()
    results = {
        "run_id": f"{sid}-20261007T090000Z-abcdef", "status": status, "error": error,
        "scenario": {"id": sid, "name": "Immediate process termination", "priority": priority,
                     "category": sid[:4], "accept": []},
        "environment": {"profile": "e2-dedicated-vm", "class": "E2"},
        "target": {"node": "shaktidb-standalone", "role": "standalone"},
        "phases": phases if phases is not None else [{"phase": "init", "outcome": "ok", "duration_s": 1.5, "error": None}],
        "verdict": None if rules is None else {"passed": status == "passed", "results": rules},
        "measured": measured or {}, "facts": {"not_measured": not_measured or {}, "cycles": cycles or []},
        "disclosures": disclosures or ["Baseline reset is not configured here."],
        "timing": {"t0_mono_ns": BASE + 10 * S},
    }
    if status != "no_result":
        (d / "results.json").write_text(json.dumps(results))
    if events is not None:
        (d / "events.jsonl").write_text("\n".join(json.dumps(e) for e in events) + "\n")
    return d


def ev(kind, t_s, source="orchestrator", **data):
    return {"kind": kind, "source": source, "t_mono_ns": int(BASE + t_s * S), "data": data}


PASS_RULE = {"predicate": "rpo_txn == 0", "outcome": "pass", "values": {"rpo_txn": 0}, "margin": 0.0, "reason": None}
FAIL_RULE = {"predicate": "rto_first_write_s <= 60", "outcome": "fail", "values": {"rto_first_write_s": 75.2},
             "margin": -15.2, "reason": None}
NM_RULE = {"predicate": "relations_left_unvacuumed == 0", "outcome": "not_measured",
           "values": {"relations_left_unvacuumed": "NOT_MEASURED"}, "margin": None,
           "reason": "relations_left_unvacuumed was not measured: no relation became eligible"}


# ------------------------------------------------------------------ JUnit mapping (T005)

def junit_of(tmp, **kw) -> ET.Element:
    d = write_run(tmp, **kw)
    return build_junit([case_from_run("NL-C-01[E2:standalone:node]", d)])


def child_kinds(root):
    tc = next(root.iter("testcase"))
    return [c.tag for c in tc if c.tag != "properties"], {p.get("name"): p.get("value") for p in tc.iter("property")}


def test_passed_run_is_a_pass_with_properties(tmp_path):
    kinds, props = child_kinds(junit_of(tmp_path, rules=[PASS_RULE]))
    assert kinds == []
    assert props["scenario_id"] == "NL-C-01" and props["priority"] == "P0" and props["status"] == "passed"
    assert props["run_id"] and props["evidence_dir"]


def test_failed_run_lists_every_non_pass_rule(tmp_path):
    root = junit_of(tmp_path, status="failed", rules=[PASS_RULE, FAIL_RULE, NM_RULE])
    tc = next(root.iter("testcase"))
    f = tc.find("failure")
    assert f is not None and f.get("type") == "verdict" and "2 of 3" in f.get("message")
    assert "rto_first_write_s <= 60 -> fail" in f.text and "75.2" in f.text
    assert "relations_left_unvacuumed == 0 -> not_measured" in f.text and "no relation became eligible" in f.text
    assert "rpo_txn == 0" not in f.text                        # passes are not listed


@pytest.mark.parametrize("status", ["aborted", "error"])
def test_run_without_verdict_is_an_error_never_a_pass(tmp_path, status):
    root = junit_of(tmp_path, status=status, error="steady state did not hold",
                    phases=[{"phase": "pre_fault", "outcome": "aborted", "duration_s": 2.0, "error": "x"}])
    kinds, props = child_kinds(root)
    assert kinds == ["error"] and props["status"] == status
    err = next(root.iter("error"))
    assert "pre_fault" in err.get("message") and "steady state did not hold" in err.get("message")


def test_missing_results_is_no_result_error(tmp_path):
    kinds, props = child_kinds(junit_of(tmp_path, status="no_result"))
    assert kinds == ["error"] and props["status"] == "no_result"


def test_dry_run_and_plan_skip_are_skipped(tmp_path):
    d = write_run(tmp_path, status="stopped_before_fault")
    skip = case_from_skip("NL-C-04", "blocked by infrastructure: ... dedicated_wal_volume",
                          {"NL-C-04": {"priority": "P0", "name": "Recovery time versus WAL volume"}})
    root = build_junit([case_from_run("NL-C-01[x]", d), skip])
    assert [tc.find("skipped") is not None for tc in root.iter("testcase")] == [True, True]
    assert skip.priority == "P0"


def test_suite_counts_match_children(tmp_path):
    cases = [case_from_run(f"c{i}", write_run(tmp_path, status=s, name=f"r{i}", rules=[]))
             for i, s in enumerate(["passed", "failed", "aborted", "stopped_before_fault"])]
    root = build_junit(cases)
    assert (root.get("tests"), root.get("failures"), root.get("errors"), root.get("skipped")) == ("4", "1", "1", "1")
    p = write_junit(cases, tmp_path / "out" / "j.xml")
    assert ET.parse(p).getroot().get("tests") == "4"


# ------------------------------------------------------------------ P0 gate (T006)

def gate_exit(tmp_path, cases) -> int:
    p = write_junit(cases, tmp_path / "gate.xml")
    return gate.main([str(p)])


def case(status, priority, sid="NL-C-01"):
    return ReportCase(case_id=f"{sid}[x]", scenario_id=sid, priority=priority, status=status, message=status)


def test_gate_fails_on_p0_failure(tmp_path):
    assert gate_exit(tmp_path, [case("passed", "P0"), case("failed", "P0")]) == 1


def test_gate_fails_on_aborted_p0(tmp_path):
    """An aborted P0 run certified nothing."""
    assert gate_exit(tmp_path, [case("aborted", "P0")]) == 1


def test_gate_passes_with_only_p1_failures_but_lists_them(tmp_path, capsys):
    assert gate_exit(tmp_path, [case("passed", "P0"), case("failed", "P1", "NL-M-03")]) == 0
    assert "NL-M-03" in capsys.readouterr().out


def test_gate_does_not_count_a_skipped_p0(tmp_path, capsys):
    assert gate_exit(tmp_path, [case("skipped", "P0", "NL-C-04")]) == 0
    assert "NL-C-04" in capsys.readouterr().out


def test_gate_unreadable_report_is_exit_2(tmp_path):
    bad = tmp_path / "bad.xml"
    bad.write_text("<not-closed")
    assert gate.main([str(bad)]) == 2
    assert gate.main([str(tmp_path / "missing.xml")]) == 2


# ------------------------------------------------------------------ run page (T012, T013)

def test_not_measured_is_a_label_never_a_value():
    assert classify("NOT_MEASURED") == {"kind": "not_measured", "display": "not measured"}
    assert classify("NOT_APPLICABLE")["kind"] == "not_applicable"
    assert classify(None)["kind"] == "missing" and classify(0)["display"] == "0"
    assert classify(False)["display"] == "false" and classify(2.5)["display"] == "2.5"


def test_aborted_page_states_no_verdict_and_never_passed(tmp_path):
    d = write_run(tmp_path, status="aborted", error="steady state did not hold (tps=60.4)",
                  phases=[{"phase": "pre_fault", "outcome": "aborted", "duration_s": 3.0, "error": "x"}])
    html = render_run(d, now=NOW).read_text()
    assert "No verdict." in html and "steady state did not hold (tps=60.4)" in html and "pre_fault" in html
    assert 'class="badge status s-aborted"' in html
    assert ">PASSED<" not in html and "status s-passed" not in html


def test_page_shows_every_rule_and_nm_reason(tmp_path):
    d = write_run(tmp_path, status="failed", rules=[PASS_RULE, FAIL_RULE, NM_RULE],
                  measured={"relations_left_unvacuumed": "NOT_MEASURED", "rpo_txn": 0},
                  not_measured={"relations_left_unvacuumed": "no relation became eligible"},
                  disclosures=["Disclosure one.", "Each kill is a crash-restart <b>of the instance</b>."])
    html = render_run(d, now=NOW).read_text()
    for pred in ("rpo_txn == 0", "rto_first_write_s &lt;= 60", "relations_left_unvacuumed == 0"):
        assert pred in html
    assert "not measured" in html and "no relation became eligible" in html
    assert "2 of 3 acceptance rules did not pass" in html
    assert "Disclosure one." in html and "&lt;b&gt;of the instance&lt;/b&gt;" in html   # escaped, shown
    view = build_run_view(load_run(d), now=NOW)
    nm = next(m for m in view["measures"] if m["name"] == "relations_left_unvacuumed")
    assert nm["kind"] == "not_measured" and nm["display"] == "not measured"


def test_timeline_uses_the_runs_own_outage_and_recovery(tmp_path):
    """Was: the report applied its own 'first successful probe' rule (145.4 s on NL-M-03 run
    065019Z) while the harness had measured recovery at 146.3 s, and drew the recovery line
    when the harness NOTICED it (147.2 s). Now: one recovery line, at T0 + the run's number."""
    events = [ev("run_start", 0), ev("phase_start", 1, phase="fault_inject"),
              ev("write_probe", 9.8, "write_prober", ok=True, t_start_mono_ns=BASE + int(9.8 * S)),
              ev("t0", 10, "injector", t0_mono_ns=BASE + 10 * S, action="kill", cycle=None),
              ev("write_probe", 11.0, "write_prober", ok=False, t_start_mono_ns=BASE + int(10.2 * S)),
              ev("write_probe", 11.5, "write_prober", ok=True, t_start_mono_ns=BASE + int(11.3 * S)),
              ev("sample", 11, "workload", tps=200, p99_ms=5, journal_p99_ms=3),
              ev("run_end", 40)]
    d = write_run(tmp_path, events=events, measured={"rto_to_slo_s": 20.0, "rto_first_write_s": 2.4})
    tl = build_run_view(load_run(d), now=NOW)["timeline"]
    kinds = {m["kind"]: m["t_s"] for m in tl}
    assert kinds["fault"] == 10.0
    assert kinds["outage_start"] == 11.0          # when the failing probe failed (harness rule)
    assert kinds["recovered"] == 12.4             # T0 + rto_first_write_s, not the report's own 11.3
    assert "first_write" not in kinds and kinds["slo_recovered"] == 30.0
    assert sum(1 for m in tl if m["kind"] == "recovered") == 1
    assert "FAULT: kill" in render_run(d, now=NOW).read_text()


def test_repeated_faults_get_one_recovery_line_each_at_t0_plus_cycle_recovery(tmp_path):
    events = [ev("run_start", 0)]
    for c, t in ((1, 10), (2, 40)):
        events += [ev("t0", t, "injector", t0_mono_ns=BASE + t * S, action="kill", cycle=c),
                   ev("write_probe", t + 0.5, "write_prober", ok=False, t_start_mono_ns=BASE + int((t + 0.2) * S)),
                   ev("cycle_recovered", t + 4, cycle=c, recovery_s=2.5)]        # noticed 1.5 s later
    events += [ev("sample", 50, "workload", tps=200, p99_ms=3, journal_p99_ms=2)]
    view = build_run_view(load_run(write_run(tmp_path, events=events)), now=NOW)
    rec = [m for m in view["timeline"] if m["kind"] == "recovered"]
    assert [m["t_s"] for m in rec] == [12.5, 42.5] and [m["cycle"] for m in rec] == [1, 2]
    assert all(m["kind"] in ("fault", "recovered") for ch in view["charts"] for m in ch["markers"])


def test_charts_are_throughput_and_latency_with_y_axis_numbers(tmp_path):
    events = [ev("run_start", 0)] + [ev("sample", t, "workload", tps=200, p99_ms=40, journal_p99_ms=5)
                                     for t in range(1, 60)]
    charts = build_run_view(load_run(write_run(tmp_path, events=events)), now=NOW)["charts"]
    assert [c["title"] for c in charts] == ["Throughput (committed transactions / s)", "Latency (ms, p99 per second)"]
    assert [t["label"] for t in charts[0]["yticks"]] == ["0", "50", "100", "150", "200"]
    html = render_run(write_run(tmp_path, events=events, name="r2"), now=NOW).read_text()
    assert ">150</text>" in html and "Failed write probes" not in html


def test_fault_without_failed_probe_is_noted_not_invented(tmp_path):
    events = [ev("run_start", 0), ev("t0", 5, "injector", t0_mono_ns=BASE + 5 * S, action="reload"),
              ev("write_probe", 6, "write_prober", ok=True, t_start_mono_ns=BASE + 6 * S)]
    tl = build_run_view(load_run(write_run(tmp_path, events=events)), now=NOW)["timeline"]
    assert not any(m["kind"] in ("outage_start", "first_write") for m in tl)
    assert any("outage not observed" in m["label"] for m in tl)


def test_long_run_series_is_downsampled(tmp_path):
    events = [ev("run_start", 0)] + [ev("sample", t, "workload", tps=200, p99_ms=t % 7, journal_p99_ms=1)
                                     for t in range(5000)]
    charts = build_run_view(load_run(write_run(tmp_path, events=events)), now=NOW)["charts"]
    s = charts[0]["series"][0]
    assert len(s["points"]) <= MAX_POINTS and s["downsampled"] is True


def test_missing_results_page_says_no_result(tmp_path):
    d = write_run(tmp_path, status="no_result", events=[ev("run_start", 0)])
    html = render_run(d, now=NOW).read_text()
    assert "No result was recorded" in html and ">PASSED<" not in html


def test_regeneration_is_identical(tmp_path):
    d = write_run(tmp_path, status="failed", rules=[FAIL_RULE],
                  events=[ev("run_start", 0), ev("sample", 1, "workload", tps=10, p99_ms=1, journal_p99_ms=1)])
    first = render_run(d, now=NOW).read_text()
    assert render_run(d, now=NOW).read_text() == first


def test_index_links_each_run(tmp_path):
    d = write_run(tmp_path, rules=[PASS_RULE])
    page = render_run(d, now=NOW)
    out = render_index([case_from_run("NL-C-01[x]", d, report_path=str(page))], tmp_path / "rep" / "index.html", now=NOW)
    html = out.read_text()
    assert "../run/report.html" in html and "NL-C-01" in html


# ------------------------------------------------------------------ unit summary (T020)

def test_unit_summary_counts_and_failures(tmp_path):
    rows = [{"nodeid": "tests/a.py::t1", "outcome": "passed", "duration_s": 0.1, "message": ""},
            {"nodeid": "tests/a.py::t2", "outcome": "failed", "duration_s": 0.2, "message": "assert 1 == 2"},
            {"nodeid": "tests/a.py::t3", "outcome": "skipped", "duration_s": 0.0, "message": "no jar"}]
    html = render_unit_summary(rows, tmp_path / "u.html", now=NOW).read_text()
    assert "passed: 1" in html and "failed: 1" in html and "skipped: 1" in html
    assert "assert 1 == 2" in html and "tests/a.py::t2" in html
