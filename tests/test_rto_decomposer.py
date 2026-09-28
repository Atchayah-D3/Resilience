"""Decomposer tests against event-stream fixtures (Arch §7.3, §13 'tests/ is not optional')."""

from pathlib import Path

import pytest

from resilience_tests.analysis.predicates import NOT_APPLICABLE
from resilience_tests.analysis.rto_decomposer import Baseline, decompose, first_write_after, slo_recovery
from resilience_tests.observability.event_stream import Event, read_stream

S = 1_000_000_000
T0 = 100 * S
BASE = Baseline(tps=1000.0, p99_ms=10.0)
FIXTURES = Path(__file__).parent / "fixtures"


def probe(t_s, ok):
    return Event(int(t_s * S), 0.0, "write_prober", "write_probe", {"ok": ok})


def sample(t_s, tps, p99):
    return Event(int(t_s * S), 0.0, "workload", "sample", {"interval_s": 1.0, "tps": tps, "p99_ms": p99})


def test_first_write_is_first_ok_after_t0():
    ev = [probe(99.8, True), probe(100.2, False), probe(124.4, False), probe(124.6, True), probe(124.8, True)]
    assert first_write_after(ev, T0) == pytest.approx(24.6)


def test_ok_before_t0_is_ignored_and_none_when_never_recovered():
    assert first_write_after([probe(99.9, True), probe(101, False)], T0) is None


def test_slo_needs_60_contiguous_compliant_seconds():
    ev = [sample(100 + i, 0, None) for i in range(1, 31)]  # down 30 s
    ev += [sample(130 + i, 500, 30) for i in range(1, 11)]  # cold: 50% tps
    ev += [sample(140 + i, 900, 12) for i in range(1, 61)]  # >= 80% and p99 <= 15 for 60 s
    start, end = slo_recovery(ev, T0, BASE)
    assert start == pytest.approx(40.0) and end == pytest.approx(100.0)


def test_one_bad_second_restarts_the_window():
    ev = [sample(100 + i, 900, 12) for i in range(1, 51)]
    ev += [sample(151, 900, 16)]  # p99 above 1.5 x baseline
    ev += [sample(151 + i, 900, 12) for i in range(1, 61)]
    start, _ = slo_recovery(ev, T0, BASE)
    assert start == pytest.approx(51.0)


def test_gap_in_samples_breaks_the_window():
    ev = [sample(100 + i, 900, 12) for i in range(1, 31)]
    ev += [sample(135 + i, 900, 12) for i in range(1, 40)]  # 5 s with no samples at all
    assert slo_recovery(ev, T0, BASE) == (None, None)


def test_standalone_cluster_components_are_not_applicable():
    d = decompose([probe(101, True)], T0, BASE, clustered=False)
    assert d.components["t_elect_s"] is NOT_APPLICABLE and d.rto_to_slo_s is None


def test_clustered_decomposition_refuses_to_fabricate():
    with pytest.raises(NotImplementedError):
        decompose([], T0, BASE, clustered=True)


def test_recorded_fixture_power_loss():
    events = read_stream(FIXTURES / "power_loss_standalone.jsonl")
    t0 = next(e for e in events if e.kind == "t0").data["t0_mono_ns"]
    d = decompose(events, t0, BASE, clustered=False)
    assert d.rto_first_write_s == pytest.approx(31.4)
    assert d.rto_to_slo_s == pytest.approx(36.0)


def log_line(t_s, text):
    return Event(int(t_s * S), 0.0, "log_tailer", "log_line", {"line": text, "node": "n"})


def test_mttd_is_measured_from_the_first_fault_evidence_in_the_log():
    """Framework §6.2: detection time is unconditional -- it applies to any target, not only
    a cluster. It is measured from the log stream when the engine provides patterns."""
    from resilience_tests.analysis.rto_decomposer import mttd_from_log

    patterns = (r"database system was interrupted", r"received SIGHUP")
    events = [
        log_line(99.0, "database system was interrupted; last known up at ..."),   # before T0
        log_line(101.5, "checkpoint starting: time"),                              # not evidence
        log_line(103.25, "database system was not properly shut down"),            # not in patterns
        log_line(104.0, "database system was interrupted; last known up at ..."),  # first match
        log_line(106.0, "received SIGHUP, reloading configuration files"),
    ]
    assert mttd_from_log(events, T0, patterns) == pytest.approx(4.0)


def test_mttd_absent_is_not_measured_rather_than_not_applicable():
    """An engine with no detection signal must say so, not claim the question is meaningless."""
    from resilience_tests.analysis.predicates import NOT_APPLICABLE, NOT_MEASURED

    d = decompose([probe(101, True)], T0, BASE, clustered=False, detection_patterns=())
    assert d.components["mttd_s"] is NOT_MEASURED
    # leader detection/election/promotion genuinely do not exist on a standalone target
    assert d.components["t_detect_s"] is NOT_APPLICABLE


def test_decomposition_is_also_reported_under_the_document_component_names():
    """Framework §6.3 names the components T_reconnect and T_warm; §6.5 requires both figures
    for every failover scenario. They are DURATIONS: on a standalone target, where detection,
    election and promotion do not exist, T_reconnect + T_warm must equal the total RTO."""
    events = [probe(124.6, True)] + [sample(100 + i, 900, 12) for i in range(1, 62)]
    d = decompose(events, T0, BASE, clustered=False)
    m = d.as_measured()
    assert m["t_reconnect_s"] == m["rto_first_write_s"] == pytest.approx(24.6)
    assert m["t_reconnect_s"] + m["t_warm_s"] == pytest.approx(m["rto_to_slo_s"])
