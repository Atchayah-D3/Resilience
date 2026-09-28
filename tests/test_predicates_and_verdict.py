import pytest

from resilience_tests.analysis import predicates as P
from resilience_tests.analysis.threshold_eval import evaluate, evaluate_one


@pytest.mark.parametrize("text", [
    "rpo_txn == 0", "rto_first_write_s <= 60", "starts_unattended", "not leader_changed",
    "p99_ms <= 5 * baseline_p99_ms", "rpo_txn == 0 or startup_refused", "a < b <= 10",
])
def test_allowed_predicates_parse(text):
    assert P.parse(text).names


@pytest.mark.parametrize("text", [
    "", "1 == 1", "true", "__import__('os')", "rpo_txn in (0, 1)", "x.y == 1", "f(x) == 1",
    "rpo_txn is 0", "lambda: 1", "rpo_txn == 'zero'",
])
def test_rejected_predicates(text):
    with pytest.raises(P.PredicateError):
        P.parse(text)


def test_evaluate_and_margin():
    pred = P.parse("rto_first_write_s <= 60")
    assert pred.evaluate({"rto_first_write_s": 58.0})
    assert P.margin(pred, {"rto_first_write_s": 58.0}) == pytest.approx(2.0)
    assert not pred.evaluate({"rto_first_write_s": 61})
    assert P.margin(pred, {"rto_first_write_s": 61}) == pytest.approx(-1.0)


def test_missing_and_not_applicable_never_pass():
    assert evaluate_one("rpo_txn == 0", {}).outcome == "missing"
    assert evaluate_one("rpo_txn == 0", {"rpo_txn": P.NOT_APPLICABLE}).outcome == "not_applicable"
    assert evaluate_one("rpo_txn == 0", {"rpo_txn": None}).outcome == "fail"
    v = evaluate(["rpo_txn == 0", "corruption_count == 0"], {"rpo_txn": 0})
    assert not v.passed and [r.outcome for r in v.results] == ["pass", "missing"]


def test_none_measure_fails_ordering_comparison():
    # a measure the harness could not produce (None) must fail, never compare as "small"
    assert evaluate_one("rto_first_write_s <= 60", {"rto_first_write_s": None}).outcome == "error"


def test_boolean_measure():
    assert evaluate_one("starts_unattended", {"starts_unattended": True}).passed
    assert evaluate_one("starts_unattended", {"starts_unattended": False}).outcome == "fail"
    assert evaluate_one("starts_unattended", {"starts_unattended": 1}).outcome == "error"


def test_empty_accept_is_not_a_pass():
    assert not evaluate([], {}).passed
