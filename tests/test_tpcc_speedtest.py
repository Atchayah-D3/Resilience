"""Unit tests of the TPC-C speed test's pure logic (tools/tpcc_speedtest/logic.py).

Each input rule names its TPC-C 5.11 clause. Each check must fail for the right reason.
"""

from __future__ import annotations

import random
from collections import Counter

import pytest

from tools.tpcc_speedtest.logic import (
    Check,
    RunConstants,
    StepEvidence,
    StepSummary,
    check_step,
    cores_needed,
    home_warehouses,
    new_order_input,
    nurand,
    parse_proc_stat,
    percentile,
    python_limited,
    tps_per_process,
)

ITEMS = 100_000


def _orders(n: int, warehouses: int = 16, home_w: int = 3, seed: int = 7):
    rng = random.Random(seed)
    consts = RunConstants.choose(rng)
    return [new_order_input(rng, home_w, warehouses, ITEMS, consts) for _ in range(n)]


# --- NURand, Clause 2.1.6 ----------------------------------------------------------------

def test_nurand_stays_in_range():
    rng = random.Random(1)
    values = [nurand(rng, 1023, 1, 3000, 259) for _ in range(50_000)]
    assert min(values) >= 1 and max(values) <= 3000


def test_nurand_is_skewed_not_uniform():
    # With a uniform draw the most frequent of 3,000 customers would get about 1/3000 of the
    # picks; NURand concentrates picks on some customers.
    rng = random.Random(2)
    counts = Counter(nurand(rng, 1023, 1, 3000, 0) for _ in range(300_000))
    assert counts.most_common(1)[0][1] > 3 * (300_000 / 3000)


# --- New-Order inputs, Clause 2.4.1 ------------------------------------------------------

def test_new_order_has_5_to_15_unique_lines_and_valid_ranges():
    for o in _orders(5_000):
        assert 5 <= o.ol_cnt <= 15
        assert len(set(o.items)) == o.ol_cnt
        assert 1 <= o.d_id <= 10 and 1 <= o.c_id <= 3000
        assert all(1 <= q <= 10 for q in o.quantities)
        assert all(1 <= w <= 16 for w in o.supply_w)
        assert o.w_id == 3


def test_lines_are_sorted_so_new_orders_cannot_deadlock():
    for o in _orders(2_000):
        lines = list(zip(o.supply_w, o.items))
        if o.rollback:
            lines = lines[:-1]          # the unknown item replaces the last line
        assert lines == sorted(lines)


def test_about_one_percent_of_new_orders_roll_back_with_unknown_last_item():
    orders = _orders(40_000)
    rollbacks = [o for o in orders if o.rollback]
    assert 0.007 <= len(rollbacks) / len(orders) <= 0.013
    for o in rollbacks:
        assert o.items[-1] == ITEMS + 1
        assert all(i <= ITEMS for i in o.items[:-1])
    for o in orders:
        if not o.rollback:
            assert all(i <= ITEMS for i in o.items)


def test_about_one_percent_of_lines_are_remote_and_never_the_home_warehouse():
    orders = _orders(20_000)
    lines = [(o.w_id, w) for o in orders for w in o.supply_w]
    remote = [w for home, w in lines if w != home]
    assert 0.007 <= len(remote) / len(lines) <= 0.013
    assert all(w != 3 for w in remote)


def test_one_warehouse_means_no_remote_lines():
    for o in _orders(3_000, warehouses=1, home_w=1):
        assert set(o.supply_w) == {1}


def test_same_seed_gives_same_transactions():
    assert _orders(200, seed=11) == _orders(200, seed=11)
    assert _orders(200, seed=11) != _orders(200, seed=12)


def test_home_warehouses_are_spread_evenly():
    homes = home_warehouses(processes=2, clerks_per_process=4, warehouses=3)
    assert homes == [[1, 2, 3, 1], [2, 3, 1, 2]]


# --- statistics and CPU -------------------------------------------------------------------

def test_percentile_nearest_rank():
    values = list(range(1, 101))
    assert percentile(values, 0.99) == 99
    assert percentile(values, 0.50) == 50
    assert percentile([], 0.99) is None


def test_python_limited_needs_every_process_busy():
    assert python_limited([0.95, 0.90])
    assert not python_limited([0.95, 0.60])
    assert not python_limited([])


def test_parse_proc_stat():
    text = "cpu  100 0 50 800 40 5 5 0 10 0\ncpu0 1 2 3 4 5 6 7 8 0 0\n"
    busy, iowait, total = parse_proc_stat(text)
    assert total == 1000 and iowait == 40 and busy == 160


def test_parse_proc_stat_refuses_other_text():
    with pytest.raises(ValueError):
        parse_proc_stat("intr 1 2 3\n")


# --- checks: each fails for the right reason ----------------------------------------------

def _evidence(**changes) -> StepEvidence:
    base = dict(
        committed_counted=3, marker_uuids=frozenset({"a", "b", "c", "d", "e"}),
        acked_uuids=frozenset({"a", "b", "c"}), aborted_uuids=frozenset({"d", "e"}),
        db_uuids=frozenset({"a", "b", "c"}), acked_lines=3, order_check_failures=0,
        other_errors=0, unknown_outcomes=0, business_rollbacks=2, new_orders_attempted=5,
        fsyncs=4, journal_lines=8,
    )
    base.update(changes)
    return StepEvidence(**base)


def _failed(ev: StepEvidence) -> set[str]:
    return {c.name for c in check_step(ev) if not c.ok}


def test_clean_evidence_passes_every_check():
    assert _failed(_evidence()) == set()


@pytest.mark.parametrize("changes, failing", [
    ({"committed_counted": 4}, "counts agree"),
    ({"acked_lines": 4}, "counts agree"),
    ({"db_uuids": frozenset({"a", "b"})}, "every committed transaction is in the database"),
    ({"db_uuids": frozenset({"a", "b", "c", "z"})}, "no unknown rows in the database"),
    ({"marker_uuids": frozenset({"a", "b", "d", "e"}), "db_uuids": frozenset({"a", "b"}),
      "acked_uuids": frozenset({"a", "b", "c"})}, "every committed line has a marker line before it"),
    ({"db_uuids": frozenset({"a", "b", "c", "d"})}, "no rolled-back transaction in the database"),
    ({"order_check_failures": 1}, "every committed New-Order is complete"),
    ({"other_errors": 1}, "no errors and no unknown outcomes"),
    ({"unknown_outcomes": 1}, "no errors and no unknown outcomes"),
    ({"fsyncs": 0}, "journal flushes really happen"),
    ({"fsyncs": 9}, "journal flushes really happen"),
])
def test_each_check_fails_for_its_own_reason(changes, failing):
    assert failing in _failed(_evidence(**changes))


def test_rollback_share_is_checked_only_with_enough_new_orders():
    few = _evidence(business_rollbacks=0, new_orders_attempted=1_000)
    assert "business rollbacks are about 1%" not in {c.name for c in check_step(few)}
    many_ok = _evidence(business_rollbacks=100, new_orders_attempted=10_000)
    assert _failed(many_ok) == set()
    many_bad = _evidence(business_rollbacks=0, new_orders_attempted=10_000)
    assert "business rollbacks are about 1%" in _failed(many_bad)


def test_check_detail_is_readable():
    lost = [c for c in check_step(_evidence(db_uuids=frozenset({"a"}))) if not c.ok]
    assert isinstance(lost[0], Check) and "2 committed but missing" in lost[0].detail


# --- the decision -------------------------------------------------------------------------

def test_cores_needed_adds_headroom_and_rounds_up():
    assert cores_needed(5_000, 1_500) == 4        # 6,000 / 1,500
    assert cores_needed(5_100, 1_500) == 5        # 6,120 / 1,500 -> rounded up
    with pytest.raises(ValueError):
        cores_needed(1_000, 0)


def test_rate_uses_the_lowest_python_limited_step():
    steps = [StepSummary(1, 1_600, True, True), StepSummary(2, 1_450, True, True),
             StepSummary(4, 900, False, True)]
    rate = tps_per_process(steps)
    assert rate is not None and rate.tps == 1_450 and rate.exact


def test_rate_is_a_lower_bound_when_python_was_never_the_limit():
    steps = [StepSummary(1, 800, False, True), StepSummary(2, 600, False, True)]
    rate = tps_per_process(steps)
    assert rate is not None and rate.tps == 800 and not rate.exact


def test_steps_with_failed_checks_are_never_used():
    steps = [StepSummary(1, 5_000, True, False), StepSummary(2, 1_000, True, True)]
    rate = tps_per_process(steps)
    assert rate is not None and rate.tps == 1_000
    assert tps_per_process([StepSummary(1, 5_000, True, False)]) is None


# --- the report names the limit -----------------------------------------------------------

from tools.tpcc_speedtest.__main__ import _decision  # noqa: E402


def _step(processes=2, tps=500.0, limited=False, ok=True, busy=None, journal=1.0, call=3.0):
    return {"processes": processes, "tps_per_process_avg": tps, "python_limited": limited, "checks_ok": ok,
            "target_cpu_busy": busy, "journal_p99_ms": journal, "call_p50_ms": call}


def test_decision_with_python_as_the_limit_gives_about_n_cores():
    d = _decision([_step(tps=1_500, limited=True)], 5_000, 8)
    assert d["cores_needed"] == 4 and d["exact"]
    assert any("about 4 Python processes" in t for t in d["text"])


def test_decision_without_python_limit_gives_at_most_n_cores_and_names_the_database():
    d = _decision([_step(busy=0.95)], 5_000, 8)
    assert d["cores_needed"] == 12 and not d["exact"]
    assert any("at most 12" in t for t in d["text"])
    assert any("the database was the limit" in t for t in d["text"])


def test_decision_names_the_harness_disk_when_the_journal_is_slower_than_the_call():
    d = _decision([_step(busy=0.30, journal=12.0, call=3.0)], None, 8)
    assert any("harness disk was the limit" in t for t in d["text"])


def test_decision_names_waiting_otherwise():
    d = _decision([_step(busy=0.30, journal=1.0, call=3.0)], None, 8)
    assert any("network or on locks" in t for t in d["text"])


def test_no_decision_when_no_step_passed_its_checks():
    d = _decision([_step(ok=False)], 5_000, 8)
    assert d["tps_per_process"] is None and d["cores_needed"] is None
