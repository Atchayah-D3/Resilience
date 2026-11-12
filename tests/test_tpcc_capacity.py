"""TPC-C build step 8: the capacity test's rules (pure; the test itself runs on a target)."""

from __future__ import annotations

from resilience_tests.execution.workload import contract
from resilience_tests.execution.workload.tpcc.capacity import Step, decide, response_ok


def step(rate, tps, ok=True, limited=False, failure=None):
    return Step(rate=rate, processes=1, tps=tps, p99_ms=10.0, worst_p90_ms={}, response_ok=ok, errors=0,
                driver_limited=limited, failure=failure)


def test_rising_until_the_database_cannot_keep_up():
    assert decide([step(200, 199)], 100_000) == (False, None, "kept up; trying a higher rate")
    stop, cap, why = decide([step(200, 199), step(300, 298), step(450, 380)], 100_000)
    assert stop and cap == 298 and "saturated" in why          # the best step that kept up


def test_a_broken_response_time_rule_ends_the_test():
    stop, cap, why = decide([step(200, 199), step(300, 299, ok=False)], 100_000)
    assert stop and cap == 199 and "response-time" in why


def test_driver_limited_gives_only_a_lower_bound_never_a_capacity():
    stop, cap, why = decide([step(200, 199), step(300, 290, limited=True)], 100_000)
    assert stop and cap is None and "at least 290" in why


def test_a_failed_first_step_has_no_capacity():
    stop, cap, why = decide([step(200, 0, failure="connect refused")], 100_000)
    assert stop and cap is None and "connect refused" in why


def test_reaching_the_maximum_rate_says_so():
    stop, cap, why = decide([step(1000, 999)], 1000)
    assert stop and cap == 999 and "at least" in why


def _sample(p90):
    return {contract.BY_TYPE: {t: {"p90_ms": p90.get(t)} for t in contract.TPCC_TYPES}}


def test_response_time_rule_uses_the_worst_second_per_type():
    ok, worst = response_ok([_sample({contract.NEW_ORDER: 40.0}), _sample({contract.NEW_ORDER: 4_900.0})])
    assert ok and worst[contract.NEW_ORDER] == 4_900.0
    assert not response_ok([_sample({contract.PAYMENT: 5_001.0})])[0]
    assert response_ok([_sample({contract.STOCK_LEVEL: 19_000.0})])[0]       # Stock-Level: 20 s
    assert not response_ok([_sample({contract.STOCK_LEVEL: 20_001.0})])[0]
