"""TPC-C build step 5: the inputs of every transaction, the mix, and where clerks work.

Pure code, no database. Each test names the TPC-C 5.11 clause it checks; statistical tests use
enough draws, and fixed seeds, that they cannot fail by chance.
"""

from __future__ import annotations

import asyncio
import random
from collections import Counter

import pytest

from catalog.schema import Workload
from resilience_tests.execution.workload.contract import (
    DELIVERY,
    NEW_ORDER,
    ORDER_STATUS,
    PAYMENT,
    STOCK_LEVEL,
)
from resilience_tests.execution.workload.driver import group_of_workers
from resilience_tests.execution.workload.tpcc import inputs
from resilience_tests.execution.workload.tpcc.inputs import RunConstants
from resilience_tests.execution.workload.tpcc.profile import TpccProfile
from resilience_tests.execution.workload.tpcc.terminals import place_clerks

CONSTS = RunConstants(c_last=20, c_id=259, ol_i_id=7911)


def rng(seed=1):
    return random.Random(seed)


# --- NURand and the run constants (Clause 2.1.6, 2.1.6.1) --------------------------------------

@pytest.mark.parametrize("a, x, y", [(255, 0, 999), (1023, 1, 3000), (8191, 1, 100_000)])
def test_nurand_stays_in_its_range(a, x, y):
    r = rng()
    values = [inputs.nurand(r, a, x, y, 123) for _ in range(50_000)]
    assert min(values) >= x and max(values) <= y


def test_nurand_is_non_uniform():
    # uniform: the most frequent of 3,000 customers would get about 1/3000 of 300,000 picks (100)
    counts = Counter(inputs.nurand(rng(2), 1023, 1, 3000, 0) for _ in range(300_000))
    assert counts.most_common(1)[0][1] > 300


@pytest.mark.parametrize("c_load, c_run, allowed", [
    (0, 65, True), (0, 119, True), (0, 64, False), (0, 120, False),
    (0, 96, False), (0, 112, False), (200, 100, True), (200, 104, False),
])
def test_c_last_run_rule(c_load, c_run, allowed):
    assert inputs.c_last_run_allowed(c_load, c_run) is allowed


def test_run_constants_always_satisfy_clause_2_1_6_1_for_every_load_constant():
    for c_load in range(0, 256):
        c = RunConstants.choose(rng(c_load), c_load)
        assert inputs.c_last_run_allowed(c_load, c.c_last)
        assert 0 <= c.c_id <= 1023 and 0 <= c.ol_i_id <= 8191


# --- last names (Clause 4.3.2.3) ------------------------------------------------------------------

@pytest.mark.parametrize("n, name", [(0, "BARBARBAR"), (371, "PRICALLYOUGHT"), (999, "EINGEINGEING")])
def test_last_names_follow_the_syllable_rule(n, name):
    assert inputs.last_name(n) == name          # 371 is the specification's own example


def test_last_name_refuses_numbers_outside_0_to_999():
    with pytest.raises(ValueError):
        inputs.last_name(1000)


# --- the mix: the deck of 23 cards (Clause 5.2.4.2, 5.2.3) ---------------------------------------

def test_every_23_cards_hold_exactly_10_10_1_1_1():
    deck = inputs.Deck(rng())
    for _ in range(200):
        hand = Counter(deck.draw() for _ in range(23))
        assert hand == {NEW_ORDER: 10, PAYMENT: 10, ORDER_STATUS: 1, DELIVERY: 1, STOCK_LEVEL: 1}


def test_the_deck_is_shuffled_differently_each_time():
    deck = inputs.Deck(rng())
    hands = {tuple(deck.draw() for _ in range(23)) for _ in range(20)}
    assert len(hands) > 15


def test_the_mix_meets_the_minimums():
    deck = inputs.Deck(rng())
    n = 23 * 1000
    share = {t: c / n for t, c in Counter(deck.draw() for _ in range(n)).items()}
    for t, minimum in inputs.MIX_MINIMUM.items():
        assert share[t] >= minimum, (t, share[t])


# --- New-Order (Clause 2.4.1) --------------------------------------------------------------------

def _orders(n=20_000, home=3, warehouses=16, seed=5):
    r = rng(seed)
    return [inputs.new_order(r, home, warehouses, CONSTS) for _ in range(n)]


def test_new_order_ranges():
    for o in _orders(5_000):
        assert o["w_id"] == 3 and 1 <= o["d_id"] <= 10 and 1 <= o["c_id"] <= 3000
        assert 5 <= o["ol_cnt"] <= 15
        assert len(o["items"]) == len(o["supply_w"]) == len(o["quantities"]) == o["ol_cnt"]
        assert all(1 <= q <= 10 for q in o["quantities"])
        assert len(set(o["items"])) == o["ol_cnt"]


def test_one_percent_of_new_orders_roll_back_on_their_last_line_only():
    orders = _orders(40_000)
    rollbacks = [o for o in orders if o["rollback"]]
    assert 0.007 <= len(rollbacks) / len(orders) <= 0.013
    for o in rollbacks:
        assert o["items"][-1] == inputs.ITEMS + 1 and all(i <= inputs.ITEMS for i in o["items"][:-1])
    assert all(i <= inputs.ITEMS for o in orders if not o["rollback"] for i in o["items"])


def test_one_percent_of_order_lines_come_from_another_warehouse():
    orders = _orders(20_000)
    lines = [w for o in orders for w in o["supply_w"]]
    remote = [w for w in lines if w != 3]
    assert 0.007 <= len(remote) / len(lines) <= 0.013 and all(1 <= w <= 16 for w in remote)
    assert all(o["all_local"] == all(w == 3 for w in o["supply_w"]) for o in orders)


def test_with_one_warehouse_every_line_is_local():
    assert all(o["all_local"] for o in _orders(3_000, home=1, warehouses=1))


def test_lines_are_sorted_so_new_orders_cannot_deadlock():
    for o in _orders(3_000):
        pairs = list(zip(o["supply_w"], o["items"]))
        if o["rollback"]:
            pairs = pairs[:-1]
        assert pairs == sorted(pairs)


# --- Payment (Clause 2.5.1) ------------------------------------------------------------------------

def _payments(n=40_000, home=3, warehouses=16):
    r = rng(7)
    return [inputs.payment(r, home, warehouses, CONSTS) for _ in range(n)]


def test_fifteen_percent_of_payments_are_by_a_customer_of_another_warehouse():
    ps = _payments()
    remote = [p for p in ps if p["remote"]]
    assert 0.14 <= len(remote) / len(ps) <= 0.16
    assert all(p["c_w_id"] != 3 for p in remote)
    assert all(p["c_w_id"] == 3 and p["c_d_id"] == p["d_id"] for p in ps if not p["remote"])


def test_sixty_percent_of_customers_are_found_by_last_name():
    ps = _payments()
    by_name = [p for p in ps if p["by_last_name"]]
    assert 0.58 <= len(by_name) / len(ps) <= 0.62
    assert all(p["c_last"] and p["c_id"] is None for p in by_name)
    assert all(p["c_id"] and p["c_last"] is None for p in ps if not p["by_last_name"])


def test_payment_amounts_are_1_to_5000_in_cents():
    amounts = [p["amount_cents"] for p in _payments(10_000)]
    assert min(amounts) >= 100 and max(amounts) <= 500_000


def test_with_one_warehouse_every_payment_is_local():
    assert not any(p["remote"] for p in _payments(3_000, home=1, warehouses=1))


# --- Order-Status, Delivery, Stock-Level (Clauses 2.6.1, 2.7.1, 2.8.1) -----------------------------

def test_order_status_finds_sixty_percent_by_last_name():
    r = rng(9)
    os_ = [inputs.order_status(r, 2, CONSTS) for _ in range(20_000)]
    assert 0.58 <= sum(o["by_last_name"] for o in os_) / len(os_) <= 0.62
    assert all(1 <= o["d_id"] <= 10 for o in os_)


def test_delivery_carrier_is_1_to_10():
    r = rng(10)
    assert {inputs.delivery(r, 2)["o_carrier_id"] for _ in range(2_000)} == set(range(1, 11))


def test_stock_level_uses_the_clerks_own_district_and_a_threshold_of_10_to_20():
    r = rng(11)
    sl = [inputs.stock_level(r, 2, 4) for _ in range(2_000)]
    assert {s["d_id"] for s in sl} == {4} and {s["threshold"] for s in sl} == set(range(10, 21))


# --- where clerks work -------------------------------------------------------------------------------

def _workload(concurrency, warehouses, groups=(), home=None):
    return Workload(profile="tpcc", concurrency=concurrency, transaction_markers=True, rate_tps=100,
                    groups=list(groups), tpcc={"warehouses": warehouses, "home_warehouse": home or {}})


def test_standard_size_gives_every_clerk_its_own_home_warehouse():
    terms = place_clerks(_workload(64, 320))
    homes = [t.home for t in terms]
    assert len(set(homes)) == 64 and min(homes) >= 1 and max(homes) <= 320
    assert max(homes) - min(homes) > 300                 # spread over the whole range


def test_a_pinned_group_works_on_its_warehouse_and_the_others_avoid_it():
    w = _workload(64, 320, [{"name": "colliding", "clerks": 6}, {"name": "non_colliding", "clerks": 58}],
                  {"colliding": 7})
    terms = place_clerks(w)
    assert [t.home for t in terms if t.group == "colliding"] == [7] * 6
    others = [t.home for t in terms if t.group == "non_colliding"]
    assert 7 not in others and len(set(others)) == 58
    assert [t.group for t in terms] == group_of_workers(w)     # the same grouping as the driver


def test_more_clerks_than_free_warehouses_share_in_turn():
    homes = [t.home for t in place_clerks(_workload(10, 4))]
    assert sorted(Counter(homes).values()) == [2, 2, 3, 3] and set(homes) == {1, 2, 3, 4}


def test_every_clerk_has_a_fixed_district():
    terms = place_clerks(_workload(25, 125))
    assert [t.district for t in terms] == [i % 10 + 1 for i in range(25)]


def test_no_free_warehouse_left_is_refused():
    w = _workload(4, 1, [{"name": "a", "clerks": 2}, {"name": "b", "clerks": 2}], {"a": 1})
    with pytest.raises(ValueError, match="no warehouse left"):
        place_clerks(w)


# --- the tpcc profile ties it together ---------------------------------------------------------------

def _profile(seed=None):
    p = TpccProfile(_workload(8, 40)) if seed is None else TpccProfile(_workload(8, 40), seed=seed)
    return p


def test_requests_need_the_run_constants_from_prepare_first():
    p = _profile()
    with pytest.raises(RuntimeError, match="prepare"):
        p.next_request(p.terminal(0))


def test_prepare_sets_run_constants_that_obey_clause_2_1_6_1():
    status = {"loaded": True, "warehouses": 40, "items": inputs.ITEMS, "loader_version": 1,
              "c_last_load": 195, "loaded_orders": 1_200_000, "current_orders": 1_200_000}

    class Adapter:
        async def tpcc_data_status(self):
            return status

        async def tpcc_consistency(self, rules, district_sample=None, seed=None):
            return {r: 0 for r in rules}

    p = _profile()
    facts = asyncio.run(p.prepare(Adapter()))["tpcc_data"]["run_constants"]
    assert inputs.c_last_run_allowed(195, facts["c_last"]) and p.constants is not None


def test_requests_follow_the_deck_and_mark_read_only_types():
    p = _profile()
    p.set_run_constants(100)
    t = p.terminal(3)
    reqs = [p.next_request(t) for _ in range(230)]
    assert Counter(r.txn_type for r in reqs) == {NEW_ORDER: 100, PAYMENT: 100, ORDER_STATUS: 10,
                                                 DELIVERY: 10, STOCK_LEVEL: 10}
    for r in reqs:
        assert r.needs_marker is (r.txn_type not in (ORDER_STATUS, STOCK_LEVEL))
        assert r.inputs["w_id"] == t.home
        if r.txn_type == STOCK_LEVEL:
            assert r.inputs["d_id"] == t.district


def test_same_seed_same_transactions_different_seed_different():
    def seq(seed):
        p = _profile(seed)
        p.set_run_constants(100)
        t = p.terminal(0)
        return [(r.txn_type, r.inputs) for r in (p.next_request(t) for _ in range(50))]
    assert seq(1) == seq(1) and seq(1) != seq(2)


def test_each_clerk_has_its_own_sequence():
    p = _profile()
    p.set_run_constants(100)
    a = [p.next_request(p.terminal(0)).txn_type for _ in range(23)]
    b = [p.next_request(p.terminal(1)).txn_type for _ in range(23)]
    assert a != b and Counter(a) == Counter(b)
