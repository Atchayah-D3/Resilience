"""TPC-C build step 4, without a database: the expected data size, the checks that refuse
unusable data, the `tpcc` profile's prepare(), and the load command's safety refusals.
(tests/test_tpcc_load_db.py runs the real load against a throw-away database.)
"""

from __future__ import annotations

import asyncio
import random

import pytest

from catalog.schema import Workload
from resilience_tests.control.profile import load_profile
from resilience_tests.execution.workload.tpcc import data, load
from resilience_tests.execution.workload.tpcc.profile import TpccDataError, TpccProfile
from tests.test_adapter_seam import NODE

GOOD = {"loaded": True, "warehouses": 4, "items": data.ITEMS, "loader_version": data.LOADER_VERSION,
        "c_last_load": 99, "loaded_orders": 120_000, "current_orders": 130_000, "size_bytes": 4e8}


# --- expected size (Clause 4.2.2) ------------------------------------------------------------

def test_expected_rows_per_warehouse():
    e = data.expected_rows(1)
    assert e["warehouse"] == (1, 1) and e["district"] == (10, 10)
    assert e["customer"] == e["history"] == e["orders"] == (30_000, 30_000)
    assert e["new_order"] == (9_000, 9_000) and e["item"] == (100_000, 100_000) and e["stock"] == (100_000, 100_000)
    assert e["order_line"] == (297_000, 303_000)                 # average 10 per order, within 1%
    assert data.expected_rows(320)["stock"] == (32_000_000, 32_000_000)


def test_row_counts_outside_the_rules_are_reported():
    good = {t: lo for t, (lo, _) in data.expected_rows(2).items()}
    assert data.row_count_problems(good, 2) == []
    bad = {**good, "new_order": 17_999, "order_line": 700_000}
    problems = data.row_count_problems(bad, 2)
    assert any("new_order: 17999" in p for p in problems) and any("order_line: 700000" in p for p in problems)
    assert any("stock: not counted" in p for p in data.row_count_problems({k: v for k, v in good.items() if k != "stock"}, 2))


# --- the data checks ---------------------------------------------------------------------------

def test_good_data_has_no_problems():
    assert data.data_problems(GOOD, 4) == []
    assert data.growth(GOOD) == pytest.approx(130_000 / 120_000)


@pytest.mark.parametrize("change, expected, message", [
    ({"loaded": False, "reason": "schema resilience_tpcc does not exist"}, 4, "does not exist; load it first"),
    ({}, 8, "the scenario expects 8"),
    ({"loader_version": data.LOADER_VERSION + 1}, 4, "loader version"),
    ({"items": 50_000}, 4, "50000 items"),
    ({"current_orders": 181_000}, 4, "grown to 1.51 times"),
    ({"current_orders": None}, 4, "growth of the data cannot be measured"),
])
def test_unusable_data_is_refused_with_the_reason(change, expected, message):
    status = {**GOOD, **change}
    problems = data.data_problems(status, expected)
    assert problems and any(message in p for p in problems), problems


def test_growth_exactly_at_the_limit_is_still_accepted():
    assert data.data_problems({**GOOD, "current_orders": 180_000}, 4) == []


def test_a_rule_that_could_not_be_evaluated_is_never_read_as_holding():
    assert data.consistency_problems({1: 0, 2: 0}) == []
    assert any("rule 3 could not be evaluated" in p for p in data.consistency_problems({1: 0, 3: None}))
    assert any("rule 4 (Clause 3.3.2.4) fails for 2" in p for p in data.consistency_problems({4: 2}))


def test_district_sample_is_distinct_and_in_range():
    w, d = data.sample_districts(random.Random(1), 3, 10)
    pairs = list(zip(w, d))
    assert len(pairs) == len(set(pairs)) == 10
    assert all(1 <= x <= 3 for x in w) and all(1 <= y <= 10 for y in d)
    assert len(data.sample_districts(random.Random(1), 1, 50)[0]) == 10      # never more than exist


def test_c_last_load_constant_is_in_its_range():
    rng = random.Random(3)
    assert all(0 <= data.choose_c_last_load(rng) <= 255 for _ in range(1000))


# --- prepare(): what init does with a tpcc scenario ----------------------------------------------

class FakeTpccAdapter:
    def __init__(self, status, consistency=None):
        self.status, self.consistency, self.calls = status, consistency or {1: 0, 2: 0, 3: 0, 4: 0}, []

    async def tpcc_data_status(self):
        return self.status

    async def tpcc_install_functions(self):
        self.calls.append(("install_functions",))

    async def tpcc_consistency(self, rules, district_sample=None, seed=None):
        self.calls.append((tuple(rules), district_sample))
        return {r: self.consistency.get(r) for r in rules}


def _profile(warehouses=4):
    return TpccProfile(Workload(profile="tpcc", concurrency=4, transaction_markers=True, rate_tps=10,
                                tpcc={"warehouses": warehouses}))


def test_prepare_accepts_good_data_and_checks_rules_1_to_4_on_a_sample():
    adapter = FakeTpccAdapter(GOOD)
    facts = asyncio.run(_profile().prepare(adapter))["tpcc_data"]
    assert adapter.calls == [("install_functions",), ((1, 2, 3, 4), data.PREPARE_DISTRICT_SAMPLE)]
    assert facts["warehouses"] == 4 and facts["consistency"] == {"1": 0, "2": 0, "3": 0, "4": 0}


def test_prepare_refuses_wrong_size_before_running_any_consistency_query():
    adapter = FakeTpccAdapter(GOOD)
    with pytest.raises(TpccDataError, match="expects 8"):
        asyncio.run(_profile(8).prepare(adapter))
    assert adapter.calls == []


@pytest.mark.parametrize("consistency, message", [
    ({1: 0, 2: 0, 3: 0, 4: 3}, "rule 4"),
    ({1: 0, 2: 0, 3: 0}, "rule 4 could not be evaluated"),        # a missing answer is not a pass
])
def test_prepare_refuses_inconsistent_data(consistency, message):
    with pytest.raises(TpccDataError, match=message):
        asyncio.run(_profile().prepare(FakeTpccAdapter(GOOD, consistency)))


# --- the load command's safety refusals ------------------------------------------------------------

class FakeSentinelAdapter:
    def __init__(self, sentinel):
        self._sentinel = sentinel

    async def sentinel(self, table, hostname):
        return self._sentinel


def _hostname(monkeypatch, name):
    from resilience_tests.execution.remote import RemoteResult

    async def fake_run_once(endpoint, command, *, timeout_s, check=True):
        return RemoteResult(0, name + "\n", "")
    monkeypatch.setattr(load, "run_once", fake_run_once)


def test_load_command_accepts_the_lab_target(monkeypatch):
    profile = load_profile("e2-dedicated-vm")
    _hostname(monkeypatch, "BDB-QA-U22-30")
    sentinel = {"inventory_tag": profile.safety.allowlist.inventory_tag}
    assert asyncio.run(load.check_target(profile, NODE, FakeSentinelAdapter(sentinel))) == "BDB-QA-U22-30"


@pytest.mark.parametrize("hostname, sentinel, environment, message", [
    ("BDB-QA-U22-30", {"inventory_tag": "resilience-lab-e2"}, "production", "production environment"),
    ("some-other-host", {"inventory_tag": "resilience-lab-e2"}, "lab", "does not match the allowlist"),
    ("BDB-QA-U22-30", None, "lab", "default deny"),
    ("BDB-QA-U22-30", {"inventory_tag": "someone-else"}, "lab", "inventory_tag"),
])
def test_load_command_refuses_anything_but_the_allowed_target(monkeypatch, hostname, sentinel, environment, message):
    profile = load_profile("e2-dedicated-vm").model_copy(update={"environment": environment})
    _hostname(monkeypatch, hostname)
    with pytest.raises(SystemExit, match=message):
        asyncio.run(load.check_target(profile, NODE, FakeSentinelAdapter(sentinel)))
