"""TPC-C build step 9: the reset before a scenario run (option A: reload), and the bounded
extension of the init phase it needs. No database: a fake adapter records what is done."""

from __future__ import annotations

import asyncio

import pytest

from catalog.schema import Workload
from resilience_tests.execution.workload.tpcc import data
from resilience_tests.execution.workload.tpcc.profile import TpccDataError, TpccProfile
from tests.test_orchestrator import env, run  # noqa: F401  (pytest fixture)

COLUMNS = {t: {"live": n, "dropped": 0} for t, n in data.EXPECTED_COLUMNS.items()}
CLEAN_RULES = {1: 0, 2: 0, 3: 0, 4: 0}
STATUS = {"loaded": True, "warehouses": 4, "items": data.ITEMS, "loader_version": data.LOADER_VERSION,
          "c_last_load": 99, "loaded_orders": 120_000, "current_orders": 130_000, "size_bytes": 4e8,
          "load_seconds": 60.0, "capacity_tps": 1472.9}


# --- the rules (pure) ------------------------------------------------------------------------

def test_clean_data_is_not_reset():
    assert data.reset_reasons(STATUS, COLUMNS, CLEAN_RULES) == []


@pytest.mark.parametrize("change, expected", [
    ({"status": {"current_orders": 150_000}}, "grown to 1.25 times"),                 # above 1.2
    ({"status": {"loader_version": 0}}, "loader version 0"),
    ({"status": {"current_orders": None}}, "growth of the data cannot be measured"),
    ({"rules": {1: 0, 2: 0, 3: 2, 4: 0}}, "rule 3"),
    ({"rules": {1: 0, 2: 0, 3: 0, 4: None}}, "rule 4 could not be evaluated"),         # unknown is not fine
    ({"rules": None}, "consistency rules could not be checked"),
    ({"columns": {**COLUMNS, "history": {"live": 9, "dropped": 0}}}, "history has 9 columns, the loader makes 8"),
    ({"columns": {**COLUMNS, "history": {"live": 8, "dropped": 1000}}}, "history has 1000 dropped columns"),
    ({"columns": {k: v for k, v in COLUMNS.items() if k != "stock"}}, "table stock is missing"),
    ({"columns": None}, "columns could not be read"),
])
def test_each_reason_to_reset(change, expected):
    reasons = data.reset_reasons({**STATUS, **change.get("status", {})}, change.get("columns", COLUMNS),
                                 change.get("rules", CLEAN_RULES))
    assert any(expected in r for r in reasons), reasons


def test_growth_up_to_the_reset_limit_is_kept_and_the_reset_comes_before_the_refusal():
    assert data.reset_reasons({**STATUS, "current_orders": 144_000}, COLUMNS, CLEAN_RULES) == []     # exactly 1.2
    assert data.RESET_GROWTH < data.GROWTH_LIMIT


def test_a_reset_never_creates_or_resizes_the_data():
    assert data.resettable(STATUS, 4)
    assert not data.resettable(STATUS, 8)                                  # another size: a person loads it
    assert not data.resettable({"loaded": False, "reason": "no schema"}, 4)
    assert not data.resettable({**STATUS, "items": 5}, 4)


def test_the_reload_bound_comes_from_the_targets_own_load_time():
    assert data.reload_budget_s(STATUS) == 2 * 60.0 + data.RELOAD_EXTRA_S
    assert data.reload_budget_s({**STATUS, "load_seconds": None}) is None


# --- reset_data(): what init does --------------------------------------------------------------

class FakeAdapter:
    def __init__(self, status=STATUS, columns=COLUMNS, rules=CLEAN_RULES, checkpoint_error=None):
        self.status, self.columns, self.rules = dict(status), columns, rules
        self.checkpoint_error, self.calls = checkpoint_error, []

    async def tpcc_data_status(self):
        return dict(self.status)

    async def tpcc_columns(self):
        return self.columns

    async def tpcc_consistency(self, rules, district_sample=None, seed=None):
        return {r: (self.rules or {}).get(r) for r in rules}

    async def tpcc_load(self, warehouses, *, jobs=4, replace=False, progress=None, seed=None):
        self.calls.append(("load", warehouses, replace))
        self.status.update(current_orders=self.status["loaded_orders"], capacity_tps=None, load_seconds=55.0)
        self.columns, self.rules = COLUMNS, CLEAN_RULES
        return {"seconds": 55.0}

    async def tpcc_record_capacity(self, tps):
        self.calls.append(("capacity", tps))
        self.status["capacity_tps"] = tps

    async def tpcc_checkpoint(self):
        self.calls.append(("checkpoint",))
        return self.checkpoint_error

    async def tpcc_install_functions(self):
        pass

    async def tpcc_clock(self):
        return "2026-11-12 10:00:00"


def _profile(warehouses=4):
    return TpccProfile(Workload(profile="tpcc", concurrency=4, transaction_markers=True, rate_tps=10,
                                tpcc={"warehouses": warehouses}))


def _reset(adapter, warehouses=4):
    extended = []
    out = asyncio.run(_profile(warehouses).reset_data(adapter, lambda s, why: extended.append((s, why))))
    return out, extended


def test_clean_data_is_used_as_it_is():
    adapter = FakeAdapter()
    out, extended = _reset(adapter)
    assert out == {"tpcc_reset": {"reloaded": False, "growth": 130_000 / 120_000, "size_bytes": 4e8}}
    assert adapter.calls == [] and extended == []


def test_grown_data_is_reloaded_with_more_time_asked_first_and_everything_recorded():
    adapter = FakeAdapter({**STATUS, "current_orders": 150_000})
    out, extended = _reset(adapter)
    assert adapter.calls == [("load", 4, True), ("capacity", 1472.9), ("checkpoint",)]   # capacity kept
    assert extended and extended[0][0] == 2 * 60.0 + data.RELOAD_EXTRA_S and "60 s" in extended[0][1]
    r = out["tpcc_reset"]
    assert r["reloaded"] and "grown to 1.25 times" in r["reasons"][0]
    assert r["after"]["growth"] == 1.0 and r["checkpoint"] == "done" and r["capacity_kept"] == 1472.9
    assert len(out["_disclosures"]) == 1 and "reloaded before this run" in out["_disclosures"][0]


def test_a_checkpoint_that_cannot_run_is_disclosed_not_hidden():
    adapter = FakeAdapter({**STATUS, "current_orders": 150_000},
                          checkpoint_error="InsufficientPrivilegeError: must be superuser or pg_checkpoint")
    out, _ = _reset(adapter)
    assert out["tpcc_reset"]["checkpoint"].startswith("InsufficientPrivilegeError")
    assert any("could not run a CHECKPOINT" in d for d in out["_disclosures"])


def test_a_column_left_behind_by_a_scenario_is_reset():
    adapter = FakeAdapter(columns={**COLUMNS, "history": {"live": 9, "dropped": 3}})
    out, _ = _reset(adapter)
    assert out["tpcc_reset"]["reloaded"] and adapter.calls[0] == ("load", 4, True)


def test_another_size_is_not_reloaded_and_prepare_then_refuses_with_the_load_command():
    adapter = FakeAdapter()
    out, extended = _reset(adapter, warehouses=8)
    assert out == {} and adapter.calls == [] and extended == []
    with pytest.raises(TpccDataError, match="reload with --warehouses 8"):
        asyncio.run(_profile(8).prepare(adapter))


# --- the orchestrator: reset after the target checks, init bound extended, disclosed -------------

class SlowReset:
    """Hooks whose reset takes 2 s: longer than a 1 s init bound unless it asks for more time."""

    def __init__(self, ask: bool):
        self.ask = ask

    async def reset_data(self, adapter, extend_phase=None):
        if self.ask:
            extend_phase(5.0, "test reload")
        await asyncio.sleep(2.0)
        return {"tpcc_reset": {"reloaded": True}, "_disclosures": ["The TPC-C data was reloaded (test)."]}

    async def prepare(self, adapter):
        return {}

    async def consistency_checks(self, adapter, run_dir):
        return {}


def _run_with(env, monkeypatch, hooks):
    from resilience_tests.control import orchestrator as orch
    monkeypatch.setattr(orch, "hooks_for", lambda workload: hooks)
    short = env.model_copy(update={"phase_timeouts_s": {**env.phase_timeouts_s, "init": 1.0}})
    return run(short, "NL-C-01")


def test_a_reset_longer_than_the_init_bound_completes_when_it_asks_for_time(env, monkeypatch):  # noqa: F811
    results = _run_with(env, monkeypatch, SlowReset(ask=True))
    init = next(p for p in results["phases"] if p["phase"] == "init")
    assert init["outcome"] == "ok", init
    assert results["facts"]["workload_reset"] == {"tpcc_reset": {"reloaded": True}}
    assert "The TPC-C data was reloaded (test)." in results["disclosures"]


def test_without_asking_the_init_bound_still_stops_it(env, monkeypatch):  # noqa: F811
    results = _run_with(env, monkeypatch, SlowReset(ask=False))
    init = next(p for p in results["phases"] if p["phase"] == "init")
    assert init["outcome"] == "timeout" and "phase exceeded 1 s" in init["error"], init
