"""TPC-C build step 7: the checks after a fault, without a database.

Each case of proposal 7.6 is simulated and must get its own label; a part that cannot be read
must leave its measure NOT MEASURED (never 0). The real database is exercised by
tests/test_tpcc_functions_db.py.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from catalog.schema import Workload, load_catalog
from resilience_tests.execution.workload.contract import DELIVERY, NEW_ORDER, PAYMENT
from resilience_tests.execution.workload.contract import TPCC_GATED_MEASURES, TPCC_GATES
from resilience_tests.execution.workload.hooks import split_checks, tpcc_override_note, with_tpcc_workload
from resilience_tests.execution.workload.tpcc import checks
from resilience_tests.execution.workload.tpcc.profile import TpccProfile

CLEAN_RULES = {r: 0 for r in checks.RULES_ALL_DATA + checks.RULES_SAMPLED}


def evidence(**changes) -> checks.Evidence:
    """A clean run: 3 New-Orders, 1 Payment, 1 Delivery committed; 1 New-Order rolled back."""
    base = dict(
        written={"n1": NEW_ORDER, "n2": NEW_ORDER, "n3": NEW_ORDER, "p1": PAYMENT, "d1": DELIVERY, "r1": NEW_ORDER},
        acked={"n1": {"w": 1, "d": 1, "o_id": 3001, "ol_cnt": 5}, "n2": {"w": 1, "d": 2, "o_id": 3001, "ol_cnt": 7},
               "n3": {"w": 1, "d": 3, "o_id": 3001, "ol_cnt": 9}, "p1": {}, "d1": {}},
        aborted={"r1": {"business_rollback": True}},
        db={"n1", "n2", "n3", "p1", "d1"},
        acked_order_check={"checked": 3, "missing": 0, "incomplete": 0},
        lost_order_check={"checked": 0, "missing": 0, "incomplete": 0},
        orders_before=1000, orders_after=1003, rules=dict(CLEAN_RULES))
    base.update(changes)
    return checks.Evidence(**base)


def test_a_clean_run_has_no_problem_of_any_kind():
    measures, facts, reasons = checks.evaluate(evidence())
    assert reasons == {}
    assert measures == {"indeterminate_txn": 0, "aborted_markers_present": 0, "partial_commits": 0,
                        "tpcc_consistency_violations": 0}
    assert facts["lost"] == facts["phantom"] == facts["rolled_back_but_kept"] == 0


def test_unknown_counts_only_transactions_neither_committed_nor_rolled_back():
    ev = evidence(written={**evidence().written, "u1": NEW_ORDER, "u2": PAYMENT})
    measures, facts, _ = checks.evaluate(ev)
    assert measures["indeterminate_txn"] == 2              # the business rollback r1 is NOT unknown
    assert facts["unknown_committed"] == 0


def test_a_rolled_back_transaction_left_in_the_database_is_caught():
    measures, facts, _ = checks.evaluate(evidence(db={"n1", "n2", "n3", "p1", "d1", "r1"}))
    assert measures["aborted_markers_present"] == 1 and facts["rolled_back_but_kept"] == 1


def test_acknowledged_new_order_with_missing_lines_is_a_partial_commit():
    measures, facts, _ = checks.evaluate(evidence(acked_order_check={"checked": 3, "missing": 0, "incomplete": 1}))
    assert measures["partial_commits"] == 1
    assert facts["partial_commit_parts"]["acknowledged_order_incomplete"] == 1


def test_acknowledged_new_order_whose_order_vanished_is_a_partial_commit():
    # marker kept, order row gone; the counter update was lost with it
    measures, _, _ = checks.evaluate(evidence(acked_order_check={"checked": 3, "missing": 1, "incomplete": 0},
                                              orders_after=1002))
    assert measures["partial_commits"] == 1


def test_lost_new_order_whose_rows_survived_is_a_partial_commit_not_only_a_loss():
    # n3's marker is gone but its order is there: rows without their marker
    ev = evidence(db={"n1", "n2", "p1", "d1"},
                  acked_order_check={"checked": 2, "missing": 0, "incomplete": 0},
                  lost_order_check={"checked": 1, "missing": 0, "incomplete": 0})
    measures, facts, _ = checks.evaluate(ev)
    assert facts["lost"] == 1                                      # still counted by rpo_txn
    assert measures["partial_commits"] == 1
    assert facts["partial_commit_parts"]["lost_but_rows_present"] == 1


def test_a_lost_new_order_that_left_nothing_is_a_loss_only():
    ev = evidence(db={"n1", "n2", "p1", "d1"}, orders_after=1002,
                  acked_order_check={"checked": 2, "missing": 0, "incomplete": 0},
                  lost_order_check={"checked": 1, "missing": 1, "incomplete": 0})
    measures, facts, _ = checks.evaluate(ev)
    assert facts["lost"] == 1 and measures["partial_commits"] == 0


def test_unknown_new_order_that_committed_completely_is_fine():
    ev = evidence(written={**evidence().written, "u1": NEW_ORDER}, db={"n1", "n2", "n3", "p1", "d1", "u1"},
                  orders_after=1004)
    measures, facts, _ = checks.evaluate(ev)
    assert measures["partial_commits"] == 0 and facts["unknown_committed"] == 1


@pytest.mark.parametrize("db_extra, orders_after, case", [
    ({"u1"}, 1003, "marker kept, order never created"),
    (set(), 1004, "order created, marker lost"),
])
def test_unknown_new_order_half_saved_is_found_by_the_order_count(db_extra, orders_after, case):
    ev = evidence(written={**evidence().written, "u1": NEW_ORDER},
                  db={"n1", "n2", "n3", "p1", "d1"} | db_extra, orders_after=orders_after)
    measures, facts, _ = checks.evaluate(ev)
    assert measures["partial_commits"] == 1, case
    assert abs(facts["partial_commit_parts"]["unexplained_order_count"]) == 1


def test_consistency_rule_violations_are_summed():
    measures, facts, _ = checks.evaluate(evidence(rules={**CLEAN_RULES, 8: 1, 10: 3}))
    assert measures["tpcc_consistency_violations"] == 4 and facts["consistency"]["10"] == 3


@pytest.mark.parametrize("changes, measure", [
    ({"db": None}, "partial_commits"),
    ({"db": None}, "aborted_markers_present"),
    ({"acked_order_check": None}, "partial_commits"),
    ({"lost_order_check": None}, "partial_commits"),
    ({"orders_before": None}, "partial_commits"),
    ({"orders_after": None}, "partial_commits"),
    ({"rules": {r: 0 for r in checks.RULES_ALL_DATA}}, "tpcc_consistency_violations"),   # sampled rules missing
    ({"rules": {**CLEAN_RULES, 4: None}}, "tpcc_consistency_violations"),
    ({"torn_aborted_lines": 1}, "aborted_markers_present"),
])
def test_a_part_that_cannot_be_read_is_not_measured_never_zero(changes, measure):
    measures, _, reasons = checks.evaluate(evidence(**changes))
    assert measure not in measures and measure in reasons


# --- the orchestrator's side: measures, facts, reasons ------------------------------------------

def test_split_checks_turns_reasons_into_not_measured_even_if_a_value_was_given():
    measures, facts, reasons = split_checks({"partial_commits": 0, "nopm": 5, "_facts": {"x": 1},
                                             "_not_measured": {"partial_commits": "could not run"}})
    assert measures == {"nopm": 5} and facts == {"x": 1} and reasons == {"partial_commits": "could not run"}


def test_split_checks_of_the_custom_workload_is_empty():
    assert split_checks({}) == ({}, {}, {})


# --- the tpcc profile reads the journals and asks the adapter ---------------------------------------

class FakeAdapter:
    def __init__(self, db, fail=None, order_checks=None, orders_after=1002):
        self.db, self.fail, self.order_checks, self.orders_after, self.calls = db, fail or set(), order_checks, orders_after, []

    async def _maybe_fail(self, name):
        if name in self.fail:
            raise ConnectionError(f"{name} failed")

    async def marker_ids(self):
        await self._maybe_fail("marker_ids")
        return set(self.db)

    async def tpcc_order_check(self, orders):
        await self._maybe_fail("tpcc_order_check")
        self.calls.append(("order_check", [o["o_id"] for o in orders]))
        return {"checked": len(orders), "missing": 0, "incomplete": 0}

    async def tpcc_data_status(self):
        await self._maybe_fail("tpcc_data_status")
        return {"current_orders": self.orders_after}

    async def tpcc_consistency(self, rules, district_sample=None, seed=None):
        await self._maybe_fail("tpcc_consistency")
        return {r: 0 for r in rules}


def _write(path: Path, records):
    path.write_text("".join(json.dumps(r) + "\n" for r in records))


def _run_dir(tmp_path):
    _write(tmp_path / "marker.jrnl", [{"seq": 1, "uuid": "a", "type": NEW_ORDER}, {"seq": 2, "uuid": "b", "type": NEW_ORDER},
                                      {"seq": 3, "uuid": "c", "type": PAYMENT}, {"seq": 4, "uuid": "r", "type": NEW_ORDER}])
    _write(tmp_path / "acked.jrnl", [{"uuid": "a", "w": 1, "d": 1, "o_id": 3001, "ol_cnt": 5},
                                     {"uuid": "b", "w": 1, "d": 2, "o_id": 3001, "ol_cnt": 6}, {"uuid": "c"}])
    _write(tmp_path / "aborted.jrnl", [{"uuid": "r", "business_rollback": True, "sqlstate": "TP001"}])
    return tmp_path


def _profile():
    p = TpccProfile(Workload(profile="tpcc", concurrency=4, transaction_markers=True, rate_tps=10, tpcc={"warehouses": 2}))
    p.orders_at_prepare = 1000
    return p


def test_profile_checks_a_clean_run(tmp_path):
    adapter = FakeAdapter(db={"a", "b", "c"})
    result = asyncio.run(_profile().consistency_checks(adapter, _run_dir(tmp_path)))
    measures, facts, reasons = split_checks(result)
    assert reasons == {} and measures["partial_commits"] == 0 and measures["aborted_markers_present"] == 0
    assert measures["indeterminate_txn"] == 0 and measures["tpcc_consistency_violations"] == 0
    assert ("order_check", [3001, 3001]) in adapter.calls          # both acknowledged New-Orders checked


def test_profile_reports_why_when_a_check_cannot_run(tmp_path):
    adapter = FakeAdapter(db={"a", "b", "c"}, fail={"tpcc_consistency"})
    measures, facts, reasons = split_checks(asyncio.run(_profile().consistency_checks(adapter, _run_dir(tmp_path))))
    assert "tpcc_consistency_violations" not in measures
    assert "ConnectionError" in reasons["tpcc_consistency_violations"]
    assert measures["partial_commits"] == 0                         # the other checks still ran


def test_profile_without_markers_measures_nothing_it_cannot_prove(tmp_path):
    adapter = FakeAdapter(db=set(), fail={"marker_ids"})
    measures, _, reasons = split_checks(asyncio.run(_profile().consistency_checks(adapter, _run_dir(tmp_path))))
    assert {"partial_commits", "aborted_markers_present", "indeterminate_txn"} <= set(reasons)
    assert not {"partial_commits", "aborted_markers_present", "indeterminate_txn"} & set(measures)


def test_profile_without_the_order_count_at_prepare_is_not_measured(tmp_path):
    p = _profile()
    p.orders_at_prepare = None
    _, _, reasons = split_checks(asyncio.run(p.consistency_checks(FakeAdapter(db={"a", "b", "c"}), _run_dir(tmp_path))))
    assert "partial_commits" in reasons


# --- the run option: a scenario tried with the TPC-C workload ---------------------------------------

def test_with_tpcc_workload_keeps_clerks_rate_and_session_and_drops_the_elle_history():
    nlc03 = load_catalog().scenarios["NL-C-03"]
    assert nlc03.workload.history == "list_append"
    tried = with_tpcc_workload(nlc03, 16)
    assert tried.workload.profile == "tpcc" and tried.workload.tpcc.warehouses == 16
    assert tried.workload.concurrency == nlc03.workload.concurrency and tried.workload.rate_tps == nlc03.workload.rate_tps
    assert tried.workload.history == "none"
    assert tried.fault == nlc03.fault
    # the scenario's own acceptance is kept, in order, and the TPC-C gates are added after it
    assert tried.accept[:len(nlc03.accept)] == nlc03.accept
    assert all(g in tried.accept for g in TPCC_GATES) and len(tried.accept) == len(set(tried.accept))
    assert set(TPCC_GATED_MEASURES) <= set(tried.measure) and len(tried.measure) == len(set(tried.measure))
    note = tpcc_override_note(16)
    assert "16 warehouses" in note and "was not changed" in note and "TPC-C-like" in note and "partial_commits == 0" in note


def test_without_the_tpcc_gates_a_partial_commit_would_have_passed_nl_c_05():
    """Was (audit, 12 November): NL-C-05 run with --tpcc-warehouses gated neither rpo_txn nor any TPC-C
    check, so a partial commit would be reported and the run would still pass."""
    from resilience_tests.analysis.threshold_eval import evaluate
    nlc05 = load_catalog().scenarios["NL-C-05"]
    clean = {"cycles_recovered": 10, "cycles_run": 10, "recovery_ratio_last_over_first": 0.3,
             "structural_integrity_errors": 0, "corruption_count": 0, "rpo_txn": 0, "partial_commits": 0,
             "aborted_markers_present": 0, "tpcc_consistency_violations": 0}
    broken = {**clean, "partial_commits": 1}
    assert evaluate(nlc05.accept, broken).passed                          # the scenario's own rules alone: PASS
    tried = with_tpcc_workload(nlc05, 32)
    assert evaluate(tried.accept, clean).passed
    assert not evaluate(tried.accept, broken).passed                      # with the gates: FAIL
    for name in TPCC_GATED_MEASURES:                                       # and a check that could not run fails too
        assert not evaluate(tried.accept, {k: v for k, v in clean.items() if k != name}).passed


def test_a_scenario_file_with_the_tpcc_profile_must_gate_the_tpcc_checks():
    from catalog.schema import Scenario
    data = load_catalog().scenarios["NL-C-01"].model_dump(mode="json")
    data["workload"] = {**data["workload"], "profile": "tpcc", "tpcc": {"warehouses": 16}, "history": "none"}
    with pytest.raises(ValueError, match="profile tpcc needs these accept predicates"):
        Scenario.model_validate(data)
    data["measure"] = list(dict.fromkeys([*data["measure"], *TPCC_GATED_MEASURES]))
    data["accept"] = [*data["accept"], *(g for g in TPCC_GATES if g not in data["accept"])]
    assert Scenario.model_validate(data).workload.profile == "tpcc"


def test_every_tpcc_run_is_labelled_tpcc_like_with_its_differences():
    p = TpccProfile(Workload(profile="tpcc", concurrency=64, transaction_markers=True, rate_tps=200,
                             tpcc={"warehouses": 32}))
    label = p.label()
    for text in ("TPC-C-like", "not an audited TPC-C", "not tpmC", "no keying or thinking time", "200 TPS",
                 "2 clerks per warehouse", "Delivery runs at once", "READ COMMITTED", "Clause 3.4", "RPO marker"):
        assert text in label, text
