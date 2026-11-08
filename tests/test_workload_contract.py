"""The workload contract (TPC-C build step 1) must describe the harness as it really is.

These tests compare the contract's names with the real driver, the real sample events, the
real outcome values, the catalog's measure rules and the predicate parser. If the code and the
contract drift apart, a test here fails, so the scenario developer can trust the contract.
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path

import pytest

from catalog.schema import CATALOG_ROOT, IDENT_RE
from resilience_tests.adapters.base import TransactionOutcome
from resilience_tests.analysis.predicates import PredicateError, parse
from resilience_tests.execution.workload import contract as c
from resilience_tests.execution.workload.driver import WorkloadDriver
from resilience_tests.execution.workload.markers import MarkerJournals
from resilience_tests.observability.event_stream import EventStream
from tests.test_adapter_seam import NODE, WORKLOAD, FakeAdapter

# --- 1. the driver functions ---------------------------------------------------------------


def test_the_driver_has_every_function_and_attribute_the_contract_names():
    for name in ("start", "wait_until_ready", "begin_window", "end_window", "stop"):
        assert callable(getattr(WorkloadDriver, name)), name

    async def go():
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            stream, journals = EventStream(d / "events.jsonl"), MarkerJournals(d)
            driver = WorkloadDriver(FakeAdapter(NODE), WORKLOAD, journals, stream)
            assert driver.failure is None
            driver.begin_window()
            assert isinstance(driver.window_t0_ns, int)
            assert isinstance(driver, c.WorkloadDriverContract)
            journals.close()
            stream.close()

    asyncio.run(go())


# --- 2. the sample fields ------------------------------------------------------------------


def _real_samples(monkeypatch) -> list[dict]:
    from resilience_tests.execution.workload import driver as driver_mod

    monkeypatch.setattr(driver_mod, "SAMPLE_INTERVAL_S", 0.2)

    async def go():
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            stream, journals = EventStream(d / "events.jsonl"), MarkerJournals(d)
            driver = WorkloadDriver(FakeAdapter(NODE), WORKLOAD, journals, stream)
            await driver.start()
            await asyncio.sleep(0.9)
            await driver.stop()
            journals.close()
            stream.close()
            return [e for e in stream.events() if e.source == c.SAMPLE_SOURCE and e.kind == c.SAMPLE_KIND]

    return [e.data for e in asyncio.run(go())]


def test_sample_fields_are_exactly_what_the_driver_emits(monkeypatch):
    samples = _real_samples(monkeypatch)
    assert samples, "the driver emitted no sample"
    for data in samples:
        # today's fields, unchanged, plus the two extra fields added in build step 2
        assert set(data) == set(c.SAMPLE_FIELDS) | {c.BY_TYPE, c.BY_GROUP}


def test_type_and_group_stats_reuse_the_top_level_names():
    top = set(c.SAMPLE_FIELDS)
    assert set(c.GroupStats.__annotations__) <= top
    assert set(c.TypeStats.__annotations__) - {"p90_ms", "business_rollbacks"} <= top


# --- 3. groups -----------------------------------------------------------------------------


def test_no_groups_is_valid():
    assert c.group_problems(64, []) == []


def test_groups_that_cover_every_clerk_are_valid():
    assert c.group_problems(64, [{"name": "colliding", "clerks": 6},
                                 {"name": "non_colliding", "clerks": 58}]) == []


@pytest.mark.parametrize("groups, problem", [
    ([{"name": "colliding", "clerks": 6}], "add up to 6"),
    ([{"name": "a", "clerks": 32}, {"name": "a", "clerks": 32}], "unique"),
    ([{"name": "all", "clerks": 64}], "reserved"),
    ([{"name": "Bad-Name", "clerks": 64}], "must match"),
    ([{"name": "a", "clerks": 0}, {"name": "b", "clerks": 64}], "clerks > 0"),
    ([{"name": "a", "clerks": True}, {"name": "b", "clerks": 63}], "clerks > 0"),
])
def test_invalid_groups_are_refused_with_the_reason(groups, problem):
    assert any(problem in p for p in c.group_problems(64, groups))


# --- 4. session settings -------------------------------------------------------------------


def test_session_settings_are_the_three_timeouts_only():
    assert c.SESSION_SETTINGS == {"lock_timeout", "statement_timeout", "idle_in_transaction_session_timeout"}


# --- 5. outcomes, types and measures -------------------------------------------------------


def test_outcomes_match_the_adapter():
    assert {c.OUTCOME_COMMITTED, c.OUTCOME_DEFINITELY_ABORTED, c.OUTCOME_UNKNOWN} == \
        {o.value for o in TransactionOutcome}
    assert c.OUTCOME_BUSINESS_ROLLBACK not in {o.value for o in TransactionOutcome}


def test_tpcc_types_and_read_only_types():
    assert len(c.TPCC_TYPES) == 5 and len(set(c.TPCC_TYPES)) == 5
    assert c.TPCC_READ_ONLY_TYPES == {c.ORDER_STATUS, c.STOCK_LEVEL}
    assert c.MARKER_TYPE not in c.TPCC_TYPES


def test_every_measure_name_is_a_valid_catalog_identifier():
    for name in c.EXISTING_MEASURES | c.NEW_MEASURES:
        assert IDENT_RE.match(name), name
    assert not c.EXISTING_MEASURES & c.NEW_MEASURES


def test_existing_measures_are_really_used_by_the_catalog():
    text = "\n".join(p.read_text() for p in (CATALOG_ROOT / "NL").glob("*.yaml"))
    for name in c.EXISTING_MEASURES:
        assert name in text, f"{name} is not used by any scenario"


def test_per_group_and_per_type_names_work_in_accept_predicates():
    group = c.measure_by_group("tps_ratio", "non_colliding")
    typ = c.measure_by_type("p90_ms", c.NEW_ORDER)
    assert group == "tps_ratio__group_non_colliding" and typ == "p90_ms__type_new_order"
    assert IDENT_RE.match(group) and IDENT_RE.match(typ)
    pred = parse(f"{group} >= 0.8")
    assert pred.evaluate({group: 0.9}) and not pred.evaluate({group: 0.5})
    assert parse(f"{typ} <= 5000").evaluate({typ: 1200})


def test_the_dotted_form_from_the_first_proposal_would_not_work():
    # Why the contract does not use "tps_ratio_by_group.non_colliding": the catalog refuses it
    # and the predicate parser reads the dot as attribute access.
    assert not IDENT_RE.match("tps_ratio_by_group.non_colliding")
    with pytest.raises(PredicateError):
        parse("tps_ratio_by_group.non_colliding >= 0.8")


# --- 6. TPC-C tables -----------------------------------------------------------------------


def test_tpcc_tables_are_the_nine_standard_tables_in_their_own_schema():
    assert set(c.TPCC_TABLES) == {"warehouse", "district", "customer", "history", "orders",
                                  "new_order", "order_line", "item", "stock"}
    for qualified in c.TPCC_TABLES.values():
        schema, _, table = qualified.partition(".")
        assert schema == c.TPCC_SCHEMA and IDENT_RE.match(table)
    assert c.TPCC_STOCK == "resilience_tpcc.stock"
    assert c.TPCC_SCHEMA != "resilience"           # never mixed with the harness's own schema
