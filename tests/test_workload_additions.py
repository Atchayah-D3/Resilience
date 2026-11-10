"""TPC-C build step 2: the small additions to shared files (docs/tpcc-build-plan.md).

The custom workload must behave exactly as before; the additions only add optional scenario
fields, extra sample fields, per-connection session settings, the profile hooks, and journal
reading that also accepts per-process files. Every test here fails for its own reason.
"""

from __future__ import annotations

import asyncio
import json
import tempfile
from pathlib import Path

import pytest
from pydantic import ValidationError

from catalog.schema import Workload, load_catalog
from resilience_tests.control.matrix import not_built_reason
from resilience_tests.execution.workload import contract as c
from resilience_tests.execution.workload.driver import WorkloadDriver, group_of_workers
from resilience_tests.execution.workload.hooks import NoProfileHooks, hooks_for
from resilience_tests.execution.workload.markers import MarkerJournals, diff_from_journals
from resilience_tests.observability.event_stream import EventStream
from tests.test_adapter_seam import NODE, WORKLOAD, FakeAdapter, FakeSession

BASE = {"profile": "oltp_write_heavy", "concurrency": 8, "transaction_markers": True, "rate_tps": 200}


def wl(**changes) -> Workload:
    return Workload(**{**BASE, **changes})


# --- the scenario file: optional fields ----------------------------------------------------

def test_every_existing_scenario_still_loads_unchanged():
    catalog = load_catalog()
    assert not catalog.errors
    for sc in catalog.scenarios.values():
        assert sc.workload.session == {} and sc.workload.groups == () and sc.workload.tpcc is None


def test_the_new_fields_are_accepted():
    w = wl(session={"lock_timeout": "5s", "statement_timeout": "30000"},
           groups=[{"name": "colliding", "clerks": 2}, {"name": "non_colliding", "clerks": 6}])
    assert w.session["lock_timeout"] == "5s" and [g.name for g in w.groups] == ["colliding", "non_colliding"]


def test_tpcc_profile_with_its_options_is_accepted_by_the_catalog():
    w = wl(profile="tpcc", groups=[{"name": "colliding", "clerks": 2}, {"name": "rest", "clerks": 6}],
           tpcc={"warehouses": 40, "home_warehouse": {"colliding": 7}})
    assert w.tpcc is not None and w.tpcc.home_warehouse == {"colliding": 7}


@pytest.mark.parametrize("changes, message", [
    ({"session": {"work_mem": "64MB"}}, "not allowed"),                        # only the three timeouts
    ({"session": {"lock_timeout": "five seconds"}}, "number with an optional unit"),
    ({"groups": [{"name": "a", "clerks": 3}]}, "add up to 3"),                 # must cover every clerk
    ({"groups": [{"name": "all", "clerks": 8}]}, "reserved"),
    ({"profile": "tpcc"}, "must be given for profile 'tpcc'"),
    ({"tpcc": {"warehouses": 10}}, "only allowed with profile 'tpcc'"),
    ({"profile": "tpcc", "tpcc": {"warehouses": 10, "home_warehouse": {"ghost": 1}}}, "not declared"),
    ({"profile": "tpcc", "groups": [{"name": "a", "clerks": 8}],
      "tpcc": {"warehouses": 10, "home_warehouse": {"a": 11}}}, "between 1 and 10"),
])
def test_invalid_new_fields_are_refused_with_the_reason(changes, message):
    with pytest.raises(ValidationError, match=message):
        wl(**changes)


# --- tpcc is refused as not built, before anything is applied -----------------------------

def test_a_registered_profile_that_is_not_built_is_refused_by_the_run_plan_and_init(monkeypatch):
    # tpcc is built since step 6; a not-built stand-in shows the refusal still works
    from resilience_tests.execution.workload.profiles import base as profiles_base
    from resilience_tests.execution.workload.tpcc.profile import TpccProfile

    class NotBuilt(TpccProfile):
        built = False
    monkeypatch.setitem(profiles_base._PROFILES, "tpcc", NotBuilt)
    sc = load_catalog().scenarios["NL-C-01"]
    tpcc_sc = sc.model_copy(update={"workload": wl(profile="tpcc", tpcc={"warehouses": 40})})
    reason = not_built_reason(tpcc_sc)
    assert reason is not None and "workload profile 'tpcc' is not built yet" in reason
    assert not_built_reason(sc) is None                      # existing scenarios are unaffected


def test_hooks_for_the_custom_workload_do_nothing_and_tpcc_has_its_own():
    from resilience_tests.execution.workload.tpcc.profile import TpccProfile
    hooks = hooks_for(WORKLOAD)
    assert isinstance(hooks, NoProfileHooks)
    assert asyncio.run(hooks.prepare(None)) == {}
    assert asyncio.run(hooks.consistency_checks(None, Path("."))) == {}
    assert isinstance(hooks_for(wl(profile="tpcc", tpcc={"warehouses": 40})), TpccProfile)


# --- the driver: groups, extra sample fields, session settings ----------------------------

def test_groups_take_workers_in_declaration_order():
    w = wl(groups=[{"name": "colliding", "clerks": 3}, {"name": "rest", "clerks": 5}])
    assert group_of_workers(w) == ["colliding"] * 3 + ["rest"] * 5
    assert group_of_workers(wl()) == ["all"] * 8


class RecordingAdapter(FakeAdapter):
    """Records the keyword arguments every session was opened with."""

    def __init__(self, node) -> None:
        super().__init__(node)
        self.session_kwargs: list[dict] = []

    async def session(self, endpoint=None, timeout_s: float = 5.0, **kwargs):
        self.session_kwargs.append(kwargs)
        return FakeSession(self.store)


def _drive(adapter, workload, seconds=0.9, monkeypatch=None):
    from resilience_tests.execution.workload import driver as driver_mod

    if monkeypatch is not None:
        monkeypatch.setattr(driver_mod, "SAMPLE_INTERVAL_S", 0.2)

    async def go():
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp)
            stream, journals = EventStream(d / "events.jsonl"), MarkerJournals(d)
            driver = WorkloadDriver(adapter, workload, journals, stream)
            await driver.start()
            await asyncio.sleep(seconds)
            await driver.stop()
            journals.close()
            stream.close()
            return [e.data for e in stream.events() if e.source == "workload" and e.kind == "sample"]

    return asyncio.run(go())


def test_samples_carry_one_type_and_one_group_for_the_custom_workload(monkeypatch):
    samples = _drive(FakeAdapter(NODE), WORKLOAD, monkeypatch=monkeypatch)
    assert samples
    for s in samples:
        assert set(s[c.BY_TYPE]) == {c.MARKER_TYPE} and set(s[c.BY_GROUP]) == {c.DEFAULT_GROUP}
        assert s[c.BY_TYPE][c.MARKER_TYPE]["commits"] == s["commits"]       # one type: the totals
        assert s[c.BY_GROUP][c.DEFAULT_GROUP]["commits"] == s["commits"]
        assert set(s[c.BY_TYPE][c.MARKER_TYPE]) == set(c.TypeStats.__annotations__)
        assert set(s[c.BY_GROUP][c.DEFAULT_GROUP]) == set(c.GroupStats.__annotations__)
    assert sum(s["commits"] for s in samples) > 0
    json.dumps(samples)                                                     # still plain JSON


def test_declared_groups_are_reported_separately_and_add_up_to_the_totals(monkeypatch):
    workload = WORKLOAD.model_copy(update={"groups": tuple(
        Workload(**{**BASE, "concurrency": WORKLOAD.concurrency,
                    "groups": [{"name": "colliding", "clerks": 4},
                               {"name": "non_colliding", "clerks": WORKLOAD.concurrency - 4}]}).groups)})
    samples = _drive(FakeAdapter(NODE), workload, monkeypatch=monkeypatch)
    for s in samples:
        assert set(s[c.BY_GROUP]) == {"colliding", "non_colliding"}
        assert sum(g["commits"] for g in s[c.BY_GROUP].values()) == s["commits"]
        assert sum(g["errors"] for g in s[c.BY_GROUP].values()) == s["errors"]
    assert sum(s[c.BY_GROUP]["colliding"]["commits"] for s in samples) > 0


def test_session_settings_are_passed_to_every_connection_only_when_declared(monkeypatch):
    plain = RecordingAdapter(NODE)
    _drive(plain, WORKLOAD, seconds=0.3, monkeypatch=monkeypatch)
    assert plain.session_kwargs and all(k == {} for k in plain.session_kwargs)   # as before: no settings

    with_settings = RecordingAdapter(NODE)
    workload = WORKLOAD.model_copy(update={"session": {"lock_timeout": "5s"}})
    _drive(with_settings, workload, seconds=0.3, monkeypatch=monkeypatch)
    assert len(with_settings.session_kwargs) >= WORKLOAD.concurrency
    assert all(k == {"settings": {"lock_timeout": "5s"}} for k in with_settings.session_kwargs)


# --- journals: per-process files are read too; a missing acked journal still fails ---------

def _write(path: Path, records: list[dict]) -> None:
    path.write_text("".join(json.dumps(r) + "\n" for r in records))


def test_single_journals_give_exactly_the_old_result():
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        _write(d / "marker.jrnl", [{"seq": 1, "uuid": "a"}, {"seq": 2, "uuid": "b"}, {"seq": 3, "uuid": "c"}])
        _write(d / "acked.jrnl", [{"uuid": "a"}, {"uuid": "b"}])
        diff, torn = diff_from_journals(d, ["a"])
        assert diff.lost == {"b"} and diff.indeterminate == {"c"} and torn == 0


def test_per_process_journals_are_all_read():
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        _write(d / "marker.0.jrnl", [{"seq": 1, "uuid": "a"}])
        _write(d / "acked.0.jrnl", [{"uuid": "a"}])
        _write(d / "marker.1.jrnl", [{"seq": 1, "uuid": "b"}])
        _write(d / "acked.1.jrnl", [{"uuid": "b"}])
        diff, _ = diff_from_journals(d, ["a"])
        assert diff.written == 2 and diff.acked == 2 and diff.lost == {"b"}


@pytest.mark.parametrize("files", [
    {"marker.jrnl": [{"seq": 1, "uuid": "a"}]},                                   # acked.jrnl missing
    {"marker.0.jrnl": [{"seq": 1, "uuid": "a"}], "marker.1.jrnl": [{"seq": 1, "uuid": "b"}],
     "acked.0.jrnl": [{"uuid": "a"}]},                                             # acked.1.jrnl missing
    {},                                                                            # no journal at all
])
def test_a_missing_journal_is_an_error_never_a_perfect_rpo(files):
    with tempfile.TemporaryDirectory() as tmp:
        d = Path(tmp)
        for name, records in files.items():
            _write(d / name, records)
        with pytest.raises(FileNotFoundError):
            diff_from_journals(d, ["a"])
