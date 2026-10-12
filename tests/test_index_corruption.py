"""NL-I-02 -- Index corruption (Framework §10.4, Arch §5, §10.1).

Built on the NL-I-01 data_corruption fault; only what NL-I-02 adds is tested here:
  the catalog    fault.relation_kind picks the index, and only for data_corruption;
  the driver     the page is PROVEN a btree leaf from its own bytes before any write, the
                 exclusions cover table and index, revert drops the table;
  the adapter    the index target, the forced and plan-checked index read, REINDEX;
  the run        every acceptance rule can fail -- one test per false-pass path.
"""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import asyncpg
import pytest
from pydantic import ValidationError

from catalog.schema import Fault, load_catalog
from resilience_tests.adapters.base import IntegrityResult
from resilience_tests.adapters.postgresql import adapter as pg
from resilience_tests.adapters.postgresql.adapter import PostgreSQLAdapter
from resilience_tests.control import orchestrator as orch
from resilience_tests.control.matrix import RunPlanItem
from resilience_tests.control.orchestrator import RunOptions, TestOrchestrator
from resilience_tests.execution.injectors import corruption as corruption_mod
from resilience_tests.execution.injectors.base import DriverNotAvailable, FaultNotLanded
from resilience_tests.execution.injectors.corruption import OsSshCorruptionDriver
from resilience_tests.execution.remote import RemoteResult
from tests.test_heap_corruption import EchoTail, TargetHost, engine_logs
from tests.test_measurement_and_safety import NODE, PROFILE
from tests.test_orchestrator import FakeFault, OutageAdapter, env, scenario, why  # noqa: F401  (fixture)

TABLE, INDEX = "resilience.corruption_target", "resilience.corruption_target_idx"
BLOCK, PATH, PAGES, BS = 19, "base/16384/16410", 57, 8192
ROWS = 20_000
LEAF_CHECK = {"byte_in_page": BS - 4, "mask": 1, "equals": 1, "meaning": "btree leaf page (BTP_LEAF set in btpo_flags)"}
TARGET = {"relation": INDEX, "relation_kind": "btree_index", "table": TABLE, "relation_path": PATH,
          "filenode": 16410, "block": BLOCK, "pages": PAGES, "block_size": BS, "byte_in_page": BS - 20,
          "page_check": LEAF_CHECK, "rows": ROWS, "integrity_exclusions": [TABLE, INDEX]}
LEAF_FLAG_OFFSET = BLOCK * BS + BS - 4
CHECKSUM_FAIL = (f'pg_checksums: error: checksum verification failed in file "{NODE.pgdata}/{PATH}", '
                 f"block {BLOCK}: calculated checksum 1A2B but block contains 3C4D\nBad checksums:  1\n")
INVALID_PAGE = f"invalid page in block {BLOCK} of relation {PATH}"


# --- the catalog -------------------------------------------------------------------------------


def test_nl_i_02_encodes_the_framework_row():
    sc = load_catalog().scenarios["NL-I-02"]
    assert (sc.category, sc.priority, sc.fault.type, sc.fault.relation_kind) == \
        ("NL-I", "P0", "data_corruption", "btree_index")
    assert {"page_checksums", "structural_integrity_check"} <= set(sc.requires)
    assert set(sc.accept) == {"wrong_results_returned == 0", "amcheck_detects_target == true",
                              "reindex_restores == true", "corruption_outside_target == 0",
                              "structural_integrity_errors == 0", "rpo_txn == 0"}
    assert not [a for a in sc.accept if "corruption_count" in a]     # failures caused on purpose
    assert load_catalog().scenarios["NL-I-01"].fault.relation_kind is None   # NL-I-01 unchanged: heap


def test_relation_kind_is_only_defined_for_data_corruption():
    with pytest.raises(ValidationError, match="relation_kind is only defined for data_corruption"):
        Fault(type="process_kill", driver="os_ssh", target="primary", relation_kind="btree_index")
    with pytest.raises(ValidationError):
        Fault(type="data_corruption", driver="os_ssh", target="primary", relation_kind="toast")


# --- the driver: the page is a leaf, proven before the write -----------------------------------


class IndexHost(TargetHost):
    """The fake target, with the btree page's flags byte readable at its own offset."""

    def __init__(self, calls, *, flags="01", **kw):
        super().__init__(calls, checksums=CHECKSUM_FAIL, **kw)
        self.flags = flags

    async def run(self, command, *, timeout_s, check=True):
        if "od -An" in command and f"-j {LEAF_FLAG_OFFSET} " in command:
            self.calls.append(command)
            return RemoteResult(0, f" {self.flags}\n", "")
        return await super().run(command, timeout_s=timeout_s, check=check)


def index_driver(monkeypatch, **host_kw):
    calls: list[str] = []
    host = IndexHost(calls, **host_kw)
    monkeypatch.setattr(corruption_mod, "RemoteHost", lambda *a, **k: host)
    driver = OsSshCorruptionDriver(PROFILE, "data_corruption")
    driver.corruption_kind = "btree_index"
    driver._target = dict(TARGET)
    return driver, calls, host


def test_the_leaf_page_is_proven_before_the_byte_is_flipped(monkeypatch):
    driver, calls, host = index_driver(monkeypatch)
    detail = asyncio.run(driver.inject(NODE))
    assert detail["page_check"]["holds"] is True and detail["page_check"]["observed"] == "01"
    assert detail["relation_kind"] == "btree_index" and detail["table"] == TABLE
    assert detail["integrity_exclusions"] == [TABLE, INDEX]
    assert "btree leaf" in detail["action"]
    flag_read = next(i for i, c in enumerate(calls) if f"-j {LEAF_FLAG_OFFSET} " in c)
    stop = next(i for i, c in enumerate(calls) if "systemctl stop" in c)
    write = next(i for i, c in enumerate(calls) if "dd of=" in c)
    assert stop < flag_read < write             # read with the database down, before the write
    assert host.state["byte"] == "b7" and host.state["running"] is True


@pytest.mark.parametrize("flags", ["02", "00", "zz"])   # internal/root page, no flags, unreadable
def test_a_page_that_is_not_a_leaf_aborts_before_any_byte_is_written(monkeypatch, flags):
    driver, calls, host = index_driver(monkeypatch, flags=flags)
    with pytest.raises(FaultNotLanded, match="is not a btree leaf page") as exc:
        asyncio.run(driver.inject(NODE))
    assert not [c for c in calls if "dd of=" in c] and host.state["byte"] == "48"
    assert exc.value.detail["page_check"]["holds"] is False


def test_a_target_whose_table_is_not_harness_owned_is_refused():
    driver = OsSshCorruptionDriver(PROFILE, "data_corruption")
    with pytest.raises(DriverNotAvailable, match="not a harness-owned relation"):
        driver._check_target(NODE, TARGET | {"table": "public.orders"})
    with pytest.raises(DriverNotAvailable, match="outside the page"):
        driver._check_target(NODE, TARGET | {"page_check": LEAF_CHECK | {"byte_in_page": BS}})


def test_preflight_asks_the_adapter_for_the_catalogs_relation_kind(monkeypatch):
    driver, calls, host = index_driver(monkeypatch)
    asked: list[str] = []

    class Adapter:
        async def prepare_corruption_target(self, kind="heap"):
            asked.append(kind)
            if kind != "btree_index":
                raise ValueError(f"no corruption target of kind {kind!r} for PostgreSQL")
            return dict(TARGET)

    driver.adapter = Adapter()
    detail = asyncio.run(driver.preflight(NODE))
    assert asked == ["btree_index"] and detail["target"]["relation"] == INDEX
    driver.corruption_kind = "toast"
    with pytest.raises(DriverNotAvailable, match="no corruption target of kind 'toast'"):
        asyncio.run(driver.preflight(NODE))


def test_revert_drops_the_table_which_takes_the_damaged_index_with_it(monkeypatch):
    driver, calls, host = index_driver(monkeypatch)
    out = asyncio.run(driver.revert(NODE, {"inject": {"relation": INDEX, "table": TABLE}}))
    assert out["action"] == f"DROP TABLE IF EXISTS {TABLE}"
    assert not [c for c in calls if f"DROP TABLE IF EXISTS {INDEX}" in c]


# --- the adapter -------------------------------------------------------------------------------


class Conn:
    """asyncpg stand-in: records statements, answers EXPLAIN and the read as told."""

    def __init__(self, log, *, plan="Index Scan", index="corruption_target_idx", rows=None, fail=None,
                 reindex_error=None):
        self.log, self.plan, self.index, self.rows, self.fail = log, plan, index, rows, fail
        self.reindex_error = reindex_error

    async def execute(self, sql, *args):
        self.log.append(sql)
        if sql.startswith("REINDEX") and self.reindex_error:
            raise self.reindex_error

    async def fetchrow(self, sql, *args):
        self.log.append(sql)
        return {"path": PATH, "filenode": 16410, "block_size": BS, "bytes": PAGES * BS}

    async def fetchval(self, sql, *args):
        self.log.append(sql)
        if sql.startswith("EXPLAIN"):
            node = {"Node Type": self.plan}
            if self.plan.startswith("Index"):
                node["Index Name"] = self.index
            return json.dumps([{"Plan": node}])
        return "base/16384/16999"

    async def fetch(self, sql, *args):
        self.log.append(sql)
        if self.fail is not None:
            raise self.fail
        return [(i,) for i in (self.rows if self.rows is not None else range(1, ROWS + 1))]

    async def close(self):
        pass


def adapter_with(log, settings=None, **conn_kw):
    adapter = PostgreSQLAdapter(NODE)

    async def connect(*a, server_settings=None, **k):
        if settings is not None:
            settings.append(server_settings)
        return Conn(log, **conn_kw)

    adapter._connect = connect
    return adapter


def test_the_index_target_is_a_leaf_page_of_a_harness_index_built_after_the_load():
    log: list[str] = []
    target = asyncio.run(adapter_with(log).prepare_corruption_target("btree_index"))
    assert target == TARGET
    insert = next(i for i, s in enumerate(log) if s.startswith("INSERT"))
    build = next(i for i, s in enumerate(log) if s.startswith("CREATE INDEX"))
    assert insert < build                        # bulk build: leaves laid out in key order
    assert "autovacuum_enabled = false" in next(s for s in log if s.startswith("CREATE TABLE"))
    with pytest.raises(ValueError, match="no corruption target of kind 'toast'"):
        asyncio.run(adapter_with(log).prepare_corruption_target("toast"))


def test_a_read_through_the_index_that_returns_the_rows_written():
    log: list[str] = []
    settings: list[Any] = []
    read = asyncio.run(adapter_with(log, settings).read_index_target(attempts=1))
    assert read["attempts"] == [{"used_index": True, "plan": ["Index Scan"], "error": None,
                                 "rows": ROWS, "matches_written": True}]
    assert settings == [{"enable_seqscan": "off", "enable_bitmapscan": "off", "enable_sort": "off"}]


@pytest.mark.parametrize("conn_kw,expected", [
    ({"rows": list(range(1, ROWS))}, {"used_index": True, "matches_written": False}),          # a row missing
    ({"rows": list(range(2, ROWS + 2))}, {"used_index": True, "matches_written": False}),      # wrong rows
    ({"plan": "Seq Scan"}, {"used_index": False, "matches_written": True}),                    # not via the index
    ({"index": "some_other_idx"}, {"used_index": False}),
])
def test_index_reads_record_wrong_rows_and_plans_that_avoid_the_index(conn_kw, expected):
    (attempt,) = asyncio.run(adapter_with([], **conn_kw).read_index_target(attempts=1))["attempts"]
    assert {k: attempt[k] for k in expected} == expected


def test_a_failed_index_read_names_the_block_and_file():
    adapter = adapter_with([], fail=asyncpg.exceptions.DataCorruptedError(INVALID_PAGE))
    read = asyncio.run(adapter.read_index_target())
    assert [(a["used_index"], a["sqlstate"], a["block"], a["relation_path"]) for a in read["attempts"]] == \
        [(True, "XX001", BLOCK, PATH)] * 2


def test_reindex_reports_success_and_failure():
    log: list[str] = []
    ok = asyncio.run(adapter_with(log).rebuild_corruption_target())
    assert ok["ok"] is True and ok["relation_path_after"] == "base/16384/16999"
    assert f"REINDEX INDEX {INDEX}" in log
    failed = asyncio.run(adapter_with([], reindex_error=asyncpg.exceptions.DataCorruptedError(INVALID_PAGE))
                         .rebuild_corruption_target())
    assert failed["ok"] is False and failed["sqlstate"] == "XX001"


@pytest.mark.parametrize("heapallindexed", [False, True])
def test_amcheck_on_one_relation_can_cross_check_the_heap(monkeypatch, heapallindexed):
    commands: list[str] = []

    class Host:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return None

        async def run(self, command, *, timeout_s, check=True):
            commands.append(command)
            return RemoteResult(0, "", "")

    monkeypatch.setattr(pg, "RemoteHost", lambda *a, **k: Host())
    out = asyncio.run(PostgreSQLAdapter(NODE).amcheck_relation(TABLE, 30, heapallindexed=heapallindexed))
    assert out["detected"] is False
    assert ("--heapallindexed" in commands[0]) is heapallindexed and f"--relation={TABLE}" in commands[0]


# --- the run: every acceptance rule can fail ---------------------------------------------------


class IndexCorruptionFault(FakeFault):
    fault_types = frozenset({"data_corruption"})
    kinds: list[str] = []

    async def preflight(self, node):
        IndexCorruptionFault.kinds.append(self.corruption_kind)
        return {"target": dict(TARGET)}

    async def inject(self, node):
        return {"action": "flip one byte in one btree leaf page", "relation": INDEX, "relation_kind": "btree_index",
                "table": TABLE, "relation_path": PATH, "block": BLOCK, "integrity_exclusions": [TABLE, INDEX],
                "cluster_state": "shut down", "page_check": LEAF_CHECK | {"holds": True},
                "pg_checksums": {"failures": [{"file": PATH, "block": BLOCK}]}, "t0_mono_ns": time.monotonic_ns()}


ERROR_READ = {"used_index": True, "error": "DataCorruptedError", "sqlstate": "XX001", "block": BLOCK,
              "relation_path": PATH, "message": INVALID_PAGE}
GOOD_READ = {"used_index": True, "error": None, "rows": ROWS, "matches_written": True}


@pytest.fixture
def damaged(env, monkeypatch):
    """The fake engine behaves like a correct one: reads through the damaged index fail with the
    data-corruption error, the server logs it, the checker sees it, REINDEX rebuilds it, and
    afterwards the index answers correctly and the cross-check is clean."""
    monkeypatch.setattr(orch, "resolve", lambda fault, prof: IndexCorruptionFault(prof, fault.type))
    monkeypatch.setattr(orch, "LogTailer", EchoTail)
    monkeypatch.setattr(orch, "CORRUPTION_LOG_WAIT_S", 0.5)
    monkeypatch.setattr(OutageAdapter, "corruption_sqlstates", ("XX001",), raising=False)
    IndexCorruptionFault.kinds = []
    b: dict[str, Any] = {"read": dict(ERROR_READ), "read_after": dict(GOOD_READ), "log": [f"ERROR:  {INVALID_PAGE}"],
                         "amcheck": {"detected": True}, "amcheck_after": {"detected": False},
                         "reindex": {"ok": True, "duration_s": 0.2, "error": None},
                         "integrity": IntegrityResult(structural_errors=0, checksum_failures=2), "calls": []}

    async def read_index(self, attempts=2):
        b["calls"].append("read")
        if b["calls"].count("read") == 1:
            engine_logs(*b["log"])
            return {"attempts": [dict(b["read"]) for _ in range(attempts)]}
        return {"attempts": [dict(b["read_after"])]}

    async def amcheck(self, relation, timeout_s, heapallindexed=False):
        b["calls"].append(f"amcheck {relation} heapallindexed={heapallindexed}")
        return dict(b["amcheck_after"] if relation == TABLE else b["amcheck"])

    async def rebuild(self):
        b["calls"].append("reindex")
        if isinstance(b["reindex"], Exception):
            raise b["reindex"]
        return dict(b["reindex"])

    async def integrity(self, timeout_s):
        assert self.integrity_exclusions == (TABLE, INDEX)
        return b["integrity"]

    async def heap_read(self, attempts=2):
        raise AssertionError("an index scenario must read through the index")

    monkeypatch.setattr(OutageAdapter, "read_index_target", read_index, raising=False)
    monkeypatch.setattr(OutageAdapter, "read_corruption_target", heap_read, raising=False)
    monkeypatch.setattr(OutageAdapter, "rebuild_corruption_target", rebuild, raising=False)
    monkeypatch.setattr(OutageAdapter, "amcheck_relation", amcheck, raising=False)
    monkeypatch.setattr(OutageAdapter, "corruption_log_locations",
                        lambda self, lines: PostgreSQLAdapter.corruption_log_locations(self, lines), raising=False)
    monkeypatch.setattr(OutageAdapter, "integrity_check", integrity)
    return b


def run_nli02(profile):
    item = RunPlanItem(scenario=scenario("NL-I-02"), env_class=profile.env_class, role="standalone",
                       node=profile.nodes[0])
    return asyncio.run(TestOrchestrator(item, profile, RunOptions()).run())


def outcome(results, predicate):
    return next(r for r in results["verdict"]["results"] if r["predicate"] == predicate)["outcome"]


def test_a_detected_and_restored_index_passes(env, damaged):
    results = run_nli02(env)
    assert results["status"] == "passed", why(results)
    m = results["measured"]
    assert (m["wrong_results_returned"], m["amcheck_detects_target"], m["reindex_restores"]) == (0, True, True)
    assert m["corruption_detected_on_read"] is True and m["detection_identifies_block"] is True
    assert m["reindex_duration_s"] == 0.2 and m["corruption_outside_target"] == 0
    assert IndexCorruptionFault.kinds == ["btree_index"]          # the catalog field reached the driver
    # the damage is observed (read, checker) before the repair destroys it; the after-check
    # covers the table with its index and cross-checks every row
    assert damaged["calls"] == ["read", f"amcheck {INDEX} heapallindexed=False", "reindex", "read",
                                f"amcheck {TABLE} heapallindexed=True"]
    assert any(TABLE in d and INDEX in d for d in results["disclosures"])


def test_an_index_that_answers_correctly_despite_the_damage_is_not_a_wrong_result(env, damaged):
    damaged["read"] = dict(GOOD_READ)
    results = run_nli02(env)
    assert outcome(results, "wrong_results_returned == 0") == "pass", why(results)
    assert results["measured"]["corruption_detected_on_read"] is False      # reported, not gated


def test_false_pass_wrong_rows_through_the_damaged_index_fail(env, damaged):
    damaged["read"] = {"used_index": True, "error": None, "rows": ROWS - 1, "matches_written": False}
    results = run_nli02(env)
    assert results["status"] == "failed", why(results)
    assert results["measured"]["wrong_results_returned"] == 2
    assert outcome(results, "wrong_results_returned == 0") == "fail"


@pytest.mark.parametrize("read", [
    {"used_index": False, "error": None, "rows": ROWS, "matches_written": True, "plan": ["Seq Scan"]},
    {"used_index": None, "error": None, "rows": ROWS, "matches_written": True},
    {"used_index": True, "error": "AdminShutdownError", "sqlstate": "57P01", "message": "terminating connection"},
])
def test_false_pass_a_read_that_cannot_speak_for_the_index_is_not_measured(env, damaged, read):
    damaged["read"] = read
    results = run_nli02(env)
    assert results["status"] == "failed", why(results)
    assert outcome(results, "wrong_results_returned == 0") == "not_measured"


@pytest.mark.parametrize("amcheck,expected", [({"detected": False}, "fail"),
                                              ({"detected": None, "note": "could not connect"}, "not_measured")])
def test_false_pass_the_checker_must_see_the_damaged_index(env, damaged, amcheck, expected):
    damaged["amcheck"] = amcheck
    results = run_nli02(env)
    assert results["status"] == "failed", why(results)
    assert outcome(results, "amcheck_detects_target == true") == expected


@pytest.mark.parametrize("change,expected", [
    ({"reindex": {"ok": False, "duration_s": None, "error": "XX001"}}, "fail"),
    ({"reindex": RuntimeError("connection lost")}, "not_measured"),
    ({"read_after": {"used_index": True, "error": None, "rows": 5, "matches_written": False}}, "fail"),
    ({"read_after": dict(ERROR_READ)}, "fail"),                                          # still damaged
    ({"read_after": {"used_index": False, "error": None, "rows": ROWS, "matches_written": True}}, "not_measured"),
    ({"read_after": {"used_index": None, "error": "ConnectionDoesNotExistError"}}, "not_measured"),
    ({"amcheck_after": {"detected": True}}, "fail"),
    ({"amcheck_after": {"detected": None, "note": "timeout"}}, "not_measured"),
])
def test_false_pass_reindex_must_be_shown_to_restore_the_index(env, damaged, change, expected):
    damaged.update(change)
    results = run_nli02(env)
    assert results["status"] == "failed", why(results)
    assert outcome(results, "reindex_restores == true") == expected


def test_false_pass_damage_logged_for_another_relation_fails(env, damaged):
    damaged["log"] = [f"ERROR:  {INVALID_PAGE}", "ERROR:  invalid page in block 9 of relation base/16384/2619"]
    results = run_nli02(env)
    assert results["status"] == "failed", why(results)
    assert results["measured"]["corruption_outside_target"] == 1


def test_damage_anywhere_else_in_the_database_fails(env, damaged):
    damaged["integrity"] = IntegrityResult(structural_errors=1, checksum_failures=2)
    results = run_nli02(env)
    assert outcome(results, "structural_integrity_errors == 0") == "fail"
