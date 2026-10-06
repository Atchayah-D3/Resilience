"""NL-I-01 -- Heap page checksum corruption (Framework §10.4, Arch §5, §10.1).

Three layers, each against fakes:
  the driver     the fault is precise and PROVEN before T0 (clean stop, byte changed,
                 pg_checksums reports exactly that block), or the run is aborted;
  the adapter    reads, checker output and log lines are interpreted correctly;
  the run        every acceptance rule can fail -- one test per false-pass path.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import asyncpg
import pytest

from catalog.schema import load_catalog
from resilience_tests.adapters.base import IntegrityResult
from resilience_tests.adapters.postgresql.adapter import PostgreSQLAdapter
from resilience_tests.control import orchestrator as orch
from resilience_tests.control.matrix import RunPlanItem
from resilience_tests.control.orchestrator import RunOptions, TestOrchestrator
from resilience_tests.execution.injectors import corruption as corruption_mod
from resilience_tests.execution.injectors.base import (
    DriverNotAvailable,
    FaultNotLanded,
    resolve,
    resolve_by_name,
)
from resilience_tests.execution.injectors.corruption import (
    OsSshCorruptionDriver,
    parse_checksum_failures,
    parse_cluster_state,
)
from resilience_tests.execution.injectors.process import OsSshProcessDriver
from resilience_tests.execution.remote import RemoteResult
from tests.test_measurement_and_safety import NODE, PROFILE
from tests.test_orchestrator import FakeFault, OutageAdapter, env, scenario, why  # noqa: F401  (fixture)

BLOCK, PATH, RELATION = 17, "base/16384/16400", "resilience.corruption_target"
TARGET = {"relation": RELATION, "relation_path": PATH, "filenode": 16400, "block": BLOCK, "pages": 35,
          "block_size": 8192, "byte_in_page": 8192 - 64, "rows": 2000}
OFFSET = BLOCK * 8192 + 8192 - 64
EXEC_STOP_FAST = ("{ path=/usr/lib/postgresql/17/bin/pg_ctl ; argv[]=/usr/lib/postgresql/17/bin/pg_ctl "
                  "stop -D /data -m fast ; ignore_errors=no }")
CHECKSUM_FAIL = (f'pg_checksums: error: checksum verification failed in file "{NODE.pgdata}/{PATH}", '
                 f"block {BLOCK}: calculated checksum 1A2B but block contains 3C4D\nBad checksums:  1\n")
INVALID_PAGE = f"invalid page in block {BLOCK} of relation {PATH}"


# --- the catalog -------------------------------------------------------------------------------


def test_nl_i_01_encodes_the_framework_row():
    sc = load_catalog().scenarios["NL-I-01"]
    assert (sc.category, sc.priority, sc.fault.type) == ("NL-I", "P0", "data_corruption")
    assert "page_checksums" in sc.requires
    assert {"corruption_detected_on_read == true", "detection_identifies_block == true",
            "amcheck_detects_target == true", "checksum_failure_reported == true",
            "corruption_outside_target == 0", "structural_integrity_errors == 0", "rpo_txn == 0"} == set(sc.accept)
    # the failures caused on purpose must never gate the run
    assert not [a for a in sc.accept if "corruption_count" in a]


def test_the_ssh_section_serves_both_drivers_by_fault_type():
    sc = load_catalog().scenarios
    assert isinstance(resolve(sc["NL-I-01"].fault, PROFILE), OsSshCorruptionDriver)
    assert type(resolve(sc["NL-C-01"].fault, PROFILE)) is OsSshProcessDriver
    assert isinstance(resolve_by_name("os_ssh", "os_ssh", PROFILE, "data_corruption"), OsSshCorruptionDriver)
    assert type(resolve_by_name("os_ssh", "os_ssh", PROFILE, "process_kill")) is OsSshProcessDriver


# --- the driver: precise, and proven before T0 -------------------------------------------------


class TargetHost:
    """Fake SSH host modelling the target: a service that stops and starts, one data file
    whose byte at OFFSET can be overwritten, and the offline tools that inspect it."""

    def __init__(self, calls: list[str], *, cluster_state="shut down", write_sticks=True,
                 checksums=CHECKSUM_FAIL, running=True):
        self.calls, self.cluster_state, self.write_sticks, self.checksums = calls, cluster_state, write_sticks, checksums
        self.state = {"running": running, "byte": "48"}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return None

    async def connect(self):
        pass

    async def close(self):
        pass

    async def run(self, command, *, timeout_s, check=True):
        self.calls.append(command)
        st = self.state
        if "systemctl stop" in command:
            st["running"] = False
            return RemoteResult(0, "", "")
        if "systemctl start" in command:
            st["running"] = True
            return RemoteResult(0, "", "")
        if "is-active" in command:
            return RemoteResult(0, "active" if st["running"] else "inactive", "")
        if "show -p ExecStop" in command:
            return RemoteResult(0, EXEC_STOP_FAST, "")
        if "show -p KillSignal" in command:
            return RemoteResult(0, "15", "")
        if "test -x" in command or "reset-failed" in command or "DROP TABLE" in command:
            return RemoteResult(0, "", "")
        if "stat -c %s" in command:
            return RemoteResult(0, "300000", "")
        if "pg_controldata" in command:
            return RemoteResult(0, f"Database cluster state:               {self.cluster_state}\n", "")
        if "od -An" in command:
            return RemoteResult(0, f" {st['byte']}\n", "")
        if "dd of=" in command:
            if self.write_sticks:
                st["byte"] = "b7"
            return RemoteResult(0, "", "")
        if "pg_checksums" in command:
            return RemoteResult(1 if "verification failed" in self.checksums else 0, "", self.checksums)
        raise AssertionError(f"unexpected command: {command}")


def corruption_driver(monkeypatch, **host_kw) -> tuple[OsSshCorruptionDriver, list[str], TargetHost]:
    calls: list[str] = []
    host = TargetHost(calls, **host_kw)
    monkeypatch.setattr(corruption_mod, "RemoteHost", lambda *a, **k: host)
    driver = OsSshCorruptionDriver(PROFILE, "data_corruption")
    driver._target = dict(TARGET)
    return driver, calls, host


def test_the_byte_is_flipped_with_the_database_cleanly_down_and_proven_by_pg_checksums(monkeypatch):
    driver, calls, host = corruption_driver(monkeypatch)
    detail = asyncio.run(driver.inject(NODE))
    assert (detail["original_byte"], detail["written_byte"], detail["cluster_state"]) == ("48", "b7", "shut down")
    assert detail["pg_checksums"]["failures"] == [{"file": f"{NODE.pgdata}/{PATH}", "block": BLOCK}]
    assert detail["integrity_exclusions"] == [RELATION] and detail["t0_mono_ns"] > 0
    order = [next(k for k in ("systemctl stop", "pg_controldata", "dd of=", "pg_checksums", "systemctl start")
                  if k in c) for c in calls
             if any(k in c for k in ("systemctl stop", "pg_controldata", "dd of=", "pg_checksums", "systemctl start"))]
    assert order == ["systemctl stop", "pg_controldata", "dd of=", "pg_checksums", "systemctl start"]
    (write,) = [c for c in calls if "dd of=" in c]
    assert f"seek={OFFSET}" in write and "\\267" in write          # 0x48 ^ 0xff = 0xb7 = octal 267
    assert host.state["running"]


def test_an_unclean_stop_aborts_before_any_byte_is_written(monkeypatch):
    """After a crash, recovery could restore the page from a full-page image and undo the
    fault -- the run would then 'pass' having tested nothing."""
    driver, calls, _ = corruption_driver(monkeypatch, cluster_state="in production")
    with pytest.raises(FaultNotLanded, match="not shut down cleanly"):
        asyncio.run(driver.inject(NODE))
    assert not [c for c in calls if "dd of=" in c]


def test_a_write_that_did_not_change_the_byte_aborts(monkeypatch):
    driver, _, _ = corruption_driver(monkeypatch, write_sticks=False)
    with pytest.raises(FaultNotLanded, match="after the write"):
        asyncio.run(driver.inject(NODE))


def test_a_page_whose_checksum_still_verifies_aborts(monkeypatch):
    """The byte changed, but if the page checksum does not notice, no read ever will."""
    driver, calls, _ = corruption_driver(monkeypatch, checksums="Bad checksums:  0\n")
    with pytest.raises(FaultNotLanded, match="exactly one checksum failure"):
        asyncio.run(driver.inject(NODE))
    assert not [c for c in calls if "systemctl start" in c]      # left down; the revert brings it back


def test_a_checksum_failure_at_another_block_aborts(monkeypatch):
    other = CHECKSUM_FAIL.replace(f"block {BLOCK}:", "block 3:")
    driver, _, _ = corruption_driver(monkeypatch, checksums=other)
    with pytest.raises(FaultNotLanded, match="exactly one checksum failure"):
        asyncio.run(driver.inject(NODE))


@pytest.mark.parametrize("override,match", [
    ({"relation": "public.orders"}, "not a harness-owned relation"),
    ({"relation_path": "base/16384/99999"}, "not the main file"),
    ({"relation_path": "global/1262"}, "not the main file"),
    ({"block": 0}, "not a populated page"),
    ({"byte_in_page": 8}, "outside the page's tuple area"),       # pd_checksum lives in the header
])
def test_a_target_that_is_not_strictly_the_harness_page_is_refused(override, match):
    with pytest.raises(DriverNotAvailable, match=match):
        OsSshCorruptionDriver(PROFILE, "data_corruption")._check_target(NODE, TARGET | override)


@pytest.mark.parametrize("running", [True, False])
def test_revert_brings_the_service_back_and_drops_only_the_harness_table(monkeypatch, running):
    driver, calls, host = corruption_driver(monkeypatch, running=running)
    out = asyncio.run(driver.revert(NODE, {"inject": {"relation": RELATION}}))
    assert host.state["running"] and out["state"] == "active"
    assert bool([c for c in calls if "systemctl start" in c]) is (not running)
    (drop,) = [c for c in calls if "DROP TABLE" in c]
    assert RELATION in drop


def test_revert_refuses_to_drop_anything_else(monkeypatch):
    driver, _, _ = corruption_driver(monkeypatch)
    with pytest.raises(RuntimeError, match="not a harness-owned relation"):
        asyncio.run(driver.revert(NODE, {"inject": {"relation": "public.orders"}}))


def test_offline_tool_output_parsing():
    assert parse_checksum_failures(CHECKSUM_FAIL) == [(f"{NODE.pgdata}/{PATH}", BLOCK)]
    assert parse_cluster_state("Database cluster state:               shut down\n") == "shut down"
    assert parse_cluster_state("garbage") is None


# --- the adapter: interpreting what the engine said -------------------------------------------


class _Conn:
    def __init__(self, result):
        self.result = result

    async def fetchval(self, sql):
        if isinstance(self.result, Exception):
            raise self.result
        return self.result

    async def close(self):
        pass


def test_a_failed_read_names_the_block_and_relation():
    adapter = PostgreSQLAdapter(NODE)

    async def connect(*a, **k):
        return _Conn(asyncpg.exceptions.DataCorruptedError(INVALID_PAGE))

    adapter._connect = connect
    read = asyncio.run(adapter.read_corruption_target())
    assert [(a["sqlstate"], a["block"], a["relation_path"]) for a in read["attempts"]] == [("XX001", BLOCK, PATH)] * 2


def test_a_read_that_returns_rows_is_recorded_as_rows_not_as_detection():
    adapter = PostgreSQLAdapter(NODE)

    async def connect(*a, **k):
        return _Conn(2000)

    adapter._connect = connect
    assert asyncio.run(adapter.read_corruption_target(attempts=1))["attempts"] == [{"error": None, "rows": 2000}]


@pytest.mark.parametrize("exit_status,output,detected", [
    (2, f'heap table "resilience.resilience.corruption_target", block {BLOCK}, offset 3:\n  ...', True),
    (1, f"pg_amcheck: error: error running query: ERROR:  {INVALID_PAGE}", True),
    (0, "", False),
    (1, "pg_amcheck: error: connection to server failed", None),     # could not tell -- never a pass
])
def test_amcheck_on_the_damaged_relation_is_read_strictly(monkeypatch, exit_status, output, detected):
    from resilience_tests.adapters.postgresql import adapter as pg

    class Host:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return None

        async def run(self, command, *, timeout_s, check=True):
            assert f"--relation={RELATION}" in command
            return RemoteResult(exit_status, output, "")

    monkeypatch.setattr(pg, "RemoteHost", lambda *a, **k: Host())
    assert asyncio.run(PostgreSQLAdapter(NODE).amcheck_relation(RELATION, 30))["detected"] is detected


def test_the_whole_database_check_excludes_only_the_damaged_relation(monkeypatch):
    from resilience_tests.adapters.postgresql import adapter as pg
    commands: list[str] = []

    class Host:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return None

        async def run(self, command, *, timeout_s, check=True):
            commands.append(command)
            return RemoteResult(0, "1" if "pg_extension" in command else "", "")

    monkeypatch.setattr(pg, "RemoteHost", lambda *a, **k: Host())
    adapter = PostgreSQLAdapter(NODE)

    async def stats():
        return {}

    adapter._checksum_stats = stats
    adapter.integrity_exclusions = (RELATION,)
    result = asyncio.run(adapter.integrity_check(30))
    (amcheck,) = [c for c in commands if "pg_amcheck" in c]
    assert f"--exclude-relation={RELATION}" in amcheck and "--heapallindexed" in amcheck
    assert result.detail["excluded_relations"] == [RELATION]


def test_log_lines_are_reduced_to_block_and_relation():
    lines = ["WARNING:  page verification failed, calculated checksum 1 but expected 2",
             f"ERROR:  {INVALID_PAGE}", "LOG:  checkpoint starting: time"]
    assert PostgreSQLAdapter(NODE).corruption_log_locations(lines) == [(BLOCK, PATH)]


# --- the run: every acceptance rule can fail ---------------------------------------------------


class CorruptionFault(FakeFault):
    fault_types = frozenset({"data_corruption"})
    lands_corruption = True

    async def preflight(self, node):
        return {"target": dict(TARGET)}

    async def inject(self, node):
        detail = {"action": "flip one byte", "relation": RELATION, "relation_path": PATH, "block": BLOCK,
                  "integrity_exclusions": [RELATION], "cluster_state": "shut down",
                  "pg_checksums": {"failures": [{"file": PATH, "block": BLOCK}]}}
        if not self.lands_corruption:
            raise FaultNotLanded("pg_checksums did not report exactly one checksum failure", detail)
        return detail | {"t0_mono_ns": time.monotonic_ns()}


class EchoTail:
    """Log tailer stand-in: the fake engine 'logs' through it."""

    stream: Any = None

    def __init__(self, node, stream):
        EchoTail.stream = stream

    def start(self):
        pass

    async def stop(self):
        pass


def engine_logs(*lines):
    for line in lines:
        EchoTail.stream.emit("log_tailer", "log_line", node="n", line=line)


@pytest.fixture
def corrupt(env, monkeypatch):
    """The fake engine behaves like a correct one: the damaged page fails to read with the
    data-corruption error, the server logs it, the checker sees it, the counter counts it."""
    monkeypatch.setattr(orch, "resolve", lambda fault, prof: CorruptionFault(prof, fault.type))
    monkeypatch.setattr(orch, "LogTailer", EchoTail)
    monkeypatch.setattr(orch, "CORRUPTION_LOG_WAIT_S", 0.5)
    monkeypatch.setattr(CorruptionFault, "lands_corruption", True)
    monkeypatch.setattr(OutageAdapter, "corruption_sqlstates", ("XX001",), raising=False)
    behaviour = {"read": {"sqlstate": "XX001", "block": BLOCK, "relation_path": PATH, "message": INVALID_PAGE},
                 "log": [f"ERROR:  {INVALID_PAGE}"], "amcheck": {"detected": True},
                 "integrity": IntegrityResult(structural_errors=0, checksum_failures=2)}

    async def read(self, attempts=2):
        engine_logs(*behaviour["log"])
        return {"attempts": [dict(behaviour["read"]) for _ in range(attempts)]}

    async def amcheck(self, relation, timeout_s):
        assert relation == RELATION
        return dict(behaviour["amcheck"])

    async def integrity(self, timeout_s):
        assert self.integrity_exclusions == (RELATION,)       # the damaged relation is left out
        return behaviour["integrity"]

    monkeypatch.setattr(OutageAdapter, "read_corruption_target", read, raising=False)
    monkeypatch.setattr(OutageAdapter, "amcheck_relation", amcheck, raising=False)
    monkeypatch.setattr(OutageAdapter, "corruption_log_locations",
                        lambda self, lines: PostgreSQLAdapter.corruption_log_locations(self, lines), raising=False)
    monkeypatch.setattr(OutageAdapter, "integrity_check", integrity)
    return behaviour


def run_nli01(profile):
    item = RunPlanItem(scenario=scenario("NL-I-01"), env_class=profile.env_class, role="standalone",
                       node=profile.nodes[0])
    return asyncio.run(TestOrchestrator(item, profile, RunOptions()).run())


def outcome(results, predicate):
    return next(r for r in results["verdict"]["results"] if r["predicate"] == predicate)["outcome"]


def test_a_correctly_detected_corruption_passes(env, corrupt):
    results = run_nli01(env)
    assert results["status"] == "passed", why(results)
    m = results["measured"]
    assert m["corruption_detected_on_read"] is True and m["detection_identifies_block"] is True
    assert m["detection_repeatable"] is True and m["corruption_outside_target"] == 0
    assert m["corruption_count"] == 2                                   # reported, and not gated
    assert any("excluded the deliberately corrupted relation" in d for d in results["disclosures"])


def test_false_pass_a_read_that_returns_rows_is_silent_corruption(env, corrupt):
    """ignore_checksum_failure / zero_damaged_pages: the read succeeds on a damaged page."""
    corrupt["read"] = {"error": None, "rows": 2000}
    results = run_nli01(env)
    assert results["status"] == "failed", why(results)
    assert outcome(results, "corruption_detected_on_read == true") == "fail"
    assert results["facts"]["rows_returned_from_damaged_relation"] == 2000


def test_false_pass_an_error_about_another_block_is_not_this_detection(env, corrupt):
    """Older, unrelated damage elsewhere must not satisfy the rule for THIS fault."""
    corrupt["read"] = {"sqlstate": "XX001", "block": 3, "relation_path": "base/16384/12345"}
    corrupt["log"] = [f"ERROR:  {INVALID_PAGE}"]
    results = run_nli01(env)
    assert results["status"] == "failed", why(results)
    assert outcome(results, "detection_identifies_block == true") == "fail"


def test_false_pass_a_connection_error_is_not_a_detection(env, corrupt):
    corrupt["read"] = {"sqlstate": "57P01", "message": "terminating connection due to administrator command"}
    results = run_nli01(env)
    assert outcome(results, "corruption_detected_on_read == true") == "fail"


def test_false_pass_damage_logged_for_another_relation_fails(env, corrupt):
    corrupt["log"] = [f"ERROR:  {INVALID_PAGE}", "ERROR:  invalid page in block 9 of relation base/16384/2619"]
    results = run_nli01(env)
    assert results["status"] == "failed", why(results)
    assert results["measured"]["corruption_outside_target"] == 1


def test_false_pass_no_server_log_means_not_measured_never_zero(env, corrupt):
    """Without the server's own report, 'nothing else was damaged' cannot be shown."""
    corrupt["log"] = []
    results = run_nli01(env)
    assert results["status"] == "failed", why(results)
    assert outcome(results, "corruption_outside_target == 0") == "not_measured"


def test_false_pass_a_failure_the_engine_did_not_count(env, corrupt):
    corrupt["integrity"] = IntegrityResult(structural_errors=0, checksum_failures=0)
    results = run_nli01(env)
    assert outcome(results, "checksum_failure_reported == true") == "fail"


@pytest.mark.parametrize("amcheck,expected", [({"detected": False}, "fail"),
                                              ({"detected": None, "note": "could not connect"}, "not_measured")])
def test_false_pass_the_checker_must_see_the_damage(env, corrupt, amcheck, expected):
    corrupt["amcheck"] = amcheck
    results = run_nli01(env)
    assert results["status"] == "failed", why(results)
    assert outcome(results, "amcheck_detects_target == true") == expected


def test_damage_anywhere_else_in_the_database_fails(env, corrupt):
    corrupt["integrity"] = IntegrityResult(structural_errors=1, checksum_failures=2)
    results = run_nli01(env)
    assert outcome(results, "structural_integrity_errors == 0") == "fail"


def test_a_fault_that_did_not_land_is_aborted_and_still_reverted(env, corrupt, monkeypatch):
    from resilience_tests.control.killswitch import ledger_for

    monkeypatch.setattr(CorruptionFault, "lands_corruption", False)
    results = run_nli01(env)
    assert results["status"] == "aborted", why(results)
    assert "data_corruption fault did not land" in results["error"] and results["verdict"] is None
    assert not ledger_for(env).outstanding()
    assert [r["fault_type"] for r in FakeFault.reverts] == ["data_corruption"]
