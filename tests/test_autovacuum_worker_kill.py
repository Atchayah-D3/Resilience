"""NL-M-03 -- autovacuum worker killed (Framework §10.7): the targeted kill and the
post-recovery vacuum verification. Spec: specs/001-nl-m-03-autovacuum-kill/.

Every measure has a test that makes it fail, for the reason under test (Constitution III).
"""

from __future__ import annotations

import asyncio
import datetime as dt
import os
import subprocess
import time
from pathlib import Path
from typing import Any

import pytest

from resilience_tests.adapters.postgresql import adapter as pg
from resilience_tests.adapters.postgresql.adapter import PostgreSQLAdapter
from resilience_tests.control.profile import load_profile
from resilience_tests.execution.injectors import process as process_mod
from resilience_tests.execution.injectors.base import DriverNotAvailable, FaultNotLanded
from resilience_tests.execution.injectors.process import OsSshProcessDriver, targeted_kill_command
from resilience_tests.execution.remote import RemoteResult

PROFILE = load_profile("e2-dedicated-vm")
NODE = PROFILE.nodes[0]
PROC_STAT_4242 = "4242 (postgres) S 100 4242 4242 0 -1 4194560 " + " ".join(["0"] * 12) + " 123456 0 0"
TARGET = {"pid": 4242, "title_marker": "autovacuum worker", "relation": "resilience.avac_target"}


# ---------------------------------------------------------------- the guarded kill, for real

@pytest.fixture
def victim(tmp_path: Path):
    """A real `sleep` process whose parent is this test, and a fake data directory whose
    postmaster.pid names this test -- so the guard's parent check is exercised for real."""
    pgdata = tmp_path / "pgdata"
    pgdata.mkdir()
    (pgdata / "postmaster.pid").write_text(f"{os.getpid()}\n")
    proc = subprocess.Popen(["sleep", "30"])
    yield proc, pgdata
    if proc.poll() is None:
        proc.kill()
    proc.wait()


def run_guard(pid: int, pgdata: Path, marker: str) -> list[str]:
    out = subprocess.run(["sh", "-c", targeted_kill_command(pid, str(pgdata), marker)],
                         capture_output=True, text=True, timeout=10)
    return out.stdout.strip().splitlines()


def test_guard_kills_a_child_of_our_postmaster_with_the_title(victim):
    proc, pgdata = victim
    lines = run_guard(proc.pid, pgdata, "sleep")
    assert lines[0] == process_mod.TARGET_KILLED
    assert lines[1] == str(os.getpid())                 # the postmaster it checked against
    assert proc.wait(timeout=5) == -9                   # SIGKILL, not a polite signal


def test_guard_refuses_a_process_of_another_postmaster(victim):
    """The lab host runs a second cluster: a PID from it must never be killed."""
    proc, pgdata = victim
    (pgdata / "postmaster.pid").write_text("1\n")
    lines = run_guard(proc.pid, pgdata, "sleep")
    assert lines[0].startswith(process_mod.TARGET_FOREIGN)
    assert proc.poll() is None                          # untouched


def test_guard_refuses_a_process_without_the_title(victim):
    """PID reuse: the PID now belongs to something that is not an autovacuum worker."""
    proc, pgdata = victim
    lines = run_guard(proc.pid, pgdata, "autovacuum worker")
    assert lines == [process_mod.TARGET_UNTITLED]
    assert proc.poll() is None


def test_guard_reports_a_process_already_gone(victim):
    proc, pgdata = victim
    proc.kill()
    proc.wait()
    assert run_guard(proc.pid, pgdata, "sleep") == [process_mod.TARGET_GONE]


# ---------------------------------------------------------------- the driver around the guard

class FakeHost:
    """Records commands; `guard_reply` is what the guarded kill prints."""

    calls: list[str] = []
    guard_reply = f"{process_mod.TARGET_KILLED}\n100\n{PROC_STAT_4242}"
    killed = False

    def __init__(self, *a, **k):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return None

    async def run(self, command, *, timeout_s, check=True):
        FakeHost.calls.append(command)
        if "kill -9" in command:
            FakeHost.killed = FakeHost.guard_reply.startswith(process_mod.TARGET_KILLED)
            return RemoteResult(0, FakeHost.guard_reply, "")
        if "/proc/4242/stat" in command:
            return RemoteResult(0, "" if FakeHost.killed else PROC_STAT_4242, "")
        if "postmaster.pid" in command:
            return RemoteResult(0, "100", "")
        return RemoteResult(0, "", "")


@pytest.fixture
def fake_host(monkeypatch):
    FakeHost.calls, FakeHost.killed = [], False
    FakeHost.guard_reply = f"{process_mod.TARGET_KILLED}\n100\n{PROC_STAT_4242}"
    monkeypatch.setattr(process_mod, "RemoteHost", FakeHost)
    return FakeHost


def targeted_driver() -> OsSshProcessDriver:
    driver = OsSshProcessDriver(PROFILE, "process_kill")
    driver.kill_target = dict(TARGET)
    return driver


def test_targeted_kill_sends_one_guarded_command_and_confirms_death(fake_host):
    detail = asyncio.run(targeted_driver().inject(NODE))
    assert not any("systemctl kill" in c for c in fake_host.calls)    # never the whole service
    kills = [c for c in fake_host.calls if "kill -9" in c]
    assert len(kills) == 1 and "T=4242" in kills[0]
    assert detail["landed"] is True and detail["target_pid"] == 4242
    assert detail["postmaster_pid"] == 100 and detail["postmaster_survived"] is True
    assert detail["death_confirmed_s"] is not None and detail["t0_mono_ns"] > 0


@pytest.mark.parametrize("reply", [process_mod.TARGET_GONE, f"{process_mod.TARGET_FOREIGN} 7 100",
                                   process_mod.TARGET_UNTITLED])
def test_a_refused_kill_changed_nothing_and_is_not_landed(fake_host, reply):
    fake_host.guard_reply = reply
    with pytest.raises(FaultNotLanded) as exc:
        asyncio.run(targeted_driver().inject(NODE))
    assert exc.value.detail["changed_nothing"] is True
    assert exc.value.detail["reason"] == reply


def test_without_a_target_the_kill_is_the_whole_service(monkeypatch):
    """NL-C-01..06 are unchanged: no kill_target, whole unit cgroup."""
    class Service(FakeHost):
        async def run(self, command, *, timeout_s, check=True):
            FakeHost.calls.append(command)
            if "systemctl kill" in command:
                FakeHost.killed = True
            if "postmaster.pid" in command:
                return RemoteResult(0, "4242", "")
            if "/proc/4242/stat" in command:
                return RemoteResult(0, "" if FakeHost.killed else PROC_STAT_4242, "")
            return RemoteResult(0, "", "")
    FakeHost.calls, FakeHost.killed = [], False
    monkeypatch.setattr(process_mod, "RemoteHost", Service)
    detail = asyncio.run(OsSshProcessDriver(PROFILE, "process_kill").inject(NODE))
    assert any("systemctl kill -s SIGKILL" in c for c in FakeHost.calls)
    assert not any("kill -9" in c for c in FakeHost.calls)
    assert "target_pid" not in detail


def test_restart_cadence_beyond_the_unit_limit_refuses_before_the_first_kill(monkeypatch):
    """Five crash-restarts may trip the unit's start limit (restart_after_crash off); refused
    in preflight, not discovered at cycle three."""
    class Unit(FakeHost):
        async def run(self, command, *, timeout_s, check=True):
            if "is-active" in command:
                return RemoteResult(0, "active", "")
            if "postmaster.pid" in command:
                return RemoteResult(0, "4242", "")
            if "-p Restart " in command or command.endswith("-p Restart --value 'shaktidb-resilience.service'"):
                return RemoteResult(0, "on-failure", "")
            if "StartLimitBurst" in command:
                return RemoteResult(0, "5", "")
            if "StartLimitIntervalUSec" in command:
                return RemoteResult(0, "10000000", "")
            if "show -p Restart" in command:
                return RemoteResult(0, "on-failure", "")
            return RemoteResult(0, "", "")
    monkeypatch.setattr(process_mod, "RemoteHost", Unit)
    driver = OsSshProcessDriver(PROFILE, "process_kill")
    driver.repeat_plan = (5, 1.0)          # a restart every second, 5 allowed per 10 s
    with pytest.raises(DriverNotAvailable, match="restarts per"):
        asyncio.run(driver.preflight(NODE))


# ---------------------------------------------------------------- adapter: finding the worker

class FakeConn:
    """Answers the adapter's NL-M-03 queries from scripted samples."""

    def __init__(self, *, naptime=0.05, workers=None, any_worker=None, relations=None):
        self.naptime = naptime
        self.workers = list(workers or [])          # one list of rows per AVAC_WORKERS_SQL call
        self.any_worker = list(any_worker or [])    # one count per AVAC_ANY_WORKER_SQL call
        self.relations = list(relations or [])      # one list of rows per AVAC_RELATIONS_SQL call
        self.now = dt.datetime(2026, 10, 6, 12, 0, tzinfo=dt.UTC)
        self.executed: list[str] = []

    async def fetchval(self, sql, *args):
        if "autovacuum_naptime" in sql:
            return self.naptime
        if "now()" in sql:
            return self.now
        if sql == pg.AVAC_ANY_WORKER_SQL:
            return self.any_worker.pop(0) if len(self.any_worker) > 1 else (self.any_worker or [0])[0]
        raise AssertionError(sql)

    async def fetch(self, sql, *args):
        if sql == pg.AVAC_WORKERS_SQL:
            return self.workers.pop(0) if len(self.workers) > 1 else (self.workers or [[]])[0]
        if sql == pg.AVAC_RELATIONS_SQL:
            return self.relations.pop(0) if len(self.relations) > 1 else (self.relations or [[]])[0]
        raise AssertionError(sql)

    async def execute(self, sql, *args):
        self.executed.append(sql)
        return "UPDATE 250000"

    async def close(self):
        pass


def adapter_with(conn: FakeConn) -> PostgreSQLAdapter:
    a = PostgreSQLAdapter(NODE)

    async def connect(*args, **kwargs):
        return conn
    a._connect = connect                                     # type: ignore[method-assign]
    return a


@pytest.fixture
def fast(monkeypatch):
    monkeypatch.setattr(pg, "AVAC_POLL_S", 0.001)
    monkeypatch.setattr(pg, "AVAC_WAIT_SLACK_S", 0.2)
    monkeypatch.setattr(pg, "AVAC_VERIFY_SAMPLE_S", 0.001)


def worker(pid, relation, query="autovacuum: VACUUM resilience.avac_target"):
    return {"pid": pid, "relation": relation, "phase": "scanning heap", "query": query}


def test_worker_found_only_on_the_harness_table(fast):
    conn = FakeConn(workers=[[], [worker(11, "resilience.churn")], [worker(12, pg.AVAC_TABLE)]])
    r = asyncio.run(adapter_with(conn).start_autovacuum_worker())
    assert r["in_progress"] is True
    assert r["kill_target"]["pid"] == 12 and r["kill_target"]["title_marker"] == "autovacuum worker"
    assert r["kill_target"]["wraparound"] is False
    assert conn.executed and conn.executed[0] == pg.AVAC_MAKE_DEAD    # work made before waiting


def test_anti_wraparound_worker_is_recorded_as_such(fast):
    q = "autovacuum: VACUUM resilience.avac_target (to prevent wraparound)"
    conn = FakeConn(workers=[[worker(13, pg.AVAC_TABLE, q)]])
    assert asyncio.run(adapter_with(conn).start_autovacuum_worker())["kill_target"]["wraparound"] is True


def test_no_worker_within_the_bound_is_not_in_progress(fast):
    r = asyncio.run(adapter_with(FakeConn(workers=[[]])).start_autovacuum_worker())
    assert r["in_progress"] is False and "within" in r["note"]


# ---------------------------------------------------------------- adapter: vacuum resumed

def rel(name, dead, threshold, vacuumed):
    return {"relation": name, "dead": dead, "threshold": threshold, "vacuumed_after": vacuumed}


def verify(conn: FakeConn) -> dict[str, Any]:
    return asyncio.run(adapter_with(conn).verify_autovacuum_resumed())


def test_every_eligible_relation_vacuumed_after_recovery_passes(fast):
    conn = FakeConn(any_worker=[1], relations=[
        [rel(pg.AVAC_TABLE, 250000, 1000, False), rel("resilience.churn", 500, 150, False)],
        [rel(pg.AVAC_TABLE, 0, 1000, True), rel("resilience.churn", 0, 150, True)]])
    r = verify(conn)
    assert r["relations_eligible"] == 2 and r["relations_left_unvacuumed"] == 0
    assert r["autovacuum_worker_respawned"] is True


def test_an_eligible_relation_never_vacuumed_is_counted_and_named(fast):
    conn = FakeConn(any_worker=[1], relations=[
        [rel(pg.AVAC_TABLE, 250000, 1000, True), rel("resilience.churn", 500, 150, False)]])
    r = verify(conn)
    assert r["relations_left_unvacuumed"] == 1 and r["unvacuumed"] == ["resilience.churn"]


def test_no_eligible_relation_is_none_never_zero(fast):
    """A vacuous '0 of 0' must not pass: no relation crossed its threshold."""
    r = verify(FakeConn(any_worker=[1], relations=[[rel("resilience.churn", 10, 150, False)]]))
    assert r["relations_eligible"] == 0 and r["relations_left_unvacuumed"] is None
    assert "no relation became eligible" in r["relations_left_unvacuumed_why"]


def test_a_vacuum_before_the_last_recovery_does_not_count(fast):
    """`vacuumed_after` is false for a last-autovacuum earlier than t_recovered."""
    r = verify(FakeConn(any_worker=[1], relations=[[rel(pg.AVAC_TABLE, 250000, 1000, False)]]))
    assert r["relations_left_unvacuumed"] == 1
    assert "$1" in pg.AVAC_RELATIONS_SQL and "last_autovacuum >= $1" in pg.AVAC_RELATIONS_SQL


def test_no_worker_after_recovery_is_reported(fast):
    r = verify(FakeConn(any_worker=[0], relations=[[rel(pg.AVAC_TABLE, 250000, 1000, True)]]))
    assert r["autovacuum_worker_respawned"] is False


def test_verification_waits_while_an_eligible_relation_is_still_being_vacuumed(fast, monkeypatch):
    """The bound is 3 x naptime + the vacuum still running: a busy worker extends it."""
    # samples 1-3: not yet vacuumed but a worker is on it -> keep waiting; sample 4: done
    conn = FakeConn(any_worker=[1], workers=[[worker(20, pg.AVAC_TABLE)]] * 3 + [[]],
                    relations=[[rel(pg.AVAC_TABLE, 250000, 1000, False)]] * 3
                    + [[rel(pg.AVAC_TABLE, 0, 1000, True)]])
    conn.naptime = 0.0                         # bound already reached at the first sample
    r = verify(conn)
    assert r["relations_left_unvacuumed"] == 0
