"""The lock held on the target host (control/target_lock.py): one run at a time on a target,
whichever harness host it comes from. The shell the lock sends over SSH runs for real, locally,
against a temporary lock directory."""

from __future__ import annotations

import asyncio
import json
import os
import shlex
import subprocess
import time
from pathlib import Path

import pytest

from resilience_tests.control import orchestrator as orch
from resilience_tests.control import target_lock
from resilience_tests.control.matrix import RunPlanItem
from resilience_tests.control.orchestrator import RunOptions, TestOrchestrator
from resilience_tests.control.profile import TargetLockSpec
from resilience_tests.control.target_lock import TargetBusy, TargetLock
from resilience_tests.execution.remote import RemoteResult
from tests.test_orchestrator import scenario, why
from tests.test_orchestrator import env  # noqa: F401  (env is a fixture)
from tests.test_safety_ledger_profile import NODE


class LocalShell:
    def __call__(self, *a, **k):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return None

    async def run(self, command: str, *, timeout_s: float, check: bool = True) -> RemoteResult:
        argv = shlex.split(command)
        done = subprocess.run(["sh", "-c", argv[argv.index("-c") + 1]], capture_output=True, text=True, timeout=30)
        return RemoteResult(done.returncode, done.stdout, done.stderr)


@pytest.fixture
def lock_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(target_lock, "RemoteHost", LocalShell())
    return tmp_path / "resilience-harness.lock"


def lock(path: Path, run_id: str, stale_after_s: float = 900.0) -> TargetLock:
    spec = TargetLockSpec(path=str(path), stale_after_s=stale_after_s, heartbeat_s=1.0)
    return TargetLock(NODE, spec, run_id=run_id, scenario="NL-N-I03a", profile="e2-dedicated-vm")


def run(coro):
    return asyncio.run(coro)


def test_the_first_run_takes_the_lock_and_records_who_it_is(lock_dir):
    out = run(lock(lock_dir, "run-A").acquire())
    assert out["acquired"] is True and lock_dir.is_dir()
    owner = json.loads((lock_dir / "owner").read_text())
    assert owner["run_id"] == "run-A" and owner["scenario"] == "NL-N-I03a" and owner["harness_host"]


def test_a_second_run_from_any_harness_is_refused_naming_the_holder(lock_dir):
    run(lock(lock_dir, "run-A").acquire())
    with pytest.raises(TargetBusy, match="in use by run run-A \\(NL-N-I03a\\)"):
        run(lock(lock_dir, "run-B").acquire())
    assert json.loads((lock_dir / "owner").read_text())["run_id"] == "run-A"


def test_a_dead_runs_lock_is_taken_over_once_its_heartbeat_is_stale(lock_dir):
    run(lock(lock_dir, "run-A").acquire())
    old = time.time() - 1000
    os.utime(lock_dir / "heartbeat", (old, old))           # run-A stopped beating long ago
    out = run(lock(lock_dir, "run-B", stale_after_s=900).acquire())
    assert out["took_over_from"]["run_id"] == "run-A" and out["stale_heartbeat_s"] >= 900
    assert json.loads((lock_dir / "owner").read_text())["run_id"] == "run-B"


def test_a_live_runs_lock_is_not_taken_over(lock_dir):
    a = lock(lock_dir, "run-A")
    run(a.acquire())
    assert run(a.heartbeat()) is True
    with pytest.raises(TargetBusy):
        run(lock(lock_dir, "run-B", stale_after_s=900).acquire())


def test_release_only_removes_its_own_lock(lock_dir):
    a, b = lock(lock_dir, "run-A"), lock(lock_dir, "run-B")
    run(a.acquire())
    assert run(b.release())["released"] is False and lock_dir.is_dir()     # never someone else's
    assert run(a.release())["released"] is True and not lock_dir.exists()
    assert run(b.acquire())["acquired"] is True                               # free again


def test_a_lock_taken_over_is_reported_lost_by_the_old_owner(lock_dir):
    a = lock(lock_dir, "run-A")
    run(a.acquire())
    old = time.time() - 1000
    os.utime(lock_dir / "heartbeat", (old, old))
    run(lock(lock_dir, "run-B").acquire())
    assert run(a.heartbeat()) is False


def test_the_profile_refuses_a_stale_limit_shorter_than_three_heartbeats():
    with pytest.raises(ValueError, match="three missed heartbeats"):
        TargetLockSpec(stale_after_s=60, heartbeat_s=30)


def test_the_default_path_is_per_database_port():
    assert target_lock.lock_path(NODE, TargetLockSpec()) == f"/var/tmp/resilience-harness-{NODE.db.port}.lock"


# --- the orchestrator holds it around the whole run ----------------------------------------

class RecordingLock:
    events: list[str] = []
    busy = False

    def __init__(self, node, spec, *, run_id, scenario, profile):
        self.run_id, self.lost = run_id, None

    async def acquire(self):
        if RecordingLock.busy:
            raise TargetBusy("in use by run other-run (NL-C-01) from another harness")
        RecordingLock.events.append("acquire")
        return {"acquired": True}

    def start_heartbeat(self):
        RecordingLock.events.append("heartbeat")

    async def release(self):
        RecordingLock.events.append("release")
        return {"released": True}


@pytest.fixture
def recording(env, monkeypatch):
    RecordingLock.events, RecordingLock.busy = [], False
    monkeypatch.setattr(orch, "TargetLock", RecordingLock)
    spec = TargetLockSpec()
    return env.model_copy(update={"safety": env.safety.model_copy(update={"target_lock": spec})})


def run_item(profile, sid="NL-C-01"):
    item = RunPlanItem(scenario=scenario(sid), env_class=profile.env_class, role="standalone", node=profile.nodes[0])
    return asyncio.run(TestOrchestrator(item, profile, RunOptions()).run())


def test_the_run_holds_the_target_lock_from_start_until_after_cleanup(recording):
    results = run_item(recording)
    assert results["status"] == "passed", why(results)
    assert RecordingLock.events == ["acquire", "heartbeat", "release"]
    assert results["facts"]["target_lock"]["released"] is True


def test_a_target_another_harness_holds_is_refused_before_anything_is_written(recording, tmp_path):
    RecordingLock.busy = True
    with pytest.raises(TargetBusy, match="another harness"):
        run_item(recording)
    RecordingLock.busy = False
    # the driver host's own lock was let go: the next run starts normally
    assert run_item(recording)["status"] == "passed"
