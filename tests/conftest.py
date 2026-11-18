"""Shared fixtures for the harness's unit tests."""

from __future__ import annotations

from typing import Any

import pytest


class InMemoryTargetLock:
    """Stands in for the lock on the target host (control/target_lock.py): a unit test never
    reaches a real target over SSH. tests/test_target_lock.py tests the real one."""

    def __init__(self, node: Any, spec: Any, *, run_id: str, scenario: str, profile: str) -> None:
        self.run_id, self.lost = run_id, None

    async def acquire(self) -> dict[str, Any]:
        return {"path": "in-memory", "acquired": True}

    def start_heartbeat(self) -> None:
        pass

    async def release(self) -> dict[str, Any]:
        return {"released": True}


@pytest.fixture(autouse=True)
def _no_target_host_lock(monkeypatch):
    from resilience_tests.control import orchestrator
    monkeypatch.setattr(orchestrator, "TargetLock", InMemoryTargetLock)


@pytest.fixture(autouse=True)
def _no_target_host_workload_lock(monkeypatch):
    """The common workload's lock on the target host (service/state.py) is read over SSH; a unit
    test never reaches a target, so it is "free" unless a test says otherwise.
    tests/test_workload_service.py tests the guards themselves."""
    from resilience_tests.execution.workload.service import state

    async def free(node: Any, path: str, stale_after_s: float) -> None:
        return None

    monkeypatch.setattr(state, "remote_lock_holder", free)
