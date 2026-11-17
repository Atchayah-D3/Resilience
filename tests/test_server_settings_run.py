"""A scenario's server settings through the real orchestrator (NL-N-I03a, shortened): planned and
journalled in init, applied before the baseline, expected by settings_unchanged, restored by
cleanup -- against the lock pile-up fake engine of test_lock_pileup_run."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from resilience_tests.adapters.base import ServerSettingsRefused, register_adapter
from resilience_tests.control import killswitch
from resilience_tests.control.killswitch import ledger_for
from resilience_tests.execution.injectors.lock_pileup import OsSshLockPileupDriver
from resilience_tests.execution.injectors.server_settings import SERVER_SETTINGS, OsSshServerSettingsDriver
from tests.test_lock_pileup_run import LOCK_TIMEOUT_S, READ_HELD_S, PileupEngine, run, short
from tests.test_lock_pileup_run import lock_env  # noqa: F401  (fixture)
from tests.test_orchestrator import env  # noqa: F401  (fixture used by lock_env)
from tests.test_orchestrator import scenario, why


class Server:
    values: dict[str, str] = {}
    calls: list[Any] = []
    refuse: str | None = None


@register_adapter
class SettingsEngine(PileupEngine):
    engine = "orch-lock-settings-fake"

    async def observe_lock_settings(self) -> dict[str, str]:
        return {"lock_timeout": "0", **Server.values}

    async def plan_server_settings(self, wanted):
        if Server.refuse:
            raise ServerSettingsRefused(Server.refuse)
        changes = {n: {"from": Server.values.get(n), "from_setting": Server.values.get(n), "to": v}
                   for n, v in wanted.items() if Server.values.get(n) != v}
        unchanged = {n: v for n, v in wanted.items() if Server.values.get(n) == v}
        return {"config_file": "/fake/postgresql.conf", "service": "fake", "changes": changes, "unchanged": unchanged}

    async def apply_server_settings(self, plan, tag):
        Server.calls.append(("apply", tag))
        for n, c in plan["changes"].items():
            Server.values[n] = c["to"]
        return {"tag": tag, "config_file": plan["config_file"], "changes": plan["changes"],
                "after": {n: Server.values[n] for n in plan["changes"]}, "verified": True}

    async def restore_server_settings(self, detail):
        Server.calls.append(("restore", detail["tag"]))
        for n, c in (detail.get("changes") or {}).items():
            Server.values[n] = c["from_setting"]
        return {"restored": True}


@pytest.fixture
def settings_env(lock_env, monkeypatch):
    Server.values = {"log_lock_waits": "off", "deadlock_timeout": "1s"}
    Server.calls, Server.refuse = [], None

    def resolve_by_name(section, driver, prof, fault_type=""):
        cls = OsSshServerSettingsDriver if fault_type == SERVER_SETTINGS else OsSshLockPileupDriver
        return cls(prof, fault_type)
    monkeypatch.setattr(killswitch, "resolve_by_name", resolve_by_name)
    return lock_env.model_copy(update={"database": lock_env.database.model_copy(update={"engine": SettingsEngine.engine})})


def run_3a(profile):
    """3a, shortened, under the catalog's own server settings (short() leaves them out)."""
    sc = short("NL-N-I03a", ddl_after_s=READ_HELD_S, ddl_lock_timeout_s=LOCK_TIMEOUT_S)
    return run(profile, sc.model_copy(update={"server_settings": scenario("NL-N-I03a").server_settings}))


def entries(profile, run_id):
    return [e for e in ledger_for(profile).entries() if e.run_id == run_id and e.fault_type == SERVER_SETTINGS]


def test_settings_are_applied_before_the_baseline_and_restored_by_cleanup(settings_env):
    results = run_3a(settings_env)            # the catalog's: log_lock_waits on, deadlock_timeout 1s
    assert results["status"] == "passed", why(results)
    assert Server.calls == [("apply", results["run_id"]), ("restore", results["run_id"])]
    assert Server.values == {"log_lock_waits": "off", "deadlock_timeout": "1s"}       # as found
    states = [e.state for e in entries(settings_env, results["run_id"])]
    assert states == ["intent", "applied", "reverted"]                               # journalled first
    plan = results["facts"]["server_settings"]["plan"]
    assert set(plan["changes"]) == {"log_lock_waits"} and plan["unchanged"] == {"deadlock_timeout": "1s"}
    disclosures = " ".join(results["disclosures"])
    assert "log_lock_waits off -> on" in disclosures and "already in effect, left unchanged: deadlock_timeout=1s" in disclosures
    # the harness's own change is expected, not reported as a change by someone else
    assert results["measured"]["settings_unchanged"] is True
    assert not results["facts"].get("cleanup_outstanding")


def test_nothing_is_changed_or_journalled_when_every_value_is_already_in_effect(settings_env):
    Server.values = {"log_lock_waits": "on", "deadlock_timeout": "1s"}
    results = run_3a(settings_env)
    assert results["status"] == "passed", why(results)
    assert Server.calls == [] and entries(settings_env, results["run_id"]) == []


def test_settings_that_cannot_be_applied_abort_the_run_before_anything_changes(settings_env):
    Server.refuse = "log_lock_waits: currently off from postgresql.auto.conf, which overrides postgresql.conf"
    results = run_3a(settings_env)
    assert results["status"] == "aborted" and "server_settings cannot be applied" in results["error"]
    assert "postgresql.auto.conf" in results["error"]
    assert Server.calls == [] and entries(settings_env, results["run_id"]) == []


def test_a_setting_changed_by_someone_else_still_fails_settings_unchanged(settings_env, monkeypatch):
    """Only the harness's own change is expected: anything else is still a change."""
    calls = {"n": 0}
    real = SettingsEngine.observe_lock_settings

    async def drifting(self):
        calls["n"] += 1
        out = await real(self)
        return {**out, "lock_timeout": "5s"} if calls["n"] > 1 else out    # changed during the run
    monkeypatch.setattr(SettingsEngine, "observe_lock_settings", drifting)
    results = run_3a(settings_env)
    assert results["measured"]["settings_unchanged"] is False
    assert results["facts"]["settings_changed"] == {"lock_timeout": {"before": "0", "after": "5s"}}
    assert ("restore", results["run_id"]) in Server.calls
