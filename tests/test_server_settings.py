"""A scenario's server settings (catalog server_settings): planned read-only, written to the
server's configuration file as one marked block, applied by a restart, verified, restored --
and journalled so the kill switch can restore them after a crash.

The shell the adapter sends over SSH is run for real, locally, against a temporary
postgresql.conf: a fake SSH host executes it with `sh -c`, and a fake server re-reads the file
on "restart". So the file the restore leaves behind is checked byte for byte.
"""

from __future__ import annotations

import asyncio
import hashlib
import re
import shlex
import subprocess
from pathlib import Path
from typing import Any

import pytest
import yaml
from pydantic import ValidationError

from catalog.schema import CATALOG_ROOT, Scenario, load_catalog
from resilience_tests.adapters.base import ServerSettingsRefused
from resilience_tests.adapters.postgresql import configuration
from resilience_tests.adapters.postgresql.configuration import ConfigurationMixin, normalized
from resilience_tests.execution.injectors.server_settings import SERVER_SETTINGS, OsSshServerSettingsDriver
from resilience_tests.execution.remote import RemoteResult
from tests.test_safety_ledger_profile import NODE, PROFILE

ORIGINAL = "# the operator's file\nport = 5433\nfsync = on\n# END resilience harness target\n"


# --- the catalog field --------------------------------------------------------------------

def raw(sid: str = "NL-N-I03a") -> dict[str, Any]:
    return yaml.safe_load((CATALOG_ROOT / "NL" / f"{sid}.yaml").read_text())


def test_scenario_3_declares_the_documents_server_and_session_settings():
    for sid in ("NL-N-I03a", "NL-N-I03b"):
        sc = load_catalog().scenarios[sid]
        assert sc.server_settings == {"log_lock_waits": "on", "deadlock_timeout": "1s"}
        assert sc.workload.session == {"lock_timeout": "0", "statement_timeout": "0", "transaction_timeout": "0"}


def test_yaml_booleans_become_the_servers_on_and_off():
    d = raw()
    d["server_settings"] = {"log_lock_waits": True, "log_checkpoints": False, "deadlock_timeout": 2}
    assert Scenario.model_validate(d).server_settings == {
        "log_lock_waits": "on", "log_checkpoints": "off", "deadlock_timeout": "2"}


@pytest.mark.parametrize("settings, message", [
    ({"Log_Lock_Waits": "on"}, "not setting names"),
    ({"log lock": "on"}, "not setting names"),
    ({"deadlock_timeout": "1s\nfsync = off"}, "one non-empty line"),
    ({"deadlock_timeout": ""}, "one non-empty line"),
])
def test_a_setting_that_could_rewrite_the_file_is_refused(settings, message):
    d = raw()
    d["server_settings"] = settings
    with pytest.raises(ValidationError, match=message):
        Scenario.model_validate(d)


# --- comparing values as the server does ----------------------------------------------------

@pytest.mark.parametrize("a, b, vartype, unit", [
    ("1s", "1000", "integer", "ms"),           # deadlock_timeout: the server reports ms
    ("1000ms", "1s", "integer", "ms"),
    ("5min", "300", "integer", "s"),           # checkpoint_timeout: reported in s
    ("128MB", "16384", "integer", "8kB"),      # shared_buffers: reported in 8 kB pages
    ("on", "true", "bool", None),
    ("OFF", "0", "bool", None),
    ("Replica", "replica", "enum", None),
])
def test_equal_values_compare_equal_whatever_their_unit(a, b, vartype, unit):
    assert normalized(a, vartype, unit) == normalized(b, vartype, unit)


@pytest.mark.parametrize("value, vartype, unit", [
    ("1MB", "integer", "ms"), ("5s", "integer", "8kB"), ("maybe", "bool", None), ("3s", "integer", None)])
def test_a_value_the_setting_cannot_hold_is_an_error(value, vartype, unit):
    with pytest.raises(ValueError):
        normalized(value, vartype, unit)


# --- a fake server and SSH host that run the real shell ------------------------------------

class FakeServer:
    """What the server reports (pg_settings) -- recomputed from the file on every restart."""

    DEFAULTS = {
        "log_lock_waits": {"setting": "off", "unit": None, "vartype": "bool", "context": "superuser"},
        "deadlock_timeout": {"setting": "1000", "unit": "ms", "vartype": "integer", "context": "superuser"},
        "block_size": {"setting": "8192", "unit": None, "vartype": "integer", "context": "internal"},
    }

    def __init__(self, conf: Path) -> None:
        self.conf, self.restarts = conf, 0
        self.auto_conf: dict[str, str] = {}      # what ALTER SYSTEM would have written
        self.ignore_file = False                 # a server that does not read the file (broken)
        self.load()

    def load(self) -> None:
        self.rows: dict[str, dict[str, Any]] = {n: {**r, "name": n, "source": "default", "sourcefile": None}
                                                for n, r in self.DEFAULTS.items()}
        if not self.ignore_file:
            for line in self.conf.read_text().splitlines():
                m = re.match(r"^\s*([a-z_]+)\s*=\s*'(.*)'\s*$", line)
                if m and m[1] in self.rows:
                    self._set(m[1], m[2].replace("''", "'"), "configuration file", str(self.conf))
        for name, value in self.auto_conf.items():
            self._set(name, value, "configuration file", str(self.conf.parent / "postgresql.auto.conf"))
        self.rows["config_file"] = {"name": "config_file", "setting": str(self.conf), "unit": None,
                                    "vartype": "string", "context": "postmaster", "source": "override",
                                    "sourcefile": None}

    def _set(self, name: str, value: str, source: str, sourcefile: str) -> None:
        row = self.rows[name]
        canon = normalized(value, row["vartype"], row["unit"])
        row["setting"] = canon[:-2] if canon.endswith("ms") else canon
        row["source"], row["sourcefile"] = source, sourcefile

    def restart(self) -> None:
        self.restarts += 1
        self.load()


class LocalShell:
    """RemoteHost stand-in: runs the shell the adapter sends, locally; a restart restarts the
    fake server."""

    def __init__(self, server: FakeServer, calls: list[str]) -> None:
        self.server, self.calls = server, calls

    def __call__(self, *a, **k):
        return self

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return None

    async def run(self, command: str, *, timeout_s: float, check: bool = True) -> RemoteResult:
        self.calls.append(command)
        if "systemctl" in command and "restart" in command:
            self.server.restart()
            return RemoteResult(0, "", "")
        argv = shlex.split(command)
        script = argv[argv.index("-c") + 1]
        done = subprocess.run(["sh", "-c", script], capture_output=True, text=True, timeout=30)
        if check and done.returncode != 0:
            raise RuntimeError(f"exit {done.returncode}: {done.stderr}")
        return RemoteResult(done.returncode, done.stdout, done.stderr)


class ConfigAdapter(ConfigurationMixin):
    def __init__(self, node: Any, server: FakeServer) -> None:
        self.node, self.server = node, server

    async def _settings_rows(self, names: list[str]) -> dict[str, dict[str, Any]]:
        return {n: {**self.server.rows[n], "shown": self._shown(self.server.rows[n])}
                for n in names if n in self.server.rows}

    @staticmethod
    def _shown(row: dict[str, Any]) -> str:
        return f"{row['setting']}{row['unit'] or ''}" if row["unit"] == "ms" else row["setting"]

    async def _restart_and_wait(self, host: Any) -> float:
        await host.run("sudo -n -- systemctl restart test.service", timeout_s=1)
        return 0.0


@pytest.fixture
def setup(tmp_path, monkeypatch):
    conf = tmp_path / "postgresql.conf"
    conf.write_text(ORIGINAL)
    server = FakeServer(conf)
    calls: list[str] = []
    monkeypatch.setattr(configuration, "RemoteHost", LocalShell(server, calls))
    node = NODE.model_copy(update={"pgdata": str(tmp_path), "config_file": str(conf)})
    return ConfigAdapter(node, server), conf, server, calls


def run(coro):
    return asyncio.run(coro)


def sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


# --- plan ----------------------------------------------------------------------------------------

def test_the_plan_separates_what_changes_from_what_is_already_in_effect(setup):
    adapter, conf, server, _ = setup
    plan = run(adapter.plan_server_settings({"log_lock_waits": "on", "deadlock_timeout": "1s"}))
    assert set(plan["changes"]) == {"log_lock_waits"} and plan["changes"]["log_lock_waits"]["from"] == "off"
    assert plan["unchanged"] == {"deadlock_timeout": "1000ms"}       # 1s is already the server's value
    assert plan["config_file"] == str(conf)


@pytest.mark.parametrize("wanted, message", [
    ({"no_such_setting": "1"}, "no such setting"),
    ({"block_size": "4096"}, "read-only"),
    ({"deadlock_timeout": "1MB"}, "not a time unit"),
])
def test_what_cannot_be_applied_is_refused_before_anything_changes(setup, wanted, message):
    adapter, conf, server, calls = setup
    with pytest.raises(ServerSettingsRefused, match=message):
        run(adapter.plan_server_settings(wanted))
    assert conf.read_text() == ORIGINAL and server.restarts == 0 and not calls


def test_a_setting_overridden_by_postgresql_auto_conf_is_refused(setup):
    """ALTER SYSTEM's file overrides postgresql.conf: a change there would never take effect,
    and the harness does not edit the operator's ALTER SYSTEM file."""
    adapter, conf, server, _ = setup
    server.auto_conf = {"log_lock_waits": "off"}
    server.load()
    with pytest.raises(ServerSettingsRefused, match="postgresql.auto.conf, which overrides"):
        run(adapter.plan_server_settings({"log_lock_waits": "on"}))


def test_a_value_auto_conf_already_sets_as_wanted_is_simply_in_effect(setup):
    adapter, _, server, _ = setup
    server.auto_conf = {"log_lock_waits": "on"}      # the lab target today
    server.load()
    plan = run(adapter.plan_server_settings({"log_lock_waits": "on"}))
    assert plan["changes"] == {} and plan["unchanged"] == {"log_lock_waits": "on"}


def test_a_profile_naming_another_configuration_file_is_refused(setup):
    adapter, conf, server, _ = setup
    adapter.node = adapter.node.model_copy(update={"config_file": "/etc/postgresql/17/main/postgresql.conf"})
    with pytest.raises(ServerSettingsRefused, match="correct the profile's config_file"):
        run(adapter.plan_server_settings({"log_lock_waits": "on"}))


# --- apply and restore, on a real file --------------------------------------------------

def apply(adapter, wanted, tag="run-1"):
    plan = run(adapter.plan_server_settings(wanted))
    return plan, run(adapter.apply_server_settings(plan, tag))


def test_apply_appends_one_block_restarts_and_verifies(setup):
    adapter, conf, server, _ = setup
    before = sha(conf)
    _, detail = apply(adapter, {"log_lock_waits": "on", "deadlock_timeout": "1500ms"})
    text = conf.read_text()
    assert text.startswith(ORIGINAL)                                     # the operator's lines untouched
    assert "# >>> resilience-harness server_settings run-1" in text
    assert "log_lock_waits = 'on'" in text and "deadlock_timeout = '1500ms'" in text
    assert server.restarts == 1 and detail["verified"] is True
    assert server.rows["log_lock_waits"]["setting"] == "on" and server.rows["deadlock_timeout"]["setting"] == "1500"
    assert detail["original_sha256"] == before and Path(detail["backup"]).read_text() == ORIGINAL
    assert detail["after"] == {"deadlock_timeout": "1500", "log_lock_waits": "on"}


def test_restore_gives_back_the_exact_file_and_values(setup):
    adapter, conf, server, _ = setup
    before = sha(conf)
    _, detail = apply(adapter, {"log_lock_waits": "on"})
    out = run(adapter.restore_server_settings(detail))
    assert sha(conf) == before and conf.read_text() == ORIGINAL
    assert out["block"] == "REMOVED" and out["file_vs_backup"] == "SAME" and out["restored"] is True
    assert not Path(detail["backup"]).exists()                           # removed once proven identical
    assert server.rows["log_lock_waits"]["setting"] == "off" and server.restarts == 2


def test_restore_is_idempotent_and_does_not_restart_twice(setup):
    adapter, conf, server, _ = setup
    _, detail = apply(adapter, {"log_lock_waits": "on"})
    run(adapter.restore_server_settings(detail))
    again = run(adapter.restore_server_settings(detail))
    assert again["block"] == "ABSENT" and "restart_s" not in again and server.restarts == 2
    assert conf.read_text() == ORIGINAL


def test_restore_keeps_an_edit_someone_else_made_during_the_run(setup):
    adapter, conf, server, _ = setup
    _, detail = apply(adapter, {"log_lock_waits": "on"})
    with conf.open("a") as f:
        f.write("work_mem = '8MB'   # the operator, during the run\n")
    out = run(adapter.restore_server_settings(detail))
    text = conf.read_text()
    assert "resilience-harness" not in text and "work_mem = '8MB'" in text and text.startswith(ORIGINAL)
    assert out["file_vs_backup"] == "DIFFERENT" and Path(out["backup_kept"]).exists()


def test_a_change_that_does_not_take_effect_is_removed_and_fails(setup):
    adapter, conf, server, _ = setup
    server.ignore_file = True              # the server does not read the change
    plan = run(adapter.plan_server_settings({"log_lock_waits": "on"}))
    with pytest.raises(RuntimeError, match="not in effect after the restart"):
        run(adapter.apply_server_settings(plan, "run-2"))
    assert conf.read_text() == ORIGINAL    # removed again


def test_restore_that_cannot_bring_a_value_back_fails_so_the_ledger_stays_open(setup):
    adapter, conf, server, _ = setup
    _, detail = apply(adapter, {"log_lock_waits": "on"})
    server.auto_conf = {"log_lock_waits": "on"}     # someone ran ALTER SYSTEM during the run
    with pytest.raises(RuntimeError, match="not back to their recorded values"):
        run(adapter.restore_server_settings(detail))


# --- the driver: what the ledger needs, and the kill switch path ------------------------

def test_the_kill_switch_restores_from_the_ledger_detail_alone(setup):
    """After a crash there is no scenario and no plan: only the ledger entry's detail."""
    adapter, conf, server, _ = setup
    driver = OsSshServerSettingsDriver(PROFILE)
    driver.use_adapter(adapter)
    plan = run(driver.plan(adapter.node, {"log_lock_waits": "on"}))
    ledger_detail = driver.prepare(plan, "run-3")
    run(driver.inject(adapter.node))
    assert "resilience-harness" in conf.read_text()
    fresh = OsSshServerSettingsDriver(PROFILE, SERVER_SETTINGS)    # what resolve_by_name builds
    fresh.use_adapter(ConfigAdapter(adapter.node, server))
    out = run(fresh.revert(adapter.node, ledger_detail))
    assert out["restored"] is True and conf.read_text() == ORIGINAL


def test_a_crash_before_anything_was_written_leaves_nothing_to_restore(setup):
    adapter, conf, server, _ = setup
    driver = OsSshServerSettingsDriver(PROFILE)
    driver.use_adapter(adapter)
    ledger_detail = driver.prepare(run(driver.plan(adapter.node, {"log_lock_waits": "on"})), "run-4")
    out = run(driver.revert(adapter.node, ledger_detail))      # intent journalled, apply never ran
    assert out["block"] == "ABSENT" and server.restarts == 0 and conf.read_text() == ORIGINAL


def test_the_driver_is_harness_setup_and_registered_for_the_kill_switch():
    from resilience_tests.execution.injectors.base import resolve_by_name
    driver = resolve_by_name("os_ssh", "os_ssh", PROFILE, SERVER_SETTINGS)
    assert isinstance(driver, OsSshServerSettingsDriver) and driver.harness_setup is True


def test_a_file_without_a_final_newline_is_restored_exactly(setup):
    adapter, conf, server, _ = setup
    conf.write_text(ORIGINAL.rstrip("\n"))          # the last line has no newline
    before = sha(conf)
    _, detail = apply(adapter, {"log_lock_waits": "on"})
    assert "fsync = on\n# END resilience harness target\n# >>> resilience-harness" in conf.read_text()
    run(adapter.restore_server_settings(detail))
    assert sha(conf) == before
