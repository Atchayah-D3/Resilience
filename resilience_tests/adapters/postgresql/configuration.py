"""PostgreSQL side of a scenario's server settings (catalog server_settings, ConfigurationSupport).

The change is ONE block appended to the node's postgresql.conf (the env profile names the file),
applied by restarting the service and verified from pg_settings. Restoring removes exactly that
block, restarts, and verifies the recorded values are back. ALTER SYSTEM is never used: it would
write postgresql.auto.conf, whose previous content the harness could not tell apart afterwards.

    # >>> resilience-harness server_settings <tag> -- written by the harness, removed after the run
    log_lock_waits = 'on'
    # <<< resilience-harness server_settings <tag>

A setting the file cannot decide is refused before anything changes: one set in
postgresql.auto.conf or on the command line (they override postgresql.conf), or for the harness's
database or role (pg_settings would then show that value, not the server's).
"""

from __future__ import annotations

import asyncio
import base64
import re
import shlex
import time
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from resilience_tests.adapters.base import ConfigurationSupport, ServerSettingsRefused
from resilience_tests.execution.remote import RemoteHost, as_user, privileged

if TYPE_CHECKING:
    # what this mixin uses from the adapter's core class (host.py)
    from resilience_tests.adapters.postgresql.host import PostgreSQLCore
else:
    PostgreSQLCore = object

q = shlex.quote

SETTINGS_SQL = """
SELECT name, setting, unit, vartype, context, source, sourcefile, current_setting(name) AS shown
  FROM pg_settings WHERE name = ANY($1::text[])
"""
BLOCK_BEGIN = "# >>> resilience-harness server_settings {tag} -- written by the harness, removed after the run"
BLOCK_END = "# <<< resilience-harness server_settings {tag}"
TAG_RE = re.compile(r"^[A-Za-z0-9-]{1,64}$")
# Sources the configuration file cannot override, or that hide the server's value from this session.
OVERRIDING_SOURCES = {"command line", "environment variable", "override", "database", "user",
                      "database user", "client", "session"}
SSH_TIMEOUT_S = 30.0
RESTART_TIMEOUT_S = 180.0     # the unit's own stop and start; a fast shutdown takes seconds
READY_TIMEOUT_S = 180.0       # until the server accepts connections again
READY_POLL_S = 1.0

TIME_MS = {"us": 0.001, "ms": 1.0, "s": 1000.0, "min": 60_000.0, "h": 3_600_000.0, "d": 86_400_000.0}
MEMORY_B = {"B": 1, "kB": 1024, "MB": 1024 ** 2, "GB": 1024 ** 3, "TB": 1024 ** 4}
TRUE_WORDS = {"on", "true", "yes", "1", "t", "y"}
FALSE_WORDS = {"off", "false", "no", "0", "f", "n"}
NUMBER_RE = re.compile(r"^\s*(-?\d+(?:\.\d+)?)\s*([A-Za-z]*)\s*$")
UNIT_RE = re.compile(r"^(\d*)([A-Za-z]+)$")


def normalized(value: str, vartype: str, unit: str | None) -> str:
    """One canonical text per value, so '1s', '1000ms' and the server's '1000' (unit ms) compare
    equal. Raises ValueError for a value the setting's type cannot hold."""
    text = str(value).strip()
    if vartype == "bool":
        word = text.lower()
        if word in TRUE_WORDS:
            return "on"
        if word in FALSE_WORDS:
            return "off"
        raise ValueError(f"{text!r} is not a boolean")
    if vartype in ("integer", "real"):
        m = NUMBER_RE.match(text)
        if not m:
            raise ValueError(f"{text!r} is not a number")
        number, given = float(m[1]), m[2]
        um = UNIT_RE.match(unit or "")
        if unit in TIME_MS:                                   # a time setting: compared in ms
            if given and given not in TIME_MS:
                raise ValueError(f"{text!r}: {given!r} is not a time unit")
            return f"{number * (TIME_MS[given] if given else TIME_MS[unit]):.6g}ms"
        if um and um[2] in MEMORY_B:                          # a size setting: compared in bytes
            if given and given not in MEMORY_B:
                raise ValueError(f"{text!r}: {given!r} is not a size unit")
            base = int(um[1] or 1) * MEMORY_B[um[2]]
            return f"{number * (MEMORY_B[given] if given else base):.6g}B"
        if given:
            raise ValueError(f"{text!r}: this setting takes no unit")
        return f"{number:.6g}"
    if vartype == "enum":
        return text.lower()
    return text


def _conf_line(name: str, value: str) -> str:
    return f"{name} = '" + value.replace("'", "''") + "'"


class ConfigurationMixin(ConfigurationSupport, PostgreSQLCore):
    """Server-settings methods of PostgreSQLAdapter. Mixed into the class in adapter.py; uses the
    core's `node` and `_connect` (host.py)."""

    def _config_file(self) -> str:
        return self.node.config_file or f"{self.node.pgdata}/postgresql.conf"

    async def _settings_rows(self, names: list[str]) -> dict[str, dict[str, Any]]:
        conn = await self._connect(timeout_s=10.0)
        try:
            rows = await conn.fetch(SETTINGS_SQL, names)
        finally:
            await conn.close()
        return {r["name"]: dict(r) for r in rows}

    async def plan_server_settings(self, wanted: Mapping[str, str]) -> dict[str, Any]:
        """Read only (see ConfigurationSupport.plan_server_settings)."""
        config_file = self._config_file()
        rows = await self._settings_rows(sorted(wanted) + ["config_file"])
        running_file = (rows.get("config_file") or {}).get("setting")
        if running_file and running_file != config_file:
            raise ServerSettingsRefused(f"the server reads {running_file}, but the environment profile names "
                                        f"{config_file}; correct the profile's config_file")
        changes: dict[str, dict[str, Any]] = {}
        unchanged: dict[str, str] = {}
        problems: list[str] = []
        for name in sorted(wanted):
            row = rows.get(name)
            if row is None:
                problems.append(f"{name}: no such setting on this server")
                continue
            if row["context"] == "internal":
                problems.append(f"{name}: a read-only setting; no configuration file can change it")
                continue
            try:
                want = normalized(wanted[name], row["vartype"], row["unit"])
                have = normalized(row["setting"], row["vartype"], row["unit"])
            except ValueError as exc:
                problems.append(f"{name}: {exc}")
                continue
            if want == have:
                unchanged[name] = row["shown"]
                continue
            source, sourcefile = row["source"], row["sourcefile"] or ""
            if source in OVERRIDING_SOURCES or sourcefile.endswith("postgresql.auto.conf"):
                where = sourcefile if sourcefile.endswith("postgresql.auto.conf") else f"source {source!r}"
                problems.append(f"{name}: currently {row['shown']} from {where}, which overrides {config_file}; "
                                "the harness edits only the configuration file, so its change would not take effect")
                continue
            changes[name] = {"from": row["shown"], "from_setting": row["setting"], "to": wanted[name],
                             "unit": row["unit"], "vartype": row["vartype"], "context": row["context"],
                             "source": source}
        if problems:
            raise ServerSettingsRefused("; ".join(problems))
        return {"config_file": config_file, "service": self.node.service, "changes": changes, "unchanged": unchanged}

    def _block(self, tag: str, changes: Mapping[str, Mapping[str, Any]]) -> tuple[str, str, str]:
        if not TAG_RE.match(tag):
            raise ValueError(f"tag {tag!r} must be letters, digits and '-'")
        begin, end = BLOCK_BEGIN.format(tag=tag), BLOCK_END.format(tag=tag)
        lines = [begin] + [_conf_line(n, str(c["to"])) for n, c in sorted(changes.items())] + [end]
        return begin, end, "\n".join(lines) + "\n"

    async def _restart_and_wait(self, host: RemoteHost) -> float:
        t = time.monotonic()
        await host.run(privileged(self.node.ssh, ["systemctl", "restart", self.node.service]),
                       timeout_s=RESTART_TIMEOUT_S)
        deadline = time.monotonic() + READY_TIMEOUT_S
        last: Exception | None = None
        while time.monotonic() < deadline:
            try:
                conn = await self._connect(timeout_s=5.0)
                try:
                    await conn.fetchval("SELECT 1")
                    return round(time.monotonic() - t, 3)
                finally:
                    await conn.close()
            except Exception as exc:  # noqa: BLE001 -- still starting; retried until the bound
                last = exc
                await asyncio.sleep(READY_POLL_S)
        raise RuntimeError(f"{self.node.service} did not accept connections within {READY_TIMEOUT_S:.0f} s "
                           f"after the restart: {last}")

    async def _check_values(self, expected: Mapping[str, Mapping[str, Any]], key: str) -> dict[str, Any]:
        """{name: {"want", "now", "ok"}} for each setting, `key` naming the expected value."""
        rows = await self._settings_rows(sorted(expected))
        out: dict[str, Any] = {}
        for name, c in expected.items():
            row = rows.get(name)
            if row is None:
                out[name] = {"want": c[key], "now": None, "ok": False}
                continue
            want = normalized(str(c[key]), row["vartype"], row["unit"])
            out[name] = {"want": c[key], "now": row["shown"], "setting": row["setting"],
                         "ok": normalized(row["setting"], row["vartype"], row["unit"]) == want}
        return out

    async def apply_server_settings(self, plan: Mapping[str, Any], tag: str) -> dict[str, Any]:
        """See ConfigurationSupport.apply_server_settings."""
        changes = plan["changes"]
        config_file = plan["config_file"]
        begin, end, block = self._block(tag, changes)
        backup = f"{config_file}.resilience-{tag}.bak"
        detail: dict[str, Any] = {"config_file": config_file, "backup": backup, "tag": tag, "begin": begin,
                                  "end": end, "changes": dict(changes)}
        encoded = base64.b64encode(block.encode()).decode()
        if not self.node.ssh.sudo_shell:
            return await self._apply_without_shell(detail, changes, config_file, backup, begin, encoded)
        script = (f"set -e; f={q(config_file)}; b={q(backup)}; test -f \"$f\"; "
                  f"if grep -qxF {q(begin)} \"$f\"; then echo PRESENT; exit 0; fi; "
                  f"cp -p \"$f\" \"$b\"; sha256sum \"$b\" | cut -d' ' -f1; "
                  # a file without a final newline gets one first, so the block starts on its own line
                  f"if [ -n \"$(tail -c1 \"$f\")\" ]; then printf '\\n' >> \"$f\"; fi; "
                  f"printf '%s' {q(encoded)} | base64 -d >> \"$f\"; sha256sum \"$f\" | cut -d' ' -f1")
        async with RemoteHost(self.node.ssh) as host:
            out = (await host.run(as_user(self.node.os_user, script), timeout_s=SSH_TIMEOUT_S)).stdout.split()
            if out[:1] != ["PRESENT"]:
                detail["original_sha256"], detail["applied_sha256"] = out[0], out[1]
            detail["restart_s"] = await self._restart_and_wait(host)
        check = await self._check_values(changes, "to")
        detail["check"] = check
        detail["after"] = {n: c.get("setting") for n, c in check.items()}
        if not all(c["ok"] for c in check.values()):
            wrong = {n: c for n, c in check.items() if not c["ok"]}
            detail["restore"] = await self.restore_server_settings(detail)
            raise RuntimeError(f"server settings not in effect after the restart: {wrong}; the change was "
                               "removed again")
        detail["verified"] = True
        return detail

    # --- the same steps on an account whose sudo runs no shell (env profile sudo_shell: false):
    # one program per sudo, as the database's OS user; pipes and redirections run in the SSH
    # user's own shell, so only the program that reads or writes the file has higher rights.

    def _conf_program(self, *argv: str) -> str:
        return privileged(self.node.ssh, list(argv), user=self.node.os_user)

    async def _sha(self, host: RemoteHost, path: str) -> str:
        return (await host.run(self._conf_program("/usr/bin/sha256sum", path), timeout_s=SSH_TIMEOUT_S)).stdout.split()[0]

    async def _has_line(self, host: RemoteHost, path: str, line: str) -> bool:
        r = await host.run(self._conf_program("/usr/bin/grep", "-qxF", line, path), timeout_s=SSH_TIMEOUT_S, check=False)
        if r.exit_status not in (0, 1):
            raise RuntimeError(f"cannot read {path}: {r.stderr.strip()}")
        return r.exit_status == 0

    async def _exists(self, host: RemoteHost, path: str) -> bool:
        r = await host.run(self._conf_program("/usr/bin/test", "-f", path), timeout_s=SSH_TIMEOUT_S, check=False)
        return r.exit_status == 0

    async def _apply_without_shell(self, detail: dict[str, Any], changes: Mapping[str, Any], config_file: str,
                                   backup: str, begin: str, encoded: str) -> dict[str, Any]:
        async with RemoteHost(self.node.ssh) as host:
            if not await self._exists(host, config_file):
                raise RuntimeError(f"{config_file} does not exist on {self.node.name}")
            if not await self._has_line(host, config_file, begin):
                await host.run(self._conf_program("/usr/bin/cp", "-p", config_file, backup), timeout_s=SSH_TIMEOUT_S)
                detail["original_sha256"] = await self._sha(host, backup)
                last = (await host.run(self._conf_program("/usr/bin/tail", "-c1", config_file),
                                       timeout_s=SSH_TIMEOUT_S)).stdout
                # a file without a final newline gets one first, so the block starts on its own line
                text = (f"printf '\\n' | {self._conf_program('/usr/bin/tee', '-a', config_file)} > /dev/null; "
                        if last not in ("", "\n") else "")
                await host.run(f"{text}printf '%s' {q(encoded)} | base64 -d | "
                               f"{self._conf_program('/usr/bin/tee', '-a', config_file)} > /dev/null",
                               timeout_s=SSH_TIMEOUT_S)
                detail["applied_sha256"] = await self._sha(host, config_file)
            detail["restart_s"] = await self._restart_and_wait(host)
        check = await self._check_values(changes, "to")
        detail["check"] = check
        detail["after"] = {n: c.get("setting") for n, c in check.items()}
        if not all(c["ok"] for c in check.values()):
            wrong = {n: c for n, c in check.items() if not c["ok"]}
            detail["restore"] = await self.restore_server_settings(detail)
            raise RuntimeError(f"server settings not in effect after the restart: {wrong}; the change was "
                               "removed again")
        detail["verified"] = True
        return detail

    async def _restore_without_shell(self, host: RemoteHost, config_file: str, backup: str, begin: str,
                                     end: str, applied: str) -> list[str]:
        """The restore script's decisions, one program at a time; returns the same two words."""
        tmp = f"{config_file}.resilience-tmp"
        have_backup = await self._exists(host, backup)
        if have_backup and await self._sha(host, config_file) == applied:
            # `cp` onto the existing file keeps its owner and mode, as `cat >` does
            await host.run(self._conf_program("/usr/bin/cp", backup, config_file), timeout_s=SSH_TIMEOUT_S)
            await host.run(self._conf_program("/usr/bin/rm", "-f", backup), timeout_s=SSH_TIMEOUT_S)
            return ["REMOVED", "SAME"]
        if await self._has_line(host, config_file, begin):
            program = "$0==b{skip=1;next} skip&&$0==e{skip=0;next} !skip"
            await host.run(f"{self._conf_program('/usr/bin/awk', '-v', f'b={begin}', '-v', f'e={end}', program, config_file)}"
                           f" | {self._conf_program('/usr/bin/tee', tmp)} > /dev/null", timeout_s=SSH_TIMEOUT_S)
            await host.run(self._conf_program("/usr/bin/cp", tmp, config_file), timeout_s=SSH_TIMEOUT_S)
            await host.run(self._conf_program("/usr/bin/rm", "-f", tmp), timeout_s=SSH_TIMEOUT_S)
            block = "REMOVED"
        else:
            block = "ABSENT"
        if not have_backup:
            return [block, "NOBACKUP"]
        same = await host.run(self._conf_program("/usr/bin/cmp", "-s", config_file, backup), timeout_s=SSH_TIMEOUT_S,
                              check=False)
        if same.exit_status == 0:
            await host.run(self._conf_program("/usr/bin/rm", "-f", backup), timeout_s=SSH_TIMEOUT_S)
            return [block, "SAME"]
        return [block, "DIFFERENT"]

    async def restore_server_settings(self, detail: Mapping[str, Any]) -> dict[str, Any]:
        """See ConfigurationSupport.restore_server_settings."""
        # from the ledger detail alone (the kill switch after a crash): markers and backup follow the tag
        config_file, tag = detail["config_file"], detail["tag"]
        begin, end, _ = self._block(tag, {})
        backup = detail.get("backup") or f"{config_file}.resilience-{tag}.bak"
        # Unchanged since the harness wrote it (same checksum): the backup goes back as it was.
        # Otherwise someone else edited it too: only the block is cut out, line by line, and
        # their edit stays. `cat >` keeps the file's owner and mode either way.
        applied = detail.get("applied_sha256") or (detail.get("apply") or {}).get("applied_sha256") or "none"
        script = (f"set -e; f={q(config_file)}; b={q(backup)}; B={q(begin)}; E={q(end)}; A={q(applied)}; "
                  f"if [ -f \"$b\" ] && [ \"$(sha256sum \"$f\" | cut -d' ' -f1)\" = \"$A\" ]; then "
                  f"cat \"$b\" > \"$f\"; rm -f \"$b\"; echo REMOVED SAME; exit 0; fi; "
                  f"if grep -qxF \"$B\" \"$f\"; then "
                  f"awk -v b=\"$B\" -v e=\"$E\" '$0==b{{skip=1;next}} skip&&$0==e{{skip=0;next}} !skip' \"$f\" > \"$f.resilience-tmp\"; "
                  f"cat \"$f.resilience-tmp\" > \"$f\"; rm -f \"$f.resilience-tmp\"; echo REMOVED; else echo ABSENT; fi; "
                  f"if [ -f \"$b\" ]; then if cmp -s \"$f\" \"$b\"; then rm -f \"$b\"; echo SAME; else echo DIFFERENT; fi; "
                  f"else echo NOBACKUP; fi")
        out: dict[str, Any] = {"config_file": config_file}
        async with RemoteHost(self.node.ssh) as host:
            if self.node.ssh.sudo_shell:
                words = (await host.run(as_user(self.node.os_user, script), timeout_s=SSH_TIMEOUT_S)).stdout.split()
            else:
                words = await self._restore_without_shell(host, config_file, backup, begin, end, applied)
            out["block"], out["file_vs_backup"] = words[0], words[1]
            if out["file_vs_backup"] == "DIFFERENT":
                # someone else edited the file during the run: their edit stays, the backup is kept
                out["backup_kept"] = backup
            if out["block"] == "REMOVED":
                out["restart_s"] = await self._restart_and_wait(host)
        check = await self._check_values(detail.get("changes") or {}, "from_setting")
        out["check"] = check
        if not all(c["ok"] for c in check.values()):
            wrong = {n: c for n, c in check.items() if not c["ok"]}
            raise RuntimeError(f"server settings not back to their recorded values: {wrong}")
        out["restored"] = True
        return out
