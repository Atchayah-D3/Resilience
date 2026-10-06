"""OS/SSH driver for the "process kill / stop" family (Arch §5): `kill -9` on the postmaster,
and a graceful service restart.

Serves NL-C-* and, later, CL-F-02 and DX-M-*. Unlike a power cut this leaves the OS page
cache intact, so it exercises crash recovery and WAL replay -- NOT durability
(Framework §13.1). The catalog keeps those scenarios apart.

Recovery is expected to be unattended: the service unit's own Restart policy brings the
database back. `revert` only makes sure the unit is not left stopped, and puts back any
parameter a reload changed.
"""

from __future__ import annotations

import asyncio
import re
import shlex
import time
from collections.abc import Mapping
from typing import Any

from resilience_tests.adapters.base import BaseDatabaseAdapter, adapter_for
from resilience_tests.adapters.postgresql.adapter import IDLE_SESSION_APPLICATION_NAME
from resilience_tests.control.profile import Node
from resilience_tests.execution.injectors.base import DriverNotAvailable, FaultInjector, FaultNotLanded, register
from resilience_tests.execution.remote import RemoteHost, as_root, as_user

q = shlex.quote  # profile fields reach root shell commands as literals, never as syntax

SSH_TIMEOUT_S = 30.0
# A parameter that is reload-only, harmless, and not read by the workload.
RELOAD_PROBE_PARAM = "log_min_duration_statement"
RELOAD_PROBE_VALUE = "250ms"
RELOAD_PROBE_VALUE_ALT = "251ms"  # used if the target already carries the probe value
# After SIGKILL the postmaster is gone once the kernel has torn it down; a zombie awaiting its
# parent's reap already counts as dead.
KILL_CONFIRM_TIMEOUT_S = 5.0
KILL_CONFIRM_POLL_S = 0.1
# A service still starting (e.g. crash recovery replaying WAL) is waited for, not restarted.
# One budget covers the WHOLE revert -- settle, start, settle again -- because the caller
# wraps the revert in a timeout of its own (killswitch.REVERT_TIMEOUT_S). If the steps each
# had their own budget, a slow-but-healthy start would be cancelled by the caller and
# journalled as revert_failed.
REVERT_TOTAL_BUDGET_S = 240.0
REVERT_SETTLE_POLL_S = 2.0
TRANSITIONAL_STATES = frozenset({"activating", "deactivating", "reloading"})

AUTO_CONF_VALUE_SQL = (
    f"SELECT setting FROM pg_file_settings WHERE name = '{RELOAD_PROBE_PARAM}' "
    "AND sourcefile LIKE '%/postgresql.auto.conf' ORDER BY seqno DESC LIMIT 1"
)

_EXEC_ARGV_RE = re.compile(r"argv\[\]=([^;]*)")
_MODES = {"s": "smart", "f": "fast", "i": "immediate"}
# PostgreSQL signal semantics: SIGTERM smart, SIGINT fast, SIGQUIT immediate.
_SIGNAL_MODES = {"2": "fast", "SIGINT": "fast", "15": "smart", "SIGTERM": "smart", "3": "immediate", "SIGQUIT": "immediate"}


def unit_stop_mode(exec_stop: str, kill_signal: str) -> str | None:
    """PostgreSQL shutdown mode a systemd unit's stop action produces: 'smart', 'fast',
    'immediate', or None when it cannot be determined (an unrecognised wrapper, say).

    `exec_stop` is `systemctl show -p ExecStop --value`; `kill_signal` is `-p KillSignal
    --value`. Without an ExecStop, systemd stops the service by sending KillSignal."""
    if exec_stop.strip():
        m = _EXEC_ARGV_RE.search(exec_stop)
        if m is None:
            return None
        argv = m.group(1).split()
        if not argv or not argv[0].endswith("pg_ctl") or "stop" not in argv:
            return None
        mode = "fast"  # pg_ctl stop's default since PostgreSQL 9.5
        for i, arg in enumerate(argv):
            if arg == "-m" and i + 1 < len(argv):
                mode = argv[i + 1]
            elif arg.startswith("--mode="):
                mode = arg.split("=", 1)[1]
            elif arg.startswith("-m") and len(arg) > 2:
                mode = arg[2:]
        return _MODES.get(mode[:1])
    return _SIGNAL_MODES.get(kill_signal.strip())


def parse_proc_stat(text: str) -> tuple[str, str] | None:
    """(state, starttime) from /proc/<pid>/stat, or None if the process does not exist. The
    command name may contain spaces or parentheses, so fields are counted from the last ')'."""
    text = text.strip()
    if not text or ")" not in text:
        return None
    fields = text[text.rindex(")") + 1:].split()
    if len(fields) < 20:
        return None
    return fields[0], fields[19]  # field 3 (state) and field 22 (starttime)


def process_gone(before: tuple[str, str], after_text: str) -> bool:
    """The process observed as `before` no longer runs: absent, a zombie, or its PID reused."""
    after = parse_proc_stat(after_text)
    return after is None or after[0] in ("Z", "X") or after[1] != before[1]


# Outcome lines of the guarded single-process kill (targeted_kill_command).
TARGET_KILLED = "KILLED"
TARGET_GONE = "NOPROC"          # the process exited before the kill: nothing changed
TARGET_FOREIGN = "PARENT"       # not a child of OUR postmaster (another cluster, or PID reuse)
TARGET_UNTITLED = "NOTITLE"     # its command line does not carry the expected marker


def targeted_kill_command(pid: int, pgdata: str, title_marker: str) -> str:
    """One POSIX-sh command that SIGKILLs `pid` only if it is still a child of the postmaster
    named in `pgdata`/postmaster.pid and its command line contains `title_marker`. It prints
    one status line; after a kill it also prints the postmaster pid and the victim's
    /proc/<pid>/stat line taken just before the kill (for death confirmation). The checks and
    the kill are a single round trip: nothing can be observed in one state and acted on in
    another -- the host may run other clusters, and PIDs are reused."""
    return (
        f"T={int(pid)}; PM=$(head -1 {q(pgdata + '/postmaster.pid')}); "
        f"S=$(cat /proc/$T/stat 2>/dev/null); "
        f'if [ -z "$S" ]; then echo {TARGET_GONE}; exit 0; fi; '
        f"PP=$(ps -o ppid= -p $T | tr -d ' '); "
        f'if [ "$PP" != "$PM" ]; then echo "{TARGET_FOREIGN} $PP $PM"; exit 0; fi; '
        f"if ! grep -qaF -- {q(title_marker)} /proc/$T/cmdline; then echo {TARGET_UNTITLED}; exit 0; fi; "
        f'kill -9 $T && echo {TARGET_KILLED} && echo "$PM" && echo "$S"'
    )


def _usec_to_s(value: str) -> float:
    """systemd reports StartLimitIntervalUSec in microseconds, or as 'infinity'."""
    v = value.strip().lower()
    if not v or v in ("infinity", "0"):
        return 0.0
    return int(v) / 1_000_000 if v.isdigit() else 0.0


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


class OsSshProcessDriver(FaultInjector):
    """`process_kill`    -- SIGKILL the postmaster: crash recovery, WAL replay (NL-C).
    `service_restart` -- graceful restart under load with a FAST shutdown: planned change,
                         which is a leading source of unplanned outage (Framework §2.3,
                         NL-M-06 "restart under load with -m fast").
    `config_reload`   -- change a parameter and SIGHUP under load: clients must not notice
                         (NL-M-07)."""

    fault_types = frozenset({"process_kill", "service_restart", "config_reload", "connection_exhaustion", "idle_in_transaction" })
    driver_name = "os_ssh"

    # ------------------------------------------------------------------ helpers

    async def _postmaster_pid(self, host: RemoteHost, node: Node) -> int | None:
        result = await host.run(as_root(f"head -1 {q(node.pgdata + '/postmaster.pid')}"),
                                timeout_s=SSH_TIMEOUT_S, check=False)
        text = result.stdout.strip()
        return int(text) if text.isdigit() else None

    async def _proc_stat(self, host: RemoteHost, pid: int) -> str:
        # as root: the SSH user cannot reliably inspect a process owned by the database user
        return (await host.run(as_root(f"cat /proc/{pid}/stat 2>/dev/null || true"),
                               timeout_s=SSH_TIMEOUT_S, check=False)).stdout

    def _psql(self, node: Node, *statements: str) -> str:
        # An explicit database: without -d, psql connects to a database named after the OS
        # user, which need not exist. The statements here are cluster-wide, so any database
        # serves; the harness database is the one the profile guarantees.
        args = " ".join(f"-c {shlex.quote(q)}" for q in statements)
        return as_user(node.os_user, f"cd /tmp && {q(node.pg_bin + '/psql')} -X -v ON_ERROR_STOP=1 "
                                     f"-p {node.db.port} -d {q(node.db.dbname)} -At {args}")

    async def _auto_conf_value(self, host: RemoteHost, node: Node) -> str | None:
        """The probe parameter's value in postgresql.auto.conf, or None if it is not set there."""
        result = await host.run(self._psql(node, AUTO_CONF_VALUE_SQL), timeout_s=SSH_TIMEOUT_S)
        text = result.stdout.strip()
        return text or None

    async def _unit(self, host: RemoteHost, node: Node, prop: str) -> str:
        return (await host.run(f"systemctl show -p {prop} --value {q(node.service)}",
                               timeout_s=SSH_TIMEOUT_S, check=False)).stdout.strip()

    # ------------------------------------------------------------------ interface

    async def preflight(self, node: Node) -> dict[str, Any]:
        detail: dict[str, Any] = {"service": node.service}
        async with RemoteHost(node.ssh) as host:
            active = (await host.run(f"systemctl is-active {q(node.service)}",
                                     timeout_s=SSH_TIMEOUT_S, check=False)).stdout.strip()
            pid = await self._postmaster_pid(host, node)
            restart = await self._unit(host, node, "Restart")
            if active != "active" or pid is None:
                raise DriverNotAvailable(f"{node.name}: service {node.service} is {active!r}, postmaster pid {pid}")
            if restart in ("", "no"):
                raise DriverNotAvailable(
                    f"{node.name}: {node.service} has Restart={restart!r}; the scenario requires unattended recovery"
                )
            detail.update(postmaster_pid=pid, restart_policy=restart)
            if self.fault_type == "service_restart":
                exec_stop = await self._unit(host, node, "ExecStop")
                kill_signal = await self._unit(host, node, "KillSignal")
                mode = unit_stop_mode(exec_stop, kill_signal)
                if mode != "fast":
                    raise DriverNotAvailable(
                        f"{node.name}: {node.service} stops PostgreSQL with a {mode or 'undeterminable'} shutdown "
                        f"(ExecStop={exec_stop or '<none>'!r}, KillSignal={kill_signal!r}); NL-M-06 requires -m fast"
                    )
                detail["stop_mode"] = mode
            if self.repeat_plan is not None:
                detail["restart_cadence"] = await self._check_restart_cadence(host, node)
            if self.fault_type == "config_reload":
                # recorded in the ledger with the injection, so revert restores exactly this
                prior = await self._auto_conf_value(host, node)
                if prior in (RELOAD_PROBE_VALUE, RELOAD_PROBE_VALUE_ALT):
                    # The harness's own probe value is already there: a previous run leaked it.
                    # Treating it as the operator's setting would make the leak permanent --
                    # every later revert would faithfully restore the harness's own value.
                    detail["auto_conf_leftover"] = prior
                    prior = None
                detail["auto_conf_prior"] = prior
            if self.fault_type == "idle_in_transaction":
                res = await host.run(self._psql(node, "SHOW idle_in_transaction_session_timeout"),
                                     timeout_s=SSH_TIMEOUT_S, check=False)
                detail["idle_timeout"] = res.stdout.strip()
            if self.fault_type == "connection_exhaustion":
                if self.duration_s is None:
                    raise DriverNotAvailable("connection_exhaustion is held and then released: "
                                             "the scenario must give fault.duration in seconds")
                detail["hold_s"] = self.duration_s
        return detail

    async def confirm(self, node: Node, detail: Mapping[str, Any]) -> dict[str, Any]:
        """process_kill: death was confirmed from /proc at injection. service_restart: the
        postmaster after recovery is a different process from the one preflight saw.
        config_reload: a fresh session reports the value the injection wrote."""
        inject = detail.get("inject") or {}
        if self.fault_type == "process_kill":
            return {"fault_confirmed": inject.get("death_confirmed_s") is not None,
                    "death_confirmed_s": inject.get("death_confirmed_s")}
        if self.fault_type == "service_restart":
            before = (detail.get("preflight") or {}).get("postmaster_pid")
            async with RemoteHost(node.ssh) as host:
                after = await self._postmaster_pid(host, node)
            if before is None or after is None:
                return {"fault_confirmed": None, "postmaster_pid_before": before, "postmaster_pid_after": after,
                        "note": "postmaster pid not readable before and after the restart"}
            return {"fault_confirmed": after != before, "postmaster_pid_before": before, "postmaster_pid_after": after}
        if self.fault_type == "config_reload":
            injected = inject.get("value")
            async with RemoteHost(node.ssh) as host:
                r = await host.run(self._psql(node, f"SHOW {RELOAD_PROBE_PARAM}"), timeout_s=SSH_TIMEOUT_S, check=False)
            live = r.stdout.strip()
            if r.exit_status != 0 or injected is None:
                return {"fault_confirmed": None, "injected": injected, "note": f"SHOW failed: {r.stderr.strip()[:200]}"}
            return {"fault_confirmed": live == injected, "injected": injected, "live_value": live}
        return await super().confirm(node, detail)

    async def _check_restart_cadence(self, host: RemoteHost, node: Node) -> dict[str, Any]:
        """A repeated-crash scenario must not trip the service manager's own restart limit.

        systemd allows StartLimitBurst restarts per StartLimitIntervalSec; beyond that it
        refuses to start the unit at all. The run would then measure a service the harness
        itself disabled, and every later cycle would read as a catastrophic recovery time.
        Checked here, before the first kill, rather than discovered at cycle six."""
        cycles, interval = self.repeat_plan
        burst = await self._unit(host, node, "StartLimitBurst")
        window_us = await self._unit(host, node, "StartLimitIntervalUSec")
        burst_n = int(burst) if burst.isdigit() else 5                  # systemd's default
        window_s = _usec_to_s(window_us)
        plan = {"cycles": cycles, "interval_s": interval, "start_limit_burst": burst_n,
                "start_limit_interval_s": window_s}
        if burst_n == 0 or window_s == 0:
            plan["verdict"] = "no restart limit configured"
            return plan
        # worst case: a cycle costs only the settle interval, so restarts arrive that fast
        restarts_in_window = window_s / interval if interval else float("inf")
        plan["worst_case_restarts_in_window"] = round(restarts_in_window, 2)
        if restarts_in_window >= burst_n:
            raise DriverNotAvailable(
                f"{node.name}: {node.service} allows {burst_n} restarts per {window_s:g} s, but this "
                f"scenario can restart it every {interval:g} s ({restarts_in_window:.1f} in that window). "
                f"systemd would refuse to start it mid-run. Raise StartLimitBurst / "
                f"StartLimitIntervalSec on the unit, or lengthen the scenario's interval_s.")
        plan["verdict"] = "cadence fits the unit's restart limit"
        return plan

    async def inject(self, node: Node) -> dict[str, Any]:
        if self.fault_type == "service_restart":
            return await self._restart(node)
        if self.fault_type == "config_reload":
            return await self._reload(node)
        if self.fault_type == "idle_in_transaction":
            return await self._inject_idle(node)
        if self.fault_type == "connection_exhaustion":
            return await self._exhaust_connections(node)
        return await self._kill(node)

    def _database_adapter(self, node: Node) -> BaseDatabaseAdapter:
        """The run's adapter when it was handed over; otherwise one built from the profile
        (the kill switch after a crash, where the run's adapter no longer exists)."""
        if self.adapter is None:
            self.adapter = adapter_for(self.profile.database.engine, node)
        return self.adapter

    async def _inject_idle(self, node: Node) -> dict[str, Any]:
        """Idle-in-transaction (NL-M-05), applied through the run's database adapter so the
        session it opens is the one the run later observes and cleanup ends. The adapter
        confirms it from the server; an unconfirmed session is a fault that did not land.
        Never a shell-held psql session: it could neither be confirmed nor found again."""
        if self.adapter is None:
            raise DriverNotAvailable("idle_in_transaction is injected through the database adapter, "
                                     "and none was handed to this driver")
        detail = dict(await self.adapter.inject_idle_transaction())
        if not detail.get("supported"):
            raise FaultNotLanded(f"idle session not established: {detail.get('error')}", detail)
        detail.update(action="idle_in_transaction", t0_mono_ns=time.monotonic_ns())
        return detail

    async def _exhaust_connections(self, node: Node) -> dict[str, Any]:
        """Connection exhaustion under load (NL-R-04): the flood is held for the scenario's
        fault.duration and released before this returns. Its sessions carry an
        application_name, so the revert terminates any that remain from any adapter."""
        assert self.duration_s is not None   # preflight refuses without it
        return await self._database_adapter(node).exhaust_connections(hold_s=self.duration_s)

    async def _reload(self, node: Node) -> dict[str, Any]:
        """Change a parameter, then SIGHUP. A reload-only parameter is used deliberately: the
        test is that clients are undisturbed, not that the value takes effect."""
        async with RemoteHost(node.ssh) as host:
            prior = await self._auto_conf_value(host, node)
            value = RELOAD_PROBE_VALUE if prior != RELOAD_PROBE_VALUE else RELOAD_PROBE_VALUE_ALT
            result = await host.run(
                self._psql(node, f"ALTER SYSTEM SET {RELOAD_PROBE_PARAM} = {_sql_literal(value)}", "SELECT pg_reload_conf()"),
                timeout_s=SSH_TIMEOUT_S,
            )
            t0_mono_ns = time.monotonic_ns()
        return {"action": f"ALTER SYSTEM SET {RELOAD_PROBE_PARAM} + pg_reload_conf()", "value": value,
                "auto_conf_prior": prior, "reload_result": result.stdout.strip().splitlines()[-1:],
                "t0_mono_ns": t0_mono_ns}

    async def _restart(self, node: Node) -> dict[str, Any]:
        """Graceful restart under load. `--no-block` returns immediately, so T0 is the moment
        the restart was requested -- the availability gap is measured from there, not from
        when the service happened to come back. Preflight has verified the unit's stop action
        is a fast shutdown."""
        async with RemoteHost(node.ssh) as host:
            await host.run(as_root(f"systemctl restart --no-block {q(node.service)}"), timeout_s=SSH_TIMEOUT_S)
            t0_mono_ns = time.monotonic_ns()
        return {"action": f"systemctl restart {node.service}", "t0_mono_ns": t0_mono_ns}

    async def arm(self, node: Node) -> None:
        """For process_kill: open the SSH session and identify the postmaster now, so that
        `inject` sends exactly one command. NL-C-02 must land its kill while a checkpoint it
        has just seen running is still running; an SSH handshake and two lookups between the
        two would give the checkpoint time to finish."""
        if self.fault_type != "process_kill":
            return
        await self.disarm()
        host = RemoteHost(node.ssh)
        await host.connect()
        try:
            pid, before = await self._identify_postmaster(host, node)
        except BaseException:
            await host.close()
            raise
        self._armed = (host, pid, before)

    async def disarm(self) -> None:
        armed, self._armed = getattr(self, "_armed", None), None
        if armed is not None:
            await armed[0].close()

    async def _identify_postmaster(self, host: RemoteHost, node: Node) -> tuple[int, tuple[str, str]]:
        pid = await self._postmaster_pid(host, node)
        if pid is None:
            raise DriverNotAvailable(f"{node.name}: no postmaster.pid in {node.pgdata}")
        before = parse_proc_stat(await self._proc_stat(host, pid))
        if before is None:
            raise DriverNotAvailable(f"{node.name}: postmaster.pid names {pid}, which is not running")
        return pid, before

    async def _kill(self, node: Node) -> dict[str, Any]:
        """SIGKILL every process in the unit's cgroup -- postmaster and backends together
        (Arch §5: `systemctl kill -s SIGKILL`). T0 is when the kill call returns; the
        postmaster's death is then confirmed from /proc, so a kill that did not land can
        never be reported as a fault.

        Killing the postmaster alone leaves its backends running in the cgroup. The service
        manager then holds the restart until they exit or its stop timeout expires, and that
        wait lands inside the measured RTO -- a recovery-time budget spent on the harness's
        own untidiness rather than on the database.

        If `arm` prepared the session, it is used as is: the kill is the next command sent.

        With `kill_target` set (fault.during located one process, e.g. an autovacuum worker),
        only that process is killed -- see `_kill_target`."""
        if self.kill_target is not None:
            return await self._kill_target(node, self.kill_target)
        armed, self._armed = getattr(self, "_armed", None), None
        if armed is None:
            async with RemoteHost(node.ssh) as host:
                pid, before = await self._identify_postmaster(host, node)
                return await self._kill_on(host, node, pid, before, pre_armed=False)
        host, pid, before = armed
        try:
            return await self._kill_on(host, node, pid, before, pre_armed=True)
        finally:
            await host.close()

    async def _kill_target(self, node: Node, target: Mapping[str, Any]) -> dict[str, Any]:
        """SIGKILL exactly one process of this instance (NL-M-03: an autovacuum worker). The
        guarded command refuses anything that is not still a child of our postmaster carrying
        the expected title; that refusal changed nothing and is reported as such, so the
        orchestrator may look for the process again. A kill is confirmed from /proc."""
        pid = int(target["pid"])
        marker = str(target["title_marker"])
        async with RemoteHost(node.ssh) as host:
            result = await host.run(as_root(targeted_kill_command(pid, node.pgdata, marker)),
                                    timeout_s=SSH_TIMEOUT_S)
            t0_mono_ns = time.monotonic_ns()
            lines = result.stdout.strip().splitlines()
            status = lines[0].split()[0] if lines else ""
            if status != TARGET_KILLED:
                raise FaultNotLanded(
                    f"{node.name}: pid {pid} was not killed ({lines[0] if lines else 'no output'})",
                    {"changed_nothing": True, "reason": lines[0] if lines else "", "target": dict(target)})
            postmaster_pid = int(lines[1]) if len(lines) > 1 and lines[1].isdigit() else None
            before = parse_proc_stat(lines[2] if len(lines) > 2 else "")
            deadline = time.monotonic() + KILL_CONFIRM_TIMEOUT_S
            while before is not None and not process_gone(before, await self._proc_stat(host, pid)):
                if time.monotonic() >= deadline:
                    raise RuntimeError(f"{node.name}: pid {pid} still alive {KILL_CONFIRM_TIMEOUT_S} s after SIGKILL")
                await asyncio.sleep(KILL_CONFIRM_POLL_S)
            confirmed_s = (time.monotonic_ns() - t0_mono_ns) / 1e9
            postmaster_after = await self._postmaster_pid(host, node)
        return {"action": f"kill -9 {pid} ({marker}, child of postmaster {postmaster_pid})",
                "target_pid": pid, "target": dict(target), "postmaster_pid": postmaster_pid,
                # restart_after_crash: the postmaster resets the instance itself and survives
                "postmaster_survived": postmaster_after is not None and postmaster_after == postmaster_pid,
                "t0_mono_ns": t0_mono_ns, "death_confirmed_s": confirmed_s if before is not None else None,
                "landed": True}

    async def _kill_on(self, host: RemoteHost, node: Node, pid: int, before: tuple[str, str], *,
                       pre_armed: bool) -> dict[str, Any]:
        await host.run(as_root(f"systemctl kill -s SIGKILL {q(node.service)}"), timeout_s=SSH_TIMEOUT_S)
        t0_mono_ns = time.monotonic_ns()
        deadline = time.monotonic() + KILL_CONFIRM_TIMEOUT_S
        while not process_gone(before, await self._proc_stat(host, pid)):
            if time.monotonic() >= deadline:
                raise RuntimeError(f"{node.name}: postmaster {pid} still alive {KILL_CONFIRM_TIMEOUT_S} s after SIGKILL")
            await asyncio.sleep(KILL_CONFIRM_POLL_S)
        confirmed_s = (time.monotonic_ns() - t0_mono_ns) / 1e9
        return {"action": f"systemctl kill -s SIGKILL {node.service} (whole unit cgroup)", "pid": pid,
                "t0_mono_ns": t0_mono_ns, "death_confirmed_s": confirmed_s, "pre_armed": pre_armed}

    async def revert(self, node: Node, detail: Mapping[str, Any] | None = None) -> dict[str, Any]:
        """Idempotent. For a reload, put the changed parameter back to its pre-fault value;
        otherwise make sure the service is running (waiting out a start already in progress)."""
        if self.fault_type == "config_reload":
            return await self._revert_reload(node, detail or {})
        if self.fault_type == "idle_in_transaction":
            return await self._revert_idle(node, detail or {})
        if self.fault_type == "connection_exhaustion":
            return await self._database_adapter(node).revert_exhaust_connections()
        deadline = time.monotonic() + REVERT_TOTAL_BUDGET_S
        async with RemoteHost(node.ssh) as host:
            state = await self._settled_state(host, node, deadline)
            if state == "active":
                return {"action": "none", "service": node.service, "state": state}
            # A unit that hit its start limit (repeated crashes -- which is what these
            # scenarios do) stays `failed`, and systemd refuses `start` until the failure is
            # cleared. Without this both the revert and the kill switch are powerless to put
            # the service back.
            reset = await host.run(as_root(f"systemctl reset-failed {q(node.service)}"),
                                   timeout_s=SSH_TIMEOUT_S, check=False)
            remaining = max(SSH_TIMEOUT_S, deadline - time.monotonic())
            await host.run(as_root(f"systemctl start {q(node.service)}"), timeout_s=remaining, check=False)
            state = await self._settled_state(host, node, deadline)
            reset_failed_issued = reset.exit_status == 0
        if state != "active":
            raise RuntimeError(f"{node.name}: {node.service} is {state!r} after systemctl start "
                               f"(waited up to {REVERT_TOTAL_BUDGET_S} s)")
        return {"action": "systemctl start", "service": node.service, "state": state,
                "reset_failed_issued": reset_failed_issued}

    async def _settled_state(self, host: RemoteHost, node: Node, deadline: float) -> str:
        """The unit's state once it has stopped changing, or when the shared budget runs out."""
        while True:
            state = (await host.run(f"systemctl is-active {q(node.service)}",
                                    timeout_s=SSH_TIMEOUT_S, check=False)).stdout.strip()
            if state not in TRANSITIONAL_STATES or time.monotonic() >= deadline:
                return state
            await asyncio.sleep(REVERT_SETTLE_POLL_S)

    async def _revert_reload(self, node: Node, detail: Mapping[str, Any]) -> dict[str, Any]:
        inject = detail.get("inject") or {}
        recorded = next((d for d in (detail.get("preflight") or {}, inject) if "auto_conf_prior" in d), None)
        prior_known = recorded is not None
        prior = recorded["auto_conf_prior"] if recorded is not None else None
        injected = inject.get("value")
        async with RemoteHost(node.ssh) as host:
            current = await self._auto_conf_value(host, node)
            if prior_known:
                if current == prior:
                    return {"action": "none", "reason": "already at the pre-fault value", "value": prior}
                target = prior
            elif current is not None and current in {injected, RELOAD_PROBE_VALUE, RELOAD_PROBE_VALUE_ALT}:
                # No record of the pre-fault value (a very old ledger entry): remove only what
                # the harness itself wrote, and say so.
                target = None
            else:
                return {"action": "none", "reason": "pre-fault value unknown and the injected value is not present",
                        "current": current}
            statement = (f"ALTER SYSTEM RESET {RELOAD_PROBE_PARAM}" if target is None
                         else f"ALTER SYSTEM SET {RELOAD_PROBE_PARAM} = {_sql_literal(target)}")
            await host.run(self._psql(node, statement, "SELECT pg_reload_conf()"), timeout_s=SSH_TIMEOUT_S)
            after = await self._auto_conf_value(host, node)
        if after != target:
            raise RuntimeError(f"{node.name}: {RELOAD_PROBE_PARAM} in postgresql.auto.conf is {after!r} after revert, "
                               f"expected {target!r}")
        return {"action": statement, "restored": target, "prior_known": prior_known}

    async def _revert_idle(self, node: Node, detail: Mapping[str, Any]) -> dict[str, Any]:
        """End the harness's own idle session, found by its application_name -- the only safe
        handle after a harness crash. Never a signal to the PID in the ledger (that backend is
        usually long gone and its PID may now belong to anything), and never every idle
        session on the cluster (other clients' sessions are not ours to end).

        With the run's adapter, it rolls back its own connection first; the server-side
        termination over SSH runs regardless, so a failed rollback cannot leave it open."""
        inject = detail.get("inject") or {}
        out: dict[str, Any] = {"pid": inject.get("pid")}
        if self.adapter is not None:
            out["adapter"] = await self.adapter.close_idle_transaction()
        term_sql = ("SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity "
                    f"WHERE application_name = {_sql_literal(IDLE_SESSION_APPLICATION_NAME)} "
                    "AND pid <> pg_backend_pid()")
        async with RemoteHost(node.ssh) as host:
            result = await host.run(self._psql(node, term_sql), timeout_s=SSH_TIMEOUT_S)
        out.update(action=f"terminated sessions named {IDLE_SESSION_APPLICATION_NAME}",
                   terminated=result.stdout.strip())
        return out


register("os_ssh", OsSshProcessDriver)
