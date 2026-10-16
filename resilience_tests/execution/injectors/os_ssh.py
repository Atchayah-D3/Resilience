"""Shared plumbing for the OS/SSH drivers (Arch §5): the service manager and psql on a node,
reached over an SSH session the calling driver opened.

The drivers use these by composition -- each driver inherits only FaultInjector, so a fault
that is not a process kill (data corruption) does not inherit a process kill's behaviour.
"""

from __future__ import annotations

import asyncio
import re
import shlex
import time

from resilience_tests.control.profile import Node
from resilience_tests.execution.remote import RemoteHost, as_user

q = shlex.quote  # profile fields reach root shell commands as literals, never as syntax

SSH_TIMEOUT_S = 30.0
# A service still starting (e.g. crash recovery replaying WAL) is waited for, not restarted.
# One budget covers the WHOLE revert -- settle, start, settle again -- because the caller
# wraps the revert in a timeout of its own (killswitch.REVERT_TIMEOUT_S). If the steps each
# had their own budget, a slow-but-healthy start would be cancelled by the caller and
# journalled as revert_failed.
REVERT_TOTAL_BUDGET_S = 240.0
REVERT_SETTLE_POLL_S = 2.0
TRANSITIONAL_STATES = frozenset({"activating", "deactivating", "reloading"})

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


def psql_command(node: Node, *statements: str) -> str:
    # An explicit database: without -d, psql connects to a database named after the OS
    # user, which need not exist. The statements here are cluster-wide, so any database
    # serves; the harness database is the one the profile guarantees.
    args = " ".join(f"-c {shlex.quote(s)}" for s in statements)
    return as_user(node.os_user, f"cd /tmp && {q(node.pg_bin + '/psql')} -X -v ON_ERROR_STOP=1 "
                                 f"-p {node.db.port} -d {q(node.db.dbname)} -At {args}")


async def unit_property(host: RemoteHost, node: Node, prop: str) -> str:
    return (await host.run(f"systemctl show -p {prop} --value {q(node.service)}",
                           timeout_s=SSH_TIMEOUT_S, check=False)).stdout.strip()


async def settled_state(host: RemoteHost, node: Node, deadline: float) -> str:
    """The unit's state once it has stopped changing, or when the shared budget runs out."""
    while True:
        state = (await host.run(f"systemctl is-active {q(node.service)}",
                                timeout_s=SSH_TIMEOUT_S, check=False)).stdout.strip()
        if state not in TRANSITIONAL_STATES or time.monotonic() >= deadline:
            return state
        await asyncio.sleep(REVERT_SETTLE_POLL_S)
