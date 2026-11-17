"""One run at a time on a target, whichever harness host or checkout it comes from (Arch §15).

The driver host's own lock file (orchestrator._acquire_target_lock) stops two runs from the same
harness host overlapping. It cannot see a run started from a second harness host against the
same database -- one whose fault then lands in this run's baseline. This lock lives ON THE
TARGET HOST, so every harness that reaches the target over SSH sees it:

  acquire    `mkdir <path>` (atomic): the directory exists = the target is in use. Inside it, an
             owner file (run id, harness host, scenario, start) and a heartbeat file.
  heartbeat  the owning run touches the heartbeat every `heartbeat_s` while it runs.
  stale      a lock whose heartbeat is older than `stale_after_s` belonged to a run that died
             (the run's own heartbeat stopped); the next run takes it over and says so. Ages are
             measured on the target's clock alone, so clock skew between hosts does not matter.
  release    only by its owner, at the end of the run (after cleanup). Never a database object:
             a run that restarts or kills the database must keep the target while it does.

The path is outside the database's data directory and survives a database restart; by
default it is per database port, so two clusters on one host are two targets.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import shlex
import socket
import time
from typing import Any

from resilience_tests.control.profile import Node, TargetLockSpec
from resilience_tests.execution.remote import RemoteHost

q = shlex.quote
SSH_TIMEOUT_S = 20.0


class TargetBusy(RuntimeError):
    """Another run holds the target."""


def lock_path(node: Node, spec: TargetLockSpec) -> str:
    return spec.path or f"/var/tmp/resilience-harness-{node.db.port}.lock"


class TargetLock:
    def __init__(self, node: Node, spec: TargetLockSpec, *, run_id: str, scenario: str, profile: str) -> None:
        self.node, self.spec, self.run_id = node, spec, run_id
        self.path = lock_path(node, spec)
        self.owner = {"run_id": run_id, "scenario": scenario, "profile": profile,
                      "harness_host": socket.gethostname(), "pid": os.getpid(), "started_wall": round(time.time(), 3)}
        self._task: asyncio.Task[None] | None = None
        self.lost: str | None = None    # set when the heartbeat found the lock no longer ours

    def _owner_b64(self) -> str:
        return base64.b64encode(json.dumps(self.owner, sort_keys=True).encode()).decode()

    async def _run(self, script: str) -> str:
        async with RemoteHost(self.node.ssh, connect_timeout_s=10.0) as host:
            return (await host.run(f"sh -c {q(script)}", timeout_s=SSH_TIMEOUT_S)).stdout.strip()

    async def acquire(self) -> dict[str, Any]:
        """Take the lock, or raise TargetBusy naming who holds it. Returns what was done."""
        p, stale = q(self.path), int(self.spec.stale_after_s)
        write_owner = f'printf %s {q(self._owner_b64())} | base64 -d > "$P/owner"; touch "$P/heartbeat"'
        script = (
            f'P={p}; if mkdir "$P" 2>/dev/null; then {write_owner}; echo ACQUIRED; exit 0; fi; '
            'now=$(date +%s); '
            'hb=$(stat -c %Y "$P/heartbeat" 2>/dev/null || stat -c %Y "$P" 2>/dev/null || echo "$now"); '
            'age=$((now - hb)); old=$(base64 -w0 < "$P/owner" 2>/dev/null); '
            f'if [ "$age" -gt {stale} ] && mv "$P" "$P.stale.$$" 2>/dev/null; then rm -rf "$P.stale.$$"; '
            f'if mkdir "$P" 2>/dev/null; then {write_owner}; echo "TAKEN_OVER $age $old"; exit 0; fi; fi; '
            'echo "BUSY $age $old"')
        words = (await self._run(script)).split()
        verdict = words[0] if words else "ERROR"
        if verdict == "ACQUIRED":
            return {"path": self.path, "acquired": True}
        age = int(words[1]) if len(words) > 1 and words[1].lstrip("-").isdigit() else None
        holder = _decode(words[2]) if len(words) > 2 else {}
        if verdict == "TAKEN_OVER":
            return {"path": self.path, "acquired": True, "took_over_from": holder, "stale_heartbeat_s": age}
        who = (f"run {holder.get('run_id')} ({holder.get('scenario')}) from {holder.get('harness_host')}"
               if holder else "an unknown run")
        raise TargetBusy(f"{self.node.name} is in use by {who}: the lock {self.path} on the target was refreshed "
                         f"{age} s ago. Wait for that run to finish (a crashed run's lock is taken over after "
                         f"{self.spec.stale_after_s:.0f} s without a heartbeat).")

    async def heartbeat(self) -> bool:
        """Refresh the heartbeat if the lock is still ours. False if it is not (taken over)."""
        script = (f'P={q(self.path)}; if grep -qF {q(self.run_id)} "$P/owner" 2>/dev/null; '
                  'then touch "$P/heartbeat"; echo OK; else echo LOST; fi')
        return (await self._run(script)) == "OK"

    async def _beat(self) -> None:
        while True:
            await asyncio.sleep(self.spec.heartbeat_s)
            try:
                if not await self.heartbeat():
                    self.lost = f"the lock {self.path} was taken over by another run"
                    return
            except Exception:  # noqa: BLE001 -- the target may be restarting or down; retried next beat
                continue

    def start_heartbeat(self) -> None:
        self._task = asyncio.create_task(self._beat(), name="target-lock-heartbeat")

    async def release(self) -> dict[str, Any]:
        """Remove the lock if it is still ours; never someone else's."""
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        script = (f'P={q(self.path)}; if grep -qF {q(self.run_id)} "$P/owner" 2>/dev/null; '
                  'then rm -rf "$P"; echo RELEASED; else echo NOT_OURS; fi')
        return {"path": self.path, "released": (await self._run(script)) == "RELEASED"}


def _decode(b64: str) -> dict[str, Any]:
    try:
        return json.loads(base64.b64decode(b64).decode())
    except Exception:  # noqa: BLE001 -- an unreadable owner file still means "busy"
        return {}
