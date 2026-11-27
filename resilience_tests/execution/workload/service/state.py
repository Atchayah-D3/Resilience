"""Where a running common workload is recorded, and the guards that protect it.

Two records, so that every harness command can tell whether a common workload is running:

  on the target host   the workload lock, a directory /var/tmp/resilience-workload-<db port>.lock
                       with an owner file and a heartbeat (the same mechanism as the scenarios'
                       lock, control/target_lock.py, at another path). Seen by every harness host;
                       a lock whose heartbeat stopped for WORKLOAD_LOCK_STALE_S belonged to a
                       workload that died.
  on the harness host  a pointer file <run_dir>/.workload-<profile>-<node>.json naming the run
                       folder and the process id, so `status` and `stop` find the run.

The guards: while a common workload runs, nothing else may empty the marker table, reload the
TPC-C data or start a second set of clerks -- each would destroy the workload's evidence or
disturb its customers. A scenario run (orchestrator init) and the standalone TPC-C commands
(load, trial, capacity: load.check_target) are refused. The service itself refuses to start
while a scenario run holds the target, because its start empties the marker table.
"""

from __future__ import annotations

import errno
import fcntl
import json
import os
import shlex
import time
from pathlib import Path
from typing import Any

from resilience_tests.control.profile import EnvProfile, Node, TargetLockSpec
from resilience_tests.control.target_lock import TargetLock, _decode, lock_path
from resilience_tests.execution.remote import RemoteHost

q = shlex.quote
WORKLOAD_LOCK_HEARTBEAT_S = 10.0
WORKLOAD_LOCK_STALE_S = 90.0
SSH_TIMEOUT_S = 20.0

STATUS_FILE = "status.json"
INFO_FILE = "service.json"              # written once the clerks run: what a scenario attaching needs
STOP_REQUEST_FILE = "stop.request"
SUMMARY_FILE = "summary.json"
SUMMARY_TEXT_FILE = "summary.txt"
EVENTS_FILE = "events.jsonl"            # the name the TPC-C checks read (profile._driver_load)
METRICS_1S_FILE = "metrics-1s.jsonl"
# Written by the harness (scenario runs), read by the service: one line per scenario event, so the
# service judges its baseline outside the scenarios and reports each scenario's window separately.
MARKS_FILE = "scenario-marks.jsonl"
MARK_KINDS = ("run_start", "baseline", "t0", "t1", "run_end")   # baseline: the scenario's baseline window opens (recovered.py)
LOG_FILE = "service.log"

# the states written in status.json; the last three are final
STARTING, WARMING_UP, RUNNING, STOPPING, CHECKING = "starting", "warming_up", "running", "stopping", "checking"
FINISHED, STOPPED, FAILED = "finished", "stopped", "failed"
FINAL_STATES = frozenset({FINISHED, STOPPED, FAILED})


class WorkloadRunning(RuntimeError):
    """A common workload is running on this target; the command would disturb it."""


class TargetInUse(RuntimeError):
    """A scenario run holds the target; the common workload may not start under it."""


def workload_lock_path(node: Node) -> str:
    return f"/var/tmp/resilience-workload-{node.db.port}.lock"


def workload_lock_spec(node: Node) -> TargetLockSpec:
    return TargetLockSpec(path=workload_lock_path(node), heartbeat_s=WORKLOAD_LOCK_HEARTBEAT_S,
                          stale_after_s=WORKLOAD_LOCK_STALE_S)


def workload_lock(node: Node, *, run_id: str, workload_id: str, profile: str) -> TargetLock:
    return TargetLock(node, workload_lock_spec(node), run_id=run_id, scenario=f"common workload {workload_id}",
                      profile=profile)


def metrics_interval_file(seconds: int) -> str:
    """One line per interval; a name of its own, so it never collides with metrics-1s.jsonl."""
    return f"intervals-{seconds}s.jsonl"


# --- the pointer on the harness host ---------------------------------------------------------


def pointer_path(profile: EnvProfile, node: Node) -> Path:
    return Path(profile.driver_host.run_dir).expanduser() / f".workload-{profile.name}-{node.name}.json"


def write_pointer(profile: EnvProfile, node: Node, folder: Path, pid: int) -> None:
    _write_json(pointer_path(profile, node), {"folder": str(folder), "pid": pid, "started_wall": round(time.time(), 3)})


def read_pointer(profile: EnvProfile, node: Node) -> dict[str, Any] | None:
    try:
        return json.loads(pointer_path(profile, node).read_text())
    except (OSError, json.JSONDecodeError):
        return None


def remove_pointer(profile: EnvProfile, node: Node, folder: Path) -> None:
    """Only our own pointer: a newer run's is left alone."""
    p = read_pointer(profile, node)
    if p is not None and p.get("folder") == str(folder):
        try:
            pointer_path(profile, node).unlink()
        except OSError:
            pass


def pid_alive(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


# --- status.json -----------------------------------------------------------------------------


def _write_json(path: Path, data: dict[str, Any]) -> None:
    """Whole file or nothing: a reader never sees half a status."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, default=str) + "\n")
    os.replace(tmp, path)


def write_status(folder: Path, data: dict[str, Any]) -> None:
    _write_json(folder / STATUS_FILE, data)


def write_info(folder: Path, data: dict[str, Any]) -> None:
    _write_json(folder / INFO_FILE, data)


def read_info(folder: Path) -> dict[str, Any] | None:
    try:
        return json.loads((folder / INFO_FILE).read_text())
    except (OSError, json.JSONDecodeError):
        return None


def read_status(folder: Path) -> dict[str, Any] | None:
    try:
        return json.loads((folder / STATUS_FILE).read_text())
    except (OSError, json.JSONDecodeError):
        return None


def request_stop(folder: Path, by: str) -> None:
    (folder / STOP_REQUEST_FILE).write_text(json.dumps({"by": by, "at_wall": round(time.time(), 3)}) + "\n")


def stop_requested(folder: Path) -> dict[str, Any] | None:
    try:
        return json.loads((folder / STOP_REQUEST_FILE).read_text())
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError):
        return {"by": "unreadable stop request"}


# --- scenario marks (written by the harness) --------------------------------------------------


def write_mark(folder: Path, kind: str, *, run_id: str, scenario: str, t_wall: float | None = None,
               **extra: Any) -> dict[str, Any]:
    """Append one scenario mark to the running workload's folder: run_start (the scenario's run
    begins), baseline (its baseline window opens: its start-up is over), t0 (its fault), t1 (its
    recovery ends), run_end (its cleanup is over). One whole line,
    written in a single append and saved to disk."""
    if kind not in MARK_KINDS:
        raise ValueError(f"mark kind {kind!r}: one of {', '.join(MARK_KINDS)}")
    rec = {"kind": kind, "run_id": run_id, "scenario": scenario,
           "t_wall": round(time.time() if t_wall is None else t_wall, 3), **extra}
    line = (json.dumps(rec, separators=(",", ":"), default=str) + "\n").encode()
    fd = os.open(folder / MARKS_FILE, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o640)
    try:
        os.write(fd, line)
        os.fsync(fd)
    finally:
        os.close(fd)
    return rec


def read_complete_lines(path: Path) -> list[dict[str, Any]]:
    """Every whole line of a JSON-lines file that may still be being written: a last line without
    its newline is left out (it is not finished yet); a whole line that is not JSON raises."""
    try:
        data = path.read_bytes()
    except FileNotFoundError:
        return []
    lines = data.split(b"\n")
    return [json.loads(x) for x in lines[:-1] if x.strip()]


def read_marks(folder: Path) -> list[dict[str, Any]]:
    return read_complete_lines(folder / MARKS_FILE)


# --- locks seen from outside -----------------------------------------------------------------


async def remote_lock_holder(node: Node, path: str, stale_after_s: float) -> dict[str, Any] | None:
    """Who holds the lock directory `path` on the target, if its heartbeat is fresh (None: free
    or stale). Read only. Raises when the target cannot be reached: the caller then refuses,
    since "could not check" is not "free"."""
    script = (f'P={q(path)}; if [ -d "$P" ]; then now=$(date +%s); '
              'hb=$(stat -c %Y "$P/heartbeat" 2>/dev/null || stat -c %Y "$P" 2>/dev/null || echo "$now"); '
              'echo "HELD $((now - hb)) $(base64 -w0 < "$P/owner" 2>/dev/null)"; else echo FREE; fi')
    async with RemoteHost(node.ssh, connect_timeout_s=10.0) as host:
        words = (await host.run(f"sh -c {q(script)}", timeout_s=SSH_TIMEOUT_S)).stdout.split()
    if not words or words[0] != "HELD":
        return None
    age = int(words[1]) if len(words) > 1 and words[1].lstrip("-").isdigit() else 0
    if age > stale_after_s:
        return None
    holder = _decode(words[2]) if len(words) > 2 else {}
    return {**holder, "heartbeat_age_s": age, "lock": path}


async def running_workload(profile: EnvProfile, node: Node) -> dict[str, Any] | None:
    """The common workload running on this target, or None. The harness host's pointer (a live
    process) or the target's workload lock (a fresh heartbeat) is enough."""
    pointer = read_pointer(profile, node)
    if pointer is not None and pid_alive(pointer.get("pid")):
        return {"folder": pointer.get("folder"), "pid": pointer.get("pid"), "seen_by": "harness host"}
    holder = await remote_lock_holder(node, workload_lock_path(node), WORKLOAD_LOCK_STALE_S)
    if holder is not None:
        return {**holder, "seen_by": "target host"}
    return None


async def refuse_if_running(profile: EnvProfile, node: Node, what: str) -> None:
    """Raise WorkloadRunning if a common workload runs on this target. `what`: the refused
    command, for the message."""
    try:
        running = await running_workload(profile, node)
    except Exception as exc:  # noqa: BLE001 -- could not check is not "none running": refuse
        raise WorkloadRunning(f"{what} refused: could not check for a running common workload on {node.name} "
                              f"({type(exc).__name__}: {exc})") from None
    if running is not None:
        where = running.get("folder") or running.get("harness_host") or "another harness host"
        raise WorkloadRunning(
            f"{what} refused: the common workload is running on {node.name} ({where}). It would empty the "
            "workload's marker table, reload its data or start a second set of clerks. Scenarios attach to the "
            "running workload in the next story; until then run them after the workload has stopped "
            "(python -m resilience_tests.execution.workload.service.cli stop --env <env>).")


def _driver_host_lock_held(profile: EnvProfile, node: Node) -> str | None:
    """The scenarios' lock file on this harness host (orchestrator._acquire_target_lock), tested
    without keeping it: the holder's text if a run holds it."""
    path = Path(profile.driver_host.run_dir).expanduser() / f".lock-{profile.name}-{node.name}"
    if not path.exists():
        return None
    with path.open("a+") as fh:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            if exc.errno not in (errno.EACCES, errno.EAGAIN):
                raise
            fh.seek(0)
            return fh.read().strip() or "unknown run"
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
    return None


async def scenario_holding_target(profile: EnvProfile, node: Node) -> str | None:
    """A scenario run using this target (its lock on this harness host, or on the target host),
    or None."""
    local = _driver_host_lock_held(profile, node)
    if local:
        return f"run {local} on this harness host"
    spec = profile.safety.target_lock
    if spec is None:
        return None
    holder = await remote_lock_holder(node, lock_path(node, spec), spec.stale_after_s)
    if holder is not None:
        return (f"run {holder.get('run_id')} ({holder.get('scenario')}) from {holder.get('harness_host')}, "
                f"lock {holder.get('lock')}")
    return None


def process_tree_rss_bytes(pid: int) -> int | None:
    """Resident memory of a process and its children (the worker processes), from /proc; None
    when it cannot be read."""
    def rss(p: int) -> int:
        with open(f"/proc/{p}/status") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
        return 0

    try:
        total = rss(pid)
    except OSError:
        return None
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/stat") as fh:
                ppid = int(fh.read().rsplit(")", 1)[1].split()[1])
            if ppid == pid:
                total += rss(int(entry))
        except (OSError, ValueError, IndexError):
            continue
    return total
