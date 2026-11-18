"""Python functions to drive the common workload (specs/007), for a runner that starts the workload,
runs scenarios against it and stops it. The command line (cli.py) uses the same functions.

    folder = start("e2-u22-3", "workloads/oltp-common.yaml")     # returns at once; runs in the background
    wait_until_running(folder)                                    # past its warm-up: scenarios may start
    info(folder)                                                  # service.json: settings used, times, files
    status(folder)                                                # status.json: live state
    summary = stop(folder)                                        # stop, wait for the end checks

For a scenario attached to the running workload:

    mark(folder, "run_start", run_id=..., scenario="NL-N-I03a")  # also "t0", "t1", "run_end"
    window(folder, from_wall, to_wall)                            # a window's figures (MeasuredWindow fields)

Every function works from the run folder, so a runner keeps only that path.
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from resilience_tests.control.profile import EnvProfile, Node, load_profile
from resilience_tests.execution.workload.service import state as st
from resilience_tests.execution.workload.service.config import load_config

REPO_ROOT = Path(__file__).resolve().parents[4]
STOP_WAIT_S = 3600.0          # the end checks read every marker and run the TPC-C rules: minutes on large data
POLL_S = 1.0


class ServiceFailed(RuntimeError):
    """The workload ended (failed, stopped or finished) or its process is gone."""


def node_of(env: EnvProfile, name: str | None) -> Node:
    return env.node(name) if name else env.nodes[0]


def start(env_name: str, config_path: str | Path, *, node: str | None = None, out: str | Path | None = None,
          runner_module: str = "resilience_tests.execution.workload.service.cli") -> Path:
    """Check the settings file, refuse if a workload from this harness host already runs on the node,
    then start the run in the background (its own session: it outlives the caller's terminal). Returns
    the run folder at once; the start-up (checks, data step, clerks, warm-up) goes on in the background."""
    env = load_profile(env_name)
    n = node_of(env, node)
    config = load_config(config_path)
    running = running_folder(env, n)
    if running is not None:
        raise ServiceFailed(f"a common workload is already running on {n.name}: {running}")
    base = Path(out).expanduser() if out else Path(env.driver_host.run_dir).expanduser()
    folder = base / f"workload-{config.id}-{time.strftime('%Y%m%d-%H%M%S')}"
    folder.mkdir(parents=True, exist_ok=False)
    cmd = [sys.executable, "-m", runner_module, "_run", "--env", env_name,
           "--config", str(Path(config_path).expanduser().resolve()), "--folder", str(folder)]
    if node:
        cmd += ["--node", node]
    with (folder / st.LOG_FILE).open("ab") as log:
        subprocess.Popen(cmd, stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT,
                         start_new_session=True, cwd=str(REPO_ROOT))
    return folder


def running_folder(env: EnvProfile, node: Node) -> Path | None:
    """The run folder of the workload running on the node (started from this harness host), or None."""
    pointer = st.read_pointer(env, node)
    if pointer is None or not st.pid_alive(pointer.get("pid")):
        return None
    return Path(pointer["folder"])


def status(folder: str | Path) -> dict[str, Any] | None:
    return st.read_status(Path(folder))


def info(folder: str | Path) -> dict[str, Any] | None:
    """service.json (None until the clerks run)."""
    return st.read_info(Path(folder))


def alive(folder: str | Path) -> bool:
    s = status(folder)
    return bool(s) and st.pid_alive(s.get("pid"))


def wait_until_running(folder: str | Path, timeout_s: float, *, poll_s: float = POLL_S) -> dict[str, Any]:
    """Wait until the workload is past its warm-up (state `running`). Raises ServiceFailed if it
    ends or its process disappears first, TimeoutError if it is not running within `timeout_s`
    (the start-up may include a data load: give it that long)."""
    folder = Path(folder)
    deadline = time.monotonic() + timeout_s
    while True:
        s = status(folder)
        if s is not None:
            if s.get("state") == st.RUNNING:
                return s
            if s.get("state") in st.FINAL_STATES:
                raise ServiceFailed(f"the workload ended before it was running: {s.get('state')}"
                                    + (f" ({s['failure']})" if s.get("failure") else ""))
            if not st.pid_alive(s.get("pid")):
                raise ServiceFailed(f"the workload's process is gone (last state {s.get('state')!r}); see "
                                    f"{folder / st.LOG_FILE}")
        if time.monotonic() >= deadline:
            raise TimeoutError(f"the workload is not running after {timeout_s:.0f} s "
                               f"(state {s.get('state') if s else 'unknown'!r}, step {s.get('step') if s else '-'!r})")
        time.sleep(poll_s)


def stop(folder: str | Path, *, by: str = "runner", wait_s: float = STOP_WAIT_S,
         poll_s: float = POLL_S) -> dict[str, Any] | None:
    """Ask the workload to stop and wait for its end checks. Returns summary.json (None if it did
    not finish within `wait_s`; it is then still stopping)."""
    folder = Path(folder)
    s = status(folder)
    pid = s.get("pid") if s else None
    if s and s.get("state") not in st.FINAL_STATES and st.pid_alive(pid):
        st.request_stop(folder, by)
    deadline = time.monotonic() + wait_s
    while time.monotonic() < deadline:
        s = status(folder)
        if (s and s.get("state") in st.FINAL_STATES) or not st.pid_alive(pid):
            break
        time.sleep(poll_s)
    s = status(folder)
    if not s or s.get("state") not in st.FINAL_STATES:
        return None
    return summary(folder)


def summary(folder: str | Path) -> dict[str, Any] | None:
    import json
    try:
        return json.loads((Path(folder) / st.SUMMARY_FILE).read_text())
    except (OSError, json.JSONDecodeError):
        return None


def mark(folder: str | Path, kind: str, *, run_id: str, scenario: str, t_wall: float | None = None,
         **extra: Any) -> dict[str, Any]:
    """A scenario mark in the running workload's folder (run_start, t0, t1, run_end): the workload
    judges its own baseline outside the scenarios' windows and reports each window separately."""
    return st.write_mark(Path(folder), kind, run_id=run_id, scenario=scenario, t_wall=t_wall, **extra)


def window(folder: str | Path, from_wall: float, to_wall: float) -> dict[str, Any]:
    """The figures of the window (from_wall, to_wall] from the running workload's 1-second samples:
    the fields of driver.MeasuredWindow plus how they were computed (windows.py)."""
    from resilience_tests.execution.workload.service.windows import window_from_file
    return window_from_file(Path(folder), from_wall, to_wall)
