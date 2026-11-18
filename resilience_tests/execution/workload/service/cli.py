"""Start, watch and stop the common long-running workload (specs/007-common-oltp-workload).

    python -m resilience_tests.execution.workload.service.cli start  --env e2-u22-3 --config workloads/oltp-common.yaml
    python -m resilience_tests.execution.workload.service.cli status --env e2-u22-3
    python -m resilience_tests.execution.workload.service.cli stop   --env e2-u22-3

`start` checks the settings file, then runs the workload in the background (it keeps running
after the terminal closes) and returns. The run ends by itself after `duration_s`, or on `stop`.
Evidence: <driver_host.run_dir>/workload-<id>-<time>/ (status.json, metrics-1s.jsonl,
intervals-<n>s.jsonl, events.jsonl, the journals, summary.json, summary.txt, service.log).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import signal
import sys
import time
from pathlib import Path
from typing import Any

from resilience_tests.adapters import postgresql  # noqa: F401  (registers the adapter)
from resilience_tests.control.profile import EnvProfile, Node, load_profile
from resilience_tests.execution.workload.service import client
from resilience_tests.execution.workload.service import state as st
from resilience_tests.execution.workload.service.config import load_config

START_WATCH_S = 15.0          # `start` watches the background run this long for an early failure
STOP_WAIT_S = client.STOP_WAIT_S   # `stop` waits this long for the end checks (large data: several minutes)


def _node(env: EnvProfile, name: str | None) -> Node:
    return env.node(name) if name else env.nodes[0]


def _current(env: EnvProfile, node: Node) -> tuple[Path | None, dict[str, Any] | None, bool]:
    """(folder, status, process alive) of the run the pointer names."""
    pointer = st.read_pointer(env, node)
    if pointer is None:
        return None, None, False
    folder = Path(pointer["folder"])
    return folder, st.read_status(folder), st.pid_alive(pointer.get("pid"))


def _print_status(folder: Path, status: dict[str, Any] | None, alive: bool) -> None:
    if status is None:
        print(f"{folder}: no status yet")
        return
    final = status.get("state") in st.FINAL_STATES
    state = status.get("state", "?")
    if not final and not alive:
        state = f"DIED (the process is gone; last status was {state!r})"
    print(f"common workload {status.get('id')}: {state}  ({status.get('step')})")
    print(f"  folder   {folder}")
    if status.get("elapsed_s") is not None:
        print(f"  time     {status['elapsed_s']:.0f} s of {status['duration_s']:.0f} s "
              f"(warm-up {status['warmup_s']:.0f} s), {status.get('remaining_s', 0):.0f} s left")
    li = status.get("last_interval")
    if li:
        print(f"  last {li.get('duration_s', 0):.0f} s: {li.get('tps', 0):.1f} TPS (rate {status.get('rate_tps')}), "
              f"p99 {li.get('p99_ms') or 0:.1f} ms, errors {li.get('errors')}, unknown {li.get('indeterminate')}, "
              f"drops {li.get('drops')}, reconnects {li.get('reconnects')}")
    if status.get("error_kinds_so_far"):
        print(f"  errors   {json.dumps(status['error_kinds_so_far'])}")
    if status.get("outage_now"):
        o = status["outage_now"]
        print(f"  OUTAGE   no commit for {o.get('seconds', 0):.0f} s (connect failures {o.get('connect_failures')})")
    print(f"  outages  {status.get('outages', 0)} so far; detection {status.get('outage_detection')}")
    for a in status.get("alerts") or []:
        print(f"  ALERT    {a}")
    if status.get("failure"):
        print(f"  FAILURE  {status['failure']}")


def cmd_start(a: argparse.Namespace) -> int:
    try:
        folder = client.start(a.env, a.config, node=a.node, out=a.out)
    except client.ServiceFailed as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(f"common workload starting in the background; evidence in {folder}")
    deadline = time.monotonic() + START_WATCH_S
    status: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        status = st.read_status(folder)
        if status and (status.get("state") in st.FINAL_STATES or status.get("state") == st.RUNNING):
            break
        time.sleep(0.5)
    if status and status.get("state") in st.FINAL_STATES:
        _print_status(folder, status, False)
        print(f"the workload ended at its start; see {folder / st.SUMMARY_TEXT_FILE} and {folder / st.LOG_FILE}",
              file=sys.stderr)
        return 1
    _print_status(folder, status, client.alive(folder) or status is None)
    print("follow it with: python -m resilience_tests.execution.workload.service.cli status --env " + a.env)
    return 0


def cmd_status(a: argparse.Namespace) -> int:
    env = load_profile(a.env)
    node = _node(env, a.node)
    if a.folder:
        folder = Path(a.folder).expanduser()
        _print_status(folder, st.read_status(folder), True)
        return 0
    folder, status, alive = _current(env, node)
    if folder is None:
        print(f"no common workload is running on {node.name} (started from this harness host)")
        return 0
    _print_status(folder, status, alive)
    return 0


def stop_running(env: EnvProfile, node: Node, *, by: str, wait_s: float) -> tuple[Path | None, dict[str, Any] | None]:
    """Ask the running workload to stop and wait for its end checks. (folder, final status)."""
    folder = client.running_folder(env, node)
    if folder is None:
        return None, None
    client.stop(folder, by=by, wait_s=wait_s)
    return folder, st.read_status(folder)


def cmd_stop(a: argparse.Namespace) -> int:
    env = load_profile(a.env)
    node = _node(env, a.node)
    folder, status, alive = _current(env, node)
    if folder is None or not alive:
        print(f"no common workload is running on {node.name} (started from this harness host)")
        return 0
    print(f"stopping the common workload in {folder}; the end checks may take several minutes ...", flush=True)
    folder, status = stop_running(env, node, by="stop command", wait_s=a.wait)
    if not status or status.get("state") not in st.FINAL_STATES:
        print(f"it has not finished within {a.wait:.0f} s; it is still stopping (see status)", file=sys.stderr)
        return 1
    summary = folder / st.SUMMARY_TEXT_FILE if folder else None
    if summary and summary.exists():
        print(summary.read_text())
    return 0 if status.get("state") != st.FAILED else 1


def cmd_run(a: argparse.Namespace) -> int:
    """The background run itself (started by `start`)."""
    from resilience_tests.execution.workload.service.runner import ServiceRunner

    env = load_profile(a.env)
    node = _node(env, a.node)
    runner = ServiceRunner(env, node, load_config(a.config), Path(a.folder))

    async def go() -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            loop.add_signal_handler(sig, runner.request_stop, f"signal {sig.name}")
        return await runner.run()

    summary = asyncio.run(go())
    print(summary_line(summary), flush=True)
    return 0 if summary["state"] != st.FAILED else 1


def summary_line(summary: dict[str, Any]) -> str:
    return (f"{summary['state']}: baseline {'valid' if summary['baseline_valid'] else 'not valid'}"
            + (f"; {summary['failure']}" if summary.get("failure") else ""))


def _args(argv: list[str]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(prog="python -m resilience_tests.execution.workload.service.cli",
                                 description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="command", required=True)
    for name in ("start", "status", "stop", "_run"):
        p = sub.add_parser(name)
        p.add_argument("--env", required=True)
        p.add_argument("--node")
        if name in ("start", "_run"):
            p.add_argument("--config", required=True, help="the workload's settings file, e.g. workloads/oltp-common.yaml")
        if name == "start":
            p.add_argument("--out", help="where the run folder is made (default: driver_host.run_dir)")
        if name == "status":
            p.add_argument("--folder", help="show this run folder's status (e.g. a finished run)")
        if name == "stop":
            p.add_argument("--wait", type=float, default=STOP_WAIT_S, help="seconds to wait for the end checks")
        if name == "_run":
            p.add_argument("--folder", required=True)
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    a = _args(sys.argv[1:] if argv is None else argv)
    try:
        return {"start": cmd_start, "status": cmd_status, "stop": cmd_stop, "_run": cmd_run}[a.command](a)
    except (ValueError, OSError) as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
