"""Follow a run as it happens, in plain words -- from its own event log, so it works for every
scenario and never touches the target. Read only; it changes nothing and judges nothing.

    python -m resilience_tests.observability.watch            # the newest run (start it before or after the run)
    python -m resilience_tests.observability.watch --next     # wait for the next scenario run to start, then follow it
    python -m resilience_tests.observability.watch --keep     # follow every scenario run of a suite, one after another
    python -m resilience_tests.observability.watch --run-dir ~/resilience-runs/<run id>   # one run (also replays a finished one)

Prints each phase starting and ending, the workload every few seconds (TPS, p99, errors), the
baseline, T0 (fault injected), the fault's own milestones, the hold, T1 (recovery measured from
here), failed write probes (an outage), notable server log lines, and at the end the verdict,
the report and the Grafana link.
"""

from __future__ import annotations

import argparse
import datetime
import json
import re
import sys
import time
from pathlib import Path
from typing import Any

DEFAULT_ROOT = Path("~/resilience-runs").expanduser()
EVENTS = "events.jsonl"
# The write prober tries every 200 ms; one attempt that times out is a slow write, several in a
# row are an outage (the database not accepting writes).
OUTAGE_PROBES = 5
LOCK_WAIT_LOG = re.compile(r"still waiting for", re.IGNORECASE)
NOTABLE_LOG = re.compile(r"still waiting for|lock timeout|deadlock detected|PANIC|FATAL|terminated by signal|"
                         r"database system is (ready|shut down)|checkpoint starting: immediate", re.IGNORECASE)


def _clock(t_wall: float) -> str:
    return datetime.datetime.fromtimestamp(t_wall).strftime("%H:%M:%S")


def happened_at(e: dict[str, Any]) -> float:
    """When the moment an event reports happened: T1 and a fault's milestones are announced after
    they happen, and record when (the same rule as the dashboard markers, observer.marker_time)."""
    d = e.get("data") or {}
    for key in ("at_mono_ns", "t0_mono_ns", "t1_mono_ns"):
        when = d.get(key)
        if isinstance(when, int) and when > 0:
            return e["t_wall"] - (e["t_mono_ns"] - when) / 1e9
    return e["t_wall"]


class Narrator:
    """Turns events into lines; the workload's 1 s samples are summarised every `every_s`."""

    def __init__(self, every_s: float = 10.0) -> None:
        self.every_s = every_s
        self.phase_start: dict[str, float] = {}
        self._window: list[dict[str, Any]] = []
        self._window_start: float | None = None
        self._probe_failures = 0        # in a row
        self._slow_probes = 0           # single failed probes since the last summary
        self._outage = False
        self._lock_wait_lines = 0       # "still waiting" log lines since the last summary

    def line(self, e: dict[str, Any]) -> list[str]:
        src, kind, d, t = e["source"], e["kind"], e.get("data") or {}, e["t_wall"]
        at = _clock(happened_at(e))
        out: list[str] = []
        if src == "workload" and kind == "sample":
            return self._sample(t, d)
        if src == "write_prober" and kind == "write_probe":
            if not d.get("ok"):
                self._probe_failures += 1
                if self._probe_failures == OUTAGE_PROBES:
                    self._outage = True
                    out.append(f"{at}  !! OUTAGE: {OUTAGE_PROBES} write probes in a row failed ({d.get('error')}) -- "
                               "the database is not accepting writes")
            else:
                if self._outage:
                    out.append(f"{at}  WRITES ACCEPTED AGAIN after {self._probe_failures} failed probes")
                elif self._probe_failures:
                    self._slow_probes += self._probe_failures
                self._probe_failures, self._outage = 0, False
            return out
        if src == "log_tailer" and kind == "log_line":
            text = str(d.get("line", ""))
            if LOCK_WAIT_LOG.search(text):       # one line per waiting session: counted, shown once
                self._lock_wait_lines += 1
                if self._lock_wait_lines == 1:
                    return [f"{at}  SERVER LOG  sessions are waiting for a lock (log_lock_waits), e.g. "
                            f"{text.split('LOG:', 1)[-1].strip()[:110]}"]
                return []
            return [f"{at}  SERVER LOG  {text[:150]}"] if NOTABLE_LOG.search(text) else []
        if kind == "run_start":
            out.append(f"{at}  ===== RUN START  {d.get('scenario')}  {d.get('run_id')}")
        elif kind == "phase_start":
            self.phase_start[d.get("phase", "?")] = t
            out.append(f"{at}  >> PHASE {d.get('phase')} started")
        elif kind == "phase_end":
            began = self.phase_start.get(d.get("phase", "?"))
            took = f" after {t - began:.0f} s" if began else ""
            err = f": {d.get('error')}" if d.get("error") else ""
            out.append(f"{at}  << PHASE {d.get('phase')} {d.get('outcome')}{took}{err}")
        elif src == "workload" and kind == "start":
            out.append(f"{at}  WORKLOAD started: profile {d.get('profile')}, {d.get('concurrency')} workers, "
                       f"target {d.get('rate_tps')} TPS")
        elif src == "workload" and kind == "stop":
            out.append(f"{at}  WORKLOAD stopped")
        elif kind == "warmup":
            out.append(f"{at}  WARM-UP {d.get('seconds')} s: the workload runs, not measured yet")
        elif kind == "baseline":
            out.append(f"{at}  BASELINE measured: {d.get('tps', 0):.1f} TPS, p99 {d.get('p99_ms', 0):.0f} ms, "
                       f"errors {d.get('errors', 0)} (steady state is checked against the scenario's limits)")
        elif kind == "t0":
            detail = ", ".join(f"{k}={d[k]}" for k in ("fault", "blocker", "ddl", "ddl_lock_timeout_s", "ddl_after_s")
                               if k in d)
            out.append(f"{at}  ***** T0  FAULT INJECTED  {detail}")
        elif kind == "not_landed":
            out.append(f"{at}  !! the fault did NOT land: {d.get('reason') or d}")
        elif kind == "fault_milestone":
            out.append(f"{at}  ***** FAULT  {d.get('text')}")
        elif kind == "hold_start":
            out.append(f"{at}  HOLD start (at most {d.get('planned_s')} s)")
        elif kind == "hold_end":
            out.append(f"{at}  HOLD end: during the hold {d.get('tps', 0):.1f} TPS, p99 {d.get('p99_ms') or 0:.0f} ms")
        elif kind == "t1":
            out.append(f"{at}  ***** T1  RECOVERY MEASURED FROM HERE  {d.get('action') or ''}")
        elif kind in ("cycle_recovered", "cycle_not_recovered", "cleanup_incomplete"):
            out.append(f"{at}  {kind.upper().replace('_', ' ')}  {json.dumps(d)[:140]}")
        elif src == "service" and kind == "outage_start":
            out.append(f"{at}  !! the common workload sees no commit (outage started)")
        elif src == "service" and kind == "outage_end":
            out.append(f"{at}  the common workload commits again after {d.get('seconds', 0):.0f} s "
                       f"(dropped {d.get('drops', 0)}, failed connection attempts {d.get('connect_failures', 0)})")
        elif kind == "run_end":
            out.append(f"{at}  ===== RUN END")
        return out

    def _sample(self, t: float, d: dict[str, Any]) -> list[str]:
        if self._window_start is None:
            self._window_start = t
        self._window.append(d)
        if t - self._window_start < self.every_s:
            return []
        w, self._window, self._window_start = self._window, [], None
        tps = sum(x.get("tps") or 0 for x in w) / len(w)
        p99 = max((x.get("p99_ms") or 0) for x in w)
        errors = sum(x.get("errors") or 0 for x in w)
        unknown = sum(x.get("indeterminate") or 0 for x in w)
        extra = ""
        if self._slow_probes:
            extra += f"   slow write probes {self._slow_probes}"
        if self._lock_wait_lines:
            extra += f"   lock-wait log lines {self._lock_wait_lines}"
        self._slow_probes = self._lock_wait_lines = 0
        return [f"{_clock(t)}     workload: {tps:6.1f} TPS   p99 {p99:7.0f} ms   errors {errors}   unknown {unknown}{extra}"]


def verdict(run_dir: Path, wait_s: float = 120.0) -> list[str]:
    results = run_dir / "results.json"
    deadline = time.time() + wait_s
    while not results.exists() and time.time() < deadline:
        time.sleep(1)
    if not results.exists():
        return ["(results.json not written yet)"]
    r = json.loads(results.read_text())
    out = [f"STATUS: {r.get('status', '?').upper()}" + (f"   {r['error']}" if r.get("error") else "")]
    for rule in (r.get("verdict") or {}).get("results", []):
        mark = "PASS" if rule.get("outcome") == "pass" else rule.get("outcome", "?").upper()
        out.append(f"   [{mark:>12}] {rule.get('predicate')}   {rule.get('values')}")
    obs = (r.get("facts") or {}).get("observability") or {}
    out.append(f"report : {run_dir / 'report.html'}")
    if obs.get("grafana_link"):
        out.append(f"Grafana: {obs['grafana_link']}")
    return out


def follow(run_dir: Path, every_s: float, out=sys.stdout) -> None:
    events = run_dir / EVENTS
    while not events.exists():
        time.sleep(0.5)
    narrator = Narrator(every_s)
    print(f"following {run_dir}", file=out, flush=True)
    with events.open() as fh:
        buffer = ""
        while True:
            chunk = fh.readline()
            if not chunk:
                time.sleep(0.5)
                continue
            buffer += chunk
            if not buffer.endswith("\n"):
                continue                       # a line still being written
            e, buffer = json.loads(buffer), ""
            for text in narrator.line(e):
                print(text, file=out, flush=True)
            if e["kind"] == "run_end":
                for text in verdict(run_dir):
                    print(text, file=out, flush=True)
                return


# The common workload's own run folder (specs/007) also has an events.jsonl; it is not a scenario
# run, and a scenario attached to it copies its samples anyway. Told apart by the files the
# workload writes from its first second (service.log at once; status.json, service.json later):
# service.json alone is written only once its clerks run, after the data step.
WORKLOAD_RUN_MARKERS = ("service.log", "status.json", "service.json")


def is_workload_run(p: Path) -> bool:
    return p.name.startswith("workload-") or any((p / m).exists() for m in WORKLOAD_RUN_MARKERS)


def scenario_runs(root: Path) -> list[Path]:
    if not root.exists():
        return []
    return [p for p in root.iterdir() if p.is_dir() and (p / EVENTS).exists() and not is_workload_run(p)]


def workload_progress(root: Path) -> str | None:
    """While no scenario runs yet: what the newest common workload is doing (its status.json)."""
    folders = [p for p in root.iterdir() if p.is_dir() and is_workload_run(p)] if root.exists() else []
    if not folders:
        return None
    folder = max(folders, key=lambda p: p.stat().st_ctime)
    try:
        s = json.loads((folder / "status.json").read_text())
    except (OSError, ValueError):
        return f"common workload {folder.name}: starting"
    state = s.get("state")
    if state in ("stopped", "finished", "failed"):
        why = f": {s['failure']}" if s.get("failure") else ""
        return f"common workload {folder.name}: {state}{why} -- its end checks are done (summary.txt in its folder)"
    last = s.get("last_interval") or {}
    # figures only while its clerks run: after the stop, the last interval is no longer live
    figures = (f", {last['tps']:.1f} TPS, p99 {last.get('p99_ms') or 0:.0f} ms"
               if state in ("warming_up", "running") and isinstance(last.get("tps"), (int, float)) else "")
    return f"common workload {folder.name}: {state} ({s.get('step')}){figures}"


def in_progress(run_dir: Path) -> bool:
    """A scenario run that has not ended yet (no results.json)."""
    return not (run_dir / "results.json").exists()


def newest(root: Path) -> Path | None:
    """The scenario run most recently started (its folder's creation, not its last write: a
    finished run's report can be rewritten later)."""
    runs = scenario_runs(root)
    return max(runs, key=lambda p: p.stat().st_ctime) if runs else None


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--run-dir", type=Path, help="this run's folder (also replays a finished run)")
    ap.add_argument("--runs-root", type=Path, default=DEFAULT_ROOT, help="where runs are written (default ~/resilience-runs)")
    ap.add_argument("--next", action="store_true", help="wait for the next scenario run to start, then follow it")
    ap.add_argument("--keep", action="store_true",
                    help="after a run ends, wait for the next one and follow it too (every scenario of a suite); Ctrl-C ends")
    ap.add_argument("--every", type=float, default=10.0, help="seconds per workload summary line (default 10)")
    args = ap.parse_args(argv)
    if args.run_dir:
        try:
            follow(args.run_dir.expanduser(), args.every)
        except KeyboardInterrupt:
            return 130
        return 0
    root = args.runs_root.expanduser()
    before = newest(root) if (args.next or args.keep) else None
    if args.keep and before is not None and in_progress(before):
        before = None                          # started while a scenario runs: follow that one first
    try:
        while True:
            if args.next or args.keep or before is not None:
                print("waiting for the next scenario run to start ...", flush=True)
            said, last_said = None, 0.0
            while True:
                run_dir = newest(root)
                if run_dir is not None and (not (args.next or args.keep or before) or run_dir != before):
                    break
                progress = workload_progress(root)        # while the scenario waits for the workload
                ended = progress is not None and "end checks are done" in progress
                if progress and (progress.split(",")[0] != said or (time.time() - last_said >= 30 and not ended)):
                    print(f"{_clock(time.time())}  {progress}", flush=True)
                    said, last_said = progress.split(",")[0], time.time()
                    if ended and args.keep:
                        print("  (the session is over when pytest has printed its summary; Ctrl-C ends this watch)",
                              flush=True)
                time.sleep(1)
            follow(run_dir, args.every)
            if not args.keep:
                return 0
            before = run_dir
            print("", flush=True)
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
