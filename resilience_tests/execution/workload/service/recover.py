"""Check a run again whose service process died before its summary (specs/007-common-oltp-workload).

    python -m resilience_tests.execution.workload.service.cli check --env e2-u22-3 --folder <run folder>

A run whose process was killed after its clerks ran (on 19 Nov 2026: out of memory in the old
in-memory end checks) still has all its evidence in the folder: the journals, the 1-second
samples, the events, service.json; and, if it got that far, the database's markers saved right
after the run (markers_db.txt.gz). This rebuilds the run's numbers from those files and runs the
end checks and the baseline rules exactly as the run would have, then writes summary.json and
summary.txt, marked as redone.

Never a false result:
  - refused while the run's process is alive, while a common workload or a scenario uses the
    target, or while clerks of a workload are still connected (they would still be changing data)
  - lost commits: compared with markers_db.txt.gz when it exists (the database as it was right
    after the run); otherwise with resilience.markers, only if no marker is newer than the run
  - TPC-C checks (orders, payments, consistency rules) read the data as it is NOW, so they run
    only when the marker table is exactly as it was after the run (a later workload or scenario
    empties it); otherwise they are NOT MEASURED, with the reason
  - what the files do not hold is NOT MEASURED (e.g. harness memory readings of a run started
    before they were saved in events.jsonl)
"""

from __future__ import annotations

import datetime
import json
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from catalog.schema import Workload
from resilience_tests.control.profile import EnvProfile, Node
from resilience_tests.execution.workload import contract
from resilience_tests.execution.workload.markers import DB_MARKERS_FILE
from resilience_tests.execution.workload.service import endchecks
from resilience_tests.execution.workload.service import state as st
from resilience_tests.execution.workload.service.config import ServiceConfig
from resilience_tests.execution.workload.service.metrics import OutageTracker, Second

MARKER_CLOCK_SLACK_S = 300.0     # database and harness clocks may differ; a newer marker is a later run's


class CheckRefused(RuntimeError):
    """The run cannot be checked again now (the reason says why and what to do)."""


class _NoLock:
    lost = None

    async def release(self) -> dict[str, Any]:
        return {"released": False, "note": "not taken by the check command"}


def _history_since(text: Any) -> datetime.datetime | None:
    if not text or text == "None":
        return None
    return datetime.datetime.fromisoformat(str(text))


def _events(folder: Path) -> dict[str, Any]:
    """What the summary needs from events.jsonl, read line by line."""
    out: dict[str, Any] = {"measure_start_s": None, "stopped": None, "memory": [], "max_cpu_share": 0.0,
                           "driver_limited": False}
    path = folder / st.EVENTS_FILE
    if not path.exists():
        return out
    with path.open("rb") as fh:
        for raw in fh:
            try:
                e = json.loads(raw)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if not isinstance(e, dict):
                continue
            src, kind, d = e.get("source"), e.get("kind"), e.get("data") or {}
            if src == "service" and kind == "measure_start":
                out["measure_start_s"] = float(d.get("elapsed_s"))
            elif src == "service" and kind == "stopped":
                out["stopped"] = d
            elif src == "service" and kind == "memory":
                out["memory"].append((float(d["elapsed_s"]), int(d["rss_bytes"])))
            elif src == "workload" and kind == "driver_load":
                out["max_cpu_share"] = max([out["max_cpu_share"], *d.get("cpu_share", [])])
            elif src == "workload" and kind == "fatal" and d.get("driver_limited"):
                out["driver_limited"] = True
    return out


async def check_folder(env: EnvProfile, node: Node, folder: Path, *, adapter: Any = None,
                       target_check: Callable[..., Any] | None = None,
                       say: Callable[[str], None] | None = None) -> dict[str, Any]:
    """Run the end checks and the baseline rules of a run whose process died; write its summary."""
    from resilience_tests.execution.workload.service.runner import ServiceRunner, summary_text  # noqa: F401
    say = say or (lambda _s: None)
    folder = Path(folder).expanduser()
    info = st.read_info(folder)
    status = st.read_status(folder)
    if info is None:
        raise CheckRefused(f"{folder} has no {st.INFO_FILE}: the clerks never started, so there is nothing to check")
    if (folder / st.SUMMARY_FILE).exists():
        raise CheckRefused(f"{folder} already has its summary ({st.SUMMARY_FILE})")
    if status and st.pid_alive(status.get("pid")):
        raise CheckRefused(f"the run's process {status.get('pid')} is still alive (state {status.get('state')!r}); "
                           "wait for it, or stop it with the stop command")

    cfg = ServiceConfig(**info["settings"]["config"])
    workload_used = Workload.model_validate(info["settings"]["workload_used"])
    r = ServiceRunner(env, node, cfg, folder, adapter=adapter, lock=_NoLock())
    adapter = r.adapter
    say("checking the target")
    if target_check is None:
        from resilience_tests.execution.workload.tpcc.load import check_target
        target_check = check_target
    hostname = await target_check(env, node, adapter, writes=False)
    if await st.running_workload(env, node) is not None:
        raise CheckRefused(f"a common workload is running on {node.name}; it has emptied the marker table of the run "
                           "being checked. Check only before the next workload starts.")
    holder = await st.scenario_holding_target(env, node)
    if holder:
        raise CheckRefused(f"a scenario run uses {node.name} ({holder}); check again when it has finished")

    # --- which evidence of the database to compare with ------------------------------------------
    events = _events(folder)
    stopped = events["stopped"]
    ended_wall = (info["clerks_started_wall"] + float(stopped["ran_s"])) if stopped else (
        float(status.get("updated_wall")) if status and status.get("updated_wall") else time.time())
    saved = folder / DB_MARKERS_FILE
    pre = await endchecks.open_check(adapter)
    try:
        clerks = await pre.sessions_named(contract.WORKLOAD_APPLICATION_NAME)
        if clerks:
            raise CheckRefused(f"{clerks} clerk connection(s) named {contract.WORKLOAD_APPLICATION_NAME!r} are still "
                               "open on the target: a workload is still changing the data")
        live = await pre.database_markers_digest()
        newest = await pre.newest_database_marker_wall()
    finally:
        await pre.close()
    if saved.exists():
        file = endchecks.file_digest(saved)
        unchanged = file["count"] == live["count"] and file["sha256"] == live["sha256"]
        db_note = ("the database's markers as saved right after the run (markers_db.txt.gz); resilience.markers "
                   + ("is still exactly the same" if unchanged else
                      f"has changed since ({live['count']} markers now, {file['count']} then)"))
        markers_from_file = True
    else:
        if newest is not None and newest > ended_wall + MARKER_CLOCK_SLACK_S:
            raise CheckRefused("markers_db.txt.gz was not saved and resilience.markers holds markers newer than the run "
                               "(a later workload or scenario wrote it): the lost commits can no longer be checked")
        unchanged = True
        db_note = "resilience.markers (no marker newer than the run)"
        markers_from_file = False

    tpcc = None
    tpcc_skip = None
    if cfg.workload.profile == "tpcc":
        if unchanged:
            t = info.get("tpcc") or {}
            tpcc = endchecks.TpccInputs(orders_before=t.get("orders_at_start"),
                                        history_since=_history_since(t.get("history_since")),
                                        warehouses=int(workload_used.tpcc.warehouses))
        else:
            tpcc_skip = ("the database has changed since the run (its marker table differs from the copy saved after "
                         "the run), so its TPC-C data no longer shows the run's end state")

    say("end checks (this reads every journal line; large runs take several minutes)")
    result = await endchecks.run_end_checks(adapter, folder, tpcc=tpcc, markers_from_file=markers_from_file, step=say)
    end = result.end
    if tpcc_skip:
        end["_not_measured"] = {m: tpcc_skip for m in ("partial_commits", "aborted_markers_present",
                                                       "tpcc_consistency_violations", "indeterminate_txn")}

    # --- the run's numbers, rebuilt from its files -----------------------------------------------
    say("rebuilding the run's numbers from metrics-1s.jsonl and events.jsonl")
    if hasattr(r.profile, "workload"):
        r.profile.workload = workload_used          # sizes and rate as the run used them
    r.started_wall = float(info["started_wall"])
    r.clerks_started_wall = float(info["clerks_started_wall"])
    r.t0 = 0.0
    r.measure_start_s = events["measure_start_s"]
    r.outages = OutageTracker(workload_used.rate_tps, cfg.no_commit_alert_s)
    for line in st.read_complete_lines(folder / st.METRICS_1S_FILE):
        sample = {k: v for k, v in line.items() if k not in ("t_wall", "elapsed_s", "part")}
        second = Second.from_sample(sample, float(line["elapsed_s"]), float(line["t_wall"]))
        r.seconds.append(second)
        for kind, n in (sample.get("error_kinds") or {}).items():
            r.error_kinds[kind] = r.error_kinds.get(kind, 0) + int(n)
        r.outages.update(second, float(line["t_wall"]))
    r.memory = events["memory"]
    r.max_cpu_share = events["max_cpu_share"]
    r.driver_limited = events["driver_limited"]
    r._lost_t_ack = result.lost_t_ack
    r.facts = {**(info.get("start_facts") or {"hostname": hostname}), **result.facts}
    r.disclosures = list(info.get("disclosures") or
                         ["the start's disclosures were not saved by this run's version (service.json); see the "
                          "settings below"])
    if stopped:
        r.ended_s = float(stopped["ran_s"])
        before = str(stopped.get("state_before"))
        final = st.FINISHED if before == "finished" else st.FAILED if before == "failed" else st.STOPPED
        r.stop_by = None if final in (st.FINISHED, st.FAILED) else before
        r.failure = (status or {}).get("failure") if final == st.FAILED else None
        why = f"the service process died during its end checks (last state {(status or {}).get('state')!r})"
    else:
        r.ended_s = r.seconds[-1].elapsed_s if r.seconds else 0.0
        final = st.FAILED
        r.failure = (f"the service process died while the clerks ran (last state {(status or {}).get('state')!r}, "
                     f"at about {r.ended_s:.0f} s)")
        why = r.failure
    if not r.memory:
        r.disclosures.append("harness memory readings were not saved by this run's version: the memory rule is "
                             "not measured")
    r.summary_extra = {
        "settings": info["settings"],
        "recovered": {"checked_at": time.strftime("%Y-%m-%d %H:%M:%S"), "why": why,
                      "database_markers": db_note, "tpcc_checks": "run" if tpcc else (tpcc_skip or "not a TPC-C run"),
                      "by": "python -m resilience_tests.execution.workload.service.cli check"},
    }
    summary = r._summary(final, end)
    st.write_status(folder, {**(status or {}), "state": final, "step": "end checks redone by the check command",
                             "pid": None, "updated_wall": round(time.time(), 3)})
    st.remove_pointer(env, node, folder)
    return summary
