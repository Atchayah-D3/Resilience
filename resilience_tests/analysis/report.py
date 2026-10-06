"""Run outputs (Arch §10.4). Phase 1 requires only the results file (Arch §16); the JUnit XML
CI gate comes from pytest --junitxml; the HTML report, MinIO evidence bundle and trend store
arrive with Phases 2 and 6.

The evidence directory (Arch §10.4 "Audit: probe stream, logs, metric snapshot, amcheck
output, marker journals") is the run directory on the driver host.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

RESULTS_FILE = "results.json"
SUMMARY_FILE = "summary.txt"


def write_results(run_dir: Path, results: dict[str, Any]) -> Path:
    path = run_dir / RESULTS_FILE
    tmp = path.with_suffix(".tmp")
    with tmp.open("w") as fh:
        json.dump(results, fh, indent=2, sort_keys=True, default=str)
        fh.flush()
        os.fsync(fh.fileno())
    tmp.replace(path)
    (run_dir / SUMMARY_FILE).write_text(render_summary(results))
    return path


def render_summary(r: dict[str, Any]) -> str:
    lines = [
        f"{r['scenario']['id']}  {r['scenario']['name']}",
        f"run {r['run_id']}  env {r['environment']['profile']} ({r['environment']['class']})  node {r['target']['node']}",
        f"STATUS: {r['status'].upper()}",
        "",
    ]
    if r.get("error"):
        lines += [f"error: {r['error']}", ""]
    lines.append("phases:")
    for p in r.get("phases", []):
        lines.append(f"  {p['phase']:<13} {p['outcome']:<9} {p.get('duration_s', 0):8.1f}s  {p.get('error') or ''}")
    verdict = r.get("verdict")
    if verdict:
        lines += ["", "acceptance (evaluated mechanically):"]
        for res in verdict["results"]:
            margin = "" if res.get("margin") is None else f"  margin {res['margin']:+.3f}"
            reason = f"  ({res['reason']})" if res.get("reason") else ""
            lines.append(f"  [{res['outcome'].upper():>14}] {res['predicate']:<28} {res['values']}{margin}{reason}")
    cycles = (r.get("facts") or {}).get("cycles")
    if cycles:
        # For a repeated scenario the shape of the trend IS the finding: one ratio cannot show
        # whether recovery crept up, jumped once, or stayed flat (Framework NL-C-05).
        lines += ["", "per cycle (T0 -> first accepted write):"]
        for c in cycles:
            got = isinstance(c.get("recovery_s"), (int, float))
            when = f"{c['recovery_s']:8.2f}s" if got else "       --"
            note = "" if got else ("  no outage seen" if not c.get("outage_observed")
                                   else "  never recovered inside its window")
            redo, rate = c.get("redo_distance_bytes"), c.get("replay_bytes_per_s")
            work = f"  replayed {redo / 1024:>8.0f} KB" if redo is not None else ""
            work += f"  ({rate / 1024:.0f} KB/s)" if rate else ""
            lines.append(f"  cycle {c['cycle']:>3}  {when}{work}{note}")
        trend = (r.get("facts") or {}).get("recovery_trend") or {}
        if trend.get("slope_s_per_cycle") is not None:
            lines.append(f"  trend       {trend['slope_s_per_cycle']:+8.3f}s per cycle "
                         f"(median {trend['median_s']:.2f}s, max {trend['max_s']:.2f}s)")
        bloat = (r.get("facts") or {}).get("bloat") or {}
        if bloat.get("bloat_ratio") is not None:
            lines.append(f"  footprint [{bloat.get('measured_on', '?')}]  "
                         f"{bloat['bytes_per_live_row_first']:.1f} -> "
                         f"{bloat['bytes_per_live_row_last']:.1f} bytes per live row "
                         f"(+{bloat['rows_added']} rows, {bloat['bytes_unexplained_by_rows']:+.0f} B unexplained)")
    elle = (r.get("facts") or {}).get("elle")
    if elle:
        lines += [
            "",
            f"elle consistency check ({elle.get('checker', 'checker')}):",
            f"  valid: {elle.get('valid')}  anomalies: {elle.get('anomalies_count')}",
        ]
    cp = (r.get("facts") or {}).get("checkpoint_injection")
    if cp:
        w_event = cp.get("wait_event")
        w_type = cp.get("wait_event_type")
        if w_event and w_type:
            event_desc = f"{w_event} ({w_type})"
        elif w_event or w_type:
            event_desc = str(w_event or w_type)
        else:
            event_desc = "none (checkpointer running, not waiting)"

        verification = (r.get("facts") or {}).get("checkpoint_verification") or {}
        lines += [
            "",
            "checkpoint fault injection (NL-C-02):",
            f"  checkpointer seen active before kill: {cp.get('checkpointer_active')}",
            f"  checkpointer pid: {cp.get('checkpointer_pid')}",
            f"  wait event: {event_desc}",
        ]
        if verification:
            lines += [
                f"  checkpoint still in flight at kill: {verification.get('in_flight')}",
                f"    redo point before CHECKPOINT: {verification.get('prior_redo_lsn')}  "
                f"recovery redo started at: {verification.get('recovery_redo_start_lsn')}",
                f"    {verification.get('note', '')}",
            ]
        if cp.get("buffers_written_during_cp") is not None:
            lines.append(f"  stat counter delta (pg_stat_checkpointer.buffers_written): {cp['buffers_written_during_cp']}")
        if cp.get("io_writes_during_cp") is not None:
            lines.append(f"  io writes delta (pg_stat_io): {cp['io_writes_during_cp']}")
    idle = (r.get("facts") or {}).get("idle_transaction")
    if idle:
        facts = r.get("facts") or {}
        idle_chk = facts.get("idle_transaction_check") or {}
        evidence = facts.get("idle_timeout_evidence") or {}
        probe = facts.get("vacuum_horizon_probe") or {}
        measured = r.get("measured") or {}
        lines += [
            "",
            "idle-in-transaction vacuum blocking (NL-M-05):",
            f"  idle backend pid: {idle.get('pid')}  state at injection: {idle.get('state')}  "
            f"backend_xid: {idle.get('backend_xid')}",
            f"  backend_xmin: {idle.get('backend_xmin')}",
            f"  path A  session ended: {idle_chk.get('terminated_by_timeout')}  "
            f"timeout log line: {'seen' if evidence.get('log_line') else 'not seen'}  "
            f"sqlstate: {evidence.get('sqlstate')}  -> timeout enforced: "
            f"{measured.get('idle_in_transaction_session_timeout_enforced')}",
            f"  path B  vacuum blocked: {measured.get('vacuum_blocked')}  "
            f"dead but not removable: {probe.get('dead_not_removable')}  "
            f"removable cutoff: {probe.get('removable_cutoff')}  "
            f"bloat alert fired: {measured.get('bloat_alert_fired')}",
        ]
    corrupted = (r.get("facts") or {}).get("data_corruption")
    if corrupted:
        facts = r.get("facts") or {}
        attempts = (facts.get("corruption_read") or {}).get("attempts") or []
        first = attempts[0] if attempts else {}
        checks = corrupted.get("pg_checksums") or {}
        lines += [
            "",
            "data-file corruption (NL-I):",
            f"  injected: {corrupted.get('relation')} block {corrupted.get('block')} "
            f"({corrupted.get('relation_path')}), byte {corrupted.get('original_byte')} -> "
            f"{corrupted.get('written_byte')}, after a {corrupted.get('cluster_state')!r} stop",
            f"  pg_checksums before restart: {checks.get('failures')}",
            f"  first read: {first.get('sqlstate') or 'no error'}  {first.get('message') or ''}"
            + (f"  ({first.get('rows')} rows returned)" if first.get("rows") is not None else ""),
            f"  pg_amcheck on the damaged relation: detected={(facts.get('amcheck_on_corrupted_relation') or {}).get('detected')}",
            f"  failures in the server log: {facts.get('corruption_log_locations')}",
        ]
    if r.get("measured"):
        rendered = []
        for k, v in sorted(r["measured"].items()):
            if v is None:
                rendered.append(f"  {k} = None (determination failed / not reached)")
            else:
                rendered.append(f"  {k} = {v}")
        lines += ["", "measured:"] + rendered
    deviations = (r.get("facts") or {}).get("config_deviations")
    if deviations:
        lines += [
            "",
            "config deviations (postgresql.auto.conf):",
            *[f"  {k} = {v}" for k, v in sorted(deviations.items())],
        ]
    if r.get("disclosures"):
        lines += ["", "disclosures (evidence limitations):"] + [f"  - {d}" for d in r["disclosures"]]
    return "\n".join(lines) + "\n"
