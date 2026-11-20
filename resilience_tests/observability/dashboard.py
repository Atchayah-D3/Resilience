"""The framework's Grafana dashboard, built from the same queries a run copies into metrics.json
(metrics.py) -- so what you see live and what a run keeps are the same series. One dashboard for
every scenario: rows by topic, the run's markers on every graph (observer.py's annotations).

    python -m resilience_tests.observability.dashboard --out infra/monitoring/grafana/dashboards/resilience-run.json

The committed JSON must equal this output (tests/test_observability.py), so edit the queries or
this layout, never the JSON by hand. Grafana loads it through file provisioning
(infra/monitoring/grafana/provisioning/dashboards/resilience.yaml).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from resilience_tests.observability.metrics import RATE, SNAPSHOT_QUERIES

UID = "resilience-run"
DATASOURCE = {"type": "prometheus", "uid": "prometheus"}
SEL = 'env="$env",node="$node"'

# (row title, [(query name, panel title, unit)]) -- every name must be one of SNAPSHOT_QUERIES
ROWS: tuple[tuple[str, tuple[tuple[str, str, str], ...]], ...] = (
    ("Database", (
        ("commits_per_s", "Commits / s", "ops"),
        ("rollbacks_per_s", "Rollbacks / s", "ops"),
        ("sessions_by_state", "Sessions by state", "short"),
        ("locks_by_mode", "Locks by mode (a waiting ALTER shows as AccessExclusiveLock)", "short"),
        ("longest_transaction_s", "Longest open transaction", "s"),
        ("cache_hit_ratio", "Cache hit ratio", "percentunit"),
        ("database_up", "Database up (1 = reachable)", "short"),
        ("database_start_time_s", "Server start time (a step = a restart)", "dateTimeAsIso"),
    )),
    ("WAL, checkpoints and temp files", (
        ("wal_size_bytes", "WAL size", "bytes"),
        ("checkpoints_per_min", "Checkpoints / min", "short"),
        ("temp_bytes_per_s", "Temp bytes written / s", "Bps"),
        ("deadlocks_per_s", "Deadlocks / s", "ops"),
    )),
    ("CPU and memory", (
        ("cpu_busy_pct", "CPU busy %", "percent"),
        ("cpu_iowait_pct", "CPU iowait %", "percent"),
        ("cpu_steal_pct", "CPU steal % (time the hypervisor gave away)", "percent"),
        ("memory_available_bytes", "Memory available", "bytes"),
    )),
    ("Disk and network", (
        ("disk_write_latency_ms", "Disk write latency (per write)", "ms"),
        ("disk_busy_pct", "Disk busy %", "percent"),
        ("disk_queue_depth", "Disk queue (I/O requests waiting or in service, average)", "short"),
        ("io_pressure", "I/O pressure (share of time stalled; empty where the kernel has no PSI)", "percentunit"),
        ("network_receive_bytes_per_s", "Network receive", "Bps"),
        ("network_transmit_bytes_per_s", "Network transmit", "Bps"),
    )),
)


# The run's markers, one switchable, coloured layer per kind of event (observer.py tags every
# marker with its event kind; the whole-run band with "run"). A layer shows a marker only when
# it carries all of its tags, so each layer holds exactly one kind for the selected env and node.
# (layer name, colour, the tag that selects it) -- every kind observer.MARKED sends has a layer.
ANNOTATION_LAYERS: tuple[tuple[str, str, str], ...] = (
    ("Whole run", "#8e8e8e", "run"),
    ("Run start", "blue", "run_start"),
    ("Phases", "blue", "phase_start"),
    ("Warm-up", "blue", "warmup"),
    ("T0: fault injected", "red", "t0"),
    ("T1: recovery from here", "red", "t1"),
    ("Fault milestones", "orange", "fault_milestone"),
    ("Fault did not land", "orange", "not_landed"),
    ("Hold start", "yellow", "hold_start"),
    ("Hold end", "yellow", "hold_end"),
    ("Cycle recovered", "purple", "cycle_recovered"),
    ("Cycle NOT recovered", "purple", "cycle_not_recovered"),
    ("Cleanup incomplete", "purple", "cleanup_incomplete"),
)


def _layer(name: str, colour: str, tag: str) -> dict[str, Any]:
    return {"name": name, "enable": True, "hide": False, "iconColor": colour,
            "datasource": {"type": "grafana", "uid": "-- Grafana --"},
            "target": {"type": "tags", "tags": ["resilience", "$env", "$node", tag], "matchAny": False,
                       "limit": 500}}


def _expr(name: str) -> str:
    template = dict(SNAPSHOT_QUERIES)[name]
    return template.format(sel=SEL, db="$db", rate=RATE)


# The harness's own database on every lab target (env profiles: node.db.dbname). The run report's
# link selects the run's database explicitly; this is only what the dashboard opens with.
DEFAULT_DB = "resilience"


def _variable(name: str, label: str, query: str, *, regex: str = "", default: str | None = None) -> dict[str, Any]:
    current = {"text": default, "value": default, "selected": True} if default else {}
    return {"name": name, "label": label, "type": "query", "datasource": DATASOURCE, "query": query,
            "definition": query, "refresh": 2, "includeAll": False, "multi": False, "sort": 1,
            "regex": regex, "current": current, "hide": 0}


def build() -> dict[str, Any]:
    panels: list[dict[str, Any]] = []
    pid, y = 1, 0
    for title, items in ROWS:
        panels.append({"id": pid, "type": "row", "title": title, "collapsed": False,
                       "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}, "panels": []})
        pid, y = pid + 1, y + 1
        for i, (name, panel_title, unit) in enumerate(items):
            panels.append({
                "id": pid, "type": "timeseries", "title": panel_title, "datasource": DATASOURCE,
                "gridPos": {"h": 8, "w": 12, "x": 12 * (i % 2), "y": y + 8 * (i // 2)},
                "fieldConfig": {"defaults": {"unit": unit}, "overrides": []},
                "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
                "targets": [{"refId": "A", "datasource": DATASOURCE, "expr": _expr(name), "legendFormat": "__auto"}],
            })
            pid += 1
        y += 8 * ((len(items) + 1) // 2)
    return {
        "uid": UID, "title": "Resilience run", "tags": ["resilience"], "editable": False,
        "timezone": "browser", "refresh": "5s", "schemaVersion": 39, "version": 1,
        "time": {"from": "now-30m", "to": "now"},
        "description": "One dashboard for every scenario. Context only: the harness decides pass/fail "
                       "(Arch §7.1). Markers are the harness's own run events, one switchable layer per "
                       "kind (top of the dashboard): grey = the whole run, blue = phases, red = T0/T1, "
                       "orange = the fault's own milestones, yellow = hold, purple = cycles and cleanup.",
        "templating": {"list": [
            _variable("env", "environment", "label_values(up, env)"),
            _variable("node", "node", 'label_values(up{env="$env"}, node)'),
            # template databases hold no data worth watching
            _variable("db", "database", f'label_values(pg_stat_database_xact_commit{{{SEL}}}, datname)',
                      regex="/^(?!template[0-9]+$).+$/", default=DEFAULT_DB),
        ]},
        "annotations": {"list": [_layer(*layer) for layer in ANNOTATION_LAYERS]},
        "panels": panels,
    }


def render() -> str:
    return json.dumps(build(), indent=2, sort_keys=True) + "\n"


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--out", help="write here instead of standard output")
    args = ap.parse_args(argv)
    if args.out:
        Path(args.out).write_text(render())
        print(f"wrote {args.out}")
    else:
        sys.stdout.write(render())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
