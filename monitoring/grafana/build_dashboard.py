#!/usr/bin/env python3
"""Generates grafana/dashboards/rrb-overview.json. Edit the PANELS list, then run:
    python3 build_dashboard.py
Rows follow a request's path: load -> app pools -> database -> hosts, so a bottleneck shows
up as the first row where something saturates."""
import json
import os

DS = {"type": "prometheus", "uid": "rrb-prom"}
API = 'uri=~"/api/v1/.*"'

# (row title, [(panel title, unit, [(expr, legend)], stacked)])
PANELS = [
    ("Load and API (server side)", [
        ("Requests/s by endpoint", "reqps",
         [(f'sum by (uri) (rate(http_server_requests_seconds_count{{{API}}}[30s]))', "{{uri}}")], False),
        ("p99 latency by endpoint (server side)", "s",
         [(f'histogram_quantile(0.99, sum by (le, uri) (rate(http_server_requests_seconds_bucket{{{API}}}[1m])))',
           "{{uri}}")], False),
        ("Logins/s by outcome", "ops", [("sum by (outcome) (rate(rrb_login_total[30s]))", "{{outcome}}")], False),
        ("Errors/s by status (5xx, 4xx except 401)", "reqps",
         [(f'sum by (status) (rate(http_server_requests_seconds_count{{{API},status=~"5..|4..",status!="401"}}[30s]))',
           "{{status}}")], True),
    ]),
    ("App connection pools", [
        ("Threads waiting for a DB connection", "short",
         [("sum by (pool) (hikaricp_connections_pending)", "{{pool}}")], False),
        ("Active DB connections", "short",
         [("sum by (pool) (hikaricp_connections_active)", "{{pool}} active"),
          ("sum by (pool) (hikaricp_connections_max)", "{{pool}} max")], False),
        ("Pool timeouts and DB errors /s", "ops",
         [("sum by (pool) (rate(hikaricp_connections_timeout_total[1m]))", "timeout {{pool}}"),
          ("sum by (kind) (rate(rrb_db_errors_total[1m]))", "db error {{kind}}")], False),
        ("App JVM CPU", "percentunit", [("process_cpu_usage{job=\"rrb-app\"}", "{{instance}}")], False),
        ("DB transaction p99 (AC-02 result <= 50 ms, AC-03 login <= 100 ms)", "s",
         [("histogram_quantile(0.99, sum by (le, op) (rate(rrb_db_tx_seconds_bucket[1m])))", "{{op}} p99"),
          ("histogram_quantile(0.999, sum by (le, op) (rate(rrb_db_tx_seconds_bucket{op=\"result\"}[1m])))",
           "result p99.9")], False),
        ("JVM GC pause time (share of wall time)", "percentunit",
         [("sum by (instance) (rate(jvm_gc_pause_seconds_sum[1m]))", "{{instance}}")], False),
    ]),
    ("Database", [
        ("Commits/s", "ops",
         [('sum by (role) (rate(pg_stat_database_xact_commit{datname="rrb"}[30s]))', "{{role}}")], False),
        ("Rows inserted/s", "ops",
         [('sum by (role) (rate(pg_stat_database_tup_inserted{datname="rrb"}[30s]))', "{{role}}")], False),
        ("Buffer cache hit ratio", "percentunit",
         [('sum by (role) (rate(pg_stat_database_blks_hit{datname="rrb"}[1m])) / '
           '(sum by (role) (rate(pg_stat_database_blks_hit{datname="rrb"}[1m])) + '
           'sum by (role) (rate(pg_stat_database_blks_read{datname="rrb"}[1m])))', "{{role}}")], False),
        ("WAL MB/s (primary)", "MBs",
         [('rate(rrb_pg_wal_wal_bytes{role="primary"}[30s]) / 1e6', "WAL"),
          ('rate(rrb_pg_wal_wal_fpi{role="primary"}[30s])', "full-page images/s")], False),
        ("Active sessions by wait event", "short",
         [("sum by (role, wait) (rrb_pg_waits_backends)", "{{role}} {{wait}}")], True),
        ("Replication lag", "bytes",
         [("rrb_pg_replication_lag_bytes", "{{standby}} ({{sync_state}})")], False),
        ("Server log growth (sdbAudit) MB/s", "MBs",
         [("clamp_min(deriv(rrb_pg_logdir_bytes[1m]), 0) / 1e6", "{{role}}")], False),
        ("Top statements by DB time (s/s)", "s",
         [('topk(8, sum by (role, queryid) (rate(pg_stat_statements_seconds_total{datname="rrb"}[1m])))',
           "{{role}} {{queryid}}")], True),
        ("Checkpoints forced /min", "short",
         [("rate(rrb_pg_checkpointer_num_requested[5m]) * 60", "{{role}} requested"),
          ("rate(rrb_pg_checkpointer_num_timed[5m]) * 60", "{{role}} timed")], False),
        ("Rows in DEFAULT partitions (must be 0)", "short",
         [("rrb_pg_default_partition_rows", "{{role}} {{partition}}")], False),
    ]),
    ("Hosts", [
        ("CPU busy by role", "percentunit",
         [('1 - avg by (role) (rate(node_cpu_seconds_total{mode=~"idle|iowait"}[30s]))', "{{role}}")], False),
        ("Hottest machine per role", "percentunit",
         [('max by (role) (1 - avg by (role, instance) (rate(node_cpu_seconds_total{mode=~"idle|iowait"}[30s])))',
           "{{role}}")], False),
        ("I/O wait (DB nodes)", "percentunit",
         [('avg by (role) (rate(node_cpu_seconds_total{mode="iowait",role=~"primary|replica"}[30s]))',
           "{{role}}")], False),
        ("Disk busy (DB nodes)", "percentunit",
         [('max by (role, device) (rate(node_disk_io_time_seconds_total{role=~"primary|replica"}[30s]))',
           "{{role}} {{device}}")], False),
        ("Disk write MB/s (DB nodes)", "MBs",
         [('sum by (role) (rate(node_disk_written_bytes_total{role=~"primary|replica",device!~"dm-.*"}[30s])) / 1e6',
           "{{role}}")], False),
        ("Network MB/s by role", "MBs",
         [('sum by (role) (rate(node_network_receive_bytes_total{device!="lo"}[30s]) + '
           'rate(node_network_transmit_bytes_total{device!="lo"}[30s])) / 1e6', "{{role}}")], False),
    ]),
]


def build():
    panels, pid, y = [], 1, 0
    for row_title, items in PANELS:
        panels.append({"type": "row", "title": row_title, "id": pid, "collapsed": False,
                       "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []})
        pid += 1
        y += 1
        for i, (title, unit, targets, stacked) in enumerate(items):
            panels.append({
                "type": "timeseries", "title": title, "id": pid, "datasource": DS,
                "gridPos": {"x": (i % 4) * 6, "y": y + (i // 4) * 8, "w": 6, "h": 8},
                "fieldConfig": {"defaults": {"unit": unit, "custom": {
                    "lineWidth": 1, "fillOpacity": 30 if stacked else 0,
                    "stacking": {"mode": "normal" if stacked else "none"}}}, "overrides": []},
                "options": {"legend": {"displayMode": "list", "placement": "bottom"},
                            "tooltip": {"mode": "multi", "sort": "desc"}},
                "targets": [{"refId": chr(65 + k), "datasource": DS, "expr": e, "legendFormat": lg}
                            for k, (e, lg) in enumerate(targets)],
            })
            pid += 1
        y += ((len(items) + 3) // 4) * 8
    return {"uid": "rrb-overview", "title": "RRB benchmark", "tags": ["rrb", "shaktidb"],
            "timezone": "browser", "refresh": "5s", "schemaVersion": 39,
            "time": {"from": "now-30m", "to": "now"}, "panels": panels,
            "annotations": {"list": []}, "templating": {"list": []}}


if __name__ == "__main__":
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboards", "rrb-overview.json")
    with open(out, "w") as f:
        json.dump(build(), f, indent=1)
    print(f"wrote {out}")
