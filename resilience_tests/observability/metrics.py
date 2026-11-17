"""What a run copies from Prometheus into its own folder (metrics.json): the same few series for
every scenario, so any two runs can be compared. Context only (Arch §7.1): no measure or verdict
reads them. The dashboard (infra/monitoring/grafana/dashboards/resilience-run.json) shows the
same series, live.

Each query is a template filled by `render`: {sel} is the run node's labels (env, node -- the
labels scrape_config.py gives every target), {db} the harness database, {rate} the rate window.
"""

from __future__ import annotations

RATE = "15s"   # at a 1 s scrape, a 15 s window smooths single slow scrapes without hiding a stall

SNAPSHOT_QUERIES: tuple[tuple[str, str], ...] = (
    # --- the database (postgres_exporter)
    ("database_up", "pg_up{{{sel}}}"),
    ("database_start_time_s", "pg_postmaster_start_time_seconds{{{sel}}}"),     # a restart moves it
    ("commits_per_s", 'sum(rate(pg_stat_database_xact_commit{{{sel},datname="{db}"}}[{rate}]))'),
    ("rollbacks_per_s", 'sum(rate(pg_stat_database_xact_rollback{{{sel},datname="{db}"}}[{rate}]))'),
    ("sessions_by_state", 'sum by (state) (pg_stat_activity_count{{{sel},datname="{db}"}})'),
    ("locks_by_mode", 'sum by (mode) (pg_locks_count{{{sel},datname="{db}"}})'),
    ("longest_transaction_s", 'max(pg_stat_activity_max_tx_duration{{{sel},datname="{db}"}})'),
    ("cache_hit_ratio",
     'sum(rate(pg_stat_database_blks_hit{{{sel},datname="{db}"}}[{rate}])) / '
     '(sum(rate(pg_stat_database_blks_hit{{{sel},datname="{db}"}}[{rate}])) + '
     'sum(rate(pg_stat_database_blks_read{{{sel},datname="{db}"}}[{rate}])) > 0)'),
    ("deadlocks_per_s", 'sum(rate(pg_stat_database_deadlocks{{{sel},datname="{db}"}}[{rate}]))'),
    ("temp_bytes_per_s", 'sum(rate(pg_stat_database_temp_bytes{{{sel},datname="{db}"}}[{rate}]))'),
    ("wal_size_bytes", "pg_wal_size_bytes{{{sel}}}"),
    ("checkpoints_per_min",
     "60 * (rate(pg_stat_checkpointer_num_timed_total{{{sel}}}[1m]) "
     "+ rate(pg_stat_checkpointer_num_requested_total{{{sel}}}[1m]))"),
    # --- the machine (node_exporter)
    ("cpu_busy_pct", '100 * (1 - avg(rate(node_cpu_seconds_total{{{sel},mode="idle"}}[{rate}])))'),
    ("cpu_iowait_pct", '100 * avg(rate(node_cpu_seconds_total{{{sel},mode="iowait"}}[{rate}]))'),
    ("cpu_steal_pct", '100 * avg(rate(node_cpu_seconds_total{{{sel},mode="steal"}}[{rate}]))'),
    ("memory_available_bytes", "node_memory_MemAvailable_bytes{{{sel}}}"),
    ("io_pressure", "rate(node_pressure_io_stalled_seconds_total{{{sel}}}[{rate}])"),
    ("disk_write_latency_ms",
     '1000 * rate(node_disk_write_time_seconds_total{{{sel},device!~"sr[0-9]+"}}[{rate}]) '
     '/ (rate(node_disk_writes_completed_total{{{sel},device!~"sr[0-9]+"}}[{rate}]) > 0)'),
    ("disk_busy_pct", '100 * rate(node_disk_io_time_seconds_total{{{sel},device!~"sr[0-9]+"}}[{rate}])'),
    ("network_receive_bytes_per_s",
     'sum by (device) (rate(node_network_receive_bytes_total{{{sel},device!="lo"}}[{rate}]))'),
    ("network_transmit_bytes_per_s",
     'sum by (device) (rate(node_network_transmit_bytes_total{{{sel},device!="lo"}}[{rate}]))'),
)


def node_selector(env: str, node: str) -> str:
    return f'env="{env}",node="{node}"'


def render(template: str, *, env: str, node: str, db: str) -> str:
    return template.format(sel=node_selector(env, node), db=db, rate=RATE)
