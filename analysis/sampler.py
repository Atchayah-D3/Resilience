#!/usr/bin/env python3
"""Records DB, OS and app metrics during a load run, for analyze_run.py.

Run ONE sampler on each database server (primary, replica) for the whole run, and one
OS-only sampler (--no-db) on each load generator. It needs only python3 and psql (no extra
packages), and connects with the usual PG* variables or --dsn.

    PGHOST=/tmp PGPORT=5432 PGUSER=postgres PGDATABASE=rrb \
      python3 sampler.py --run R-ON --name primary --app http://app1:8081 --app http://app2:8081
    python3 sampler.py --run R-ON --name gen1 --no-db          # on a load generator

With --run, everything goes into that run's folder, <kit>/results/<run>/ (change the parent
with --results; use the same RUN name as run_test.sh / run_k6.sh):
  sampler-<name>.jsonl          the samples
  feature_status-<name>.json    TDE / sdbAudit / sdbImmutable state of this server at the start
--out <file> writes the samples somewhere else instead.
Stop it with Ctrl+C / SIGTERM (or --duration) after the load has finished.

Every --interval seconds (default 1, as proposal 10.3 asks for OS metrics) it writes one JSON
line of cumulative counters (deltas are taken by analyze_run.py):
  os     THIS host: CPU user/system/iowait/steal, context switches, run queue, memory,
         disk IOPS / MB/s / busy time / read+write latency, network
  db     pg_stat_database; wal pg_stat_wal; ckpt pg_stat_checkpointer (count and duration);
         repl pg_stat_replication; replay delay (standby); logdir_bytes (sdbAudit writes there);
         tables: schema rrb totals
  waits  wait events of active client backends, sampled --wait-samples times per interval
  app    per app server (Spring /actuator/prometheus): Hikari pools, timeouts, DB errors, logins,
         CPU, GC pauses, HTTP status counts, and histograms of the DB-side transaction time
         (rrb_db_tx_seconds, for AC-02/AC-03) and server-side HTTP time per endpoint
Every --detail-every intervals (default 5) also:
  pgss   top statements from pg_stat_statements (if readable on this node)
  hot    the rrb tables with most dead tuples: live/dead tuples, autovacuum count and age
The connecting role needs pg_monitor (or superuser) for pg_ls_logdir and other sessions' waits.
Use a role the app does NOT use (rrb_monitor or postgres): statements run by the sampler's own
role are left out of the top-statements list.
"""
import argparse
import json
import os
import re
import signal
import socket
import subprocess
import sys
import time
import urllib.request

COUNTERS_SQL = r"""
SELECT json_build_object(
  'type', 'tick',
  'ts', extract(epoch FROM clock_timestamp()),
  'standby', pg_is_in_recovery(),
  'db', (SELECT row_to_json(d) FROM (
          SELECT numbackends, xact_commit, xact_rollback, blks_read, blks_hit, tup_returned,
                 tup_fetched, tup_inserted, tup_updated, tup_deleted, temp_bytes, deadlocks, conflicts
          FROM pg_stat_database WHERE datname = current_database()) d),
  'wal', (SELECT row_to_json(w) FROM (SELECT wal_records, wal_fpi, wal_bytes, wal_buffers_full FROM pg_stat_wal) w),
  'ckpt', (SELECT row_to_json(c) FROM (SELECT num_timed, num_requested, write_time, sync_time, buffers_written
                                       FROM pg_stat_checkpointer) c),
  'repl', (SELECT json_agg(json_build_object(
              'name', application_name, 'state', state, 'sync', sync_state,
              'lag_bytes', pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn),
              'replay_lag_s', extract(epoch FROM replay_lag)))
           FROM pg_stat_replication),
  -- standby: WAL received but not yet replayed (bytes); 0 when caught up, unlike replay_delay_s
  'standby_lag_bytes', CASE WHEN pg_is_in_recovery()
                            THEN pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn()) END,
  'replay_delay_s', CASE WHEN pg_is_in_recovery()
                         THEN extract(epoch FROM clock_timestamp() - pg_last_xact_replay_timestamp()) END,
  -- only when the logging collector writes files (otherwise there is no log directory)
  'logdir_bytes', CASE WHEN current_setting('logging_collector') = 'on'
                       THEN (SELECT sum(size) FROM pg_ls_logdir()) END,
  'tables', (SELECT json_build_object('n_live_tup', sum(n_live_tup), 'n_dead_tup', sum(n_dead_tup),
                                      'autovacuum', sum(autovacuum_count), 'seq_scan', sum(seq_scan),
                                      'idx_scan', sum(idx_scan))
             FROM pg_stat_user_tables WHERE schemaname = 'rrb')
  {detail}
);
"""

PGSS_PART = r""",
  'pgss', (SELECT json_agg(json_build_object('id', queryid::text, 'calls', calls, 'ms', total_exec_time,
                                             'rows', rows, 'hit', shared_blks_hit, 'read', shared_blks_read,
                                             'wal', wal_bytes))
           FROM (SELECT * FROM pg_stat_statements
                 WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
                   AND userid <> (SELECT oid FROM pg_roles WHERE rolname = current_user)  -- not the sampler's own
                 ORDER BY total_exec_time DESC LIMIT 40) s)"""

HOT_PART = r""",
  'hot', (SELECT json_agg(json_build_object('t', relname, 'live', n_live_tup, 'dead', n_dead_tup,
                                            'ins', n_tup_ins, 'av', autovacuum_count,
                                            'av_age_s', extract(epoch FROM clock_timestamp() - last_autovacuum)))
          FROM (SELECT * FROM pg_stat_user_tables WHERE schemaname = 'rrb'
                ORDER BY n_dead_tup DESC, n_tup_ins DESC LIMIT 8) h)"""

WAIT_SQL = r"""
SELECT json_build_object('type', 'waits', 'w', (
  SELECT json_object_agg(k, n) FROM (
    SELECT coalesce(wait_event_type, 'CPU') || ':' || coalesce(wait_event, 'running') AS k, count(*) AS n
    FROM pg_stat_activity
    WHERE backend_type = 'client backend' AND state = 'active' AND pid <> pg_backend_pid()
      AND application_name <> 'rrb_sampler'          -- other samplers on the same server
    GROUP BY 1) x),
  'iit', (SELECT count(*) FROM pg_stat_activity WHERE state LIKE 'idle in transaction%'));
SELECT pg_sleep({sleep});
"""

PGSS_TEXT_SQL = r"""
SELECT json_build_object('type', 'pgss_text', 'q', (
  SELECT json_object_agg(queryid::text, left(regexp_replace(query, '\s+', ' ', 'g'), 300))
  FROM (SELECT DISTINCT ON (queryid) queryid, query FROM pg_stat_statements
        WHERE dbid = (SELECT oid FROM pg_database WHERE datname = current_database())
          AND userid <> (SELECT oid FROM pg_roles WHERE rolname = current_user)
        ORDER BY queryid, total_exec_time DESC) s));
"""

APP_METRICS = {
    # metric name -> (key in the output, label to split by or None); summed over other labels
    "hikaricp_connections_pending": ("pending", "pool"),
    "hikaricp_connections_active": ("active", "pool"),
    "hikaricp_connections_max": ("max", "pool"),
    "hikaricp_connections_timeout_total": ("timeouts", "pool"),
    "rrb_db_errors_total": ("db_errors", "kind"),
    "rrb_login_total": ("logins", "outcome"),
    "process_cpu_usage": ("process_cpu", None),
    "system_cpu_usage": ("system_cpu", None),
    "http_server_requests_seconds_count": ("http", "status"),
    "jvm_gc_pause_seconds_sum": ("gc_s", None),
    "jvm_gc_pause_seconds_count": ("gc_n", None),
    "jvm_gc_pause_seconds_max": ("gc_max", "max"),       # max over collectors, not a sum
}
# histograms: metric -> (prefix, label that names the series)
HIST_METRICS = {
    "rrb_db_tx_seconds_bucket": ("db", "op"),
    "http_server_requests_seconds_bucket": ("http", "uri"),
}
# label values may contain "}" (uri="/api/v1/results/{cenId}"), so match quoted values as a whole
LINE_RE = re.compile(r'^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{(?:[^"}]|"(?:[^"\\]|\\.)*")*\})?\s+(\S+)')
DISK_RE = re.compile(r"^(sd[a-z]+|vd[a-z]+|xvd[a-z]+|nvme\d+n\d+|dm-\d+|md\d+)$")


def psql(args, sql, timeout):
    r = subprocess.run(args + ["-c" if "\n" not in sql else "-f", sql if "\n" not in sql else "-"],
                       input=sql if "\n" in sql else None, capture_output=True, text=True, timeout=timeout)
    if r.returncode != 0:
        raise RuntimeError(r.stderr.strip())
    return [json.loads(l) for l in r.stdout.splitlines() if l.startswith("{")]


def read_os():
    o = {}
    with open("/proc/stat") as f:
        for line in f:
            p = line.split()
            if p[0] == "cpu":
                v = [int(x) for x in p[1:9]]
                o["cpu"] = dict(zip(["user", "nice", "system", "idle", "iowait", "irq", "softirq", "steal"], v))
            elif p[0] == "ctxt":
                o["ctxt"] = int(p[1])
            elif p[0] == "procs_running":
                # run queue incl. the running ones, minus this sampler (which is running while it
                # reads the file; vmstat subtracts itself the same way)
                o["procs_running"] = max(0, int(p[1]) - 1)
            elif p[0] == "procs_blocked":
                o["procs_blocked"] = int(p[1])      # waiting on I/O
    mem = {}
    with open("/proc/meminfo") as f:
        for line in f:
            k, v = line.split(":", 1)
            if k in ("MemTotal", "MemAvailable", "Cached", "Dirty", "Writeback", "SwapTotal", "SwapFree"):
                mem[k] = int(v.split()[0]) * 1024
    o["mem"] = mem
    disks = {}
    with open("/proc/diskstats") as f:
        for line in f:
            p = line.split()
            if not DISK_RE.match(p[2]):
                continue
            # reads, sectors read, ms reading, writes, sectors written, ms writing, io_ticks (ms busy)
            disks[p[2]] = {"r": int(p[3]), "rsec": int(p[5]), "rms": int(p[6]),
                           "w": int(p[7]), "wsec": int(p[9]), "wms": int(p[10]), "ticks": int(p[12])}
            if len(p) >= 20:
                # flush requests (fsync / FUA, kernel 5.5+): WAL commits wait on these
                disks[p[2]]["f"], disks[p[2]]["fms"] = int(p[18]), int(p[19])
    o["disk"] = disks
    rx = tx = lrx = ltx = 0
    with open("/proc/net/dev") as f:
        for line in f.readlines()[2:]:
            name, data = line.split(":", 1)
            d = data.split()
            if name.strip() == "lo":           # loopback: kept apart (only traffic on a one-machine test)
                lrx += int(d[0]); ltx += int(d[8])
                continue
            rx += int(d[0]); tx += int(d[8])
    o["net"] = {"rx": rx, "tx": tx, "lo_rx": lrx, "lo_tx": ltx}
    with open("/proc/loadavg") as f:
        o["load1"] = float(f.read().split()[0])
    return o


def read_app(url, timeout=2.0):
    out = {}
    try:
        with urllib.request.urlopen(url.rstrip("/") + "/actuator/prometheus", timeout=timeout) as r:
            text = r.read().decode()
    except Exception as e:  # app down or restarting: record it, keep sampling
        return {"error": str(e)[:200]}
    hist = {}
    for line in text.splitlines():
        if line.startswith("#"):
            continue
        m = LINE_RE.match(line)
        if not m:
            continue
        name, labels = m.group(1), m.group(2) or ""
        try:
            val = float(m.group(3))
        except ValueError:
            continue
        if name in HIST_METRICS:
            prefix, lab = HIST_METRICS[name]
            series = re.search(lab + r'="([^"]*)"', labels)
            le = re.search(r'le="([^"]*)"', labels)
            if not series or not le:
                continue
            if prefix == "http" and not series.group(1).startswith("/api/"):
                continue                                   # only the benchmark endpoints
            h = hist.setdefault(f"{prefix}:{series.group(1)}", {})
            h[le.group(1)] = h.get(le.group(1), 0.0) + val  # summed over status/method/outcome
            continue
        if name not in APP_METRICS:
            continue
        key, split = APP_METRICS[name]
        if split == "max":
            out[key] = max(out.get(key, 0.0), val)
        elif split:
            lm = re.search(split + r'="([^"]*)"', labels)
            sub = lm.group(1) if lm else "_"
            out.setdefault(key, {})
            out[key][sub] = out[key].get(sub, 0.0) + val
        else:
            out[key] = out.get(key, 0.0) + val
    if hist:
        out["hist"] = hist
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--name", required=True, help="label for this machine, e.g. primary, replica, gen1")
    ap.add_argument("--run", help="run name: write into <results>/<run>/ (same name as for run_test.sh)")
    ap.add_argument("--results", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "results"),
                    help="parent folder of the run folders (default: <kit>/results)")
    ap.add_argument("--out", help="explicit output .jsonl instead of --run (appended)")
    ap.add_argument("--no-db", action="store_true",
                    help="OS metrics only (load generators: proposal 9.2 voids a run above 70%% generator CPU)")
    ap.add_argument("--role", default=None, help="db (default) or generator (default with --no-db)")
    ap.add_argument("--dsn", default="", help="psql connection string (default: PG* env vars)")
    ap.add_argument("--psql", default="psql")
    ap.add_argument("--interval", type=float, default=1.0, help="seconds between samples (default 1)")
    ap.add_argument("--wait-samples", type=int, default=10, help="wait-event samples per interval (default 10)")
    ap.add_argument("--detail-every", type=int, default=5,
                    help="pg_stat_statements + hot tables every N intervals (default 5)")
    ap.add_argument("--app", action="append", default=[], help="app server management URL, e.g. http://app1:8081 (repeatable)")
    ap.add_argument("--duration", type=float, default=0, help="stop after N seconds (default: until Ctrl+C)")
    ap.add_argument("--feature-status", default=os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                                             "..", "features", "feature_status.sql"))
    a = ap.parse_args()
    if not a.out and not a.run:
        ap.error("give --run <name> (or --out <file>)")
    role = a.role or ("generator" if a.no_db else "db")
    run_dir = os.path.join(os.path.abspath(a.results), a.run) if a.run else os.path.dirname(os.path.abspath(a.out))
    a.out = a.out or os.path.join(run_dir, f"sampler-{a.name}.jsonl")
    os.environ["PGAPPNAME"] = "rrb_sampler"   # lets other samplers skip this session's waits

    base = [a.psql, "-X", "-A", "-t", "-q", "-v", "ON_ERROR_STOP=1"] + (["-d", a.dsn] if a.dsn else [])
    stop = {"flag": False}
    signal.signal(signal.SIGINT, lambda *_: stop.update(flag=True))
    signal.signal(signal.SIGTERM, lambda *_: stop.update(flag=True))

    has_pgss, features = False, None
    if not a.no_db:
        # The extension can exist (it replicates) while the library is not preloaded on this
        # node, so test that the view is actually readable.
        has_pgss = subprocess.run(base + ["-c", "SELECT count(*) FROM pg_stat_statements"],
                                  capture_output=True, text=True).returncode == 0
        if os.path.exists(a.feature_status):
            r = subprocess.run(base + ["-f", a.feature_status], capture_output=True, text=True)
            try:
                features = json.loads(r.stdout)
            except ValueError:
                features = {"error": (r.stderr or r.stdout).strip()[:500]}
    n_wait = max(1, a.wait_samples)
    # the wait samples cover ~70 % of the interval; psql start-up, the counters query and the app
    # scrape use the rest, and the loop sleeps to a fixed-rate deadline (no drift: 1 tick per interval)
    waits_sql = WAIT_SQL.replace("{sleep}", f"{0.7 * a.interval / n_wait:.3f}") * n_wait
    sql_light = COUNTERS_SQL.replace("{detail}", "") + waits_sql
    sql_detail = COUNTERS_SQL.replace("{detail}", (PGSS_PART if has_pgss else "") + HOT_PART) + waits_sql

    os.makedirs(os.path.dirname(os.path.abspath(a.out)), exist_ok=True)
    if features is not None:
        with open(os.path.join(run_dir, f"feature_status-{a.name}.json"), "w") as fs:
            json.dump(features, fs, indent=1)
    out = open(a.out, "a", buffering=1)
    meta = {"type": "meta", "name": a.name, "role": role, "host": socket.gethostname(), "ncpu": os.cpu_count(),
            "clk_tck": os.sysconf("SC_CLK_TCK"), "started": time.time(), "interval": a.interval,
            "wait_samples": n_wait, "detail_every": a.detail_every, "pgss": has_pgss, "apps": a.app,
            "features": features, "mem_total": read_os()["mem"].get("MemTotal")}
    out.write(json.dumps(meta) + "\n")
    what = "OS only" if a.no_db else f"pg_stat_statements={'yes' if has_pgss else 'NO'}"
    print(f"sampler {a.name} ({role}): writing {a.out} every {a.interval}s ({what})", file=sys.stderr)

    t_end = time.time() + a.duration if a.duration else None
    n = 0
    t_next = time.time()
    while not stop["flag"] and (t_end is None or time.time() < t_end):
        now = time.time()
        if now < t_next:
            time.sleep(t_next - now)
        t_next = max(t_next + a.interval, time.time())       # a slow tick does not cause a burst
        os_ts = time.time()
        os_snap = read_os()
        apps = {u: read_app(u) for u in a.app}
        if a.no_db:
            tick = {"type": "tick", "ts": os_ts}
        else:
            try:
                rows = psql(base, sql_detail if n % max(1, a.detail_every) == 0 else sql_light,
                            timeout=a.interval * 4 + 10)
            except Exception as e:
                out.write(json.dumps({"type": "error", "ts": time.time(), "error": str(e)[:500]}) + "\n")
                print(f"sampler {a.name}: sample failed: {str(e)[:300]}", file=sys.stderr)
                time.sleep(a.interval)
                continue
            tick = rows[0]
            waits, iit = {}, 0
            for w in rows[1:]:
                for k, v in (w.get("w") or {}).items():
                    waits[k] = waits.get(k, 0) + v
                iit = max(iit, w.get("iit") or 0)
            tick["waits"] = waits
            tick["wait_samples"] = len(rows) - 1
            tick["iit_max"] = iit
        tick["os"] = os_snap
        tick["os_ts"] = os_ts
        tick["app"] = apps
        out.write(json.dumps(tick, separators=(",", ":")) + "\n")
        n += 1

    if has_pgss:
        try:
            for r in psql(base, PGSS_TEXT_SQL, timeout=60):
                out.write(json.dumps(r) + "\n")
        except Exception as e:
            print(f"pg_stat_statements text not saved: {e}", file=sys.stderr)
    out.write(json.dumps({"type": "end", "ts": time.time(), "ticks": n}) + "\n")
    print(f"sampler {a.name}: stopped after {n} ticks", file=sys.stderr)


if __name__ == "__main__":
    main()
