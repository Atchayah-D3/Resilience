#!/usr/bin/env python3
"""Per-second metric files for one run, in <run>/metrics/ (like the Resilience scenario runs:
one timestamped event stream plus plain files anyone can open in Excel / pandas / grep).

    python3 export_metrics.py results/<run>      # analyze_run.py calls this automatically

Reads the raw sampler-*.jsonl files (cumulative counters, one line per second) and writes the
per-second VALUES (rates, %, ms), so nobody has to diff counters by hand:

  metrics/db-<sampler>.csv      database: commits/s, rows/s, cache hit %, WAL MB/s, checkpoints,
                                active sessions, lock waits, replication lag, log growth, ...
  metrics/waits-<sampler>.csv   wait events: average active sessions per wait event, per second
  metrics/os-<sampler>.csv      host: CPU user/sys/iowait, run queue, context switches, memory,
                                swap, IOPS, MB/s, disk latency, network
  metrics/app.csv               app servers (summed): requests/s by status and endpoint, server-side
                                p50/p99 per endpoint, DB transaction p99 (AC-02/AC-03), pool
                                active/waiting/timeouts, JVM CPU, GC
  metrics/events.jsonl          everything above as one time-ordered stream, plus run_start,
                                step_start/step_end, interrupted and run_end markers
  metrics/README.md             what every column means

Every row has utc (ISO 8601, UTC), epoch (seconds) and step (the load step the second belongs to,
"transition" between steps, "" before/after the load). A value is empty when it was not measured.
Per-second percentiles come from histogram buckets (~10-20 % wide) and from few requests at low
rates, so they are noisier than the per-step values in report.html.
"""
import csv
import glob
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

DB_COLS = [
    ("standby", "1 = this server is a hot standby (read replica)"),
    ("backends", "connections to the rrb database"),
    ("commits_s", "transactions committed per second"),
    ("rollbacks_s", "transactions rolled back per second"),
    ("rows_inserted_s", "rows inserted per second"),
    ("rows_updated_s", "rows updated per second"),
    ("rows_deleted_s", "rows deleted per second"),
    ("rows_fetched_s", "rows fetched by index scans per second"),
    ("rows_returned_s", "rows returned (incl. sequential scans) per second"),
    ("blocks_read_s", "8 kB blocks read from disk / OS cache per second"),
    ("cache_hit_pct", "shared-buffer hit ratio in this second"),
    ("temp_MB_s", "temporary file MB written per second"),
    ("deadlocks", "deadlocks in this second"),
    ("conflicts", "standby: queries cancelled by recovery conflicts in this second"),
    ("wal_MB_s", "WAL MB generated per second (primary)"),
    ("wal_records_s", "WAL records per second"),
    ("wal_fpi_s", "full-page images per second (high after a checkpoint)"),
    ("wal_buffers_full_s", "WAL writes forced because wal_buffers was full, per second"),
    ("checkpoints_timed", "timed checkpoints started in this second"),
    ("checkpoints_requested", "forced checkpoints (max_wal_size reached) in this second"),
    ("checkpoint_write_ms", "checkpoint write time spent in this second, ms"),
    ("checkpoint_buffers", "buffers written by checkpoints in this second"),
    ("active_sessions", "average client sessions running a query (from the wait samples)"),
    ("lock_waiting_sessions", "average sessions waiting on a lock (Lock:* waits)"),
    ("idle_in_tx_max_s", "longest idle-in-transaction session, seconds"),
    ("repl_lag_MB", "primary: largest replica lag, MB of WAL not yet replayed"),
    ("repl_replay_lag_s", "primary: largest replica replay lag, seconds"),
    ("replicas_streaming", "primary: replicas in state streaming"),
    ("standby_lag_MB", "standby: WAL received but not yet replayed, MB (0 = caught up)"),
    ("replay_delay_s", "standby: age of the last replayed transaction, seconds (grows when idle)"),
    ("log_MB_s", "server log growth MB/s (sdbAudit writes here; needs logging_collector=on)"),
    ("dead_tuples", "dead rows in schema rrb"),
    ("idx_scans_s", "index scans on schema rrb per second"),
    ("seq_scans_s", "sequential scans on schema rrb per second"),
]
OS_COLS = [
    ("cpu_busy_pct", "CPU busy (not idle, not iowait), all cores"),
    ("cpu_user_pct", "user CPU"),
    ("cpu_system_pct", "kernel CPU (incl. irq/softirq)"),
    ("cpu_iowait_pct", "CPU idle waiting for disk"),
    ("cpu_steal_pct", "CPU taken by the hypervisor"),
    ("run_queue", "runnable processes (above the CPU count = CPU saturation)"),
    ("blocked", "processes blocked on I/O"),
    ("context_switches_s", "context switches per second"),
    ("mem_available_MB", "memory available"),
    ("mem_used_pct", "memory used (1 - available / total)"),
    ("swap_used_MB", "swap in use"),
    ("dirty_MB", "dirty page cache waiting to be written"),
    ("disk_read_iops", "read operations per second, all disks"),
    ("disk_write_iops", "write operations per second, all disks"),
    ("disk_read_MB_s", "MB read per second, all disks"),
    ("disk_write_MB_s", "MB written per second, all disks"),
    ("busiest_disk", "disk with the most busy time in this second"),
    ("busiest_disk_util_pct", "busy time of that disk"),
    ("busiest_disk_latency_ms", "average latency per I/O on that disk (read + write)"),
    ("disk_flush_s", "flush requests per second, all disks (fsync; WAL commits wait on these)"),
    ("disk_flush_latency_ms", "average latency per flush, all disks"),
    ("net_rx_MB_s", "network received MB/s"),
    ("net_tx_MB_s", "network sent MB/s"),
    ("net_loopback_MB_s", "loopback traffic MB/s (rx; app <-> DB <-> load tool when all run on one machine)"),
    ("load1", "1-minute load average"),
]
WAITS_COLS = [("wait_event", "wait event type:name"),
              ("avg_sessions", "average sessions in that wait during the second")]
APP_ENDPOINTS = {"login": "/api/v1/auth/login", "result": "/api/v1/results/{cenId}",
                 "cutoff": "/api/v1/cutoffs/{cenId}"}


def utc(ts):
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ts)) + f".{int((ts % 1) * 1000):03d}Z"


def r(v, nd=2):
    return None if v is None else round(v, nd)


def d(a, b, k):
    va, vb = (a or {}).get(k), (b or {}).get(k)
    return None if va is None or vb is None else vb - va


def per_s(a, b, k, dt, scale=1.0, nd=2):
    x = d(a, b, k)
    return None if x is None else round(max(0.0, x) / dt / scale, nd)


def step_of(ts, windows, start, end):
    for label, a, b in windows:
        if a <= ts < b:
            return label
    if start is not None and end is not None and start <= ts <= end:
        return "transition"
    return ""


def db_row(p0, p1, dt):
    a, b = p0.get("db"), p1.get("db")
    if a is None or b is None:
        return None
    hit, rd = d(a, b, "blks_hit"), d(a, b, "blks_read")
    ns = p0.get("wait_samples") or 0
    waits = p0.get("waits") or {}
    w0, w1 = p0.get("wal"), p1.get("wal")
    c0, c1 = p0.get("ckpt"), p1.get("ckpt")
    t0, t1 = p0.get("tables"), p1.get("tables")
    repl = p1.get("repl") or []
    lags = [x.get("lag_bytes") for x in repl if x.get("lag_bytes") is not None]
    rlag = [x.get("replay_lag_s") for x in repl if x.get("replay_lag_s") is not None]
    lg = d(p0, p1, "logdir_bytes")
    return {
        "standby": 1 if p1.get("standby") else 0,
        "backends": b.get("numbackends"),
        "commits_s": per_s(a, b, "xact_commit", dt),
        "rollbacks_s": per_s(a, b, "xact_rollback", dt),
        "rows_inserted_s": per_s(a, b, "tup_inserted", dt),
        "rows_updated_s": per_s(a, b, "tup_updated", dt),
        "rows_deleted_s": per_s(a, b, "tup_deleted", dt),
        "rows_fetched_s": per_s(a, b, "tup_fetched", dt),
        "rows_returned_s": per_s(a, b, "tup_returned", dt),
        "blocks_read_s": per_s(a, b, "blks_read", dt),
        "cache_hit_pct": r(100.0 * hit / (hit + rd)) if hit is not None and rd is not None and hit + rd > 0 else None,
        "temp_MB_s": per_s(a, b, "temp_bytes", dt, 1e6, 3),
        "deadlocks": d(a, b, "deadlocks"),
        "conflicts": d(a, b, "conflicts") if p1.get("standby") else None,
        "wal_MB_s": per_s(w0, w1, "wal_bytes", dt, 1e6, 4) if not p1.get("standby") else None,
        "wal_records_s": per_s(w0, w1, "wal_records", dt) if not p1.get("standby") else None,
        "wal_fpi_s": per_s(w0, w1, "wal_fpi", dt) if not p1.get("standby") else None,
        "wal_buffers_full_s": per_s(w0, w1, "wal_buffers_full", dt) if not p1.get("standby") else None,
        "checkpoints_timed": d(c0, c1, "num_timed"),
        "checkpoints_requested": d(c0, c1, "num_requested"),
        "checkpoint_write_ms": d(c0, c1, "write_time"),
        "checkpoint_buffers": d(c0, c1, "buffers_written"),
        "active_sessions": r(sum(waits.values()) / ns) if ns else None,
        "lock_waiting_sessions": r(sum(v for k, v in waits.items() if k.startswith("Lock:")) / ns) if ns else None,
        "idle_in_tx_max_s": p1.get("iit_max"),
        "repl_lag_MB": r(max(lags) / 1e6, 3) if lags else None,
        "repl_replay_lag_s": r(max(rlag), 3) if rlag else None,
        "replicas_streaming": sum(1 for x in repl if x.get("state") == "streaming") if not p1.get("standby") else None,
        "standby_lag_MB": r(p1["standby_lag_bytes"] / 1e6, 3) if p1.get("standby_lag_bytes") is not None else None,
        "replay_delay_s": r(p1.get("replay_delay_s"), 3),
        "log_MB_s": None if lg is None else round(max(0, lg) / dt / 1e6, 4),
        "dead_tuples": (t1 or {}).get("n_dead_tup"),
        "idx_scans_s": per_s(t0, t1, "idx_scan", dt),
        "seq_scans_s": per_s(t0, t1, "seq_scan", dt),
    }


def os_row(p0, p1, dt):
    o0, o1 = p0.get("os"), p1.get("os")
    if not o0 or not o1:
        return None
    c0, c1 = o0["cpu"], o1["cpu"]
    dc = {k: c1[k] - c0.get(k, 0) for k in c1}
    tot = sum(dc.values()) or 1
    pct = lambda *ks: round(100.0 * sum(dc.get(k, 0) for k in ks) / tot, 2)
    mem = o1.get("mem") or {}
    rio = wio = rsec = wsec = fl = flms = 0
    has_fl = False
    best = None
    for dev, y in (o1.get("disk") or {}).items():
        x = (o0.get("disk") or {}).get(dev)
        if not x or dev.startswith(("dm-", "md")):
            continue                       # count physical disks only (no double counting)
        rio += y["r"] - x["r"]
        wio += y["w"] - x["w"]
        rsec += y["rsec"] - x["rsec"]
        wsec += y["wsec"] - x["wsec"]
        if "f" in y and "f" in x:
            has_fl = True
            fl += y["f"] - x["f"]
            flms += y["fms"] - x["fms"]
        busy = y["ticks"] - x["ticks"]
        if best is None or busy > best[1]:
            best = (dev, busy, x, y)
    lat = util = None
    if best and best[1] > 0:
        dev, busy, x, y = best
        util = round(min(100.0, busy / (dt * 1000.0) * 100.0), 1)
        ops = (y["r"] - x["r"]) + (y["w"] - x["w"])
        if ops and "rms" in y:
            lat = round(((y["rms"] - x["rms"]) + (y["wms"] - x["wms"])) / ops, 3)
    n0, n1 = o0.get("net") or {}, o1.get("net") or {}
    return {
        "cpu_busy_pct": round(100.0 - pct("idle", "iowait"), 2),
        "cpu_user_pct": pct("user", "nice"),
        "cpu_system_pct": pct("system", "irq", "softirq"),
        "cpu_iowait_pct": pct("iowait"),
        "cpu_steal_pct": pct("steal"),
        "run_queue": o1.get("procs_running"),
        "blocked": o1.get("procs_blocked"),
        "context_switches_s": per_s(o0, o1, "ctxt", dt, nd=0),
        "mem_available_MB": r(mem["MemAvailable"] / 1e6, 0) if "MemAvailable" in mem else None,
        "mem_used_pct": r(100.0 * (1 - mem["MemAvailable"] / mem["MemTotal"])) if mem.get("MemTotal") else None,
        "swap_used_MB": r((mem["SwapTotal"] - mem["SwapFree"]) / 1e6, 0) if "SwapTotal" in mem else None,
        "dirty_MB": r(mem["Dirty"] / 1e6, 1) if "Dirty" in mem else None,
        "disk_read_iops": round(rio / dt, 1),
        "disk_write_iops": round(wio / dt, 1),
        "disk_read_MB_s": round(rsec * 512 / dt / 1e6, 3),
        "disk_write_MB_s": round(wsec * 512 / dt / 1e6, 3),
        "busiest_disk": best[0] if best and best[1] > 0 else None,
        "busiest_disk_util_pct": util,
        "busiest_disk_latency_ms": lat,
        "disk_flush_s": round(fl / dt, 1) if has_fl else None,
        "disk_flush_latency_ms": (round(flms / fl, 3) if fl else None) if has_fl else None,
        "net_rx_MB_s": per_s(n0, n1, "rx", dt, 1e6, 3),
        "net_tx_MB_s": per_s(n0, n1, "tx", dt, 1e6, 3),
        "net_loopback_MB_s": per_s(n0, n1, "lo_rx", dt, 1e6, 3),
        "load1": o1.get("load1"),
    }


def sum_hist(apps, key):
    out = {}
    for a in apps:
        for le, c in ((a.get("hist") or {}).get(key) or {}).items():
            out[le] = out.get(le, 0.0) + c
    return out


def app_row(p0, p1, dt, urls):
    from analyze_run import hist_quantiles
    a0 = [((p0.get("app") or {}).get(u) or {}) for u in urls]
    a1 = [((p1.get("app") or {}).get(u) or {}) for u in urls]
    if not any(a1):
        return None
    row = {}

    def dsum(key):                  # {label: per-second delta} summed over app servers
        out = {}
        for x, y in zip(a0, a1):
            for lab, v in (y.get(key) or {}).items():
                out[lab] = out.get(lab, 0.0) + max(0.0, v - (x.get(key) or {}).get(lab, 0.0))
        return {k: v / dt for k, v in out.items()}

    http = dsum("http")
    row["http_req_s"] = round(sum(http.values()), 2)
    row["http_2xx_s"] = round(sum(v for k, v in http.items() if k.startswith("2")), 2)
    row["http_401_s"] = round(http.get("401", 0.0), 2)
    row["http_4xx_other_s"] = round(sum(v for k, v in http.items() if k.startswith("4") and k != "401"), 2)
    row["http_5xx_s"] = round(sum(v for k, v in http.items() if k.startswith("5")), 2)
    lg = dsum("logins")
    row["logins_ok_s"] = round(lg.get("success", 0.0), 2)
    row["logins_failed_s"] = round(sum(v for k, v in lg.items() if k != "success"), 2)
    for ep, uri in APP_ENDPOINTS.items():
        q = hist_quantiles(sum_hist(a0, "http:" + uri), sum_hist(a1, "http:" + uri), (0.5, 0.99)) or {}
        row[f"{ep}_req_s"] = round(q.get("count", 0) / dt, 2) if q else None
        row[f"{ep}_p50_ms"] = r(q.get("p50"))
        row[f"{ep}_p99_ms"] = r(q.get("p99"))
    for op in ("login", "result", "cutoff"):
        q = hist_quantiles(sum_hist(a0, "db:" + op), sum_hist(a1, "db:" + op), (0.5, 0.99)) or {}
        row[f"db_{op}_tx_s"] = round(q.get("count", 0) / dt, 2) if q else None
        row[f"db_{op}_p50_ms"] = r(q.get("p50"))
        row[f"db_{op}_p99_ms"] = r(q.get("p99"))
    pools = sorted({p for y in a1 for p in (y.get("max") or {})})
    for pool in pools:
        row[f"pool_{pool}_active"] = sum((y.get("active") or {}).get(pool, 0) for y in a1)
        row[f"pool_{pool}_waiting"] = sum((y.get("pending") or {}).get(pool, 0) for y in a1)
        row[f"pool_{pool}_max"] = sum((y.get("max") or {}).get(pool, 0) for y in a1)
        row[f"pool_{pool}_timeouts_s"] = round(dsum("timeouts").get(pool, 0.0), 2)
    row["db_errors_s"] = round(sum(dsum("db_errors").values()), 2)
    cpus = [y.get("process_cpu") for y in a1 if y.get("process_cpu") is not None]
    row["jvm_cpu_max_pct"] = r(100.0 * max(cpus)) if cpus else None
    gc = sum(max(0.0, (y.get("gc_s") or 0) - (x.get("gc_s") or 0)) for x, y in zip(a0, a1))
    row["gc_ms_s"] = round(1000.0 * gc / dt, 2)
    gmax = [y.get("gc_max") for y in a1 if y.get("gc_max") is not None]
    row["gc_max_pause_ms"] = r(1000.0 * max(gmax)) if gmax else None
    return row


APP_DOC = [
    ("http_req_s", "HTTP responses per second, all endpoints and app servers"),
    ("http_2xx_s / http_401_s / http_4xx_other_s / http_5xx_s", "responses per second by status (401 = wrong DOB, expected)"),
    ("logins_ok_s / logins_failed_s", "login outcomes per second"),
    ("<endpoint>_req_s, _p50_ms, _p99_ms", "server-side requests/s and latency per endpoint (login, result, cutoff)"),
    ("db_<op>_tx_s, _p50_ms, _p99_ms", "DB transactions/s and DB-side latency incl. pool wait + commit "
                                       "(op: login = primary, result / cutoff = replica; AC-02, AC-03)"),
    ("pool_<pool>_active / _waiting / _max", "app connection pool: busy connections, threads waiting, size"),
    ("pool_<pool>_timeouts_s", "pool waits that timed out per second (= HTTP 503)"),
    ("db_errors_s", "DB errors per second seen by the app"),
    ("jvm_cpu_max_pct", "highest JVM process CPU among the app servers"),
    ("gc_ms_s / gc_max_pause_ms", "GC pause time per second / longest recent pause"),
]


def write_csv(path, rows, fields):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["utc", "epoch", "step"] + fields)
        for row in rows:
            w.writerow([row["utc"], row["epoch"], row["step"]] + ["" if row.get(k) is None else row.get(k) for k in fields])


def export(run, samplers=None, windows=None, start=None, end=None, interrupted_at=None, run_meta=None):
    """samplers: {name: (meta, ticks, texts)}; windows: [(label, from_epoch, to_epoch)] on the wall clock;
    start/end: load start/end epoch. Returns the metrics directory."""
    from analyze_run import load_jsonl
    if samplers is None:
        samplers = {}
        for p in sorted(glob.glob(os.path.join(run, "sampler-*.jsonl"))):
            meta, ticks, texts = load_jsonl(p)
            samplers[meta.get("name") or os.path.basename(p)[8:-6]] = (meta, ticks, texts)
    windows = windows or []
    out = os.path.join(run, "metrics")
    os.makedirs(out, exist_ok=True)
    for old in glob.glob(os.path.join(out, "*")):
        os.remove(old)
    events = []

    def ev(ts, source, kind, data):
        events.append({"t_wall": round(ts, 3), "utc": utc(ts), "source": source, "kind": kind, "data": data})

    if run_meta:
        ev(run_meta.get("started_epoch") or start or 0, "orchestrator", "run_start",
           {k: run_meta.get(k) for k in ("run", "profile", "tool", "status", "settings") if k in run_meta})
    if start:
        ev(start, "workload", "load_start", {})
    for label, a, b in windows:
        ev(a, "workload", "step_start", {"step": label})
        ev(b, "workload", "step_end", {"step": label})
    if interrupted_at:
        ev(interrupted_at, "orchestrator", "interrupted", {})
    if end:
        ev(end, "workload", "load_end", {})

    files = []
    app_done = False
    for name, (meta, ticks, _) in samplers.items():
        is_db = meta.get("role", "db") == "db"
        dbr, osr, wr, apr = [], [], [], []
        for p0, p1 in zip(ticks, ticks[1:]):
            dt = p1["ts"] - p0["ts"]
            if dt <= 0:
                continue
            ts = p1["ts"]
            base = {"utc": utc(ts), "epoch": round(ts, 3), "step": step_of(ts, windows, start, end)}
            o = os_row(p0, p1, dt)
            if o:
                osr.append({**base, **o})
                ev(ts, f"os:{name}", "metrics", o)
            if is_db:
                x = db_row(p0, p1, dt)
                if x:
                    dbr.append({**base, **x})
                    ev(ts, f"db:{name}", "metrics", x)
                ns = p0.get("wait_samples") or 0
                if ns:
                    wv = {k: round(v / ns, 3) for k, v in sorted((p0.get("waits") or {}).items())}
                    for k, v in wv.items():
                        wr.append({**base, "wait_event": k, "avg_sessions": v})
                    if wv:
                        ev(ts, f"db:{name}", "waits", wv)
            if meta.get("apps") and not app_done:
                x = app_row(p0, p1, dt, meta["apps"])
                if x:
                    apr.append({**base, **x})
                    ev(ts, "app", "metrics", x)
        if osr:
            write_csv(os.path.join(out, f"os-{name}.csv"), osr, [c for c, _ in OS_COLS])
            files.append(f"os-{name}.csv")
        if dbr:
            write_csv(os.path.join(out, f"db-{name}.csv"), dbr, [c for c, _ in DB_COLS])
            files.append(f"db-{name}.csv")
        if is_db:
            write_csv(os.path.join(out, f"waits-{name}.csv"), wr, [c for c, _ in WAITS_COLS])
            files.append(f"waits-{name}.csv")
        if apr:
            fields = list(dict.fromkeys(k for row in apr for k in row if k not in ("utc", "epoch", "step")))
            write_csv(os.path.join(out, "app.csv"), apr, fields)
            files.append("app.csv")
            app_done = True

    events.sort(key=lambda e: e["t_wall"])
    with open(os.path.join(out, "events.jsonl"), "w") as f:
        for e in events:
            f.write(json.dumps(e, separators=(",", ":"), default=str) + "\n")
    files.append("events.jsonl")

    doc = ["# Per-second metrics", "",
           f"Run `{os.path.basename(run)}`. One row per sampler interval (1 s by default). "
           "Columns common to every CSV: `utc` (UTC, ISO 8601), `epoch` (seconds), `step` (load step "
           "the second belongs to, `transition` between steps, empty outside the load). "
           "Empty cell = not measured. Rates are per second over the interval that ENDS at `utc`.", "",
           "Files: " + ", ".join(f"`{x}`" for x in sorted(files)), "",
           "`events.jsonl`: one JSON object per line, time ordered: `{t_wall, utc, source, kind, data}`. "
           "source = orchestrator | workload | db:<sampler> | os:<sampler> | app; kind = run_start, "
           "load_start, step_start, step_end, interrupted, load_end, metrics, waits. "
           "`data` of a metrics event has the same fields as the matching CSV row.", "",
           "Per-second percentiles come from histogram buckets (~10-20 % wide) and few requests at low "
           "rates; use the per-step values in report.html for pass/fail.", "",
           "The samplers' own queries show up in the database counters (about 10-20 commits/s and some "
           "rows fetched per database even with no load); compare steps with the idle rows (empty `step`).", ""]
    for title, cols in (("db-&lt;sampler&gt;.csv", DB_COLS), ("os-&lt;sampler&gt;.csv", OS_COLS),
                        ("waits-&lt;sampler&gt;.csv", WAITS_COLS), ("app.csv", APP_DOC)):
        doc += [f"## {title}", "", "| column | meaning |", "|---|---|"] + [f"| `{c}` | {m} |" for c, m in cols] + [""]
    with open(os.path.join(out, "README.md"), "w") as f:
        f.write("\n".join(doc).replace("&lt;", "<").replace("&gt;", ">"))
    return out


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    run = os.path.abspath(sys.argv[1])
    s = os.path.join(run, "summary.json")
    windows, start, end, inter, meta = [], None, None, None, None
    if os.path.exists(s):          # reuse the step placement analyze_run.py computed
        with open(s) as f:
            sm = json.load(f)
        windows = [(str(x.get("label", x["target"])), x["from_epoch"], x["to_epoch"]) for x in sm.get("steps", [])
                   if x.get("from_epoch")]
        start, end, inter, meta = sm.get("start"), sm.get("load_end"), sm.get("interrupted_at"), sm.get("run_meta")
    out = export(run, windows=windows, start=start, end=end, interrupted_at=inter, run_meta=meta)
    print(f"metrics: {out}")


if __name__ == "__main__":
    main()
