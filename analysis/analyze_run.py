#!/usr/bin/env python3
"""Analyzes one run directory: per-step load results + DB/OS/app metrics -> knee point.

    python3 analyze_run.py results/<run> [--p99 login=500,result=300,cutoff=300]

The run directory holds (any of):
  k6-summary-<node>.json, k6-plan-<node>.json   from load/k6/run_k6.sh (one pair per generator)
  jmeter-plan.json + *.jtl                       from load/jmeter/run_jmeter.sh (instead of k6)
  sampler-<name>.jsonl                           from analysis/sampler.py (primary, replica, ...)
  feature_status.json                            optional, from features/feature_status.sql

Writes report.html, report.md, steps.csv and summary.json (input for compare_runs.py) into the run dir.

A step PASSES when, over its measured window (hold minus settle):
  achieved sessions/s >= --min-achieved x target, request error rate < --max-error,
  and p99 of every endpoint is under its limit. The knee is the highest step that passes
  with every lower step passing too. With several generators, p99 is the WORST generator's
  p99 (k6 cannot merge percentiles), which is conservative.
"""
import argparse
import csv
import glob
import json
import os
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from report_html import render_run  # noqa: E402
from build_index import build as build_index  # noqa: E402
import export_metrics  # noqa: E402

ENDPOINTS = ["login", "result", "cutoff"]


# ----------------------------------------------------------------------------- inputs
def load_jsonl(path):
    meta, ticks, texts = {}, [], {}
    with open(path) as f:
        for line in f:
            try:
                r = json.loads(line)
            except ValueError:
                continue
            t = r.get("type")
            if t == "meta":
                meta = r
            elif t == "tick":
                ticks.append(r)
            elif t == "pgss_text":
                texts.update(r.get("q") or {})
    ticks.sort(key=lambda r: r["ts"])
    return meta, ticks, texts


def load_k6(run_dir):
    summaries = []
    for p in sorted(glob.glob(os.path.join(run_dir, "k6-summary-*.json"))):
        with open(p) as f:
            summaries.append(json.load(f)["metrics"])
    plans = []
    for p in sorted(glob.glob(os.path.join(run_dir, "k6-plan-*.json"))):
        with open(p) as f:
            plans.append(json.load(f))
    return summaries, plans


def k6_step(summaries, label):
    """Merge one step across generators."""
    out = {"iterations": 0, "sessions_ok": 0, "login_401": 0, "req_failed": 0, "req_total": 0}
    for e in ENDPOINTS:
        out[e] = {}
    for m in summaries:
        g = lambda k: m.get(k.replace("@", label)) or {}
        out["iterations"] += g("iterations{step:@}").get("count", 0)
        out["sessions_ok"] += g("rrb_sessions_ok{step:@}").get("count", 0)
        out["login_401"] += g("rrb_login_failed{step:@}").get("count", 0)
        f = g("http_req_failed{step:@}")
        out["req_failed"] += f.get("passes", 0)
        out["req_total"] += f.get("passes", 0) + f.get("fails", 0)
        for e in ENDPOINTS:
            d = g("http_req_duration{name:%s,step:@}" % e)
            if not d:
                continue
            cur = out[e]
            cur["count"] = cur.get("count", 0) + d.get("count", 0)
            for s in ("med", "p(95)", "p(99)", "p(99.9)", "max"):
                if s in d:
                    cur[s] = max(cur.get(s, 0), d[s])
    return out


def load_jmeter(run_dir):
    """Per-step stats from a JMeter .jtl (CSV) using the plan written by run_jmeter.sh."""
    plan_p = os.path.join(run_dir, "jmeter-plan.json")
    jtls = glob.glob(os.path.join(run_dir, "*.jtl"))
    if not os.path.exists(plan_p) or not jtls:
        return None, None
    with open(plan_p) as f:
        plan = json.load(f)
    start = plan["start_ms"] / 1000.0
    wins = [(w["label"], start + w["from"], start + w["to"]) for w in plan["windows"]]
    hist = {w[0]: {e: {} for e in ENDPOINTS} for w in wins}
    agg = {w[0]: {"login_200": 0, "login_401": 0, "req_failed": 0, "req_total": 0, "sessions_ok": 0} for w in wins}
    for p in jtls:
        with open(p, newline="") as f:
            for r in csv.DictReader(f):
                try:
                    ts = int(r["timeStamp"]) / 1000.0
                    el = int(r["elapsed"])
                except (KeyError, ValueError):
                    continue
                lab = next((w[0] for w in wins if w[1] <= ts < w[2]), None)
                name = r.get("label")
                if lab is None or name not in ENDPOINTS:
                    continue
                b = el if el < 1000 else el - el % 10            # 1 ms buckets < 1 s, 10 ms above
                h = hist[lab][name]
                h[b] = h.get(b, 0) + 1
                a = agg[lab]
                a["req_total"] += 1
                ok = r.get("success") == "true"
                if not ok:
                    a["req_failed"] += 1
                if name == "login":
                    a["login_200"] += r.get("responseCode") == "200"
                    a["login_401"] += r.get("responseCode") == "401"
                if name == "cutoff" and ok:
                    a["sessions_ok"] += 1

    def pct(h, q):
        n = sum(h.values())
        if not n:
            return None
        k, c = q * n, 0
        for b in sorted(h):
            c += h[b]
            if c >= k:
                return float(b)
        return float(max(h))

    steps = {}
    for lab, _, _ in wins:
        a = agg[lab]
        s = {"iterations": a["login_200"] + a["login_401"], "sessions_ok": a["sessions_ok"],
             # the plan's assertion accepts 401 (bad-DOB rows), so JMeter marks those successful
             "login_401": a["login_401"], "req_failed": a["req_failed"],
             "req_total": a["req_total"]}
        for e in ENDPOINTS:
            h = hist[lab][e]
            s[e] = {"count": sum(h.values()), "med": pct(h, .5), "p(95)": pct(h, .95), "p(99)": pct(h, .99),
                "p(99.9)": pct(h, .999),
                    "max": float(max(h)) if h else None}
        steps[lab] = s
    return plan, steps


# ----------------------------------------------------------------------------- sampler windows
def delta(a, b, key):
    va, vb = (a or {}).get(key), (b or {}).get(key)
    if va is None or vb is None:
        return None
    return vb - va


def hist_quantiles(h0, h1, qs=(0.5, 0.95, 0.99, 0.999)):
    """Percentiles from the change of a cumulative Prometheus histogram ({le: count}) between two
    samples, by linear interpolation inside the bucket (like PromQL histogram_quantile). Accuracy
    is limited by the bucket width (Micrometer buckets are ~10-20 % apart)."""
    if not h1:
        return None
    edges = []
    for le, c1 in h1.items():
        c = c1 - (h0 or {}).get(le, 0.0)
        edges.append((float("inf") if le in ("+Inf", "Inf") else float(le), max(0.0, c)))
    edges.sort()
    total = edges[-1][1] if edges else 0
    if total <= 0:
        return {"count": 0}
    out = {"count": int(total)}
    for q in qs:
        rank, prev_le, prev_c = q * total, 0.0, 0.0
        val = None
        for le, c in edges:
            if c >= rank:
                if le == float("inf"):
                    val = prev_le                      # above the last finite bucket
                else:
                    frac = 0.0 if c == prev_c else (rank - prev_c) / (c - prev_c)
                    val = prev_le + (le - prev_le) * frac
                break
            prev_le, prev_c = le, c
        out[f"p{q * 100:g}"] = None if val is None else val * 1000.0     # ms
    return out


def window_metrics(meta, ticks, texts, a, b, sessions):
    ts = [t for t in ticks if a - 1 <= t["ts"] <= b + 1]
    if len(ts) < 2:
        return None
    t0, t1 = ts[0], ts[-1]
    dt = t1["ts"] - t0["ts"]
    if dt <= 0:
        return None
    m = {"seconds": round(dt, 1), "standby": t1.get("standby"), "role": meta.get("role", "db"), "ncpu": meta.get("ncpu")}
    per_s = lambda v: None if v is None else v / dt
    per_sess = lambda v: None if (v is None or not sessions) else v / sessions

    # ---------------------------------------------------------------- database
    if m["role"] != "generator":
        db0, db1 = t0.get("db"), t1.get("db")
        m["commits_s"] = per_s(delta(db0, db1, "xact_commit"))
        m["rollbacks_s"] = per_s(delta(db0, db1, "xact_rollback"))
        m["inserts_s"] = per_s(delta(db0, db1, "tup_inserted"))
        hit, read = delta(db0, db1, "blks_hit"), delta(db0, db1, "blks_read")
        m["hit_ratio"] = None if not hit and not read else hit / max(1, hit + read)
        m["blks_read_s"] = per_s(read)
        m["temp_mb_s"] = None if delta(db0, db1, "temp_bytes") is None else delta(db0, db1, "temp_bytes") / dt / 1e6
        m["deadlocks"] = delta(db0, db1, "deadlocks")
        wal = delta(t0.get("wal"), t1.get("wal"), "wal_bytes")
        m["wal_mb_s"] = None if wal is None else wal / dt / 1e6
        m["wal_kb_session"] = None if per_sess(wal) is None else per_sess(wal) / 1e3
        m["wal_fpi_s"] = per_s(delta(t0.get("wal"), t1.get("wal"), "wal_fpi"))
        m["wal_buffers_full"] = delta(t0.get("wal"), t1.get("wal"), "wal_buffers_full")
        c0k, c1k = t0.get("ckpt") or {}, t1.get("ckpt") or {}
        m["ckpt_requested"] = delta(c0k, c1k, "num_requested")
        m["ckpt_timed"] = delta(c0k, c1k, "num_timed")
        n_ck = (m["ckpt_requested"] or 0) + (m["ckpt_timed"] or 0)
        ck_ms = (delta(c0k, c1k, "write_time") or 0) + (delta(c0k, c1k, "sync_time") or 0)
        m["ckpt_count"] = n_ck
        m["ckpt_avg_ms"] = ck_ms / n_ck if n_ck else None          # write + sync time per checkpoint
        lg = None if t0.get("logdir_bytes") is None or t1.get("logdir_bytes") is None \
            else max(0, t1["logdir_bytes"] - t0["logdir_bytes"])
        m["log_mb_s"] = None if lg is None else lg / dt / 1e6
        m["log_kb_session"] = None if per_sess(lg) is None else per_sess(lg) / 1e3
        m["dead_tup_end"] = (t1.get("tables") or {}).get("n_dead_tup")
        m["autovacuums"] = delta(t0.get("tables"), t1.get("tables"), "autovacuum")
        lags = [r.get("lag_bytes") or 0 for t in ts for r in (t.get("repl") or [])]
        m["repl_lag_mb_max"] = max(lags) / 1e6 if lags else None
        rl = [r.get("replay_lag_s") or 0 for t in ts for r in (t.get("repl") or [])]
        m["repl_lag_s_max"] = max(rl) if rl else None
        sl = [t["standby_lag_bytes"] for t in ts if t.get("standby_lag_bytes") is not None]
        m["standby_lag_mb_max"] = max(sl) / 1e6 if sl else None
        m["standby"] = bool(t1.get("standby"))
        m["conflicts"] = delta(db0, db1, "conflicts") if t1.get("standby") else None
        m["iit_max"] = max(t.get("iit_max", 0) for t in ts)
        # wait events: each tick's waits were sampled AFTER its counters, so use t0 .. t1-1
        wsum, nsamp = {}, 0
        for t in ts[:-1]:
            nsamp += t.get("wait_samples", 0)
            for k, v in (t.get("waits") or {}).items():
                wsum[k] = wsum.get(k, 0) + v
        tot = sum(wsum.values())
        m["active_backends_avg"] = tot / nsamp if nsamp else None
        m["lock_waits_avg"] = (sum(v for k, v in wsum.items() if k.startswith("Lock:")) / nsamp) if nsamp else None
        m["waits"] = sorted(((k, 100.0 * v / tot) for k, v in wsum.items()), key=lambda x: -x[1])[:6] if tot else []
        # pg_stat_statements / hot tables come every --detail-every ticks: first and last in the window
        pt = [t for t in ts if t.get("pgss") is not None]
        stm = []
        if len(pt) >= 2:
            pdt = pt[-1]["ts"] - pt[0]["ts"]
            p0 = {r["id"]: r for r in pt[0]["pgss"]}
            for r in pt[-1]["pgss"]:
                b0 = p0.get(r["id"], {})
                calls = r["calls"] - b0.get("calls", 0)
                ms = r["ms"] - b0.get("ms", 0)
                if calls <= 0:
                    continue
                stm.append({"id": r["id"], "calls_s": calls / pdt, "mean_ms": ms / calls, "total_ms": ms,
                            "rows_call": (r["rows"] - b0.get("rows", 0)) / calls,
                            "read_call": (r["read"] - b0.get("read", 0)) / calls,
                            "query": texts.get(r["id"], "")})
            m["statements_s"] = sum(s["calls_s"] for s in stm) if pdt > 0 else None
        tms = sum(s["total_ms"] for s in stm) or 1
        for s in stm:
            s["time_pct"] = 100.0 * s["total_ms"] / tms
        m["statements"] = sorted(stm, key=lambda s: -s["total_ms"])[:8]
        hot = [t for t in ts if t.get("hot") is not None]
        if hot:
            m["hot_tables"] = [{"table": h["t"], "live": h["live"], "dead": h["dead"],
                                "dead_pct": 100.0 * h["dead"] / max(1, h["live"] + h["dead"]),
                                "autovacuums": h["av"], "last_autovacuum_s": h["av_age_s"]}
                               for h in (hot[-1]["hot"] or []) if h["dead"] or h["ins"]][:5]

    # ---------------------------------------------------------------- OS of the sampler host
    c0, c1 = t0["os"]["cpu"], t1["os"]["cpu"]
    d = {k: c1[k] - c0[k] for k in c0}
    total = sum(d.values()) or 1
    busy = total - d["idle"] - d["iowait"]
    m["cpu_busy_pct"] = 100.0 * busy / total
    m["cpu_user_pct"] = 100.0 * (d["user"] + d["nice"]) / total
    m["cpu_sys_pct"] = 100.0 * (d["system"] + d["irq"] + d["softirq"]) / total
    m["cpu_iowait_pct"] = 100.0 * d["iowait"] / total
    m["cpu_steal_pct"] = 100.0 * d["steal"] / total
    m["cpu_ms_session"] = None if not sessions else busy / meta.get("clk_tck", 100) * 1000 / sessions
    # per-interval CPU (generator rule and peaks)
    ivc = []
    for p, q in zip(ts, ts[1:]):
        e = {k: q["os"]["cpu"][k] - p["os"]["cpu"][k] for k in p["os"]["cpu"]}
        tt = sum(e.values()) or 1
        ivc.append(100.0 * (tt - e["idle"] - e["iowait"]) / tt)
    m["cpu_busy_max_pct"] = max(ivc) if ivc else None
    dto = (t1.get("os_ts") or t1["ts"]) - (t0.get("os_ts") or t0["ts"])
    if t0["os"].get("ctxt") is not None:
        m["ctx_switches_s"] = (t1["os"]["ctxt"] - t0["os"]["ctxt"]) / dto
        rq = [t["os"].get("procs_running", 0) for t in ts]
        m["run_queue_avg"], m["run_queue_max"] = sum(rq) / len(rq), max(rq)
        m["procs_blocked_max"] = max(t["os"].get("procs_blocked", 0) for t in ts)
    mem = [t["os"].get("mem") or {} for t in ts]
    if mem and mem[0].get("MemTotal"):
        m["mem_available_gb_min"] = min(x.get("MemAvailable", 0) for x in mem) / 2**30
        m["mem_used_pct_max"] = max(100.0 * (1 - x.get("MemAvailable", 0) / x["MemTotal"]) for x in mem)
        m["swap_used_gb_max"] = max((x.get("SwapTotal", 0) - x.get("SwapFree", 0)) for x in mem) / 2**30
    util, wmb, rmb, iops, fl, flms, has_fl = {}, 0.0, 0.0, 0.0, 0, 0, False
    for dev, s1 in t1["os"]["disk"].items():
        s0 = t0["os"]["disk"].get(dev)
        if not s0:
            continue
        util[dev] = 100.0 * (s1["ticks"] - s0["ticks"]) / max(1.0, dto * 1000)
        if not dev.startswith(("dm-", "md")):
            wmb += (s1["wsec"] - s0["wsec"]) * 512 / dto / 1e6
            rmb += (s1["rsec"] - s0["rsec"]) * 512 / dto / 1e6
            iops += ((s1["r"] - s0["r"]) + (s1["w"] - s0["w"])) / dto
            if "f" in s1 and "f" in s0:
                has_fl = True
                fl += s1["f"] - s0["f"]
                flms += s1["fms"] - s0["fms"]
    if util:
        dev = max(util, key=util.get)
        m["disk_util_pct"], m["disk_busiest"] = min(100.0, util[dev]), dev
        s0, s1 = t0["os"]["disk"][dev], t1["os"]["disk"][dev]
        if "rms" in s1:
            ops = (s1["r"] - s0["r"]) + (s1["w"] - s0["w"])
            m["disk_await_ms"] = ((s1["rms"] - s0["rms"]) + (s1["wms"] - s0["wms"])) / ops if ops else None
            # per-interval average latency of the busiest disk; AC-06 uses its p99 (a true per-I/O
            # p99 needs block-layer tracing, e.g. biolatency)
            aw = []
            for p, q in zip(ts, ts[1:]):
                x, y = p["os"]["disk"].get(dev), q["os"]["disk"].get(dev)
                if x and y:
                    o = (y["r"] - x["r"]) + (y["w"] - x["w"])
                    if o > 0:
                        aw.append(((y["rms"] - x["rms"]) + (y["wms"] - x["wms"])) / o)
            if aw:
                aw.sort()
                m["disk_await_p99_ms"] = aw[min(len(aw) - 1, int(0.99 * len(aw)))]
    m["disk_write_mb_s"], m["disk_read_mb_s"], m["disk_iops"] = wmb, rmb, iops
    m["disk_flush_s"] = fl / dto if has_fl else None
    m["disk_flush_ms"] = (flms / fl if fl else None) if has_fl else None
    m["net_mb_s"] = ((t1["os"]["net"]["rx"] - t0["os"]["net"]["rx"]) +
                     (t1["os"]["net"]["tx"] - t0["os"]["net"]["tx"])) / dto / 1e6
    lo0, lo1 = t0["os"]["net"].get("lo_rx"), t1["os"]["net"].get("lo_rx")
    m["net_lo_mb_s"] = None if lo0 is None or lo1 is None else (lo1 - lo0) / dto / 1e6

    # ---------------------------------------------------------------- app servers (sampler given --app)
    apps = meta.get("apps") or []
    if apps:
        pend, cpu, gcmax = {}, [], []
        for t in ts:
            for url, s in (t.get("app") or {}).items():
                for pool, v in (s.get("pending") or {}).items():
                    pend.setdefault(pool, {}).setdefault(t["ts"], 0)
                    pend[pool][t["ts"]] += v
                if s.get("process_cpu") is not None:
                    cpu.append(100.0 * s["process_cpu"])
                if s.get("gc_max") is not None:
                    gcmax.append(1000.0 * s["gc_max"])
        m["app_pending_max"] = {p: max(v.values()) for p, v in pend.items()}

        def app_delta(key):
            out = {}
            for url in apps:
                s0, s1 = (t0.get("app") or {}).get(url) or {}, (t1.get("app") or {}).get(url) or {}
                for k, v in (s1.get(key) or {}).items():
                    out[k] = out.get(k, 0) + v - (s0.get(key) or {}).get(k, 0)
            return out
        m["app_pool_timeouts"] = app_delta("timeouts")
        m["app_db_errors"] = app_delta("db_errors")
        m["app_http"] = app_delta("http")
        m["app_cpu_max_pct"] = max(cpu) if cpu else None
        gcs = sum(((((t1.get("app") or {}).get(u) or {}).get("gc_s") or 0) -
                   ((((t0.get("app") or {}).get(u) or {}).get("gc_s")) or 0)) for u in apps)
        m["app_gc_pct"] = 100.0 * gcs / dt / len(apps)            # share of time in GC pauses, per JVM
        m["app_gc_max_ms"] = max(gcmax) if gcmax else None
        # histograms merged over app servers: DB-side transaction time and server-side HTTP time
        names = set()
        for u in apps:
            names |= set((((t1.get("app") or {}).get(u) or {}).get("hist") or {}))
        lat = {}
        for n in names:
            h0, h1 = {}, {}
            for u in apps:
                for le, c in ((((t0.get("app") or {}).get(u) or {}).get("hist") or {}).get(n) or {}).items():
                    h0[le] = h0.get(le, 0) + c
                for le, c in ((((t1.get("app") or {}).get(u) or {}).get("hist") or {}).get(n) or {}).items():
                    h1[le] = h1.get(le, 0) + c
            lat[n] = hist_quantiles(h0, h1)
        m["db_latency"] = {n.split(":", 1)[1]: v for n, v in lat.items() if n.startswith("db:")}
        m["http_latency"] = {n.split(":", 1)[1]: v for n, v in lat.items() if n.startswith("http:")}
    return m


# ----------------------------------------------------------------------------- verdicts
def bottleneck_hints(step, dbm, dropped):
    h = []
    if dropped:
        h.append(f"load generator dropped {dropped} sessions over the run (VU pool or generator CPU)")
    for name, m in dbm.items():
        if not m:
            continue
        if m.get("role") == "generator":
            if m["cpu_busy_pct"] > 70:
                h.append(f"load generator {name} CPU {m['cpu_busy_pct']:.0f}% (> 70 %: the run is void, proposal 9.2)")
            continue
        if m.get("app_pool_timeouts") and sum(m["app_pool_timeouts"].values()) > 0:
            h.append(f"app DB-pool timeouts {fdict(m['app_pool_timeouts'])} (pool wait > limit -> HTTP 503)")
        elif m.get("app_pending_max") and max(m["app_pending_max"].values()) > 0:
            h.append(f"app threads waiting for a DB connection (max pending {fdict(m['app_pending_max'])})")
        if m.get("app_cpu_max_pct") is not None and m["app_cpu_max_pct"] >= 85:
            h.append(f"app server CPU {m['app_cpu_max_pct']:.0f}%")
        if (m.get("app_gc_pct") or 0) >= 5:
            h.append(f"app JVM spends {m['app_gc_pct']:.0f}% of the time in GC pauses (max {fmt(m.get('app_gc_max_ms'), 0)} ms)")
        if m["cpu_busy_pct"] >= 85:
            h.append(f"{name} host CPU {m['cpu_busy_pct']:.0f}% busy")
        if m.get("ncpu") and (m.get("run_queue_avg") or 0) > 2 * m["ncpu"]:
            h.append(f"{name} run queue {m['run_queue_avg']:.0f} (more runnable processes than CPUs)")
        if m.get("disk_util_pct", 0) >= 90 or m["cpu_iowait_pct"] >= 20 or (m.get("disk_await_p99_ms") or 0) > 10:
            h.append(f"{name} disk {m.get('disk_busiest')} {m.get('disk_util_pct', 0):.0f}% busy, iowait "
                     f"{m['cpu_iowait_pct']:.0f}%, latency p99 {fmt(m.get('disk_await_p99_ms'), 1)} ms")
        if (m.get("mem_used_pct_max") or 0) >= 95 or (m.get("swap_used_gb_max") or 0) > 0.5:
            h.append(f"{name} memory {m.get('mem_used_pct_max', 0):.0f}% used, swap {fmt(m.get('swap_used_gb_max'), 1)} GB")
        # wait shares only mean something when sessions are actually piling up
        busy_db = (m.get("active_backends_avg") or 0) >= 1
        for k, pct in (m.get("waits") or [])[:3] if busy_db else []:
            if k.startswith("CPU:") or pct < 25:
                continue
            kind = k.split(":")[0]
            what = {"Lock": "row/table lock contention", "LWLock": "internal lock contention (WAL, buffers)",
                    "IO": "I/O waits", "Client": "waiting on the client/network (app side)",
                    "IPC": "inter-process waits (sync replication?)"}.get(kind, kind)
            h.append(f"{name} wait {k} = {pct:.0f}% of active time ({what})")
        if (m.get("repl_lag_s_max") or 0) > 5:
            h.append(f"replica falling behind: replay lag up to {m['repl_lag_s_max']:.1f}s")
        if m.get("ckpt_requested"):
            h.append(f"{name}: {m['ckpt_requested']} checkpoints forced by WAL volume (raise max_wal_size)")
    return h


def evaluate_acs(measured, samplers, limits, dropped, target, headroom):
    """Acceptance criteria of proposal 11.2, evaluated at the step whose rate is the target
    (25,000 sessions/s). Below the target nothing is certified: values are shown for information."""
    at = next((s for s in measured if s["target"] >= target), None)
    ev = at or (measured[-1] if measured else None)
    rows = []
    if ev is None:
        return {"target": target, "step": None, "at_target": False, "rows": []}
    note_nt = (f"target {target:,}/s not reached in this run (highest measured {ev['target']:,}/s); "
               "value shown for information")

    def row(ac, crit, thr, measured_txt, ok):
        if ok is None:
            st = "NOT MEASURED"
        elif not at:
            st = "NOT AT TARGET"
        else:
            st = "PASS" if ok else "FAIL"
        rows.append({"id": ac, "criterion": crit, "threshold": thr, "measured": measured_txt,
                     "step": ev["target"], "status": st, "note": "" if at or ok is None else note_nt})

    # AC-01: sustained rate and headroom
    hr = next((s for s in measured if s["target"] >= target * headroom), None)
    if not at:
        rows.append({"id": "AC-01", "criterion": "Peak login rate sustained", "status": "NOT AT TARGET",
                     "threshold": f"{target:,}/s for 10 min; {target * headroom:,.0f}/s without breaching AC-02..05",
                     "measured": f"highest measured step {ev['target']:,}/s", "step": ev["target"], "note": ""})
    else:
        ok = at["pass"] and at["seconds"] >= 540
        txt = f"{at['target']:,}/s {'passed' if at['pass'] else 'FAILED'} over {at['seconds']:.0f} s measured"
        if hr is None:
            txt += f"; {target * headroom:,.0f}/s not tested"
        else:
            txt += f"; {hr['target']:,}/s {'passed' if hr['pass'] else 'FAILED'}"
        status = "FAIL" if not at["pass"] or (hr and not hr["pass"]) else ("PASS" if ok and hr else "NOT MEASURED")
        rows.append({"id": "AC-01", "criterion": "Peak login rate sustained", "status": status,
                     "threshold": f"{target:,}/s for 10 min; {target * headroom:,.0f}/s without breaching AC-02..05",
                     "measured": txt, "step": at["target"],
                     "note": "" if at["seconds"] >= 540 else f"measured window only {at['seconds']:.0f} s (needs 10 min hold)"})
    # AC-02 / AC-03: DB-side latency (app-measured histogram)
    dbl = next(((ev["db"].get(n) or {}).get("db_latency") for n in samplers
                if (ev["db"].get(n) or {}).get("db_latency")), None) or {}
    r_, l_ = dbl.get("result") or {}, dbl.get("login") or {}
    row("AC-02", "Read latency at DB (result read)", "p99 <= 50 ms, p99.9 <= 200 ms",
        f"p99 {fmt(r_.get('p99'), 1)} ms, p99.9 {fmt(r_.get('p99.9'), 1)} ms ({r_.get('count', 0):,} reads)",
        None if r_.get("p99") is None else (r_["p99"] <= 50 and (r_.get("p99.9") or 0) <= 200))
    row("AC-03", "Write / login transaction latency at DB", "p99 <= 100 ms",
        f"p99 {fmt(l_.get('p99'), 1)} ms ({l_.get('count', 0):,} transactions)",
        None if l_.get("p99") is None else l_["p99"] <= 100)
    # AC-04: end-to-end
    e2e = {e: ev[e].get("p(99)") for e in ENDPOINTS}
    row("AC-04", "End-to-end API latency", "p99 <= 300 ms reads, <= 500 ms writes",
        ", ".join(f"{e} {fmt(v, 0)} ms" for e, v in e2e.items()),
        None if None in e2e.values() else (e2e["login"] <= 500 and e2e["result"] <= 300 and e2e["cutoff"] <= 300))
    # AC-05: errors + late (dropped) sessions
    err = ev.get("error_rate")
    late_note = f", {dropped:,} late (dropped) sessions in the whole run" if dropped else ", 0 late sessions"
    row("AC-05", "Error and late-transaction rate", "< 0.1 %",
        f"{fmt(None if err is None else 100 * err, 3)} % request errors{late_note}",
        None if err is None else (err < 0.001 and not dropped))
    # AC-06: DB host headroom
    parts, oks = [], []
    for n in samplers:
        m = ev["db"].get(n) or {}
        if m and m.get("role") != "generator":
            parts.append(f"{n} CPU {fmt(m['cpu_busy_pct'], 0)}%, disk p99 {fmt(m.get('disk_await_p99_ms'), 2)} ms")
            oks.append(m["cpu_busy_pct"] <= 70 and (m.get("disk_await_p99_ms") is not None and m["disk_await_p99_ms"] <= 2))
    row("AC-06", "Resource headroom on the DB servers", "CPU <= 70 %, disk latency p99 <= 2 ms",
        "; ".join(parts) or "no DB sampler", all(oks) if oks else None)
    rows.append({"id": "AC-07", "criterion": "Soak stability", "threshold": "p99 drift <= 10 % over 8 h",
                 "measured": "-", "step": None, "status": "NOT APPLICABLE", "note": "soak runs (SK-01/02) only"})
    rows.append({"id": "AC-08", "criterion": "Failover", "threshold": "restored <= 30 s, no committed loss",
                 "measured": "-", "step": None, "status": "NOT APPLICABLE", "note": "failover runs (HA-01) only"})
    rows.append({"id": "AC-09", "criterion": "Extension overhead", "threshold": "reported, no pass/fail",
                 "measured": "compare this run with its features-off run (compare_runs.py)", "step": None,
                 "status": "INFO", "note": ""})
    return {"target": target, "step": ev["target"], "at_target": bool(at), "rows": rows}


def fdict(d):
    """{"primary": 0.0, "replica": 2.0} -> 'primary 0, replica 2'"""
    if not d:
        return "-"
    return ", ".join(f"{k} {v:,.0f}" for k, v in sorted(d.items()))


def fmt(v, nd=1, suffix=""):
    if v is None:
        return "-"
    if isinstance(v, float):
        return f"{v:,.{nd}f}{suffix}"
    return f"{v:,}{suffix}"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("run_dir")
    ap.add_argument("--p99", default="login=500,result=300,cutoff=300", help="p99 limits in ms")
    ap.add_argument("--max-error", type=float, default=0.001, help="max request error rate (default 0.1%%)")
    ap.add_argument("--min-achieved", type=float, default=0.98, help="achieved/target sessions (default 0.98)")
    ap.add_argument("--no-index", action="store_true", help="do not refresh <results>/index.html")
    ap.add_argument("--target-rate", type=int, default=25000,
                    help="sessions/s the acceptance criteria are evaluated at (proposal: 25,000)")
    ap.add_argument("--headroom", type=float, default=1.2, help="AC-01 headroom factor (proposal: 1.2)")
    a = ap.parse_args()
    limits = {k: float(v) for k, v in (x.split("=") for x in a.p99.split(","))}
    run = os.path.abspath(a.run_dir)
    name = os.path.basename(run)

    summaries, plans = load_k6(run)
    if plans:
        plan = plans[0]
        start = statistics.median(p["start_ms"] for p in plans) / 1000.0
        tool = f"k6, {len(summaries)} generator(s)"
        dropped = sum((m.get("dropped_iterations") or {}).get("count", 0) for m in summaries)
        steps_load = {w["label"]: k6_step(summaries, w["label"]) for w in plan["windows"]}
        # Steps are defined on k6's scheduling clock; samplers use the wall clock. If the wall clock
        # drifted during the run (seen on WSL2), stretch the windows to the measured wall duration.
        # (teardown runs right after the last iterations, so end_ms ~ start + scenario length)
        if plan.get("end_ms") and plan.get("scenario_s"):
            ratio = (plan["end_ms"] - plan["start_ms"]) / 1000.0 / plan["scenario_s"]
            if 1.0 < ratio < 1.1:
                for w in plan["windows"]:
                    w["from"], w["to"] = w["from"] * ratio, w["to"] * ratio
                plan["wall_ratio"] = ratio
    else:
        plan, steps_load = load_jmeter(run)
        if not plan:
            sys.exit(f"{run}: no k6-plan-*.json, nor jmeter-plan.json + .jtl")
        start = plan["start_ms"] / 1000.0
        tool, dropped = "JMeter", 0

    samplers = {}
    for p in sorted(glob.glob(os.path.join(run, "sampler-*.jsonl"))):
        meta, ticks, texts = load_jsonl(p)
        samplers[meta.get("name") or os.path.basename(p)[8:-6]] = (meta, ticks, texts)
    features = None
    fs = next((p for p in (os.path.join(run, "feature_status.json"), os.path.join(run, "feature_status-primary.json"))
               if os.path.exists(p)), "")
    if fs:
        with open(fs) as f:
            features = json.load(f)
    elif samplers:
        prim = samplers.get("primary") or next(iter(samplers.values()))
        features = prim[0].get("features")

    run_meta = None
    rm = os.path.join(run, "run.json")
    if os.path.exists(rm):
        with open(rm) as f:
            run_meta = json.load(f)
    # when the load was interrupted (Ctrl+C): run_test.sh records it in run.json, run_k6.sh in
    # k6-interrupted-<node>. A step still running at that moment is not measured, not failed.
    stops = [float(run_meta["interrupted_epoch"])] if run_meta and run_meta.get("interrupted_epoch") else []
    for p in glob.glob(os.path.join(run, "k6-interrupted-*")):
        try:
            with open(p) as f:
                stops.append(float(f.read().strip()))
        except (OSError, ValueError):
            pass
    interrupted_at = min(stops) if stops else None
    interrupted = bool(interrupted_at or (run_meta and run_meta.get("status") == "interrupted"))

    steps = []
    for w in plan["windows"]:
        L = steps_load[w["label"]]
        secs = (w["to"] - w["from"]) / plan.get("wall_ratio", 1.0)     # duration on the load tool's clock
        st = {"label": w["label"], "target": w["target"], "seconds": secs,
              "from_epoch": start + w["from"], "to_epoch": start + w["to"],
              "achieved": L["iterations"] / secs if secs else 0, "sessions": L["iterations"],
              "sessions_ok": L["sessions_ok"], "login_401": L["login_401"],
              "error_rate": (L["req_failed"] / L["req_total"]) if L["req_total"] else None}
        for e in ENDPOINTS:
            st[e] = L[e]
        why = []
        cut = interrupted_at is not None and st["to_epoch"] > interrupted_at
        if cut or not st["sessions"]:
            # nothing ran in this window (run stopped early, or the step never started):
            # a label with a reason, never a pass and never a fail
            st["status"], st["pass"] = "not_measured", None
            st["fail_reasons"] = [("run interrupted during this step" if st["from_epoch"] < interrupted_at
                                   else "run interrupted before this step") if cut
                                  else "run interrupted before this step" if interrupted
                                  else "no sessions in this window"]
        else:
            if st["target"] and st["achieved"] < a.min_achieved * st["target"]:
                why.append(f"achieved {st['achieved']:.1f}/s < {a.min_achieved:.0%} of {st['target']}/s")
            if st["error_rate"] is not None and st["error_rate"] >= a.max_error:
                why.append(f"errors {100 * st['error_rate']:.2f}%")
            for e in ENDPOINTS:
                p99 = st[e].get("p(99)")
                if p99 is not None and p99 >= limits.get(e, 1e18):
                    why.append(f"{e} p99 {p99:.0f} ms")
            st["pass"], st["fail_reasons"] = not why, why
            st["status"] = "pass" if not why else "fail"
        st["db"] = {n: window_metrics(m, t, tx, st["from_epoch"], st["to_epoch"], st["sessions"])
                    for n, (m, t, tx) in samplers.items()}
        steps.append(st)

    ordered = sorted(steps, key=lambda s: s["target"])
    measured = [s for s in ordered if s["status"] != "not_measured"]
    not_measured = [s for s in ordered if s["status"] == "not_measured"]
    knee, first_fail = None, None
    for s in measured:
        if s["pass"] and first_fail is None:
            knee = s
        elif not s["pass"] and first_fail is None:
            first_fail = s
    focus = first_fail or (measured[-1] if measured else ordered[-1])
    hints = bottleneck_hints(focus, focus["db"], dropped)
    gens = [n for n, (m, _, _) in samplers.items() if m.get("role") == "generator"]
    dbs = [n for n, (m, _, _) in samplers.items() if m.get("role") != "generator"]
    gen_over = [(s["target"], n, s["db"][n]["cpu_busy_pct"]) for s in measured for n in gens
                if s["db"].get(n) and s["db"][n]["cpu_busy_pct"] > 70]
    acs = evaluate_acs(measured, samplers, limits, dropped, a.target_rate, a.headroom)

    if not measured:
        verdict = {"status": "NOT MEASURED", "label": "NOT MEASURED",
                   "text": "No step was measured, so this run has no result."}
    elif knee and first_fail:
        verdict = {"status": "KNEE", "label": f"KNEE {knee['target']:,}/s",
                   "text": f"Knee at {knee['target']:,} sessions/s: the highest step that passed. First failing step "
                           f"{first_fail['target']:,}/s ({'; '.join(first_fail['fail_reasons'])})."}
    elif knee:
        verdict = {"status": "PASS", "label": f"PASS up to {knee['target']:,}/s",
                   "text": f"All {len(measured)} measured steps passed, up to {knee['target']:,} sessions/s; "
                           "the knee is above the measured range."}
    else:
        verdict = {"status": "FAIL", "label": "FAIL",
                   "text": f"No step passed. Lowest step {measured[0]['target']:,}/s: "
                           f"{'; '.join(measured[0]['fail_reasons'])}."}
    if not_measured:
        verdict["text"] += (f" {len(not_measured)} step(s) not measured ("
                            + ", ".join(f"{s['target']:,}/s" for s in not_measured)
                            + "), so the result only covers the measured steps.")
    verdict["interrupted"] = interrupted
    verdict["void_reasons"] = ([f"{dropped:,} dropped (late) sessions: the generator could not start them on time"]
                               if dropped else []) + \
        [f"load generator {n} at {v:.0f}% CPU during the {t:,}/s step (limit 70 %, proposal 9.2)" for t, n, v in gen_over]
    if not gens:
        verdict["void_reasons_unchecked"] = "generator CPU not sampled (run sampler.py --no-db on each generator)"
    verdict["void"] = bool(verdict["void_reasons"])
    verdict["acs"] = {r["id"]: r["status"] for r in acs["rows"]}
    verdict["not_measured"] = [s["target"] for s in not_measured]

    # time series per sampler for the charts (one point per sample interval)
    timeseries = {}
    for n, (meta, ticks, _) in samplers.items():
        ser = {"t": [], "cpu": [], "iowait": [], "commits": [], "wal": [], "active": [], "await": [], "runq": [],
               "app_rps": [], "app_5xx": [], "app_pending": {}, "role": meta.get("role", "db")}
        for p0, p1 in zip(ticks, ticks[1:]):
            dt = p1["ts"] - p0["ts"]
            if dt <= 0:
                continue
            c0, c1 = p0["os"]["cpu"], p1["os"]["cpu"]
            d = {k: c1[k] - c0[k] for k in c0}
            tot = sum(d.values()) or 1
            ser["t"].append(p1["ts"])
            ser["cpu"].append(round(100.0 * (tot - d["idle"] - d["iowait"]) / tot, 2))
            ser["iowait"].append(round(100.0 * d["iowait"] / tot, 2))
            ser["runq"].append(p1["os"].get("procs_running"))
            best, bt = None, -1
            for dev, y in p1["os"]["disk"].items():
                x = p0["os"]["disk"].get(dev)
                if x and "rms" in y and not dev.startswith(("dm-", "md")) and y["ticks"] - x["ticks"] > bt:
                    best, bt = (x, y), y["ticks"] - x["ticks"]
            if best:
                x, y = best
                ops = (y["r"] - x["r"]) + (y["w"] - x["w"])
                ser["await"].append(round(((y["rms"] - x["rms"]) + (y["wms"] - x["wms"])) / ops, 3) if ops else None)
            else:
                ser["await"].append(None)
            dc = delta(p0.get("db"), p1.get("db"), "xact_commit")
            ser["commits"].append(None if dc is None else round(dc / dt, 2))
            dw = delta(p0.get("wal"), p1.get("wal"), "wal_bytes")
            ser["wal"].append(None if dw is None else round(dw / dt / 1e6, 4))
            ns = p0.get("wait_samples") or 0
            ser["active"].append(round(sum((p0.get("waits") or {}).values()) / ns, 2) if ns else None)
            if meta.get("apps"):
                rps = r5 = 0.0
                for url in meta["apps"]:
                    h0 = ((p0.get("app") or {}).get(url) or {}).get("http") or {}
                    h1 = ((p1.get("app") or {}).get(url) or {}).get("http") or {}
                    for code, v in h1.items():
                        dv = max(0.0, v - h0.get(code, 0))
                        rps += dv
                        r5 += dv if code.startswith("5") else 0
                    for pool, v in (((p1.get("app") or {}).get(url) or {}).get("pending") or {}).items():
                        ser["app_pending"].setdefault(pool, [0.0] * (len(ser["t"]) - 1))
                ser["app_rps"].append(round(rps / dt, 2))
                ser["app_5xx"].append(round(r5 / dt, 2))
                for pool in ser["app_pending"]:
                    total = sum(((((p1.get("app") or {}).get(u) or {}).get("pending") or {}).get(pool, 0))
                                for u in meta["apps"])
                    ser["app_pending"][pool].append(total)
        timeseries[n] = ser

    # ------------------------------------------------------------------ report
    R = [f"# Run {name}", ""]
    R.append(f"Load tool: {tool}. Started {time.strftime('%Y-%m-%d %H:%M:%S %Z', time.localtime(start))}. "
             "Steps measured after settle. "
             f"Pass = achieved >= {a.min_achieved:.0%} of target, errors < {100 * a.max_error:.1f}%, "
             f"p99 " + ", ".join(f"{k} < {v:.0f} ms" for k, v in limits.items()) + ".")
    R.append("")
    if features:
        R += ["## Features", "",
              f"- table access methods: `{json.dumps(features.get('table_access_methods'))}`"
              f" (TDE extension: {features.get('tde_extension') or 'none'})",
              f"- immutable tables: {', '.join(features.get('immutable_tables') or []) or 'none'}",
              f"- sdbaudit.log: `{features.get('sdbaudit_log')}` (loaded: {features.get('sdbaudit_loaded')})",
              f"- shared_preload_libraries: `{features.get('shared_preload_libraries')}`",
              f"- synchronous_standby_names: `{features.get('synchronous_standby_names') or ''}`", ""]
    R += ["## Verdict", "", f"**{verdict['label']}.** {verdict['text']}"]
    if interrupted:
        R.append("\n**Run interrupted** before it finished; steps that did not complete are marked *not measured*.")
    for vr in verdict["void_reasons"]:
        R.append(f"\n**Run void: {vr}.** Results after that point measure the generator, not the system.")
    if verdict.get("void_reasons_unchecked"):
        R.append(f"\nNote: {verdict['void_reasons_unchecked']}.")
    R.append("")
    R.append(f"Signals at the {'first failing' if first_fail else 'highest measured'} step ({focus['target']:,}/s):")
    R += [f"- {h}" for h in hints] or ["- none of the saturation checks fired"]
    R.append("")
    R += ["## Acceptance criteria (proposal 11.2)", ""]
    if acs["step"] is None:
        R += ["No step was measured.", ""]
    else:
        R.append(f"Evaluated at the {acs['step']:,} sessions/s step"
                 + ("." if acs["at_target"] else
                    f": the {acs['target']:,}/s target was not reached in this run, so nothing is certified "
                    "(*not at target*); values are shown for information.") + "")
        R += ["", "| ID | criterion | threshold | measured | status |", "|---|---|---|---|---|"]
        for r in acs["rows"]:
            st = {"PASS": "pass", "FAIL": "FAIL"}.get(r["status"], "not measured" if r["status"] == "NOT MEASURED"
                                                     else r["status"].lower())
            R.append(f"| {r['id']} | {r['criterion']} | {r['threshold']} | {r['measured']}"
                     f"{' (' + r['note'] + ')' if r['note'] else ''} | {st} |")
        R += ["", "AC-02/AC-03 use the app-measured DB transaction time (pool wait + statements + commit); "
              "AC-06 disk latency is the p99 of 1-second average latencies of the busiest disk.", ""]

    R += ["## Load steps (client side, k6/JMeter)", "",
          "| step (sessions/s) | achieved/s | % | sessions | p99 login | p99 result | p99 cutoff | p99.9 login | "
          "p99.9 result | max | errors | verdict |",
          "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|"]
    for s in ordered:
        mx = max([x for x in (s[e].get("max") for e in ENDPOINTS) if x is not None], default=None)
        R.append("| {:,} | {} | {} | {:,} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
            s["target"], fmt(s["achieved"]), fmt(100 * s["achieved"] / s["target"] if s["target"] else None, 0, "%"),
            s["sessions"], fmt(s["login"].get("p(99)"), 0, " ms"), fmt(s["result"].get("p(99)"), 0, " ms"),
            fmt(s["cutoff"].get("p(99)"), 0, " ms"), fmt(s["login"].get("p(99.9)"), 0, " ms"),
            fmt(s["result"].get("p(99.9)"), 0, " ms"), fmt(mx, 0, " ms"),
            fmt(100 * s["error_rate"] if s["error_rate"] is not None else None, 2, "%"),
            "pass" if s["status"] == "pass" else
            ("not measured: " + "; ".join(s["fail_reasons"])) if s["status"] == "not_measured" else
            "FAIL: " + "; ".join(s["fail_reasons"])))
    R.append("")

    appn = next((n for n in samplers if samplers[n][0].get("apps")), None)
    if appn:
        R += [f"## App-side latency (all app servers, sampled by {appn})", "",
              "DB = time of one DB transaction measured in the app (pool wait + statements + commit; AC-02/AC-03). "
              "Server = time inside the app per endpoint. From histograms, accurate to ~10-20 %.", "",
              "| step | DB login p50 | DB login p99 | DB login p99.9 | DB result p50 | DB result p99 | DB result p99.9 | "
              "cut-off loads | server p99 login | server p99 result | server p99 cutoff |",
              "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
        for s in ordered:
            m = s["db"].get(appn) or {}
            dl, hl = m.get("db_latency") or {}, m.get("http_latency") or {}
            L, Rr, C = dl.get("login") or {}, dl.get("result") or {}, dl.get("cutoff") or {}
            hp = lambda key: (hl.get(key) or {}).get("p99")
            R.append("| {:,} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
                s["target"], fmt(L.get("p50"), 2, " ms"), fmt(L.get("p99"), 2, " ms"), fmt(L.get("p99.9"), 2, " ms"),
                fmt(Rr.get("p50"), 2, " ms"), fmt(Rr.get("p99"), 2, " ms"), fmt(Rr.get("p99.9"), 2, " ms"),
                f"{C.get('count', 0):,}", fmt(hp("/api/v1/auth/login"), 1, " ms"),
                fmt(hp("/api/v1/results/{cenId}"), 1, " ms"), fmt(hp("/api/v1/cutoffs/{cenId}"), 1, " ms")))
        R.append("")

    for sname in dbs:
        R += [f"## {sname}: database", "",
              "| step | commits/s | statements/s | inserts/s | WAL MB/s | WAL KB/session | log MB/s | hit % | "
              "checkpoints (avg ms) | active backends | lock waits | top waits | replica lag |",
              "|---:|---:|---:|---:|---:|---:|---:|---:|---|---:|---:|---|---:|"]
        for s in ordered:
            m = s["db"].get(sname)
            if not m:
                R.append(f"| {s['target']:,} | no samples in window |" + " |" * 11)
                continue
            ck = "-" if not m.get("ckpt_count") else f"{m['ckpt_count']} ({fmt(m.get('ckpt_avg_ms'), 0)} ms)"
            R.append("| {:,} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
                s["target"], fmt(m["commits_s"], 0), fmt(m.get("statements_s"), 0), fmt(m["inserts_s"], 0),
                fmt(m["wal_mb_s"], 2), fmt(m["wal_kb_session"], 2), fmt(m["log_mb_s"], 2),
                fmt(100 * m["hit_ratio"] if m["hit_ratio"] is not None else None, 2), ck,
                fmt(m["active_backends_avg"], 1), fmt(m.get("lock_waits_avg"), 2),
                ", ".join(f"{k} {p:.0f}%" for k, p in m["waits"][:3]) or "-",
                (f"{fmt(m.get('standby_lag_mb_max'), 2)} MB not replayed, {fmt(m.get('conflicts'), 0)} conflicts"
                 if m.get("standby") else
                 "-" if m.get("repl_lag_s_max") is None else f"{m['repl_lag_s_max']:.2f} s / {m['repl_lag_mb_max']:.1f} MB")))
        R.append("")
        fm = focus["db"].get(sname)
        if fm and fm.get("hot_tables"):
            R += [f"### {sname}: tables with most dead tuples at the end of the {focus['target']:,}/s step", "",
                  "| table | live | dead | dead % | autovacuums | last autovacuum |", "|---|---:|---:|---:|---:|---:|"]
            for h in fm["hot_tables"]:
                age = "never" if h["last_autovacuum_s"] is None else f"{h['last_autovacuum_s']:.0f} s ago"
                R.append(f"| {h['table']} | {h['live']:,} | {h['dead']:,} | {h['dead_pct']:.1f}% | {h['autovacuums']} | {age} |")
            R.append("")
        if fm and fm["statements"]:
            R += [f"### {sname}: top statements at {focus['target']:,}/s", "",
                  "| % DB time | calls/s | mean ms | rows/call | blocks read/call | query |", "|---:|---:|---:|---:|---:|---|"]
            for st in fm["statements"][:6]:
                q = st["query"].replace("|", "\\|")[:140] or st["id"]
                R.append(f"| {st['time_pct']:.0f}% | {st['calls_s']:,.0f} | {st['mean_ms']:.3f} | "
                         f"{st['rows_call']:.1f} | {st['read_call']:.2f} | `{q}` |")
            R.append("")

    for sname in list(samplers):
        role = samplers[sname][0].get("role", "db")
        R += [f"## {sname}: host ({'load generator' if role == 'generator' else 'database server'}, "
              f"{samplers[sname][0].get('host', '?')}, {samplers[sname][0].get('ncpu', '?')} CPUs)", "",
              "| step | CPU avg (max) | user / sys / iowait | CPU ms/session | run queue avg (max) | context sw/s | "
              "memory free | swap used | IOPS | disk read / write MB/s | busiest disk | disk latency avg / p99 | "
              "flushes/s (avg ms) | net MB/s |",
              "|---:|---:|---|---:|---:|---:|---:|---:|---:|---|---|---|---|---:|"]
        for s in ordered:
            m = s["db"].get(sname)
            if not m:
                R.append(f"| {s['target']:,} | no samples in window |" + " |" * 11)
                continue
            R.append("| {:,} | {} ({}) | {} / {} / {} | {} | {} ({}) | {} | {} | {} | {} | {} / {} | {} {} | {} / {} | {} | {} |".format(
                s["target"], fmt(m["cpu_busy_pct"], 0, "%"), fmt(m.get("cpu_busy_max_pct"), 0, "%"),
                fmt(m["cpu_user_pct"], 0, "%"), fmt(m["cpu_sys_pct"], 0, "%"), fmt(m["cpu_iowait_pct"], 0, "%"),
                fmt(m["cpu_ms_session"], 2) if role != "generator" else "-",
                fmt(m.get("run_queue_avg"), 1), fmt(m.get("run_queue_max")), fmt(m.get("ctx_switches_s"), 0),
                fmt(m.get("mem_available_gb_min"), 1, " GB"), fmt(m.get("swap_used_gb_max"), 2, " GB"),
                fmt(m.get("disk_iops"), 0), fmt(m.get("disk_read_mb_s"), 1), fmt(m.get("disk_write_mb_s"), 1),
                m.get("disk_busiest", "-"), fmt(m.get("disk_util_pct"), 0, "%"),
                fmt(m.get("disk_await_ms"), 2, " ms"), fmt(m.get("disk_await_p99_ms"), 2, " ms"),
                "-" if m.get("disk_flush_s") is None else f"{m['disk_flush_s']:,.0f} ({fmt(m.get('disk_flush_ms'), 2)} ms)",
                fmt(m["net_mb_s"], 1) + ("" if m.get("net_lo_mb_s") is None else f" (lo {m['net_lo_mb_s']:.1f})")))
        R.append("")
        if samplers[sname][0].get("apps"):
            R += [f"### App servers (sampled by {sname})", "",
                  "| step | max threads waiting for a connection | pool timeouts | DB errors | max app CPU | "
                  "GC time | max GC pause | HTTP responses by status |",
                  "|---:|---|---|---|---:|---:|---:|---|"]
            for s in ordered:
                m = s["db"].get(sname) or {}
                R.append("| {:,} | {} | {} | {} | {} | {} | {} | {} |".format(
                    s["target"], fdict(m.get("app_pending_max")), fdict(m.get("app_pool_timeouts")),
                    fdict({k: v for k, v in (m.get("app_db_errors") or {}).items() if v})
                    if any((m.get("app_db_errors") or {}).values()) or not m.get("app_http") else "0",
                    fmt(m.get("app_cpu_max_pct"), 0, "%"), fmt(m.get("app_gc_pct"), 2, "%"),
                    fmt(m.get("app_gc_max_ms"), 0, " ms"), fdict(m.get("app_http"))))
            R.append("")
    R += ["## Notes", "",
          "- p99/p95 with several generators = worst generator (conservative); counts are summed.",
          "- Host CPU, disk and network are those of the machine each sampler ran on. On a shared machine "
          "(local tests) they include the app and the load generator.",
          "- CPU ms/session = busy CPU time of that host in the window / sessions started in the window.",
          "- Flushes = fsync/FUA requests to the disks (WAL commits wait on them); their latency is not part of "
          "the read/write disk latency.",
          "- Network = all interfaces except loopback; loopback traffic is shown in brackets (lo): on a "
          "one-machine test the app, DB and load tool talk only over loopback.",
          "- Run queue is an instant reading per sample; samplers sharing one machine read it at different "
          "moments, so their values differ slightly.",
          "- CPU avg (max): average over the step window (max of the 1-second samples). A load generator "
          "above 70 % CPU voids the run (proposal 9.2).",
          "- Disk latency: average per I/O over the window, and the p99 of the 1-second averages of the busiest disk.",
          "- log MB/s = growth of the server log directory (sdbAudit writes its records there).", ""]

    with open(os.path.join(run, "report.md"), "w") as f:
        f.write("\n".join(R))
    with open(os.path.join(run, "steps.csv"), "w", newline="") as f:
        w = csv.writer(f)
        cols = ["cpu_busy_pct", "cpu_busy_max_pct", "cpu_user_pct", "cpu_sys_pct", "cpu_iowait_pct", "cpu_ms_session",
                "run_queue_avg", "ctx_switches_s", "mem_available_gb_min", "swap_used_gb_max", "disk_iops",
                "disk_util_pct", "disk_await_ms", "disk_await_p99_ms", "net_mb_s",
                "commits_s", "statements_s", "inserts_s", "wal_mb_s", "wal_kb_session", "log_mb_s", "log_kb_session",
                "hit_ratio", "ckpt_count", "ckpt_avg_ms", "active_backends_avg", "lock_waits_avg", "repl_lag_s_max",
                "app_gc_pct", "app_gc_max_ms"]
        hdr = ["step", "achieved", "sessions", "error_rate", "status"] + \
              [f"{e}_{s}" for e in ENDPOINTS for s in ("med", "p95", "p99", "p99.9", "max")] + \
              ["db_login_p99", "db_login_p99.9", "db_result_p99", "db_result_p99.9"] + \
              [f"{n}_{c}" for n in samplers for c in cols]
        w.writerow(hdr)
        for s in ordered:
            row = [s["target"], round(s["achieved"], 2), s["sessions"], s["error_rate"], s["status"]]
            row += [s[e].get(k) for e in ENDPOINTS for k in ("med", "p(95)", "p(99)", "p(99.9)", "max")]
            dl = next(((s["db"].get(n) or {}).get("db_latency") for n in samplers
                       if (s["db"].get(n) or {}).get("db_latency")), None) or {}
            row += [(dl.get(o) or {}).get(q) for o in ("login", "result") for q in ("p99", "p99.9")]
            row += [(s["db"].get(n) or {}).get(c) for n in samplers for c in cols]
            w.writerow(row)
    load_end = (plan["end_ms"] / 1000.0) if plan.get("end_ms") else \
        max([st["to_epoch"] for st in steps] or [start])
    if interrupted_at:
        load_end = min(load_end, interrupted_at)
    summary = {"run": name, "tool": tool, "profile": plan.get("profile"), "base_url": plan.get("base_url"),
               "start": start, "limits": limits, "min_achieved": a.min_achieved, "max_error": a.max_error,
               "dropped": dropped, "features": features, "run_meta": run_meta,
               "knee": knee["target"] if knee else None, "first_fail": first_fail["target"] if first_fail else None,
               "verdict": verdict, "hints": hints, "acceptance": acs, "steps": ordered, "timeseries": timeseries,
               "sampler_roles": {n: m.get("role", "db") for n, (m, _, _) in samplers.items()},
               "load_end": load_end, "interrupted_at": interrupted_at}
    with open(os.path.join(run, "summary.json"), "w") as f:
        json.dump(summary, f, indent=1, default=str)
    summary = json.loads(json.dumps(summary, default=str))
    try:      # per-second CSV / events.jsonl files in <run>/metrics/ (never fails the analysis)
        mdir = export_metrics.export(run, samplers, [(st["label"], st["from_epoch"], st["to_epoch"]) for st in steps],
                                     start, load_end, interrupted_at, run_meta)
    except Exception as e:                                       # noqa: BLE001
        mdir = None
        print(f"warning: per-second metric export failed: {e}", file=sys.stderr)
    render_run(summary, "\n".join(R), os.path.join(run, "report.html"))
    if not a.no_index:
        idx = build_index(os.path.dirname(run))
    print("\n".join(R[:next(i for i, l in enumerate(R) if l.startswith("## Load steps"))]))
    print(f"report: {os.path.join(run, 'report.html')}  (also report.md, steps.csv, summary.json)")
    if mdir:
        print(f"metrics: {mdir}/  (per-second CSV files + events.jsonl, see metrics/README.md)")
    if not a.no_index and idx:
        print(f"index:  {idx}")


if __name__ == "__main__":
    main()
