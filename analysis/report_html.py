"""Self-contained HTML pages for the RRB runs: run report, ON/OFF comparison, results index.

One file per page: inline CSS and inline SVG charts, no scripts, no external files, so a page
can be copied anywhere (mailed, attached, opened from Windows) and still reads the same.
Pages only show what analyze_run.py / compare_runs.py recorded; they decide nothing.
"""
import glob
import html
import math
import os
import time

from mdhtml import md_fragment, page

EXTRA_CSS = """
.badge{display:inline-block;border-radius:6px;padding:.15rem .6rem;font-size:.95rem;font-weight:700;
       vertical-align:middle;margin-left:.5rem;color:#fff}
.b-pass{background:#1a7f37}.b-knee{background:#9a6700}.b-fail{background:#cf222e}.b-nm{background:#6e7781}
.b-warn{background:#bc4c00}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(270px,1fr));gap:.35rem 1.2rem;
      background:#f6f8fa;border:1px solid #d0d7de;border-radius:6px;padding:.7rem 1rem;font-size:.9rem}
.banner{border-radius:6px;padding:.6rem .9rem;margin:.6rem 0;font-size:.92rem}
.banner.err{background:#ffebe9;border:1px solid #ff8182}
.banner.warn{background:#fff8c5;border:1px solid #d4a72c}
.banner.ok{background:#dafbe1;border:1px solid #4ac26b}
.charts{display:grid;grid-template-columns:repeat(auto-fill,minmax(560px,1fr));gap:1rem}
figure{margin:0;border:1px solid #d0d7de;border-radius:6px;padding:.5rem .6rem .2rem;background:#fff}
figcaption{font-weight:600;font-size:.9rem;margin-bottom:.2rem}
svg text{font-family:inherit;font-size:11px;fill:#57606a}
.legend{font-size:.8rem;color:#57606a;margin:.1rem 0 .3rem}
.legend span{display:inline-block;margin-right:.9rem}
.legend i{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:.3rem;vertical-align:-1px}
.mono{font-family:ui-monospace,SFMono-Regular,Consolas,monospace}
"""

PALETTE = ["#0969da", "#1a7f37", "#8250df", "#bc4c00", "#cf222e", "#1b7c83", "#6e7781", "#bf3989"]
W, H, PADL, PADR, PADT, PADB = 560, 220, 54, 12, 10, 34


def esc(v):
    return html.escape(str(v), quote=True)


# ----------------------------------------------------------------------------- axes helpers
def _nice_max(v):
    if not v or v <= 0 or math.isnan(v):
        return 1.0
    e = 10 ** math.floor(math.log10(v))
    for m in (1, 1.2, 1.5, 2, 2.5, 3, 4, 5, 6, 8, 10):
        if v <= m * e:
            return m * e
    return 10 * e


def _num(v):
    if v >= 1e6:
        return f"{v / 1e6:g}M"
    if v >= 1e3:
        return f"{v / 1e3:g}k"
    return f"{v:g}" if v == int(v) else f"{v:.2g}"


def _legend(items):
    return '<div class="legend">' + "".join(
        f'<span><i style="background:{c}"></i>{esc(n)}</span>' for n, c in items) + "</div>"


def _frame(ymax, unit):
    ih = H - PADT - PADB
    out = []
    for k in range(5):
        v = ymax * k / 4
        y = PADT + ih - ih * k / 4
        out.append(f'<line x1="{PADL}" x2="{W - PADR}" y1="{y:.1f}" y2="{y:.1f}" stroke="#eaeef2"/>')
        out.append(f'<text x="{PADL - 6}" y="{y + 4:.1f}" text-anchor="end">{_num(v)}{esc(unit) if k == 4 else ""}</text>')
    return out


def _svg(parts):
    return f'<svg viewBox="0 0 {W} {H}" width="100%" role="img">' + "".join(parts) + "</svg>"


# ----------------------------------------------------------------------------- charts
def chart_steps_bars(title, cats, series, unit=""):
    """Grouped bars per step. series: [(name, color, [value per cat])]"""
    vals = [v for _, _, vs in series for v in vs if v is not None]
    ymax = _nice_max(max(vals) if vals else 1)
    iw, ih = W - PADL - PADR, H - PADT - PADB
    parts = _frame(ymax, unit)
    n, k = max(1, len(cats)), max(1, len(series))
    gw = iw / n
    bw = gw * 0.7 / k
    for i, c in enumerate(cats):
        x0 = PADL + i * gw + gw * 0.15
        for j, (_, color, vs) in enumerate(series):
            v = vs[i]
            if v is None:
                continue
            h = ih * v / ymax
            parts.append(f'<rect x="{x0 + j * bw:.1f}" y="{PADT + ih - h:.1f}" width="{bw - 1:.1f}" '
                         f'height="{h:.1f}" fill="{color}"><title>{esc(c)}: {v:,.1f}{esc(unit)}</title></rect>')
        parts.append(f'<text x="{PADL + i * gw + gw / 2:.1f}" y="{H - PADB + 14}" text-anchor="middle">{esc(c)}</text>')
    parts.append(f'<text x="{PADL + iw / 2:.1f}" y="{H - 4}" text-anchor="middle">step (target sessions/s)</text>')
    return _fig(title, series, parts)


def chart_steps_lines(title, cats, series, unit="", limits=()):
    """Lines across steps (categorical x). limits: [(value, label, color)] drawn dashed."""
    vals = [v for _, _, vs in series for v in vs if v is not None] + [l[0] for l in limits]
    ymax = _nice_max(max(vals) * 1.05 if vals else 1)
    iw, ih = W - PADL - PADR, H - PADT - PADB
    parts = _frame(ymax, unit)
    n = max(1, len(cats))
    xs = [PADL + iw * (i + 0.5) / n for i in range(n)]
    y = lambda v: PADT + ih - ih * v / ymax
    for v, label, color in limits:
        parts.append(f'<line x1="{PADL}" x2="{W - PADR}" y1="{y(v):.1f}" y2="{y(v):.1f}" stroke="{color}" '
                     f'stroke-dasharray="5,4"/><text x="{W - PADR - 2}" y="{y(v) - 3:.1f}" text-anchor="end" '
                     f'style="fill:{color}">{esc(label)}</text>')
    for _, color, vs in series:
        pts = [(xs[i], y(v), v) for i, v in enumerate(vs) if v is not None]
        if len(pts) > 1:
            parts.append(f'<polyline fill="none" stroke="{color}" stroke-width="2" points="'
                         + " ".join(f"{a:.1f},{b:.1f}" for a, b, _ in pts) + '"/>')
        for a, b, v in pts:
            parts.append(f'<circle cx="{a:.1f}" cy="{b:.1f}" r="3.5" fill="{color}"><title>{v:,.1f}{esc(unit)}</title></circle>')
    for i, c in enumerate(cats):
        parts.append(f'<text x="{xs[i]:.1f}" y="{H - PADB + 14}" text-anchor="middle">{esc(c)}</text>')
    parts.append(f'<text x="{PADL + iw / 2:.1f}" y="{H - 4}" text-anchor="middle">step (target sessions/s)</text>')
    return _fig(title, series, parts)


MAX_POINTS = 1200      # per series; 1-second samples over long runs are averaged down to this


def _downsample(pts):
    pts = [(t, v) for t, v in pts if v is not None]
    if len(pts) <= MAX_POINTS:
        return pts
    k = -(-len(pts) // MAX_POINTS)
    return [(sum(t for t, _ in pts[i:i + k]) / len(pts[i:i + k]), sum(v for _, v in pts[i:i + k]) / len(pts[i:i + k]))
            for i in range(0, len(pts), k)]


def chart_time(title, series, t0, t1, unit="", bands=(), ymax=None, limits=()):
    """Time series. series: [(name, color, [(epoch, value)])]; bands: [(from, to, label, status)];
    limits: [(value, label, color)] drawn dashed."""
    series = [(n, c, _downsample(pts)) for n, c, pts in series]
    vals = [v for _, _, pts in series for _, v in pts if v is not None]
    if not vals:
        return ""
    ymax = ymax or _nice_max(max(vals + [l[0] for l in limits]) * 1.05)
    iw, ih = W - PADL - PADR, H - PADT - PADB
    span = max(1.0, t1 - t0)
    x = lambda t: PADL + iw * (t - t0) / span
    y = lambda v: PADT + ih - ih * min(v, ymax) / ymax
    parts = []
    for a, b, label, status in bands:
        fill = {"pass": "#dafbe1", "fail": "#ffebe9"}.get(status, "#fff8c5")
        parts.append(f'<rect x="{x(a):.1f}" y="{PADT}" width="{max(1, x(b) - x(a)):.1f}" height="{ih}" fill="{fill}">'
                     f'<title>step {esc(label)}/s: {esc(status)}</title></rect>'
                     f'<text x="{(x(a) + x(b)) / 2:.1f}" y="{PADT + 11}" text-anchor="middle">{esc(label)}</text>')
    parts += _frame(ymax, unit)
    for v, label, color in limits:
        parts.append(f'<line x1="{PADL}" x2="{W - PADR}" y1="{y(v):.1f}" y2="{y(v):.1f}" stroke="{color}" '
                     f'stroke-dasharray="5,4"/><text x="{W - PADR - 2}" y="{y(v) - 3:.1f}" text-anchor="end" '
                     f'style="fill:{color}">{esc(label)}</text>')
    for k in range(6):
        t = t0 + span * k / 5
        parts.append(f'<text x="{x(t):.1f}" y="{H - PADB + 14}" text-anchor="middle">'
                     f'{int((t - t0) // 60)}:{int((t - t0) % 60):02d}</text>')
    parts.append(f'<text x="{PADL + iw / 2:.1f}" y="{H - 4}" text-anchor="middle">minutes from run start</text>')
    for _, color, pts in series:
        pts = [(t, v) for t, v in pts if v is not None and t0 <= t <= t1]
        if len(pts) > 1:
            parts.append(f'<polyline fill="none" stroke="{color}" stroke-width="1.6" points="'
                         + " ".join(f"{x(t):.1f},{y(v):.1f}" for t, v in pts) + '"/>')
    return _fig(title, series, parts)


def _fig(title, series, parts):
    return (f"<figure><figcaption>{esc(title)}</figcaption>"
            + _legend([(n, c) for n, c, _ in series]) + _svg(parts) + "</figure>")


# ----------------------------------------------------------------------------- run report
def verdict_badge(v):
    cls = {"PASS": "b-pass", "KNEE": "b-knee", "FAIL": "b-fail", "NOT MEASURED": "b-nm"}.get(v["status"], "b-nm")
    out = f'<span class="badge {cls}">{esc(v["label"])}</span>'
    if v.get("interrupted"):
        out += '<span class="badge b-warn">INTERRUPTED</span>'
    if v.get("void"):
        out += '<span class="badge b-warn">GENERATOR LIMITED</span>'
    return out


def features_short(f):
    """One line: TDE (any non-heap table access method in schema rrb), immutable tables, sdbAudit."""
    if not f:
        return "unknown (no feature status recorded)"
    am = [k for k in (f.get("table_access_methods") or {}) if k not in ("heap", "(none)")]
    imm = ", ".join(f.get("immutable_tables") or []) or "none"
    return (f"TDE {'on (' + ', '.join(am) + ')' if am else 'off'} · immutable: {imm} · "
            f"sdbAudit: {f.get('sdbaudit_log') or 'none'}")


FILE_DOC = [
    ("metrics/README.md", "what every column of the per-second files means"),
    ("metrics/events.jsonl", "one time-ordered event stream: run/step markers + every per-second metric"),
    ("metrics/app.csv", "per second: requests by status/endpoint, server + DB p50/p99, pools, GC"),
    ("metrics/db-*.csv", "per second, per database: commits, rows, WAL, checkpoints, sessions, replication"),
    ("metrics/waits-*.csv", "per second, per database: active sessions by wait event"),
    ("metrics/os-*.csv", "per second, per host: CPU, run queue, memory, IOPS, disk latency, network"),
    ("steps.csv", "one row per load step (what the tables above show)"),
    ("summary.json", "everything this report shows, machine-readable"),
    ("report.md", "this report as Markdown"),
    ("run.json", "how the run was started (settings, status, times)"),
    ("k6-dashboard-*.html", "k6's own dashboard: client-side request rate, latency and VUs over time"),
    ("k6-summary-*.json", "k6 per-step client-side results"),
    ("k6-*.log", "k6 console output"),
    ("jmeter.jtl", "JMeter raw results (one line per request)"),
    ("report/index.html", "JMeter's own dashboard"),
    ("sampler-*.jsonl", "raw 1-second samples (cumulative counters) the files above are computed from"),
    ("feature_status-*.json", "TDE / sdbAudit / sdbImmutable state of each database at the start"),
    ("os-vmstat.txt", "vmstat -t -w 1"),
    ("os-iostat.txt", "iostat -dxmt 1"),
    ("os-pidstat.txt", "pidstat -u -d -h 1 (per process CPU and disk)"),
    ("os-sar.data", "sar binary data: sar -f os-sar.data -u (or -r, -b, -n DEV)"),
]


def data_files(run_dir):
    """Links to every file in the run folder, grouped as in FILE_DOC (only the ones that exist)."""
    rows, seen = [], set()
    for pat, what in FILE_DOC:
        hits = sorted(os.path.relpath(p, run_dir) for p in glob.glob(os.path.join(run_dir, pat)))
        hits = [h for h in hits if h not in seen]
        if not hits:
            continue
        seen.update(hits)
        links = ", ".join(f'<a class="mono" href="{esc(h)}">{esc(h)}</a>' for h in hits)
        rows.append(f"<tr><td>{links}</td><td>{esc(what)}</td></tr>")
    if not rows:
        return ""
    return ('<h2>Data files</h2><p class="meta">Everything for this run is in this folder. '
            'The per-second files (metrics/) open in Excel.</p>'
            '<table><tr><th>file</th><th>contents</th></tr>' + "".join(rows) + "</table>")


def render_run(summary, md_body, out_path):
    s = summary
    steps = [st for st in s["steps"]]
    cats = [f"{st['target']:,}" for st in steps]
    meta = s.get("run_meta") or {}
    v = s["verdict"]
    start = s["start"]
    end = max([st["to_epoch"] for st in steps] + [start])
    head = [f'<h1>RRB run <span class="mono">{esc(s["run"])}</span>{verdict_badge(v)}</h1>',
            '<div class="grid">',
            f'<div><b>Profile</b> {esc(meta.get("profile") or s.get("profile") or "?")} &middot; {esc(s["tool"])}</div>',
            f'<div><b>Started</b> {time.strftime("%Y-%m-%d %H:%M:%S %Z", time.localtime(start))}</div>',
            f'<div><b>Steps</b> {len(steps)} ({esc(cats[0]) if cats else "-"} &rarr; {esc(cats[-1]) if cats else "-"} sessions/s)</div>',
            f'<div><b>Target</b> {esc(meta.get("target") or s.get("base_url") or "?")}</div>',
            f'<div style="grid-column:1/-1"><b>Features</b> {esc(features_short(s.get("features")))}</div>',
            f'<div style="grid-column:1/-1"><b>Pass rule per step</b> achieved &ge; {s.get("min_achieved", 0.98):.0%} of target, '
            f'errors &lt; {100 * s.get("max_error", 0.001):g}%, '
            + ", ".join(f"p99 {esc(k)} &lt; {v_:g} ms" for k, v_ in (s.get("limits") or {}).items()) + "</div>",
            "</div>"]
    if v.get("interrupted"):
        head.append('<div class="banner warn"><b>Run interrupted.</b> Steps that did not complete are shown as '
                    '<i>not measured</i>; they are never counted as passed or failed.</div>')
    for vr in v.get("void_reasons") or ([f'{s["dropped"]:,} dropped iterations'] if v.get("void") else []):
        head.append(f'<div class="banner err"><b>Run void: {esc(vr)}.</b> Results after that point measure the '
                    'load generator, not the system (proposal 9.2).</div>')
    cls = {"PASS": "ok", "KNEE": "warn", "FAIL": "err"}.get(v["status"], "warn")
    head.append(f'<div class="banner {cls}">{esc(v["text"])}</div>')
    acc = s.get("acceptance") or {}
    if acc.get("rows"):
        col = {"PASS": "b-pass", "FAIL": "b-fail"}
        chips = "".join(f'<span class="badge {col.get(r["status"], "b-nm")}" title="{esc(r["criterion"])}: '
                        f'{esc(r["measured"])}">{esc(r["id"])} {esc(r["status"].lower())}</span>'
                        for r in acc["rows"] if r["status"] not in ("NOT APPLICABLE", "INFO"))
        where = (f'at {acc["step"]:,}/s' if acc.get("at_target") else
                 f'target {acc["target"]:,}/s not reached (highest measured {acc["step"]:,}/s): nothing certified')
        head.append(f'<div class="grid" style="display:block"><b>Acceptance criteria</b> '
                    f'<span class="meta">({esc(where)})</span><br>{chips}</div>')

    charts = []
    measured = [st for st in steps if st["status"] != "not_measured"]
    if steps:
        charts.append(chart_steps_bars("Sessions/s per step: target vs achieved", cats, [
            ("target", "#afb8c1", [st["target"] for st in steps]),
            ("achieved", "#0969da", [st["achieved"] if st["status"] != "not_measured" else None for st in steps])]))
        lim = s.get("limits") or {}
        charts.append(chart_steps_lines("p99 latency per step (dashed = limit)", cats, [
            (e, PALETTE[i], [st[e].get("p(99)") if st["status"] != "not_measured" else None for st in steps])
            for i, e in enumerate(("login", "result", "cutoff"))], " ms",
            limits=[(lim["login"], f"login < {lim['login']:g} ms", "#cf222e")] +
                   ([(lim["result"], f"result/cutoff < {lim['result']:g} ms", "#bc4c00")] if "result" in lim else [])))
        def db_latency(st):        # from whichever sampler polled the app servers
            for m in (st.get("db") or {}).values():
                if m and m.get("db_latency"):
                    return m["db_latency"]
            return {}
        dbl = [db_latency(st) for st in steps]
        if any(dbl):
            charts.append(chart_steps_lines("DB-side p99 per step (AC-02 read <= 50 ms, AC-03 login <= 100 ms)", cats, [
                ("login transaction", PALETTE[0], [(d.get("login") or {}).get("p99") if st["status"] != "not_measured"
                                                   else None for d, st in zip(dbl, steps)]),
                ("result read", PALETTE[1], [(d.get("result") or {}).get("p99") if st["status"] != "not_measured"
                                             else None for d, st in zip(dbl, steps)])], " ms",
                limits=[(100, "AC-03 100 ms", "#cf222e"), (50, "AC-02 50 ms", "#bc4c00")]))
    ts = s.get("timeseries") or {}
    bands = [(st["from_epoch"], st["to_epoch"], f"{st['target']:,}",
              {"pass": "pass", "fail": "fail"}.get(st["status"], "nm")) for st in steps]
    t0, t1 = start - 5, end + 10
    gens = [n for n in ts if ts[n].get("role") == "generator"]
    if gens:
        charts.append(chart_time("Load generator CPU busy % (run void above 70 %)", [
            (n, PALETTE[i], list(zip(ts[n]["t"], ts[n]["cpu"]))) for i, n in enumerate(gens)],
            t0, t1, "%", bands, ymax=100, limits=[(70, "70 % limit", "#cf222e")]))
    names = [n for n in ts if ts[n].get("role") != "generator"]
    if names:
        charts.append(chart_time("Disk latency, busiest disk (ms per I/O, AC-06 p99 <= 2 ms)", [
            (n, PALETTE[i], list(zip(ts[n]["t"], ts[n].get("await") or []))) for i, n in enumerate(names)],
            t0, t1, " ms", bands, limits=[(2, "2 ms", "#cf222e")]))
        charts.append(chart_time("Host CPU busy % (database servers)", [
            (n, PALETTE[i], list(zip(ts[n]["t"], ts[n]["cpu"]))) for i, n in enumerate(names)] + [
            (f"{n} iowait", PALETTE[i + 3], list(zip(ts[n]["t"], ts[n]["iowait"]))) for i, n in enumerate(names)],
            t0, t1, "%", bands, ymax=100, limits=[(70, "AC-06 70 %", "#cf222e")]))
        charts.append(chart_time("DB commits/s", [
            (n, PALETTE[i], list(zip(ts[n]["t"], ts[n]["commits"]))) for i, n in enumerate(names)], t0, t1, "", bands))
        charts.append(chart_time("WAL MB/s", [
            (n, PALETTE[i], list(zip(ts[n]["t"], ts[n]["wal"]))) for i, n in enumerate(names)
            if any(ts[n]["wal"])], t0, t1, "", bands))
        charts.append(chart_time("Active DB sessions (average of wait samples)", [
            (n, PALETTE[i], list(zip(ts[n]["t"], ts[n]["active"]))) for i, n in enumerate(names)], t0, t1, "", bands))
        for n in names:
            if ts[n].get("app_rps"):
                charts.append(chart_time("App: HTTP responses/s (all app servers)", [
                    ("all", "#0969da", list(zip(ts[n]["t"], ts[n]["app_rps"]))),
                    ("5xx", "#cf222e", list(zip(ts[n]["t"], ts[n]["app_5xx"])))], t0, t1, "", bands))
                charts.append(chart_time("App: threads waiting for a DB connection", [
                    (pool, PALETTE[i], list(zip(ts[n]["t"], vals)))
                    for i, (pool, vals) in enumerate(sorted(ts[n]["app_pending"].items()))], t0, t1, "", bands))
                break
    body = "\n".join(head)
    if charts:
        body += '\n<h2>Charts</h2>\n<p class="meta">Shaded bands are the measured windows of each step ' \
                '(green pass, red fail, yellow not measured). Hover a point for its value.</p>\n' \
                '<div class="charts">' + "".join(c for c in charts if c) + "</div>"
    # the Markdown report from its Verdict section on: title, run line and Features are already
    # in the header above
    md = md_body[md_body.index("## Verdict"):] if "## Verdict" in md_body else md_body
    body += "\n" + md_fragment(md)
    body += "\n" + data_files(os.path.dirname(os.path.abspath(out_path)))
    html_text = page(f"RRB run {s['run']} - {v['label']}", body,
                     f"Generated {time.strftime('%Y-%m-%d %H:%M:%S %Z')} by analysis/analyze_run.py. "
                     "Everything for this run is in this folder; regenerate with "
                     "python3 analysis/analyze_run.py <run folder>.", EXTRA_CSS)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html_text)
    return out_path


# ----------------------------------------------------------------------------- comparison
def render_compare(A, B, na, nb, md_body, out_path, server="primary"):
    sa = {s["target"]: s for s in A["steps"] if s.get("status") != "not_measured"}
    sb = {s["target"]: s for s in B["steps"] if s.get("status") != "not_measured"}
    common = sorted(set(sa) & set(sb))
    cats = [f"{t:,}" for t in common]
    charts = []
    if common:
        for e, i in (("login", 0), ("result", 1)):
            charts.append(chart_steps_lines(f"p99 {e} per step: {na} vs {nb}", cats, [
                (na, "#cf222e", [sa[t][e].get("p(99)") for t in common]),
                (nb, "#0969da", [sb[t][e].get("p(99)") for t in common])], " ms"))
        for key, title, unit in (("cpu_ms_session", f"{server} CPU ms per session", " ms"),
                                 ("wal_kb_session", "WAL KB per session", " KB"),
                                 ("log_kb_session", "server log KB per session (sdbAudit)", " KB")):
            get = lambda S, t: ((S[t].get("db") or {}).get(server) or {}).get(key)
            if any(get(sa, t) is not None for t in common):
                charts.append(chart_steps_bars(title, cats, [(na, "#cf222e", [get(sa, t) for t in common]),
                                                             (nb, "#0969da", [get(sb, t) for t in common])], unit))
    head = [f"<h1>{esc(na)} vs {esc(nb)}</h1>", '<div class="grid">',
            *[f'<div style="grid-column:1/-1"><b>{esc(n)}</b> <a class="mono" href="../{esc(X["run"])}/report.html">'
              f'{esc(X["run"])}</a> {verdict_badge(X["verdict"]) if X.get("verdict") else ""}'
              f'<br><span class="meta">{esc(features_short(X.get("features")))}</span></div>' for n, X in ((na, A), (nb, B))],
            "</div>"]
    body = "\n".join(head)
    if charts:
        body += '\n<h2>Charts</h2><div class="charts">' + "".join(charts) + "</div>"
    md = md_body[md_body.index("## Capacity"):] if "## Capacity" in md_body else md_body
    body += "\n" + md_fragment(md)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(page(f"RRB {na} vs {nb}", body,
                     f"Generated {time.strftime('%Y-%m-%d %H:%M:%S %Z')} by analysis/compare_runs.py.", EXTRA_CSS))
    return out_path


# ----------------------------------------------------------------------------- index
def _ac_summary(s):
    acc = s.get("acceptance") or {}
    rows = [r for r in acc.get("rows") or [] if r["status"] not in ("NOT APPLICABLE", "INFO")]
    if not rows:
        return "-"
    if not acc.get("at_target"):
        return f'<span class="meta">not at target ({acc["target"]:,}/s)</span>'
    n = {k: sum(1 for r in rows if r["status"] == k) for k in ("PASS", "FAIL", "NOT MEASURED")}
    return (f'<span class="badge b-pass">{n["PASS"]} pass</span>' if n["PASS"] else "") + \
           (f'<span class="badge b-fail">{n["FAIL"]} fail</span>' if n["FAIL"] else "") + \
           (f'<span class="badge b-nm">{n["NOT MEASURED"]} n/m</span>' if n["NOT MEASURED"] else "")


def render_index(runs, compares, out_path):
    """runs: [{folder, summary or None, run_meta or None}], compares: [{folder, meta}]"""
    rows = []
    for r in runs:
        s, m, folder = r.get("summary"), r.get("run_meta") or {}, r["folder"]
        if s:
            v = s.get("verdict") or {"status": "?", "label": "?"}
            st = [x["target"] for x in s["steps"]]
            rows.append(
                f'<tr><td class="mono"><a href="{esc(folder)}/report.html">{esc(folder)}</a></td>'
                f'<td>{time.strftime("%Y-%m-%d %H:%M", time.localtime(s["start"]))}</td>'
                f'<td>{esc(m.get("profile") or s.get("profile") or "?")}</td><td>{esc(s["tool"])}</td>'
                f'<td>{esc(features_short(s.get("features")))}</td>'
                f'<td class="r">{(f"{min(st):,} – {max(st):,}") if st else "-"}</td>'
                f'<td class="r">{f"{s["knee"]:,}" if s.get("knee") else "-"}</td>'
                f'<td>{verdict_badge(v)}</td><td>{_ac_summary(s)}</td></tr>')
        else:
            why = m.get("status") or "not analyzed"
            rows.append(f'<tr><td class="mono"><a href="{esc(folder)}/">{esc(folder)}</a></td>'
                        f'<td>{esc(m.get("started_utc", "-"))}</td><td>{esc(m.get("profile", "-"))}</td>'
                        f'<td>{esc(m.get("tool", "-"))}</td><td>-</td><td>-</td><td>-</td>'
                        f'<td><span class="badge b-nm">{esc(why.upper())}</span></td><td>-</td></tr>')
    body = ['<h1>RRB benchmark runs</h1>',
            f'<p class="meta">{len(runs)} run folder(s) and {len(compares)} comparison(s) in this results folder. '
            'Newest first. Each link opens a self-contained page.</p>',
            '<h2>Runs</h2><table><thead><tr><th>run</th><th>started</th><th>profile</th><th>tool</th>'
            '<th>features</th><th class="r">steps (sessions/s)</th><th class="r">knee</th><th>verdict</th><th>acceptance (11.2)</th>'
            '</tr></thead><tbody>', *rows, "</tbody></table>"]
    if compares:
        body.append('<h2>Comparisons</h2><table><thead><tr><th>comparison</th><th>A</th><th>B</th>'
                    '<th class="r">knee A</th><th class="r">knee B</th><th class="r">CPU/session</th>'
                    '<th class="r">WAL/session</th></tr></thead><tbody>')
        for c in compares:
            m = c.get("meta") or {}
            pct = lambda k: "-" if m.get(k) is None else f"{m[k]:+.1f}%"
            body.append(f'<tr><td class="mono"><a href="{esc(c["folder"])}/compare.html">{esc(c["folder"])}</a></td>'
                        f'<td>{esc(m.get("name_a", "?"))} ({esc(m.get("run_a", "?"))})</td>'
                        f'<td>{esc(m.get("name_b", "?"))} ({esc(m.get("run_b", "?"))})</td>'
                        f'<td class="r">{m.get("knee_a") or "-"}</td><td class="r">{m.get("knee_b") or "-"}</td>'
                        f'<td class="r">{pct("cpu_cost_pct")}</td><td class="r">{pct("wal_cost_pct")}</td></tr>')
        body.append("</tbody></table>")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(page("RRB benchmark runs", "\n".join(body),
                     f"Generated {time.strftime('%Y-%m-%d %H:%M:%S %Z')} by analysis/build_index.py.", EXTRA_CSS))
    return out_path
