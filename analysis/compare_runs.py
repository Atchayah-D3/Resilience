#!/usr/bin/env python3
"""Compares two analyzed runs, e.g. features ON (headline) vs features OFF.

    python3 compare_runs.py results/R-ON results/R-OFF [--name-a ON --name-b OFF] [--server primary]

Both run directories must already contain summary.json (run analyze_run.py first).
Writes compare.html and compare.md into <results>/compare-<A>-vs-<B>/ (its own folder) and prints it.

Cost of A relative to B, per step with the same target rate:
  latency cost     p99 A vs B per endpoint
  resource cost    DB-host CPU ms per session, WAL KB per session, server-log KB per session
  capacity cost    knee A vs B
The resource cost per session is the most stable number when both runs pass every step.
"""
import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from report_html import render_compare  # noqa: E402
from build_index import build as build_index  # noqa: E402


def load(d):
    with open(os.path.join(d, "summary.json")) as f:
        return json.load(f)


def pct(a, b):
    if a is None or b in (None, 0):
        return None
    return 100.0 * (a - b) / b


def f(v, nd=1, suf=""):
    return "-" if v is None else f"{v:,.{nd}f}{suf}"


def sgn(v):
    return "-" if v is None else f"{v:+.1f}%"


def feat_line(s):
    ft = s.get("features") or {}
    return (f"access methods `{json.dumps(ft.get('table_access_methods'))}`, "
            f"immutable: {', '.join(ft.get('immutable_tables') or []) or 'none'}, "
            f"sdbaudit.log `{ft.get('sdbaudit_log')}`")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("run_a")
    ap.add_argument("run_b")
    ap.add_argument("--name-a", default=None)
    ap.add_argument("--name-b", default=None)
    ap.add_argument("--out", default=None,
                    help="output folder (default: <results>/compare-<A>-vs-<B>, next to the run folders)")
    ap.add_argument("--server", default="primary", help="sampler name for the DB cost columns")
    a = ap.parse_args()
    A, B = load(a.run_a), load(a.run_b)
    na, nb = a.name_a or A["run"], a.name_b or B["run"]
    sv = a.server

    R = [f"# {na} vs {nb}", "",
         f"- **{na}** ({A['run']}): {feat_line(A)}",
         f"- **{nb}** ({B['run']}): {feat_line(B)}", ""]

    ka, kb = A.get("knee"), B.get("knee")
    R += ["## Capacity (knee)", "",
          f"| | {na} | {nb} | cost |", "|---|---:|---:|---:|",
          f"| highest passing step (sessions/s) | {f(ka, 0) if ka else 'none'} | {f(kb, 0) if kb else 'none'} | "
          # a capacity cost needs a real knee (a failing step) in BOTH runs; "passed up to the
          # highest tested/measured step" says nothing about capacity
          f"{sgn(pct(ka, kb)) if ka and kb and A.get('first_fail') and B.get('first_fail') else '-'} |",
          f"| first failing step | {f(A.get('first_fail'), 0) if A.get('first_fail') else 'none'} | "
          f"{f(B.get('first_fail'), 0) if B.get('first_fail') else 'none'} | |", ""]
    if not (A.get("first_fail") and B.get("first_fail")):
        R += ["Capacity cost not measured: it needs a real knee (a failing step) in both runs. "
              "Use the latency and per-session costs below.", ""]
    for X, nx in ((A, na), (B, nb)):
        nm = (X.get("verdict") or {}).get("not_measured") or []
        if (X.get("verdict") or {}).get("interrupted") or nm:
            R += [f"**{nx} has steps that were not measured** ({', '.join(f'{t:,}/s' for t in nm) or 'run interrupted'}); "
                  "only steps measured in both runs are compared.", ""]

    # steps that were not measured (run interrupted, window empty) are left out of the comparison
    sa = {s["target"]: s for s in A["steps"] if s.get("status", "pass") != "not_measured"}
    sb = {s["target"]: s for s in B["steps"] if s.get("status", "pass") != "not_measured"}
    common = sorted(set(sa) & set(sb))
    R += ["## Latency at the same offered rate (p99, ms)", "",
          "| step | " + " | ".join(f"{e} {na} | {e} {nb} | {e} cost" for e in ("login", "result", "cutoff")) +
          f" | {na} | {nb} |",
          "|---:|" + "---:|" * 9 + "---|---|"]
    for t in common:
        x, y = sa[t], sb[t]
        cells = []
        for e in ("login", "result", "cutoff"):
            pa, pb = x[e].get("p(99)"), y[e].get("p(99)")
            cells += [f(pa, 0), f(pb, 0), sgn(pct(pa, pb))]
        R.append(f"| {t:,} | " + " | ".join(cells) +
                 f" | {'pass' if x['pass'] else 'FAIL'} | {'pass' if y['pass'] else 'FAIL'} |")
    R.append("")

    R += [f"## Resource cost per session ({sv})", "",
          f"| step | CPU ms {na} | CPU ms {nb} | CPU cost | WAL KB {na} | WAL KB {nb} | WAL cost | "
          f"log KB {na} | log KB {nb} | host CPU {na} | host CPU {nb} |",
          "|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for t in common:
        da, db = (sa[t].get("db") or {}).get(sv) or {}, (sb[t].get("db") or {}).get(sv) or {}
        R.append("| {:,} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |".format(
            t, f(da.get("cpu_ms_session"), 2), f(db.get("cpu_ms_session"), 2),
            sgn(pct(da.get("cpu_ms_session"), db.get("cpu_ms_session"))),
            f(da.get("wal_kb_session"), 2), f(db.get("wal_kb_session"), 2),
            sgn(pct(da.get("wal_kb_session"), db.get("wal_kb_session"))),
            f(da.get("log_kb_session"), 2), f(db.get("log_kb_session"), 2),
            f(da.get("cpu_busy_pct"), 0, "%"), f(db.get("cpu_busy_pct"), 0, "%")))
    R.append("")

    # average over the common steps, weighted by sessions
    def wavg(S, key):
        num = den = 0.0
        for t in common:
            d = (S[t].get("db") or {}).get(sv) or {}
            if d.get(key) is not None:
                num += d[key] * S[t]["sessions"]
                den += S[t]["sessions"]
        return num / den if den else None
    ca, cb = wavg(sa, "cpu_ms_session"), wavg(sb, "cpu_ms_session")
    wa, wb = wavg(sa, "wal_kb_session"), wavg(sb, "wal_kb_session")
    la, lb = wavg(sa, "log_kb_session"), wavg(sb, "log_kb_session")
    R += ["## Summary", "",
          f"- {sv} CPU per session: {f(ca, 2)} vs {f(cb, 2)} ms ({sgn(pct(ca, cb))})",
          f"- WAL per session: {f(wa, 2)} vs {f(wb, 2)} KB ({sgn(pct(wa, wb))})",
          f"- server log per session: {f(la, 2)} vs {f(lb, 2)} KB",
          f"- knee: {f(ka, 0) if ka else 'none'} vs {f(kb, 0) if kb else 'none'} sessions/s"
          + ("" if (A.get("first_fail") or B.get("first_fail")) else " (neither run reached its knee)"),
          "", "Positive cost = run A needs more than run B. Host CPU includes everything on that host.", ""]

    out = a.out or os.path.join(os.path.dirname(os.path.abspath(a.run_a)),
                                f"compare-{A['run']}-vs-{B['run']}".replace("/", "_"))
    os.makedirs(out, exist_ok=True)
    with open(os.path.join(out, "compare.md"), "w") as fh:
        fh.write("\n".join(R))
    render_compare(A, B, na, nb, "\n".join(R), os.path.join(out, "compare.html"), server=sv)
    with open(os.path.join(out, "compare.json"), "w") as fh:
        json.dump({"name_a": na, "name_b": nb, "run_a": A["run"], "run_b": B["run"], "knee_a": ka, "knee_b": kb,
                   "cpu_cost_pct": pct(ca, cb), "wal_cost_pct": pct(wa, wb),
                   "log_kb_session_a": la, "log_kb_session_b": lb, "steps_compared": common}, fh, indent=1)
    idx = build_index(os.path.dirname(out))
    print("\n".join(R))
    print(f"written: {os.path.join(out, 'compare.html')} (and compare.md, compare.json)")
    if idx:
        print(f"index:   {idx}")


if __name__ == "__main__":
    main()
