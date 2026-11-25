#!/usr/bin/env python3
"""Writes <results>/index.html: every run folder and comparison, newest first.

    python3 build_index.py [results folder]      # default: <kit>/results

analyze_run.py and compare_runs.py call this automatically, so the index is always current.
Run folders that were never analyzed (or were interrupted before analysis) are listed too,
with their status from run.json, so nothing silently disappears from the list.
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from report_html import render_index  # noqa: E402

DEFAULT_RESULTS = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "results")


def _load(path):
    try:
        with open(path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def build(results=DEFAULT_RESULTS):
    results = os.path.abspath(results)
    if not os.path.isdir(results):
        return None
    runs, compares = [], []
    for name in os.listdir(results):
        d = os.path.join(results, name)
        if not os.path.isdir(d):
            continue
        if name.startswith("compare-"):
            compares.append({"folder": name, "meta": _load(os.path.join(d, "compare.json")),
                             "mtime": os.path.getmtime(d)})
            continue
        summary = _load(os.path.join(d, "summary.json"))
        meta = _load(os.path.join(d, "run.json"))
        if summary is None and meta is None and not any(
                f.startswith(("k6-", "sampler-", "jmeter")) for f in os.listdir(d)):
            continue                                   # not a run folder
        when = (summary or {}).get("start") or (meta or {}).get("started_epoch") or os.path.getmtime(d)
        runs.append({"folder": name, "summary": summary, "run_meta": meta, "when": when})
    runs.sort(key=lambda r: r["when"], reverse=True)
    compares.sort(key=lambda c: c["mtime"], reverse=True)
    return render_index(runs, compares, os.path.join(results, "index.html"))


if __name__ == "__main__":
    out = build(sys.argv[1] if len(sys.argv) > 1 else DEFAULT_RESULTS)
    print(f"wrote {out}" if out else "no results folder")
