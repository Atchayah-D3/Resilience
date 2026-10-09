"""HTML reports (Arch §10.4: "Run report -- HTML (Jinja2) with an annotated timeline").

    python -m resilience_tests.reporting.html RUN_DIR [RUN_DIR ...] [--index OUT.html]

Each page is one self-contained file (inline CSS, inline SVG, no scripts) generated only from
the run's own evidence; it shows the run's status as recorded and never decides anything."""

from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
from pathlib import Path
from typing import Any

from jinja2 import Environment, PackageLoader, StrictUndefined

from resilience_tests.reporting.cases import ReportCase, case_from_run
from resilience_tests.reporting.evidence import load_run
from resilience_tests.reporting.view import REPORT_FILE, build_run_view

_ENV: Environment | None = None


def env() -> Environment:
    global _ENV
    if _ENV is None:
        # autoescape: log lines, errors and disclosures are arbitrary text
        _ENV = Environment(loader=PackageLoader("resilience_tests.reporting", "templates"),
                           autoescape=True, undefined=StrictUndefined, trim_blocks=True, lstrip_blocks=True)
    return _ENV


def render_run(run_dir: Path | str, now: dt.datetime | None = None) -> Path:
    run_dir = Path(run_dir)
    view = build_run_view(load_run(run_dir), now=now)
    out = run_dir / REPORT_FILE
    out.write_text(env().get_template("run_report.html.j2").render(v=view), encoding="utf-8")
    return out


def _rel(target: str | None, base: Path) -> str | None:
    if not target:
        return None
    try:
        return os.path.relpath(target, base)
    except ValueError:          # different drive
        return target


def render_index(cases: list[ReportCase], path: Path | str, title: str = "Scenario runs",
                 now: dt.datetime | None = None, gate_text: str | None = None) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = [{"c": c, "link": _rel(c.report_path, path.parent)} for c in cases]
    counts: dict[str, int] = {}
    for c in cases:
        counts[c.status] = counts.get(c.status, 0) + 1
    html = env().get_template("index.html.j2").render(
        title=title, rows=rows, counts=sorted(counts.items()), gate_text=gate_text,
        generated_at=(now or dt.datetime.now(dt.UTC)).strftime("%Y-%m-%d %H:%M:%S UTC"))
    path.write_text(html, encoding="utf-8")
    return path


def render_unit_summary(results: list[dict[str, Any]], path: Path | str,
                        now: dt.datetime | None = None) -> Path:
    """results: [{nodeid, outcome (passed|failed|skipped|error), duration_s, message}]"""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    counts = {k: sum(1 for r in results if r["outcome"] == k) for k in ("passed", "failed", "error", "skipped")}
    html = env().get_template("unit_summary.html.j2").render(
        results=results, counts=counts, total=len(results),
        not_passed=[r for r in results if r["outcome"] in ("failed", "error")],
        generated_at=(now or dt.datetime.now(dt.UTC)).strftime("%Y-%m-%d %H:%M:%S UTC"))
    path.write_text(html, encoding="utf-8")
    return path


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="(Re)generate run reports from run directories.")
    ap.add_argument("run_dirs", nargs="+")
    ap.add_argument("--index", help="also write an index page over these runs")
    args = ap.parse_args(argv)
    ok, cases = True, []
    for d in args.run_dirs:
        try:
            out = render_run(d)
            print(f"wrote {out}")
            cases.append(case_from_run(Path(d).name, d, report_path=str(out)))
        except Exception as exc:  # noqa: BLE001 -- reported per run; others still rendered
            ok = False
            print(f"FAILED {d}: {type(exc).__name__}: {exc}", file=sys.stderr)
            cases.append(case_from_run(Path(d).name, d, report_error=f"{type(exc).__name__}: {exc}"))
    if args.index:
        print(f"wrote {render_index(cases, args.index)}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
