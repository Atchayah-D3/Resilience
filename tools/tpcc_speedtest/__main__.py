"""TPC-C speed test (build step 0, docs/tpcc-build-plan.md): can Python drive enough load?

    python -m tools.tpcc_speedtest all      --env e2-dedicated-vm [--target-max-tps N]
    python -m tools.tpcc_speedtest setup    --env e2-dedicated-vm [--warehouses 16]
    python -m tools.tpcc_speedtest run      --env e2-dedicated-vm [--processes 1,2,4,8] ...
    python -m tools.tpcc_speedtest teardown --env e2-dedicated-vm

`all` = setup, run, teardown (teardown always runs, also after an error).

Safety, as the harness does it (Arch §15): the target's hostname, read over SSH, must match
the environment's allowlist, and the sentinel table must hold a row for that hostname.
The test creates only schema resilience_speedtest and drops it in teardown; it changes no
server or role setting.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import time
from pathlib import Path
from typing import Any

import asyncpg

from resilience_tests.control.profile import EnvProfile, load_profile
from resilience_tests.execution.remote import RemoteHost
from tools.tpcc_speedtest import runner
from tools.tpcc_speedtest.logic import (
    HEADROOM,
    StepSummary,
    cores_needed,
    parse_proc_stat,
    tps_per_process,
)


def _args(argv: list[str]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(prog="python -m tools.tpcc_speedtest", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("command", choices=["all", "setup", "run", "teardown"])
    ap.add_argument("--env", required=True, help="environment profile, e.g. e2-dedicated-vm")
    ap.add_argument("--node", help="node name in the profile (default: the first node)")
    ap.add_argument("--warehouses", type=int, default=16, help="setup: warehouses to load (default 16)")
    ap.add_argument("--items", type=int, default=100_000, help="setup: items (TPC-C: 100000)")
    ap.add_argument("--processes", default="1,2,4,8", help="run: process counts to measure (default 1,2,4,8)")
    ap.add_argument("--clerks", type=int, default=16, help="run: clerks (connections) per process (default 16)")
    ap.add_argument("--duration", type=float, default=60.0, help="run: measured seconds per step (default 60)")
    ap.add_argument("--warmup", type=float, default=5.0, help="run: warm-up seconds per step (default 5)")
    ap.add_argument("--seed", type=int, default=1, help="run: random seed (same seed, same transactions)")
    ap.add_argument("--target-max-tps", type=float,
                    help="the maximum TPS of the biggest target to be tested; gives the cores needed")
    ap.add_argument("--no-target-cpu", action="store_true", help="do not read the target's CPU over SSH")
    ap.add_argument("--out", help="output folder (default: <driver_host.run_dir>/speedtest-<time>)")
    return ap.parse_args(argv)


# --- safety and connections --------------------------------------------------------------

async def _check_safety(profile: EnvProfile, node_name: str | None, host: RemoteHost) -> dict[str, Any]:
    node = profile.node(node_name) if node_name else profile.nodes[0]
    hostname = (await host.run("hostname", timeout_s=10)).stdout.strip()
    if not re.search(profile.safety.allowlist.hostname_pattern, hostname):
        raise SystemExit(f"refused: target hostname {hostname!r} does not match the allowlist "
                         f"{profile.safety.allowlist.hostname_pattern!r} of {profile.name}")
    ep = node.client
    dsn = {"host": ep.host, "port": ep.port, "database": ep.dbname, "user": ep.user}
    conn = await asyncpg.connect(**dsn, timeout=5)   # password from ~/.pgpass
    try:
        try:
            row = await conn.fetchrow(f"SELECT 1 FROM {profile.safety.sentinel_table} WHERE hostname = $1",
                                      hostname)
        except asyncpg.UndefinedTableError:
            row = None
    finally:
        await conn.close()
    if row is None:
        raise SystemExit(f"refused: no row for {hostname!r} in {profile.safety.sentinel_table}")
    return dsn


CONNECTION_MARGIN = 10   # connection slots always left free for other users and the harness


async def _free_connections(conn: asyncpg.Connection) -> int:
    """Connection slots the test may use without filling the target: never all of them."""
    row = await conn.fetchrow(
        "SELECT current_setting('max_connections')::int AS max_conn, "
        "       current_setting('superuser_reserved_connections')::int AS reserved, "
        "       (SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend') AS used")
    return row["max_conn"] - row["reserved"] - row["used"] - CONNECTION_MARGIN


def _cpu_reader(host: RemoteHost) -> runner.CpuReader:
    async def read() -> runner.CpuSnapshot:
        return parse_proc_stat((await host.run("head -1 /proc/stat", timeout_s=10)).stdout)
    return read


# --- report ------------------------------------------------------------------------------

def _fmt(v: float | None, spec: str = ".1f") -> str:
    return "–" if v is None else format(v, spec)


def _summary_md(meta: dict[str, Any], steps: list[dict[str, Any]], decision: dict[str, Any]) -> str:
    lines = [
        "# TPC-C speed test result",
        "",
        f"- Date: {meta['started']}",
        f"- Environment: {meta['env']}; harness machine cores: {meta['harness_cores']}",
        f"- Data: {meta['warehouses']} warehouses, {meta['items']} items",
        f"- Each step: {meta['clerks']} clerks per process, {meta['warmup_s']:.0f} s warm-up, "
        f"{meta['duration_s']:.0f} s measured",
        "",
        "## Speed",
        "",
        "| Processes | Clerks | TPS total | TPS per process | Call p50 / p99 (ms) | Journal p99 (ms) "
        "| Python CPU (lowest process) | Python is the limit | Target CPU busy / iowait | Checks |",
        "|---|---|---|---|---|---|---|---|---|---|",
    ]
    for s in steps:
        busy = s["target_cpu_busy"]
        target = "–" if busy is None else f"{busy:.0%} / {s['target_cpu_iowait']:.0%}"
        lines.append(
            f"| {s['processes']} | {s['clerks_total']} | {s['tps_total']:.0f} | {s['tps_per_process_avg']:.0f} "
            f"| {_fmt(s['call_p50_ms'])} / {_fmt(s['call_p99_ms'])} | {_fmt(s['journal_p99_ms'])} "
            f"| {min(s['python_cpu_share']):.0%} | {'yes' if s['python_limited'] else 'no'} "
            f"| {target} | {'pass' if s['checks_ok'] else '**FAIL**'} |")
    lines += ["", "## Checks (is the measurement true?)", ""]
    for s in steps:
        lines.append(f"**{s['processes']} process(es)**")
        lines.append("")
        for c in s["checks"]:
            lines.append(f"- {'✔' if c['ok'] else '✘'} {c['name']}: {c['detail']}")
        if s["error_samples"]:
            lines.append(f"- error samples: {s['error_samples']}")
        lines.append("")
    lines += ["## Decision input", ""]
    lines += [f"- {line}" for line in decision["text"]]
    lines.append("")
    return "\n".join(lines)


TARGET_BUSY_CPU_SHARE = 0.85


def _what_limited(step: dict[str, Any]) -> str:
    """Names the likely limit of a step in which Python was not busy."""
    busy = step["target_cpu_busy"]
    where = f"In the {step['processes']}-process step"
    if busy is not None and busy >= TARGET_BUSY_CPU_SHARE:
        return f"{where} the target's CPU was {busy:.0%} busy: the database was the limit."
    journal, call = step["journal_p99_ms"], step["call_p50_ms"]
    if journal is not None and call is not None and journal > call:
        return (f"{where} the journal flush on the harness disk (p99 {journal:.1f} ms) took longer than "
                f"a database call (p50 {call:.1f} ms): the harness disk was the limit. Run the test with "
                f"more clerks per process, or put the journals on a faster disk.")
    busy_text = "unknown" if busy is None else f"{busy:.0%}"
    return (f"{where} neither Python nor the target's CPU (busy: {busy_text}) was the limit: the clerks "
            f"were waiting on the network or on locks. Run the test with more clerks per process.")


def _decision(steps: list[dict[str, Any]], target_max_tps: float | None, harness_cores: int) -> dict[str, Any]:
    summaries = [StepSummary(s["processes"], s["tps_per_process_avg"], s["python_limited"], s["checks_ok"])
                 for s in steps]
    rate = tps_per_process(summaries)
    text: list[str] = []
    out: dict[str, Any] = {"tps_per_process": None, "exact": None, "cores_needed": None, "text": text}
    if rate is None:
        text.append("No step passed its checks: no decision can be taken from this run.")
        return out
    out["tps_per_process"], out["exact"] = rate.tps, rate.exact
    if rate.exact:
        text.append(f"One Python process drives about {rate.tps:.0f} TPS while Python is the limit.")
    else:
        text.append(f"Python was never the limit. One Python process drove at least {rate.tps:.0f} TPS "
                    f"(a lower bound).")
        text.append(_what_limited(steps[-1]))
    if target_max_tps:
        cores = cores_needed(target_max_tps, rate.tps)
        out["cores_needed"] = cores
        amount = "about" if rate.exact else "at most"
        text.append(f"Target maximum {target_max_tps:.0f} TPS × {HEADROOM} = {target_max_tps * HEADROOM:.0f} TPS "
                    f"needed → {amount} {cores} Python processes (one core each) on the harness machine(s).")
        text.append(f"This harness machine has {harness_cores} cores.")
    else:
        text.append(f"Cores needed = (target maximum TPS × {HEADROOM}) ÷ {rate.tps:.0f}. "
                    f"Give --target-max-tps to calculate it.")
    return out


# --- commands ----------------------------------------------------------------------------

def _make_out_dir(a: argparse.Namespace, profile: EnvProfile, stamp: str) -> Path:
    """Creates the output folder before any database work, so a folder that cannot be written
    fails at once, not after the data is loaded."""
    out = Path(a.out).expanduser() if a.out else Path(profile.driver_host.run_dir) / f"speedtest-{stamp}"
    try:
        out.mkdir(parents=True, exist_ok=False)
    except FileExistsError:
        raise SystemExit(f"refused: output folder {out} already exists; choose another with --out") from None
    except OSError as exc:
        raise SystemExit(f"refused: cannot create output folder {out} ({exc.strerror}); "
                         f"choose a folder you can write with --out, e.g. --out ~/resilience-runs/speedtest-{stamp}"
                         ) from None
    return out


async def _main(a: argparse.Namespace) -> int:
    profile = load_profile(a.env)
    node = profile.node(a.node) if a.node else profile.nodes[0]
    stamp = time.strftime("%Y%m%d-%H%M%S")
    out = _make_out_dir(a, profile, stamp) if a.command in ("all", "run") else None
    async with RemoteHost(node.ssh) as host:
        dsn = await _check_safety(profile, a.node, host)
        conn = await asyncpg.connect(**dsn, timeout=5,
                                     server_settings={"application_name": "resilience-speedtest"})
        try:
            if a.command == "setup":
                secs = await runner.setup(conn, a.warehouses, a.items)
                print(f"loaded {a.warehouses} warehouses, {a.items} items in {secs:.0f} s")
                return 0
            if a.command == "teardown":
                gone = await runner.teardown(conn)
                print("schema resilience_speedtest dropped" if gone else "ERROR: schema still exists")
                return 0 if gone else 1
            if a.command == "all":
                secs = await runner.setup(conn, a.warehouses, a.items)
                print(f"loaded {a.warehouses} warehouses, {a.items} items in {secs:.0f} s")
            try:
                assert out is not None
                return await _run(a, profile, dsn, conn, host, out, stamp)
            finally:
                if a.command == "all":
                    gone = await runner.teardown(conn)
                    print("schema resilience_speedtest dropped" if gone else "ERROR: schema still exists")
        finally:
            await conn.close()


async def _run(a: argparse.Namespace, profile: EnvProfile, dsn: dict[str, Any], conn: asyncpg.Connection,
               host: RemoteHost, out: Path, stamp: str) -> int:
    processes = [int(p) for p in a.processes.split(",")]
    harness_cores = os.cpu_count() or 1
    warehouses, items = await runner.read_meta(conn)
    meta = {"started": stamp, "env": profile.name, "harness_cores": harness_cores, "warehouses": warehouses,
            "items": items, "clerks": a.clerks, "warmup_s": a.warmup, "duration_s": a.duration,
            "processes": processes, "seed": a.seed}
    cpu_reader = None if a.no_target_cpu else _cpu_reader(host)
    free = await _free_connections(conn)
    steps = []
    for p in processes:
        if p * a.clerks > free:
            print(f"SKIPPED: {p} processes × {a.clerks} clerks = {p * a.clerks} connections, but the target has "
                  f"only {free} free (max_connections minus reserved, used and a margin of {CONNECTION_MARGIN})")
            continue
        if p > harness_cores:
            print(f"WARNING: {p} processes on {harness_cores} cores: processes share cores, "
                  f"so this step does not show the speed of one process")
        print(f"step: {p} process(es) × {a.clerks} clerks, {a.duration:.0f} s ...", flush=True)
        step = await runner.run_step(dsn, conn, runner.StepConfig(p, a.clerks, a.duration, a.warmup, a.seed),
                                     out, cpu_reader)
        steps.append(step)
        print(f"  {step['tps_total']:.0f} TPS total, {step['tps_per_process_avg']:.0f} per process, "
              f"python limit: {'yes' if step['python_limited'] else 'no'}, "
              f"checks: {'pass' if step['checks_ok'] else 'FAIL'}", flush=True)
    if not steps:
        print("no step could run: see the SKIPPED lines above")
        return 1
    decision = _decision(steps, a.target_max_tps, harness_cores)
    runner.write_json(out / "results.json", {"meta": meta, "steps": steps, "decision": decision})
    (out / "summary.md").write_text(_summary_md(meta, steps, decision))
    print(f"\nresults: {out}/summary.md")
    print("\n".join(decision["text"]))
    return 0 if all(s["checks_ok"] for s in steps) else 1


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(_main(_args(sys.argv[1:] if argv is None else argv)))


if __name__ == "__main__":
    raise SystemExit(main())
