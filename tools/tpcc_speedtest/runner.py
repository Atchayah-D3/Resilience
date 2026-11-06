"""Setup, one measured step, the checks after it, and teardown.

Independent of environment files and SSH: the command line (__main__.py) does the safety
checks and passes in a connection target and, optionally, a reader of the target's CPU.
"""

from __future__ import annotations

import asyncio
import json
import multiprocessing
import random
import time
from collections.abc import Awaitable, Callable
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import asyncpg

from resilience_tests.execution.workload.markers import read_journal
from tools.tpcc_speedtest.logic import (
    RunConstants,
    StepEvidence,
    check_step,
    home_warehouses,
    percentile,
    python_limited,
)
from tools.tpcc_speedtest.worker import WorkerConfig, run_worker

SCHEMA = "resilience_speedtest"
SCHEMA_SQL = Path(__file__).with_name("schema.sql")
STARTUP_S = 5.0     # time for the worker processes to start and connect, before warm-up ends

CpuSnapshot = tuple[int, int, int]   # (busy, iowait, total) jiffies of the target host
CpuReader = Callable[[], Awaitable[CpuSnapshot]]


# --- setup and teardown ------------------------------------------------------------------

async def schema_exists(conn: asyncpg.Connection) -> bool:
    return bool(await conn.fetchval("SELECT count(*) FROM pg_namespace WHERE nspname = $1", SCHEMA))


async def setup(conn: asyncpg.Connection, warehouses: int, items: int) -> float:
    """Creates and loads the schema; returns the load time in seconds."""
    if await schema_exists(conn):
        raise RuntimeError(f"schema {SCHEMA} already exists: run teardown first, or run without setup")
    t0 = time.perf_counter()
    async with conn.transaction():
        await conn.execute(SCHEMA_SQL.read_text())
        await conn.execute(f"SELECT {SCHEMA}.load($1, $2)", warehouses, items)
    for table in ("item", "warehouse", "district", "customer", "stock"):
        await conn.execute(f"VACUUM (ANALYZE) {SCHEMA}.{table}")
    return time.perf_counter() - t0


async def read_meta(conn: asyncpg.Connection) -> tuple[int, int]:
    row = await conn.fetchrow(f"SELECT warehouses, items FROM {SCHEMA}.meta")
    if row is None:
        raise RuntimeError(f"{SCHEMA}.meta is empty: the load did not finish; run teardown and setup again")
    return row["warehouses"], row["items"]


async def teardown(conn: asyncpg.Connection) -> bool:
    """Drops the schema; returns True when it is gone afterwards."""
    await conn.execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    return not await schema_exists(conn)


# --- one step ----------------------------------------------------------------------------

@dataclass
class StepConfig:
    processes: int
    clerks_per_process: int
    duration_s: float
    warmup_s: float
    seed: int


def _cpu_share(pair: tuple[CpuSnapshot, CpuSnapshot] | None) -> tuple[float | None, float | None]:
    if pair is None:
        return None, None
    (b0, w0, t0), (b1, w1, t1) = pair
    total = t1 - t0
    if total <= 0:
        return None, None
    return (b1 - b0) / total, (w1 - w0) / total


async def _per_order_failures(conn: asyncpg.Connection, acked: list[dict[str, Any]]) -> int:
    """Acknowledged New-Orders whose order, new_order row or exact number of lines is missing."""
    rows = [(r["w"], r["d"], r["o_id"], r["ol_cnt"]) for r in acked]
    async with conn.transaction():
        await conn.execute("CREATE TEMP TABLE acked_orders (w int, d int, o_id int, ol_cnt int) ON COMMIT DROP")
        await conn.copy_records_to_table("acked_orders", records=rows)
        return await conn.fetchval(f"""
            SELECT count(*)
              FROM acked_orders a
              LEFT JOIN {SCHEMA}.orders o ON (o.o_w_id, o.o_d_id, o.o_id) = (a.w, a.d, a.o_id)
              LEFT JOIN {SCHEMA}.new_order n ON (n.no_w_id, n.no_d_id, n.no_o_id) = (a.w, a.d, a.o_id)
              LEFT JOIN LATERAL (SELECT count(*) AS lines FROM {SCHEMA}.order_line l
                                  WHERE (l.ol_w_id, l.ol_d_id, l.ol_o_id) = (a.w, a.d, a.o_id)) l ON true
             WHERE o.o_id IS NULL OR n.no_o_id IS NULL
                OR o.o_ol_cnt <> a.ol_cnt OR l.lines <> a.ol_cnt""")


def _read_all(step_dir: Path, prefix: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for path in sorted(step_dir.glob(f"{prefix}.*.jrnl")):
        records.extend(read_journal(path).records)
    return records


async def run_step(dsn: dict[str, Any], conn: asyncpg.Connection, step: StepConfig, out_dir: Path,
                   cpu_reader: CpuReader | None) -> dict[str, Any]:
    warehouses, items = await read_meta(conn)
    await conn.execute(f"TRUNCATE {SCHEMA}.markers")
    step_dir = out_dir / f"processes-{step.processes}"
    step_dir.mkdir(parents=True, exist_ok=False)

    consts = RunConstants.choose(random.Random(step.seed))
    homes = home_warehouses(step.processes, step.clerks_per_process, warehouses)
    measure_from = time.time() + STARTUP_S + step.warmup_s
    stop_at = measure_from + step.duration_s
    configs = [
        WorkerConfig(index=p, dsn=dsn, step_dir=str(step_dir), home_warehouses=homes[p],
                     warehouses=warehouses, items=items,
                     constants=(consts.c_customer, consts.c_item),
                     seed=step.seed * 100 + p, measure_from=measure_from, stop_at=stop_at)
        for p in range(step.processes)
    ]

    loop = asyncio.get_running_loop()
    with ProcessPoolExecutor(max_workers=step.processes,
                             mp_context=multiprocessing.get_context("spawn")) as pool:
        futures = [asyncio.wrap_future(pool.submit(run_worker, c), loop=loop) for c in configs]
        cpu_pair = None
        if cpu_reader is not None:
            await asyncio.sleep(max(0.0, measure_from - time.time()))
            before = await cpu_reader()
            await asyncio.sleep(max(0.0, stop_at - time.time()))
            cpu_pair = (before, await cpu_reader())
        results = await asyncio.gather(*futures)

    # --- evidence and checks ---
    marker = _read_all(step_dir, "marker")
    acked = _read_all(step_dir, "acked")
    aborted = _read_all(step_dir, "aborted")
    db_uuids = frozenset(str(u) for u in await conn.fetchval(f"SELECT array_agg(uuid) FROM {SCHEMA}.markers") or [])
    evidence = StepEvidence(
        committed_counted=sum(r["committed_total"] for r in results),
        marker_uuids=frozenset(r["uuid"] for r in marker),
        acked_uuids=frozenset(r["uuid"] for r in acked),
        aborted_uuids=frozenset(r["uuid"] for r in aborted),
        db_uuids=db_uuids,
        acked_lines=len(acked),
        order_check_failures=await _per_order_failures(conn, acked) if acked else 0,
        other_errors=sum(r["other_errors"] for r in results),
        unknown_outcomes=sum(r["unknown_outcomes"] for r in results),
        business_rollbacks=sum(r["business_rollbacks"] for r in results),
        new_orders_attempted=sum(r["new_orders_total"] for r in results),
        fsyncs=sum(r["fsyncs"] for r in results),
        journal_lines=sum(r["journal_lines"] for r in results),
    )
    checks = check_step(evidence)

    # --- speed ---
    call_ms = [v for r in results for v in r["call_ms"]]
    journal_ms = [v for r in results for v in r["journal_ms"]]
    per_process_tps = [r["committed_in_window"] / step.duration_s for r in results]
    cpu_shares = [r["cpu_s"] / r["wall_s"] if r["wall_s"] > 0 else 0.0 for r in results]
    target_busy, target_iowait = _cpu_share(cpu_pair)
    return {
        "processes": step.processes,
        "clerks_per_process": step.clerks_per_process,
        "clerks_total": step.processes * step.clerks_per_process,
        "duration_s": step.duration_s,
        "tps_total": sum(per_process_tps),
        "tps_per_process": per_process_tps,
        "tps_per_process_avg": sum(per_process_tps) / len(per_process_tps),
        "call_p50_ms": percentile(call_ms, 0.50),
        "call_p99_ms": percentile(call_ms, 0.99),
        "journal_p99_ms": percentile(journal_ms, 0.99),
        "python_cpu_share": cpu_shares,
        "python_limited": python_limited(cpu_shares),
        "target_cpu_busy": target_busy,
        "target_cpu_iowait": target_iowait,
        "fsyncs_per_journal_line": evidence.fsyncs / evidence.journal_lines if evidence.journal_lines else None,
        "business_rollbacks": evidence.business_rollbacks,
        "new_orders_attempted": evidence.new_orders_attempted,
        "connect_failures": sum(r["connect_failures"] for r in results),
        "error_samples": [e for r in results for e in r["error_samples"]][:10],
        "checks": [{"name": c.name, "ok": c.ok, "detail": c.detail} for c in checks],
        "checks_ok": all(c.ok for c in checks),
    }


def write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, indent=2, default=str) + "\n")
