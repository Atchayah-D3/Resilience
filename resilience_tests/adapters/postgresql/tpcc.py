"""TPC-C support of the PostgreSQL adapter (interface group TpccSupport, adapters/base.py).

All TPC-C SQL is here and in sql/tpcc_*.sql (constitution V): the `tpcc` profile and the load
command never see SQL. Every object lives in schema resilience_tpcc (docs/tpcc-contract.md §5).

Loading (TPC-C build step 4): tables, the loader functions and the consistency function are
created; items are loaded once and the warehouses in parallel, each in its own transaction;
then primary keys and indexes, VACUUM (ANALYZE), exact row counts and all 12 consistency
rules. Only when every check passed is the meta row written -- a load that stopped part-way
has none, and init refuses the data.
"""

from __future__ import annotations

import asyncio
import random
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

from resilience_tests.adapters.base import TpccSupport
from resilience_tests.execution.workload.contract import TPCC_META, TPCC_SCHEMA, TPCC_TABLE_NAMES
from resilience_tests.execution.workload.tpcc import data

if TYPE_CHECKING:
    # what this mixin uses from the adapter's core class (host.py)
    from resilience_tests.adapters.postgresql.host import PostgreSQLCore
else:
    PostgreSQLCore = object

SQL_DIR = Path(__file__).with_name("sql")
SETUP_FILES = ("tpcc_schema.sql", "tpcc_load.sql", "tpcc_consistency.sql")
INDEX_FILE = "tpcc_indexes.sql"


class TpccLoadError(RuntimeError):
    """The loaded data failed a check; no meta row was written, so it will not be used."""

    def __init__(self, problems: list[str]) -> None:
        super().__init__("; ".join(problems))
        self.problems = problems


def _sql(name: str) -> str:
    return (SQL_DIR / name).read_text()


class TpccMixin(TpccSupport, PostgreSQLCore):

    async def tpcc_load(self, warehouses: int, *, jobs: int = 4, replace: bool = False,
                        progress: Callable[[str], None] | None = None, seed: int | None = None) -> dict[str, Any]:
        if warehouses < 1 or jobs < 1:
            raise ValueError("warehouses and jobs must be at least 1")
        say = progress or (lambda _msg: None)
        started = time.perf_counter()
        c_last = data.choose_c_last_load(random.Random(seed))

        conn = await self._connect(timeout_s=10.0, command_timeout=None)
        try:
            if await self._tpcc_schema_exists(conn):
                if not replace:
                    raise RuntimeError(f"schema {TPCC_SCHEMA} already exists; load with replace (--replace) "
                                       "to drop and load it again")
                say(f"dropping the existing schema {TPCC_SCHEMA}")
                await conn.execute(f"DROP SCHEMA {TPCC_SCHEMA} CASCADE")
            for name in SETUP_FILES:
                await conn.execute(_sql(name))
            await conn.execute(f"SELECT {TPCC_SCHEMA}.load_items($1)", data.ITEMS)
            say(f"items loaded ({data.ITEMS})")
        finally:
            await conn.close()

        pending: asyncio.Queue[int] = asyncio.Queue()
        for w in range(1, warehouses + 1):
            pending.put_nowait(w)
        done = 0

        async def loader() -> None:
            nonlocal done
            c = await self._connect(timeout_s=10.0, command_timeout=None)
            try:
                while True:
                    try:
                        w = pending.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    await c.execute(f"SELECT {TPCC_SCHEMA}.load_warehouse($1, $2, $3)", w, data.ITEMS, c_last)
                    done += 1
                    say(f"warehouse {w} loaded ({done}/{warehouses})")
            finally:
                await c.close()

        await asyncio.gather(*(loader() for _ in range(min(jobs, warehouses))))

        conn = await self._connect(timeout_s=10.0, command_timeout=None)
        try:
            say("adding primary keys and indexes")
            await conn.execute(_sql(INDEX_FILE))
            say("VACUUM (ANALYZE)")
            for table in TPCC_TABLE_NAMES:
                await conn.execute(f"VACUUM (ANALYZE) {TPCC_SCHEMA}.{table}")
            say("counting rows and checking all consistency rules")
            counts = {t: await conn.fetchval(f"SELECT count(*) FROM {TPCC_SCHEMA}.{t}") for t in TPCC_TABLE_NAMES}
            rules = {r: await conn.fetchval(f"SELECT {TPCC_SCHEMA}.consistency($1)", r) for r in data.LOAD_RULES}
            problems = data.row_count_problems(counts, warehouses) + data.consistency_problems(rules)
            if problems:
                raise TpccLoadError(problems)
            seconds = time.perf_counter() - started
            await conn.execute(
                f"INSERT INTO {TPCC_META} (warehouses, items, loader_version, c_last_load, loaded_orders, load_seconds) "
                "VALUES ($1, $2, $3, $4, $5, $6)",
                warehouses, data.ITEMS, data.LOADER_VERSION, c_last, data.loaded_orders(warehouses), seconds)
            size = await self._tpcc_size(conn)
        finally:
            await conn.close()
        say(f"done in {seconds:.0f} s")
        return {"warehouses": warehouses, "seconds": seconds, "rows": counts, "rules": rules,
                "c_last_load": c_last, "size_bytes": size}

    async def tpcc_drop(self) -> bool:
        conn = await self._connect(timeout_s=10.0, command_timeout=None)
        try:
            await conn.execute(f"DROP SCHEMA IF EXISTS {TPCC_SCHEMA} CASCADE")
            return not await self._tpcc_schema_exists(conn)
        finally:
            await conn.close()

    async def tpcc_data_status(self) -> dict[str, Any]:
        conn = await self._connect(timeout_s=10.0)
        try:
            if not await self._tpcc_schema_exists(conn):
                return {"loaded": False, "reason": f"schema {TPCC_SCHEMA} does not exist"}
            meta = await conn.fetchrow(f"SELECT * FROM {TPCC_META} ORDER BY loaded_at DESC LIMIT 1")
            if meta is None:
                return {"loaded": False, "reason": "the TPC-C load did not finish (no meta row)"}
            current = await conn.fetchval(f"SELECT sum(d_next_o_id - 1) FROM {TPCC_SCHEMA}.district")
            return {
                "loaded": True,
                "warehouses": meta["warehouses"], "items": meta["items"],
                "loader_version": meta["loader_version"], "c_last_load": meta["c_last_load"],
                "loaded_orders": meta["loaded_orders"], "current_orders": int(current) if current is not None else None,
                "loaded_at": meta["loaded_at"].isoformat(), "load_seconds": float(meta["load_seconds"]),
                "capacity_tps": float(meta["capacity_tps"]) if meta["capacity_tps"] is not None else None,
                "size_bytes": await self._tpcc_size(conn),
            }
        finally:
            await conn.close()

    async def tpcc_consistency(self, rules: Sequence[int], district_sample: int | None = None,
                               seed: int | None = None) -> dict[int, int]:
        conn = await self._connect(timeout_s=10.0, command_timeout=None)
        try:
            w_list = d_list = None
            if district_sample:
                warehouses = await conn.fetchval(f"SELECT count(*) FROM {TPCC_SCHEMA}.warehouse")
                w_list, d_list = data.sample_districts(random.Random(seed), int(warehouses), district_sample)
            return {r: await conn.fetchval(f"SELECT {TPCC_SCHEMA}.consistency($1, $2::int[], $3::int[])",
                                           r, w_list, d_list)
                    for r in rules}
        finally:
            await conn.close()

    # --- helpers (private to this mixin) ---------------------------------------------------

    @staticmethod
    async def _tpcc_schema_exists(conn: Any) -> bool:
        return bool(await conn.fetchval("SELECT count(*) FROM pg_namespace WHERE nspname = $1", TPCC_SCHEMA))

    @staticmethod
    async def _tpcc_size(conn: Any) -> int:
        size = await conn.fetchval(
            "SELECT coalesce(sum(pg_total_relation_size(c.oid)), 0) FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = $1 AND c.relkind = 'r'", TPCC_SCHEMA)
        return int(size)
