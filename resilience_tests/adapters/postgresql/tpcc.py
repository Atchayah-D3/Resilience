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
import uuid as uuidlib
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

import asyncpg

from resilience_tests.adapters.base import TpccSupport
from resilience_tests.execution.workload.contract import (
    DELIVERY,
    NEW_ORDER,
    ORDER_STATUS,
    OUTCOME_BUSINESS_ROLLBACK,
    OUTCOME_COMMITTED,
    OUTCOME_DEFINITELY_ABORTED,
    OUTCOME_UNKNOWN,
    PAYMENT,
    STOCK_LEVEL,
    TPCC_META,
    TPCC_SCHEMA,
    TPCC_TABLE_NAMES,
)
from resilience_tests.execution.workload.tpcc import data

if TYPE_CHECKING:
    # what this mixin uses from the adapter's core class (host.py)
    from resilience_tests.adapters.postgresql.host import PostgreSQLCore
else:
    PostgreSQLCore = object

SQL_DIR = Path(__file__).with_name("sql")
SETUP_FILES = ("tpcc_schema.sql", "tpcc_load.sql", "tpcc_consistency.sql", "tpcc_functions.sql")
INDEX_FILE = "tpcc_indexes.sql"
DROP_LOCK_TIMEOUT = "30s"     # dropping the schema for a reload waits at most this long for locks
FUNCTIONS_FILE = "tpcc_functions.sql"

# --- one TPC-C transaction = one call (build step 6) --------------------------------------

S = TPCC_SCHEMA
TPCC_CALL_SQL = {
    NEW_ORDER: f"SELECT {S}.new_order($1, $2, $3, $4, $5, $6::int[], $7::int[], $8::int[])",
    PAYMENT: f"SELECT {S}.payment($1, $2, $3, $4, $5, $6, $7, $8, $9)",
    ORDER_STATUS: f"SELECT {S}.order_status($1, $2, $3, $4)",
    DELIVERY: f"SELECT {S}.delivery($1, $2, $3, $4)",
    STOCK_LEVEL: f"SELECT {S}.stock_level($1, $2, $3)",
}
BUSINESS_ROLLBACK_SQLSTATE = "TP001"     # New-Order's unknown item (Clause 2.4.1.4), raised in tpcc_functions.sql
NO_CUSTOMER_SQLSTATE = "TP002"
# The server answered with an error, so the call (marker row included) did NOT happen: a sure
# rollback. lock_timeout and statement_timeout end the statement the same way (scenarios 2, 4).
_SURE_ROLLBACK = (asyncpg.exceptions.IntegrityConstraintViolationError, asyncpg.exceptions.SerializationError,
                  asyncpg.exceptions.DeadlockDetectedError, asyncpg.exceptions.LockNotAvailableError,
                  asyncpg.exceptions.QueryCanceledError)
# Anything else -- a lost connection, a timeout, a server shutting down -- may or may not have
# committed: unknown, never guessed (the harness's rule, adapter.py _CONNECTION_ERRORS).
_UNKNOWN = (OSError, asyncpg.PostgresError, asyncpg.InterfaceError, TimeoutError)


def _call_args(txn_type: str, v: Mapping[str, Any], marker: tuple[int, str] | None) -> tuple:
    def m() -> tuple:
        if marker is None:
            raise ValueError(f"{txn_type} is a write transaction and needs a marker")
        return (uuidlib.UUID(marker[1]), marker[0])
    if txn_type == NEW_ORDER:
        return (*m(), v["w_id"], v["d_id"], v["c_id"], list(v["items"]), list(v["supply_w"]), list(v["quantities"]))
    if txn_type == PAYMENT:
        return (*m(), v["w_id"], v["d_id"], v["c_w_id"], v["c_d_id"], v["c_id"], v["c_last"], v["amount_cents"])
    if txn_type == ORDER_STATUS:
        return (v["w_id"], v["d_id"], v["c_id"], v["c_last"])
    if txn_type == DELIVERY:
        return (*m(), v["w_id"], v["o_carrier_id"])
    if txn_type == STOCK_LEVEL:
        return (v["w_id"], v["d_id"], v["threshold"])
    raise ValueError(f"unknown TPC-C transaction type {txn_type!r}")


async def call_tpcc(conn: asyncpg.Connection, txn_type: str, inputs: Mapping[str, Any],
                    marker: tuple[int, str] | None) -> tuple[str, dict[str, Any], str | None]:
    """Run one TPC-C transaction as one call, outside BEGIN ... COMMIT, and say how it ended.
    The call returning means committed: PostgreSQL commits a lone statement before it answers."""
    args = _call_args(txn_type, inputs, marker)
    try:
        value = await conn.fetchval(TPCC_CALL_SQL[txn_type], *args)
    except asyncpg.PostgresError as exc:
        if exc.sqlstate == BUSINESS_ROLLBACK_SQLSTATE:
            return OUTCOME_BUSINESS_ROLLBACK, {}, exc.sqlstate
        if isinstance(exc, _SURE_ROLLBACK) or exc.sqlstate == NO_CUSTOMER_SQLSTATE:
            return OUTCOME_DEFINITELY_ABORTED, {}, exc.sqlstate
        return OUTCOME_UNKNOWN, {}, exc.sqlstate
    except _UNKNOWN:
        return OUTCOME_UNKNOWN, {}, None
    if txn_type == NEW_ORDER:
        # the per-order check after a fault looks for exactly this order with exactly these lines
        return OUTCOME_COMMITTED, {"w": inputs["w_id"], "d": inputs["d_id"], "o_id": value,
                                   "ol_cnt": len(inputs["items"])}, None
    return OUTCOME_COMMITTED, {}, None


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
                # a session left holding a lock on a TPC-C table (e.g. by a crashed run) makes the
                # drop fail with a clear error instead of waiting until the phase bound
                await conn.execute(f"SET lock_timeout = '{DROP_LOCK_TIMEOUT}'")
                await conn.execute(f"DROP SCHEMA {TPCC_SCHEMA} CASCADE")
                await conn.execute("RESET lock_timeout")
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

    async def tpcc_order_check(self, orders: Sequence[Mapping[str, Any]]) -> dict[str, int]:
        if not orders:
            return {"checked": 0, "missing": 0, "incomplete": 0}
        rows = [(int(o["w"]), int(o["d"]), int(o["o_id"]), int(o["ol_cnt"])) for o in orders]
        conn = await self._connect(timeout_s=10.0, command_timeout=None)
        try:
            async with conn.transaction():
                await conn.execute("CREATE TEMP TABLE tpcc_acked (w int, d int, o_id int, ol_cnt int) ON COMMIT DROP")
                await conn.copy_records_to_table("tpcc_acked", records=rows)
                # A New-Order can be delivered afterwards (new_order row gone, carrier set): that is
                # complete too. Missing = no order row. Incomplete = an order row with a wrong line
                # count, or no new_order row on an order no Delivery has touched.
                row = await conn.fetchrow(f"""
                    SELECT count(*) FILTER (WHERE o.o_id IS NULL) AS missing,
                           count(*) FILTER (WHERE o.o_id IS NOT NULL AND (o.o_ol_cnt <> a.ol_cnt OR l.lines <> a.ol_cnt
                                                  OR (n.no_o_id IS NULL AND o.o_carrier_id IS NULL))) AS incomplete
                      FROM tpcc_acked a
                      LEFT JOIN {S}.orders o ON (o.o_w_id, o.o_d_id, o.o_id) = (a.w, a.d, a.o_id)
                      LEFT JOIN {S}.new_order n ON (n.no_w_id, n.no_d_id, n.no_o_id) = (a.w, a.d, a.o_id)
                      LEFT JOIN LATERAL (SELECT count(*) AS lines FROM {S}.order_line l
                                          WHERE (l.ol_w_id, l.ol_d_id, l.ol_o_id) = (a.w, a.d, a.o_id)) l ON true""")
                return {"checked": len(rows), "missing": int(row["missing"]), "incomplete": int(row["incomplete"])}
        finally:
            await conn.close()

    async def tpcc_orders_incomplete(self, orders: Sequence[Mapping[str, Any]]) -> int:
        result = await self.tpcc_order_check(orders)
        return result["missing"] + result["incomplete"]

    async def tpcc_columns(self) -> dict[str, dict[str, int]]:
        conn = await self._connect(timeout_s=10.0)
        try:
            rows = await conn.fetch(
                "SELECT c.relname, count(*) FILTER (WHERE NOT a.attisdropped) AS live, "
                "count(*) FILTER (WHERE a.attisdropped) AS dropped "
                "FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                "WHERE n.nspname = $1 AND c.relkind = 'r' AND a.attnum > 0 GROUP BY c.relname", TPCC_SCHEMA)
            return {r["relname"]: {"live": int(r["live"]), "dropped": int(r["dropped"])} for r in rows}
        finally:
            await conn.close()

    async def tpcc_checkpoint(self) -> str | None:
        """CHECKPOINT after a reload, so the database's writing of the new data is done before the
        baseline. Returns None, or why it could not run (PostgreSQL 15+: pg_checkpoint needed)."""
        conn = await self._connect(timeout_s=10.0, command_timeout=None)
        try:
            await conn.execute("CHECKPOINT")
            return None
        except asyncpg.PostgresError as exc:
            return f"{type(exc).__name__}: {exc}"
        finally:
            await conn.close()

    async def tpcc_record_capacity(self, tps: float) -> None:
        conn = await self._connect(timeout_s=10.0)
        try:
            await conn.execute(f"UPDATE {TPCC_META} SET capacity_tps = $1", tps)
        finally:
            await conn.close()

    async def tpcc_install_functions(self) -> None:
        conn = await self._connect(timeout_s=10.0)
        try:
            await conn.execute(_sql(FUNCTIONS_FILE))
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
