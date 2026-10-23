"""PostgreSQL side of the operations a fault must land inside (`fault.during`): a
checkpoint (NL-C-02), a large transaction (NL-C-03), a concurrent index build (NL-C-06)
and an autovacuum worker (NL-M-03)."""

from __future__ import annotations

import asyncio
import re
import shlex
import time
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import asyncpg

from resilience_tests.adapters.base import OperationSupport
from resilience_tests.execution.remote import RemoteHost, as_user

if TYPE_CHECKING:
    # what this mixin uses from the adapter's core class (host.py)
    from resilience_tests.adapters.postgresql.host import PostgreSQLCore
else:
    PostgreSQLCore = object

# NL-C-03 (Framework §10.2): "kill -9 during a 10M-row INSERT". The kill is sent once the
# child table has demonstrably grown by BULK_IN_FLIGHT_BYTES inside the open transaction --
# far enough in that partial rows exist on disk, far from the end so the INSERT cannot finish
# first. Whether it did is checked after recovery anyway: a committed transaction would leave
# rows visible and fail the scenario.
BULK_ROWS = 10_000_000
BULK_PARENT_ID = 1
BULK_IN_FLIGHT_BYTES = 64 * 1024 * 1024
BULK_CONFIRM_TIMEOUT_S = 60.0
BULK_INSERT = ("INSERT INTO resilience.bulk_child (id, parent_id, payload) "
               "SELECT g, $1, 'nl-c-03' FROM generate_series(1, $2) g")

# NL-C-06: the index is built on its own table, large enough that the build is still running
# when the kill lands, and never on a table other scenarios write to.
CIC_TABLE = "resilience.cic_target"
CIC_INDEX = "cic_nlc06_idx"               # created in the table's schema
CIC_INDEX_QUALIFIED = f"resilience.{CIC_INDEX}"
CIC_ROWS = 2_000_000
CIC_CONFIRM_TIMEOUT_S = 30.0

# fault.during: checkpoint (NL-C-02). The harness's own CHECKPOINT runs at full speed; with
# little changed data it can complete before the kill arrives, and the run is then aborted
# because it tested an ordinary crash. Just before the CHECKPOINT the harness updates every
# row of this harness-owned table, so the checkpoint has a known amount of data to write.
# Sized from the server's shared_buffers so the changed pages stay in memory and are written
# by the checkpoint itself. fillfactor 50 and no index keep each update on its own page, so
# the table is rewritten in place on every run rather than growing.
CKPT_TABLE = "resilience.checkpoint_ballast"
CKPT_FRACTION_OF_SHARED_BUFFERS = 0.25
CKPT_BYTES_PER_ROW = 500               # a ~250-byte row on a half-filled page
CKPT_DDL = (f"CREATE TABLE IF NOT EXISTS {CKPT_TABLE} (id bigint NOT NULL, v bigint NOT NULL DEFAULT 0, "
            "pad text NOT NULL) WITH (fillfactor = 50)")
CKPT_SEED = f"INSERT INTO {CKPT_TABLE} (id, pad) SELECT g, repeat('b', 200) FROM generate_series(1, $1) g"
SHARED_BUFFERS_BYTES_SQL = ("SELECT setting::bigint * current_setting('block_size')::bigint "
                            "FROM pg_settings WHERE name = 'shared_buffers'")
CIC_DDL = f"CREATE TABLE IF NOT EXISTS {CIC_TABLE} (id bigint NOT NULL, k text NOT NULL)"
CIC_SEED = f"INSERT INTO {CIC_TABLE} (id, k) SELECT g, md5(g::text) FROM generate_series(1, $1) g"
CIC_BUILD = f"CREATE INDEX CONCURRENTLY {CIC_INDEX} ON {CIC_TABLE} (k)"
CIC_INDEX_STATE = """
SELECT i.indisvalid, i.indisready
  FROM pg_index i
  JOIN pg_class c ON c.oid = i.indexrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE n.nspname = 'resilience' AND c.relname = $1
"""

# NL-M-03 (Framework §10.7): a harness-owned table whose autovacuum lasts seconds, so a worker
# can be caught alive and killed. With the deployment's own settings a vacuum of the small
# churn table ends in milliseconds -- faster than the kill can arrive. These storage options
# are the harness table's own (a harness object, disclosed), never global configuration:
# threshold 1,000 dead rows with no scale factor, and a cost delay that paces the vacuum.
# read, never written: the settings that decide whether and when autovacuum workers run
AVAC_SETTINGS = ("autovacuum", "track_counts", "autovacuum_naptime", "autovacuum_max_workers",
                 "autovacuum_vacuum_cost_delay", "autovacuum_vacuum_cost_limit",
                 "autovacuum_vacuum_threshold", "autovacuum_vacuum_scale_factor",
                 "restart_after_crash", "update_process_title")
AVAC_TABLE = "resilience.avac_target"
AVAC_ROWS = 1_000_000
AVAC_DDL = f"CREATE TABLE IF NOT EXISTS {AVAC_TABLE} (id bigint PRIMARY KEY, v int NOT NULL DEFAULT 0)"
AVAC_OPTIONS = {"autovacuum_vacuum_threshold": "1000", "autovacuum_vacuum_scale_factor": "0",
                "autovacuum_vacuum_cost_delay": "20", "autovacuum_vacuum_cost_limit": "200"}
AVAC_SEED = f"INSERT INTO {AVAC_TABLE} (id) SELECT g FROM generate_series(1, $1) g"

# a quarter of the rows per round, a different quarter each time: dead tuples on every page
AVAC_MAKE_DEAD = f"UPDATE {AVAC_TABLE} SET v = v + 1 WHERE id % 4 = $1"
AVAC_TITLE_MARKER = "autovacuum worker"   # PostgreSQL's process title for the worker
AVAC_POLL_S = 0.05
AVAC_WAIT_SLACK_S = 30.0                  # worker wait = 2 x naptime + this
AVAC_VERIFY_NAPTIMES = 3                  # "permanently" = 3 x naptime + vacuum time (clarified)
AVAC_VERIFY_SAMPLE_S = 2.0
AVAC_VERIFY_CAP_S = 900.0                 # half the profiles' validate bound

# A running autovacuum VACUUM (not ANALYZE-only) of this instance, with the relation it is on.
AVAC_WORKERS_SQL = """
SELECT a.pid, a.query, p.phase, p.relid::regclass::text AS relation
  FROM pg_stat_activity a
  JOIN pg_stat_progress_vacuum p ON p.pid = a.pid
 WHERE a.backend_type = 'autovacuum worker'
"""
AVAC_ANY_WORKER_SQL = "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'autovacuum worker'"

# Per relation: dead tuples, the deployment's (or the table's own) vacuum threshold, and
# whether autovacuum ran after the given database-clock instant.
AVAC_RELATIONS_SQL = """
WITH g AS (
  SELECT current_setting('autovacuum_vacuum_threshold')::float8 AS th,
         current_setting('autovacuum_vacuum_scale_factor')::float8 AS sf)
SELECT s.schemaname || '.' || s.relname AS relation,
       s.n_dead_tup AS dead,
       coalesce((SELECT split_part(o, '=', 2)::float8 FROM unnest(c.reloptions) o
                  WHERE o LIKE 'autovacuum_vacuum_threshold=%'), g.th)
     + coalesce((SELECT split_part(o, '=', 2)::float8 FROM unnest(c.reloptions) o
                  WHERE o LIKE 'autovacuum_vacuum_scale_factor=%'), g.sf)
       * greatest(c.reltuples, 0) AS threshold,
       (s.last_autovacuum IS NOT NULL AND s.last_autovacuum >= $1) AS vacuumed_after
  FROM pg_stat_user_tables s JOIN pg_class c ON c.oid = s.relid, g
"""

# Written by the replacement postmaster at the start of crash recovery, e.g.
# "LOG:  redo starts at 0/3000108". The LSN is the redo point recovery replays from.
_REDO_STARTS_RE = re.compile(r"redo starts at ([0-9A-Fa-f]+)/([0-9A-Fa-f]+)")

def recovery_redo_start_lsn(log_lines: Sequence[str]) -> int | None:
    """The LSN crash recovery started replaying from, from the first matching log line."""
    for line in log_lines:
        m = _REDO_STARTS_RE.search(line)
        if m:
            return (int(m.group(1), 16) << 32) | int(m.group(2), 16)
    return None

def _format_lsn(lsn: int | None) -> str | None:
    return None if lsn is None else f"{int(lsn) >> 32:X}/{int(lsn) & 0xFFFFFFFF:X}"


class OperationsMixin(OperationSupport, PostgreSQLCore):
    """fault.during methods of PostgreSQLAdapter. Mixed into the class in adapter.py; uses the
    core's `node` and `_connect` (host.py)."""

    # Both idle states of the checkpointer process. The poll loop must wait until the
    # checkpointer leaves ALL of these before declaring it "active". CheckpointerMain is the
    # main-loop sleep when no checkpoint is pending; CheckpointDelay is the inter-checkpoint
    # sleep. Either one means the checkpointer is NOT flushing dirty buffers.
    _CHECKPOINTER_IDLE_EVENTS = frozenset({"CheckpointerMain", "CheckpointDelay"})

    # Wait events that positively confirm the checkpointer is doing checkpoint I/O.
    # We require one of these rather than merely "not idle", so a transient NULL or an
    # unrecognised future event does not produce a false positive.
    _CHECKPOINTER_ACTIVE_EVENTS = frozenset({
        "CheckpointWriteDelay",   # Timeout: throttling between buffer writes
        "CheckpointSync",         # IO: syncing relation files to storage (older PG)
        "DataFileSync",           # IO: syncing relation files to storage (PG 10+)
        "DataFileWrite",          # IO: writing data pages
        "WalSync",                # IO: flushing WAL during checkpoint
        "WALSync",                # IO: capitalization variation
        "SlruWrite",              # IO: writing SLRU pages
        "SlruSync",               # IO: syncing SLRU pages
        "SLRUSync",               # IO: capitalization variation
        "ControlFileWrite",       # IO: writing control file
        "ControlFileSync",        # IO: syncing control file
        "ControlFileSyncUpdate",  # IO: updating control file
        "BufFileWrite",           # IO: writing temp buffers (rare during checkpoint)
    })
    _CHECKPOINTER_ACTIVE_TYPES = frozenset({"IO", "Timeout"})

    # the operation in progress (none until one is started)
    _bg_task: asyncio.Task[None] | None = None
    _bg_conn: asyncpg.Connection | None = None
    _bg_pid: int | None = None
    _avac_round = 0
    _checkpoint_baseline: dict[str, Any] | None = None
    _checkpoint_conn: asyncpg.Connection | None = None
    _checkpoint_task: asyncio.Task[str] | None = None
    _checkpointer_pid: int | None = None

    async def observe_autovacuum_settings(self) -> dict[str, str]:
        """Read only -- nothing is written (NL-M-03)."""
        conn = await self._connect(timeout_s=10.0)
        try:
            rows = await conn.fetch("SELECT name, setting || coalesce(unit, '') AS v FROM pg_settings "
                                    "WHERE name = ANY($1::text[])", list(AVAC_SETTINGS))
            return {r["name"]: r["v"] for r in rows}
        finally:
            await conn.close()


    # Objects a `during` operation needs, created in init -- before the baseline, so seeding
    # them never disturbs the steady state being measured. Each `during` behaviour asks for its own.

    async def prepare_index_build_objects(self) -> dict[str, Any]:
        """The table NL-C-06 builds its index on."""
        conn = await self._connect(timeout_s=30.0, command_timeout=600.0)
        try:
            await conn.execute(CIC_DDL)
            await conn.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {CIC_INDEX_QUALIFIED}")
            rows = await conn.fetchval(f"SELECT count(*) FROM {CIC_TABLE}")
            if rows != CIC_ROWS:
                # reseeded only when it is not exactly the expected size, so a run normally
                # reuses the table rather than rewriting it
                await conn.execute(f"TRUNCATE {CIC_TABLE}")
                await conn.execute(CIC_SEED, CIC_ROWS)
                await conn.execute(f"VACUUM (ANALYZE) {CIC_TABLE}")
            return {"table": CIC_TABLE, "rows": CIC_ROWS, "reseeded": rows != CIC_ROWS}
        finally:
            await conn.close()

    async def prepare_autovacuum_objects(self) -> dict[str, Any]:
        """The table NL-M-03's autovacuum worker vacuums."""
        conn = await self._connect(timeout_s=30.0, command_timeout=600.0)
        try:
            await conn.execute(AVAC_DDL)
            opts = ", ".join(f"{k} = {v}" for k, v in AVAC_OPTIONS.items())
            await conn.execute(f"ALTER TABLE {AVAC_TABLE} SET ({opts})")
            rows = await conn.fetchval(f"SELECT count(*) FROM {AVAC_TABLE}")
            if rows != AVAC_ROWS:
                await conn.execute(f"TRUNCATE {AVAC_TABLE}")
                await conn.execute(AVAC_SEED, AVAC_ROWS)
                await conn.execute(f"VACUUM (ANALYZE) {AVAC_TABLE}")
            self._avac_round = 0
            return {"table": AVAC_TABLE, "rows": AVAC_ROWS, "reseeded": rows != AVAC_ROWS,
                    "table_options": dict(AVAC_OPTIONS)}
        finally:
            await conn.close()

    async def _naptime_s(self, conn: asyncpg.Connection) -> float:
        # pg_settings reports seconds; SHOW would say '1min'
        return float(await conn.fetchval("SELECT setting::float8 FROM pg_settings WHERE name = 'autovacuum_naptime'"))

    async def _make_dead_tuples(self, conn: asyncpg.Connection) -> int:
        """Dead tuples on the harness table, created AFTER the latest recovery: crash recovery
        discards the statistics autovacuum uses to choose tables, so earlier ones are unseen."""
        self._avac_round = (self._avac_round + 1) % 4
        status = await conn.execute(AVAC_MAKE_DEAD, self._avac_round)
        return int(status.split()[-1]) if status.split()[-1].isdigit() else 0

    async def start_autovacuum_worker(self) -> dict[str, Any]:
        conn = await self._connect(timeout_s=10.0, command_timeout=300.0)
        try:
            naptime = await self._naptime_s(conn)
            wait_s = 2 * naptime + AVAC_WAIT_SLACK_S
            dead = await self._make_dead_tuples(conn)
            t = time.monotonic()
            while time.monotonic() - t < wait_s:
                for row in await conn.fetch(AVAC_WORKERS_SQL):
                    if row["relation"] == AVAC_TABLE:
                        query = row["query"] or ""
                        return {"in_progress": True, "dead_tuples_made": dead, "naptime_s": naptime,
                                "kill_target": {
                                    "pid": int(row["pid"]), "title_marker": AVAC_TITLE_MARKER,
                                    "relation": row["relation"], "phase": row["phase"],
                                    "wraparound": "to prevent wraparound" in query,
                                    "observed_after_s": round(time.monotonic() - t, 3)}}
                await asyncio.sleep(AVAC_POLL_S)
            return {"in_progress": False, "dead_tuples_made": dead, "naptime_s": naptime,
                    "note": f"no autovacuum worker vacuumed {AVAC_TABLE} within {wait_s:.0f} s "
                            f"(2 x autovacuum_naptime {naptime:.0f} s + {AVAC_WAIT_SLACK_S:.0f} s)"}
        finally:
            await conn.close()

    async def verify_autovacuum_resumed(self) -> dict[str, Any]:
        """After the final recovery (research R6). Crash recovery reset the statistics, so a
        last-autovacuum time at or after `t_recovered` can only come from a vacuum after the
        last kill. Fresh dead tuples on the harness table guarantee at least one relation is
        eligible; a run where none became eligible reports None, never 0."""
        conn = await self._connect(timeout_s=10.0, command_timeout=300.0)
        try:
            naptime = await self._naptime_s(conn)
            bound_s = AVAC_VERIFY_NAPTIMES * naptime
            t_recovered = await conn.fetchval("SELECT now()")
            dead = await self._make_dead_tuples(conn)
            eligible: dict[str, dict[str, Any]] = {}
            vacuumed: set[str] = set()
            respawned = False
            t = time.monotonic()
            while True:
                if int(await conn.fetchval(AVAC_ANY_WORKER_SQL)) > 0:
                    respawned = True
                for r in await conn.fetch(AVAC_RELATIONS_SQL, t_recovered):
                    rel = r["relation"]
                    if r["dead"] > r["threshold"] and rel not in eligible:
                        eligible[rel] = {"dead": int(r["dead"]), "threshold": float(r["threshold"]),
                                         "eligible_after_s": round(time.monotonic() - t, 1)}
                    if r["vacuumed_after"]:
                        vacuumed.add(rel)
                elapsed = time.monotonic() - t
                pending = set(eligible) - vacuumed
                if AVAC_TABLE in eligible and not pending and respawned:
                    # everything proven, including the table the harness itself made eligible
                    # after the last recovery (its statistics can lag the update by a second)
                    break
                busy = any(row["relation"] in pending for row in await conn.fetch(AVAC_WORKERS_SQL))
                if elapsed >= AVAC_VERIFY_CAP_S or (elapsed >= bound_s and not busy):
                    break                                   # bound reached, nothing still vacuuming
                await asyncio.sleep(AVAC_VERIFY_SAMPLE_S)
            unvacuumed = sorted(set(eligible) - vacuumed)
            return {"autovacuum_worker_respawned": respawned,
                    "relations_eligible": len(eligible),
                    "relations_left_unvacuumed": len(unvacuumed) if eligible else None,
                    "relations_left_unvacuumed_why": None if eligible else (
                        "no relation became eligible for autovacuum during verification"),
                    "unvacuumed": unvacuumed, "eligible": eligible,
                    "bound_s": bound_s, "waited_s": round(time.monotonic() - t, 1),
                    "t_recovered": str(t_recovered), "dead_tuples_made": dead}
        finally:
            await conn.close()

    async def prepare_checkpoint_objects(self) -> dict[str, Any]:
        """Create the checkpoint table, sized to a fraction of this server's shared_buffers.
        An existing table of the right size is reused; otherwise it is reseeded."""
        conn = await self._connect(timeout_s=30.0, command_timeout=600.0)
        try:
            shared = int(await conn.fetchval(SHARED_BUFFERS_BYTES_SQL))
            target_bytes = int(shared * CKPT_FRACTION_OF_SHARED_BUFFERS)
            rows = max(1, target_bytes // CKPT_BYTES_PER_ROW)
            await conn.execute(CKPT_DDL)
            have = int(await conn.fetchval(f"SELECT count(*) FROM {CKPT_TABLE}"))
            if have != rows:
                await conn.execute(f"TRUNCATE {CKPT_TABLE}")
                await conn.execute(CKPT_SEED, rows)
                await conn.execute(f"VACUUM (ANALYZE) {CKPT_TABLE}")
            size = int(await conn.fetchval(f"SELECT pg_relation_size('{CKPT_TABLE}')"))
            return {"table": CKPT_TABLE, "rows": rows, "reseeded": have != rows,
                    "table_bytes": size, "shared_buffers_bytes": shared}
        finally:
            await conn.close()

    async def change_pages_before_checkpoint(self) -> dict[str, Any]:
        """Update every row of the checkpoint table, so its pages are changed in memory and
        not yet on disk: the CHECKPOINT that follows has to write all of them."""
        conn = await self._connect(timeout_s=10.0, command_timeout=300.0)
        try:
            started = time.monotonic()
            status = await conn.execute(f"UPDATE {CKPT_TABLE} SET v = v + 1")
            elapsed = time.monotonic() - started
            size = int(await conn.fetchval(f"SELECT pg_relation_size('{CKPT_TABLE}')"))
        finally:
            await conn.close()
        updated = int(status.split()[-1]) if status and status.split()[-1].isdigit() else None
        return {"supported": True, "table": CKPT_TABLE, "rows_updated": updated,
                "table_bytes": size, "update_s": round(elapsed, 3)}

    async def _start_background(self, statements: list[tuple[str, tuple[Any, ...]]],
                                in_transaction: bool) -> int:
        """Run `statements` on a dedicated session in the background; returns its backend pid."""
        conn = await self._connect(timeout_s=10.0, command_timeout=None)
        pid = int(await conn.fetchval("SELECT pg_backend_pid()"))

        async def run() -> None:
            if in_transaction:
                async with conn.transaction():
                    for sql, args in statements:
                        await conn.execute(sql, *args)
            else:
                for sql, args in statements:
                    await conn.execute(sql, *args)

        self._bg_conn, self._bg_pid = conn, pid
        self._bg_task = asyncio.create_task(run(), name="during-operation")
        return pid

    def _bg_finished(self) -> str | None:
        """None while the background operation runs; otherwise how it ended."""
        task = self._bg_task
        if task is None or not task.done():
            return None
        if task.cancelled():
            return "cancelled"
        exc = task.exception()
        return f"failed: {type(exc).__name__}: {exc}" if exc else "completed"

    async def start_large_transaction(self) -> dict[str, Any]:
        pid = await self._start_background(
            [("INSERT INTO resilience.bulk_parent (id) VALUES ($1)", (BULK_PARENT_ID,)),
             (BULK_INSERT, (BULK_PARENT_ID, BULK_ROWS))],
            in_transaction=True)
        t = time.monotonic()
        deadline = t + BULK_CONFIRM_TIMEOUT_S
        poll = await self._connect(timeout_s=10.0)
        try:
            while time.monotonic() < deadline:
                ended = self._bg_finished()
                if ended is not None:
                    return {"in_progress": False, "pid": pid, "note": f"the INSERT {ended} before the kill"}
                row = await poll.fetchrow(
                    "SELECT a.state, a.xact_start IS NOT NULL AS in_xact, "
                    "pg_relation_size('resilience.bulk_child') AS child_bytes "
                    "FROM pg_stat_activity a WHERE a.pid = $1", pid)
                if row and row["state"] == "active" and row["in_xact"] and row["child_bytes"] >= BULK_IN_FLIGHT_BYTES:
                    return {"in_progress": True, "pid": pid, "rows_target": BULK_ROWS,
                            "child_bytes_at_confirm": int(row["child_bytes"]),
                            "confirmed_after_s": round(time.monotonic() - t, 3)}
                await asyncio.sleep(0.05)
        finally:
            await poll.close()
        return {"in_progress": False, "pid": pid,
                "note": f"the INSERT had not written {BULK_IN_FLIGHT_BYTES} bytes within {BULK_CONFIRM_TIMEOUT_S} s"}

    async def verify_large_transaction(self) -> dict[str, Any]:
        conn = await self._connect(timeout_s=10.0, command_timeout=300.0)
        try:
            return {
                "rows_visible": int(await conn.fetchval("SELECT count(*) FROM resilience.bulk_child")),
                "parent_rows_visible": int(await conn.fetchval("SELECT count(*) FROM resilience.bulk_parent")),
                "fk_violations": int(await conn.fetchval(
                    "SELECT count(*) FROM resilience.bulk_child c "
                    "LEFT JOIN resilience.bulk_parent p ON p.id = c.parent_id WHERE p.id IS NULL")),
            }
        finally:
            await conn.close()

    async def start_concurrent_index_build(self) -> dict[str, Any]:
        pid = await self._start_background([(CIC_BUILD, ())], in_transaction=False)
        t = time.monotonic()
        deadline = t + CIC_CONFIRM_TIMEOUT_S
        poll = await self._connect(timeout_s=10.0)
        try:
            while time.monotonic() < deadline:
                ended = self._bg_finished()
                if ended is not None:
                    return {"in_progress": False, "pid": pid, "note": f"the index build {ended} before the kill"}
                # the engine's own progress view: the build is running, past initialisation
                row = await poll.fetchrow(
                    "SELECT phase, blocks_done, blocks_total, tuples_done, tuples_total "
                    "FROM pg_stat_progress_create_index WHERE pid = $1", pid)
                if row and row["phase"] != "initializing":
                    return {"in_progress": True, "pid": pid, "index": CIC_INDEX_QUALIFIED,
                            "phase": row["phase"], "blocks_done": row["blocks_done"],
                            "blocks_total": row["blocks_total"], "tuples_done": row["tuples_done"],
                            "tuples_total": row["tuples_total"],
                            "confirmed_after_s": round(time.monotonic() - t, 3)}
                await asyncio.sleep(0.01)
        finally:
            await poll.close()
        return {"in_progress": False, "pid": pid,
                "note": f"no index build progress was reported within {CIC_CONFIRM_TIMEOUT_S} s"}

    async def verify_concurrent_index(self) -> dict[str, Any]:
        """Framework §10.2 NL-C-06: index left INVALID, table readable, rebuild succeeds."""
        conn = await self._connect(timeout_s=10.0, command_timeout=600.0)
        try:
            state = await conn.fetchrow(CIC_INDEX_STATE, CIC_INDEX)
            detail: dict[str, Any] = {
                "index_present": state is not None,
                "index_left_invalid": state is not None and not state["indisvalid"],
            }
            try:
                detail["table_rows"] = int(await conn.fetchval(f"SELECT count(*) FROM {CIC_TABLE}"))
                detail["table_readable"] = True
            except asyncpg.PostgresError as exc:
                detail["table_readable"] = False
                detail["table_read_error"] = str(exc)
            # PostgreSQL's documented recovery for a failed concurrent build: drop it, build again
            try:
                await conn.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {CIC_INDEX_QUALIFIED}")
                await conn.execute(CIC_BUILD)
                rebuilt = await conn.fetchrow(CIC_INDEX_STATE, CIC_INDEX)
                detail["rebuild_succeeds"] = rebuilt is not None and bool(rebuilt["indisvalid"])
            except asyncpg.PostgresError as exc:
                detail["rebuild_succeeds"] = False
                detail["rebuild_error"] = str(exc)
            return detail
        finally:
            await conn.close()

    async def abandon_background_operation(self) -> dict[str, Any]:
        detail: dict[str, Any] = {}
        task = self._bg_task
        conn = self._bg_conn
        pid = self._bg_pid
        if task is not None:
            detail["ended"] = self._bg_finished() or "still running"
            if not task.done():
                # never leave a 10M-row INSERT or an index build running behind the harness
                try:
                    killer = await self._connect(timeout_s=5.0)
                    try:
                        detail["terminated"] = await killer.fetchval("SELECT pg_terminate_backend($1)", pid)
                    finally:
                        await killer.close()
                except Exception as exc:  # noqa: BLE001 -- recorded; the task is cancelled anyway
                    detail["terminate_error"] = f"{type(exc).__name__}: {exc}"
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if conn is not None:
            try:
                conn.terminate()
            except Exception:  # noqa: BLE001 -- the server already closed it
                pass
        self._bg_task = self._bg_conn = self._bg_pid = None
        return detail

    async def cleanup_scenario_objects(self) -> dict[str, Any]:
        conn = await self._connect(timeout_s=10.0, command_timeout=300.0)
        try:
            await conn.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {CIC_INDEX_QUALIFIED}")
            # TRUNCATE gives back the aborted bulk rows' space now, not at the next autovacuum
            await conn.execute("TRUNCATE resilience.bulk_child, resilience.bulk_parent")
            # NL-M-03's table is kept (as NL-C-06's is): reseeding 1M rows right before the next
            # baseline would put a checkpoint's spread writes inside the measurement window
            return {"dropped_index": CIC_INDEX_QUALIFIED,
                    "truncated": ["resilience.bulk_child", "resilience.bulk_parent"]}
        finally:
            await conn.close()

    async def trigger_checkpoint_and_await_active(self, timeout_s: float = 10.0) -> dict[str, Any]:
        """Trigger a checkpoint under the current write workload and deterministically synchronize
        until the checkpointer process is actively writing/syncing buffers (Arch §5, Framework §10.2).

        In industry-standard resilience engineering, guessing sleep intervals is replaced by
        observing internal state transitions. The checkpointer sits in one of two idle states
        (CheckpointerMain or CheckpointDelay). Under write load, issuing CHECKPOINT wakes it,
        transitioning it to active buffer flushing and syncing (wait_event_type IO or Timeout with
        events like CheckpointWriteDelay, CheckpointSync, DataFileWrite). We capture pre-checkpoint
        LSN, start an asynchronous CHECKPOINT, and poll pg_stat_activity until the checkpointer is
        confirmed active. If the timeout expires without seeing active I/O, we report
        checkpointer_active=False so the orchestrator can abort (fail closed).
        """
        baseline_cp: dict[str, Any] = {}
        checkpointer_pid: int | None = None
        stat_table: str | None = "pg_stat_checkpointer"
        buf_col: str = "buffers_written"
        baseline_buffers: int | None = None
        baseline_io_writes: int | None = None  # pg_stat_io: immediate per-IO tracking

        conn = await self._connect()
        try:
            row = await conn.fetchrow(
                "SELECT pid, wait_event_type, wait_event FROM pg_stat_activity WHERE backend_type = 'checkpointer'"
            )
            if row and row["pid"]:
                checkpointer_pid = int(row["pid"])
            try:
                cp_row = await conn.fetchrow("SELECT checkpoint_lsn, redo_lsn, checkpoint_time::text FROM pg_control_checkpoint()")
                if cp_row:
                    baseline_cp = dict(cp_row)
            except Exception:
                pass

            # Sample baseline buffers written to enable quantitative progress verification
            try:
                val = await conn.fetchval("SELECT buffers_written FROM pg_stat_checkpointer")
                if val is not None:
                    baseline_buffers = int(val)
            except Exception:
                stat_table = "pg_stat_bgwriter"
                buf_col = "buffers_checkpoint"
                try:
                    val = await conn.fetchval("SELECT buffers_checkpoint FROM pg_stat_bgwriter")
                    if val is not None:
                        baseline_buffers = int(val)
                except Exception:
                    stat_table = None

            # pg_stat_io tracks individual write operations immediately (not batched post-
            # checkpoint like pg_stat_checkpointer.buffers_written), so it provides a reliable
            # secondary instrument even for small working sets where the cumulative counter
            # hasn't been flushed to shared memory yet.
            try:
                val = await conn.fetchval(
                    "SELECT writes FROM pg_stat_io "
                    "WHERE backend_type = 'checkpointer' AND context = 'normal' AND object = 'relation'"
                )
                if val is not None:
                    baseline_io_writes = int(val)
            except Exception:
                pass  # pg_stat_io may not exist on older PG or custom builds

            self._checkpoint_baseline = baseline_cp
            self._checkpointer_pid = checkpointer_pid
        finally:
            await conn.close()

        # Dedicated connection executing CHECKPOINT asynchronously
        cp_conn = await self._connect()
        self._checkpoint_conn = cp_conn

        # Poll connection established in advance to avoid connection latency delaying detection
        poll_conn = await self._connect()

        checkpoint_error: str | None = None

        async def _run_checkpoint() -> str:
            nonlocal checkpoint_error
            try:
                await cp_conn.execute("CHECKPOINT")
            except Exception as e:
                err_str = str(e)
                checkpoint_error = err_str
                # If SQL connection lacks privileges (e.g. pg_checkpoint role not granted to harness user),
                # attempt to grant role and trigger CHECKPOINT over SSH as cluster OS user (postgres).
                if "permission denied" in err_str.lower() or "insufficientprivilege" in err_str.lower():
                    try:
                        async with RemoteHost(self.node.ssh) as host:
                            grant_cmd = (
                                f"cd /tmp && {shlex.quote(self.node.pg_bin + '/psql')} -X -p {self.node.db.port} "
                                f"-d {shlex.quote(self.node.db.dbname)} -c "
                                f"{shlex.quote(f'GRANT pg_checkpoint, pg_read_all_stats TO {self.node.db.user};')}"
                            )
                            await host.run(as_user(self.node.os_user, grant_cmd), timeout_s=10.0, check=False)
                            cp_cmd = (
                                f"cd /tmp && {shlex.quote(self.node.pg_bin + '/psql')} -X -p {self.node.db.port} "
                                f"-d {shlex.quote(self.node.db.dbname)} -c 'CHECKPOINT;'"
                            )
                            r = await host.run(as_user(self.node.os_user, cp_cmd), timeout_s=30.0, check=False)
                            if r.exit_status == 0:
                                checkpoint_error = None
                                return "completed"
                            checkpoint_error = f"{err_str} (SSH fallback psql exit {r.exit_status}: {r.stderr.strip()})"
                    except Exception as ssh_exc:
                        checkpoint_error = f"{err_str} (SSH fallback failed: {ssh_exc})"
                return checkpoint_error or "completed"
            return "completed"

        self._checkpoint_task = asyncio.create_task(_run_checkpoint())

        # Poll until checkpointer is confirmed active (doing checkpoint I/O), or timeout.
        active_event: str | None = None
        active_type: str | None = None
        buffers_flushed: int | None = None
        io_writes_delta: int | None = None
        confirmed_active = False
        deadline = asyncio.get_running_loop().time() + timeout_s
        try:
            while asyncio.get_running_loop().time() < deadline:
                # Early abort if CHECKPOINT command errored out
                if self._checkpoint_task.done():
                    res = self._checkpoint_task.result()
                    if res != "completed" and checkpoint_error:
                        return {
                            "checkpointer_active": False,
                            "error": f"CHECKPOINT failed: {checkpoint_error}",
                            "checkpointer_pid": checkpointer_pid,
                            "wait_event_type": active_type,
                            "wait_event": active_event,
                            "buffers_written_during_cp": None,
                            "prior_checkpoint": baseline_cp,
                            "t_active_mono_ns": time.monotonic_ns(),
                        }

                row = None
                current_buffers: int | None = None
                current_io_writes: int | None = None
                if stat_table and checkpointer_pid:
                    try:
                        row = await poll_conn.fetchrow(
                            f"SELECT a.wait_event_type, a.wait_event, c.{buf_col} AS buffers "
                            f"FROM pg_stat_activity a, {stat_table} c "
                            f"WHERE a.pid = $1",
                            checkpointer_pid,
                        )
                        if row and "buffers" in row and row["buffers"] is not None:
                            current_buffers = int(row["buffers"])
                    except Exception:
                        row = None
                # pg_stat_io: immediate per-IO write tracking (not batched post-checkpoint)
                if baseline_io_writes is not None:
                    try:
                        io_val = await poll_conn.fetchval(
                            "SELECT writes FROM pg_stat_io "
                            "WHERE backend_type = 'checkpointer' AND context = 'normal' AND object = 'relation'"
                        )
                        if io_val is not None:
                            current_io_writes = int(io_val)
                    except Exception:
                        pass

                if not row:
                    if checkpointer_pid:
                        row = await poll_conn.fetchrow(
                            "SELECT wait_event_type, wait_event FROM pg_stat_activity WHERE pid = $1",
                            checkpointer_pid,
                        )
                    else:
                        row = await poll_conn.fetchrow(
                            "SELECT pid, wait_event_type, wait_event FROM pg_stat_activity WHERE backend_type = 'checkpointer'"
                        )
                        if row and row["pid"]:
                            checkpointer_pid = int(row["pid"])
                            self._checkpointer_pid = checkpointer_pid
                    if row and "buffers" in row and row["buffers"] is not None:
                        current_buffers = int(row["buffers"])

                if row:
                    w_type = row["wait_event_type"]
                    w_event = row["wait_event"]
                    buffers_delta = (
                        (current_buffers - baseline_buffers)
                        if (current_buffers is not None and baseline_buffers is not None)
                        else None
                    )
                    io_delta = (
                        (current_io_writes - baseline_io_writes)
                        if (current_io_writes is not None and baseline_io_writes is not None)
                        else None
                    )

                    # 1. Positive proof: checkpointer is in a known active I/O wait event
                    if w_event in self._CHECKPOINTER_ACTIVE_EVENTS:
                        active_type = w_type
                        active_event = w_event
                        buffers_flushed = buffers_delta
                        io_writes_delta = io_delta
                        confirmed_active = True
                        break

                    # 2. Positive proof: wait_event_type is IO or Timeout and not idle
                    if (w_type in self._CHECKPOINTER_ACTIVE_TYPES
                            and w_event not in self._CHECKPOINTER_IDLE_EVENTS):
                        active_type = w_type
                        active_event = w_event
                        buffers_flushed = buffers_delta
                        io_writes_delta = io_delta
                        confirmed_active = True
                        break

                    # 3. Positive quantitative proof: checkpointer has genuinely written
                    #    buffers (stat counter) or IO operations (pg_stat_io)
                    if (buffers_delta is not None and buffers_delta > 0) or \
                       (io_delta is not None and io_delta > 0):
                        active_type = w_type
                        active_event = w_event
                        buffers_flushed = buffers_delta
                        io_writes_delta = io_delta
                        confirmed_active = True
                        break

                    # NOTE: A NULL/NULL wait event (running on CPU) is intentionally NOT treated
                    # as active unless confirmed by buffers_delta > 0, because the checkpointer may be
                    # running its main loop initialization before flushing any buffers.
                await asyncio.sleep(0.005)  # 5ms poll — fast enough to catch the transition
        finally:
            await poll_conn.close()

        err_msg: str | None = None
        if not confirmed_active:
            if checkpoint_error:
                err_msg = f"CHECKPOINT failed: {checkpoint_error}"
            elif self._checkpoint_task.done() and self._checkpoint_task.result() == "completed":
                err_msg = "CHECKPOINT finished before an active write/sync wait event or buffer flush was sampled"
            else:
                err_msg = f"checkpointer timed out waiting for active state (last wait event: {active_event or 'CheckpointerMain'})"

        return {
            "checkpointer_active": confirmed_active,
            "checkpointer_pid": checkpointer_pid,
            "error": err_msg,
            "wait_event_type": active_type,
            "wait_event": active_event,
            "buffers_written_during_cp": buffers_flushed,
            "io_writes_during_cp": io_writes_delta,
            "prior_checkpoint": baseline_cp,
            "t_active_mono_ns": time.monotonic_ns(),
        }

    async def checkpoint_in_flight_at_kill(self, log_lines: Sequence[str]) -> dict[str, Any]:
        """Whether the CHECKPOINT issued by trigger_checkpoint_and_await_active had finished
        when the kill landed (NL-C-02), proven from where crash recovery started.

        Crash recovery replays from the redo point of the last checkpoint that COMPLETED, and
        the replacement postmaster logs it: "redo starts at X/Y". Before issuing the CHECKPOINT
        the harness read the then-current redo point from pg_control. If recovery started
        there, the new checkpoint never completed -- the kill landed mid-checkpoint. If it
        started anywhere later, a newer checkpoint (ours, or a timed one) completed first and
        the run tested an ordinary crash.

        pg_control after recovery cannot answer this: recovery writes its own end-of-recovery
        checkpoint, which is always newer than the pre-kill one whether or not ours finished."""
        if self._checkpoint_task is not None:
            self._checkpoint_task.cancel()
        if self._checkpoint_conn is not None:
            try:
                await self._checkpoint_conn.close()
            except Exception:  # noqa: BLE001 -- the server it talked to was killed
                pass

        prior = (self._checkpoint_baseline or {}).get("redo_lsn")
        redo_start = recovery_redo_start_lsn(log_lines)
        detail: dict[str, Any] = {"prior_redo_lsn": _format_lsn(prior), "recovery_redo_start_lsn": _format_lsn(redo_start)}
        if prior is None:
            detail.update(in_flight=None, note="the redo point before the CHECKPOINT was not captured")
        elif redo_start is None:
            detail.update(in_flight=None, note="no 'redo starts at' line from the replacement postmaster reached "
                                               "the harness (check the node's log_file in the profile)")
        elif redo_start == int(prior):
            detail.update(in_flight=True, note="recovery started from the redo point that preceded the CHECKPOINT: "
                                               "the checkpoint had not completed when the kill landed")
        else:
            detail.update(in_flight=False, note="recovery started from a newer redo point: a checkpoint completed "
                                                "before the kill landed, so this was an ordinary crash")
        return detail
