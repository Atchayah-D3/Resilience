"""PostgreSQL side of NL-N-I01, noisy analytics (fault type analytics_load): report sessions of
the environment's analytics login read the TPC-C tables back to back next to the workload
(the 3 November document, `docs/Noisy Neighbour/Resilience Scenario 1 ...`, section 4).

  full-table sessions  Q1 -> Q2 -> Q3, repeated: whole-table reads of order_line with the helpers
                       the role allows (totals, a join with orders, a big sort to temporary files).
                       Sessions 1 and 2 first run a limit test.
  index sessions       Q4, repeated: 2,000 old orders of one district of an extra warehouse (one
                       the workload does not use) through order_line's primary key, a new random
                       range each time; it pulls cold pages into the memory cache.
  limit test 1         a sort of every order line with an extra column and no helpers: it needs
                       more temporary space than the role's temp_file_limit (expected: 53400)
  limit test 2         a calculation over the whole table that runs longer than the role's
                       statement_timeout (expected: 57014)

Reports run as EXPLAIN (ANALYZE, BUFFERS, TIMING OFF): they run in full, and the database also
returns their pages and helpers, kept as evidence. The sessions carry
application_name = 'resilience-analytics', so cleanup and the kill switch end exactly them. The
login's settings are read, never written; a limit test changes only its own transaction
(SET LOCAL). The harness role may not end another role's sessions, so the server-side backstop
runs as the cluster's OS user.
"""

from __future__ import annotations

import asyncio
import itertools
import json
import random
import time
from typing import TYPE_CHECKING, Any

import asyncpg

from resilience_tests.adapters.base import AnalyticsSupport
from resilience_tests.execution.remote import RemoteHost
from resilience_tests.execution.workload.contract import TPCC_SCHEMA

if TYPE_CHECKING:
    # what this mixin uses from the adapter's core class (host.py)
    from resilience_tests.adapters.postgresql.host import PostgreSQLCore
else:
    PostgreSQLCore = object

ANALYTICS_APPLICATION_NAME = "resilience-analytics"
S = TPCC_SCHEMA

# The TPC-C tables the reports read, and the customers' own (every other TPC-C table): the
# customers' cache hit ratio is measured on the latter, which no report touches.
REPORT_TABLES = ("order_line", "orders")
CUSTOMER_TABLES = ("warehouse", "district", "customer", "history", "new_order", "item", "stock")

# Q1: totals per order-line number (CH-benCHmark query 1)
Q1_SQL = (f"SELECT ol_number, sum(ol_quantity) AS sum_qty, sum(ol_amount) AS sum_amount, "
          f"avg(ol_quantity) AS avg_qty, avg(ol_amount) AS avg_amount, count(*) AS count_order "
          f"FROM {S}.order_line WHERE ol_delivery_d > '2007-01-02 00:00:00' "
          f"GROUP BY ol_number ORDER BY ol_number")
# Q2: orders by number of lines and by carrier (CH-benCHmark query 12, upper date moved to 2100)
Q2_SQL = (f"SELECT o.o_ol_cnt, sum(CASE WHEN o.o_carrier_id IN (1, 2) THEN 1 ELSE 0 END) AS high_line_count, "
          f"sum(CASE WHEN o.o_carrier_id NOT IN (1, 2) THEN 1 ELSE 0 END) AS low_line_count "
          f"FROM {S}.orders o JOIN {S}.order_line ol "
          f"ON ol.ol_w_id = o.o_w_id AND ol.ol_d_id = o.o_d_id AND ol.ol_o_id = o.o_id "
          f"WHERE o.o_entry_d <= ol.ol_delivery_d AND ol.ol_delivery_d < '2100-01-01 00:00:00' "
          f"GROUP BY o.o_ol_cnt ORDER BY o.o_ol_cnt")
# Q3: the biggest order lines -- a big sort that writes temporary files ($1: half the rows, so the
# whole table is sorted, not only a top-N)
Q3_SQL = (f"SELECT ol_w_id, ol_d_id, ol_o_id, ol_amount FROM {S}.order_line "
          f"ORDER BY ol_amount DESC, ol_o_id OFFSET $1 LIMIT 100")
# Q4: 2,000 old orders of one district of an extra warehouse
Q4_SQL = (f"SELECT sum(ol_amount), count(*) FROM {S}.order_line "
          f"WHERE ol_w_id = $1 AND ol_d_id = $2 AND ol_o_id BETWEEN $3 AND $3 + 1999")
Q4_FIRST_ORDER_MAX = 1001        # orders 1..3,000 per district at load; 2,000 of them per query
DISTRICTS = 10
# Limit test 1: every order line sorted with an extra 50-character column, without helpers
# (temp_file_limit counts per process; split over 3 processes each part could stay under it).
# $1: the rows minus 10, so the whole table must be sorted but only 10 rows come back.
LIMIT_TEMP_SQL = (f"SELECT *, repeat('x', 50) AS pad FROM {S}.order_line "
                  f"ORDER BY ol_amount DESC, ol_o_id, ol_number OFFSET $1 LIMIT 10")
# Limit test 2: a calculation over the whole table that runs far longer than statement_timeout
LIMIT_TIMEOUT_SQL = f"SELECT sum(ol_amount * g) FROM {S}.order_line, generate_series(1, 1000) AS g"
EXPLAIN = "EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, FORMAT JSON) "

FULL_TABLE_CYCLE = ("q1", "q2", "q3")
CONNECT_TIMEOUT_S = 10.0
STOP_TIMEOUT_S = 15.0            # a cancelled query and the session's close
SERVER_GONE_POLLS = 50           # terminated backends take a moment to leave pg_stat_activity
SERVER_GONE_PAUSE_S = 0.2
KEEP_RECORDS = 40                # first report records kept in full (plus every one that did not complete)
KEEP_FAILED = 200

ROLE_SETTINGS = ("max_parallel_workers_per_gather", "work_mem", "temp_file_limit", "statement_timeout")
SERVER_SETTINGS = ("shared_buffers", "max_parallel_workers", "max_parallel_workers_per_gather", "max_worker_processes",
                   "max_connections")
ROLE_SQL = ("SELECT r.rolcanlogin, r.rolconnlimit, "
            "coalesce((SELECT array_agg(c ORDER BY s.setdatabase) FROM pg_db_role_setting s, unnest(s.setconfig) c "
            "WHERE s.setrole = r.oid AND s.setdatabase IN (0, (SELECT oid FROM pg_database "
            "WHERE datname = current_database()))), '{}') AS config "
            "FROM pg_roles r WHERE r.rolname = $1")
SAMPLE_SQL = """
SELECT json_build_object(
  'sessions_open', (SELECT count(*) FROM pg_stat_activity
                     WHERE application_name = $1 AND backend_type = 'client backend'),
  'sessions_active', (SELECT count(*) FROM pg_stat_activity
                       WHERE application_name = $1 AND backend_type = 'client backend' AND state = 'active'),
  'helpers', (SELECT coalesce(json_object_agg(leader_pid, n), '{}'::json) FROM
               (SELECT leader_pid, count(*) AS n FROM pg_stat_activity
                 WHERE backend_type = 'parallel worker' AND leader_pid IN
                       (SELECT pid FROM pg_stat_activity WHERE application_name = $1
                         AND backend_type = 'client backend')
                 GROUP BY leader_pid) h),
  'helpers_all', (SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'parallel worker'),
  'temp_files', (SELECT temp_files FROM pg_stat_database WHERE datname = current_database()),
  'temp_bytes', (SELECT temp_bytes FROM pg_stat_database WHERE datname = current_database()))
"""
CACHE_SQL = (f"SELECT coalesce(sum(heap_blks_hit + coalesce(idx_blks_hit, 0)), 0) AS hit, "
             f"coalesce(sum(heap_blks_read + coalesce(idx_blks_read, 0)), 0) AS read "
             f"FROM pg_statio_user_tables WHERE schemaname = '{S}' AND relname = ANY($1::text[])")
TAGGED_SQL = (f"SELECT count(*) FROM pg_stat_activity WHERE application_name = '{ANALYTICS_APPLICATION_NAME}' "
              "AND backend_type = 'client backend'")
TERMINATE_SQL = (f"SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity "
                 f"WHERE application_name = '{ANALYTICS_APPLICATION_NAME}' AND pid <> pg_backend_pid()")


def plan_figures(explain: Any) -> dict[str, Any]:
    """From EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON): the report's pages (its helpers' included), its
    temporary blocks and the helpers planned and launched."""
    doc = json.loads(explain) if isinstance(explain, str) else explain
    top = (doc[0] if isinstance(doc, list) else doc)["Plan"]
    planned = launched = 0
    stack = [top]
    while stack:
        node = stack.pop()
        planned = max(planned, int(node.get("Workers Planned", 0)))
        launched = max(launched, int(node.get("Workers Launched", 0)))
        stack.extend(node.get("Plans", []))
    return {"shared_hit": int(top.get("Shared Hit Blocks", 0)), "shared_read": int(top.get("Shared Read Blocks", 0)),
            "temp_written": int(top.get("Temp Written Blocks", 0)), "workers_planned": planned,
            "workers_launched": launched}


def parse_role_config(config: list[str]) -> dict[str, str]:
    """pg_db_role_setting entries ("name=value"), the database's own after the cluster-wide ones."""
    out: dict[str, str] = {}
    for entry in config:
        name, _, value = entry.partition("=")
        out[name.strip()] = value.strip()
    return out


class _Kind:
    """Counts and plan figures of one kind of report query."""

    def __init__(self) -> None:
        self.count = self.completed = self.cancelled = 0
        self.errors: dict[str, int] = {}
        self.total_s = self.max_s = 0.0
        self.shared_hit = self.shared_read = self.temp_written = 0
        self.workers_launched_max = 0

    def as_dict(self) -> dict[str, Any]:
        return {"count": self.count, "completed": self.completed, "cancelled": self.cancelled, "errors": self.errors,
                "total_s": round(self.total_s, 3), "max_s": round(self.max_s, 3), "shared_hit": self.shared_hit,
                "shared_read": self.shared_read, "temp_written": self.temp_written,
                "workers_launched_max": self.workers_launched_max}


class AnalyticsMixin(AnalyticsSupport, PostgreSQLCore):
    """Noisy-analytics methods of PostgreSQLAdapter (NL-N-I01). Mixed into the class in adapter.py;
    uses the core's `node`, `_connect` and `_psql_as_os_user` (host.py)."""

    _reports: dict[str, Any] | None = None
    _report_tasks: tuple[asyncio.Task[None], ...] = ()     # replaced, never appended to, per start
    _report_conns: tuple[asyncpg.Connection, ...] = ()
    _report_stop: asyncio.Event | None = None
    _sample_conn: asyncpg.Connection | None = None

    # --- reading -------------------------------------------------------------------------------

    async def observe_analytics_settings(self) -> dict[str, str | None]:
        login = self.node.analytics
        out: dict[str, str | None] = {"analytics_login": login.user if login else None}
        conn = await self._connect(timeout_s=CONNECT_TIMEOUT_S)
        try:
            for name in SERVER_SETTINGS:
                out[name] = str(await conn.fetchval("SELECT current_setting($1)", name))
            row = await conn.fetchrow(ROLE_SQL, login.user) if login else None
        finally:
            await conn.close()
        out["role_exists"] = "true" if row else "false"
        out["role_can_login"] = ("true" if row["rolcanlogin"] else "false") if row else None
        out["role_connection_limit"] = str(row["rolconnlimit"]) if row else None
        config = parse_role_config(list(row["config"])) if row else {}
        for name in ROLE_SETTINGS:
            out[f"role_{name}"] = config.get(name)
        out["cpu_cores"] = await self._cpu_cores()
        return out

    async def _cpu_cores(self) -> str | None:
        try:
            async with RemoteHost(self.node.ssh) as host:
                r = await host.run("nproc", timeout_s=15.0, check=False)
        except Exception:  # noqa: BLE001 -- unreadable is None; the plan then refuses
            return None
        value = r.stdout.strip()
        return value if r.exit_status == 0 and value.isdigit() else None

    async def analytics_sample(self) -> dict[str, Any]:
        conn = self._sample_conn
        if conn is None or conn.is_closed():
            conn = self._sample_conn = await self._connect(timeout_s=5.0, server_settings={
                "application_name": "resilience-analytics-sampler"})
        try:
            sample = json.loads(await conn.fetchval(SAMPLE_SQL, ANALYTICS_APPLICATION_NAME, timeout=5.0))
        except Exception:
            await self._close_sampler()
            raise
        helpers = [int(v) for v in (sample.pop("helpers", None) or {}).values()]
        sample["helpers_max"] = max(helpers, default=0)       # most helpers of one report
        sample["helpers_total"] = sum(helpers)
        sample["t_mono_ns"] = time.monotonic_ns()
        reports = self._reports
        if reports is not None and not reports["stopped"]:
            reports["samples"].append(dict(sample))           # kept for the fault's confirmation (minute 1)
        return sample

    async def _close_sampler(self) -> None:
        conn, self._sample_conn = self._sample_conn, None
        if conn is not None and not conn.is_closed():
            try:
                await asyncio.wait_for(conn.close(), 5.0)
            except Exception:  # noqa: BLE001
                conn.terminate()

    async def customer_cache_counters(self) -> dict[str, Any]:
        conn = await self._connect(timeout_s=CONNECT_TIMEOUT_S)
        try:
            customers = await conn.fetchrow(CACHE_SQL, list(CUSTOMER_TABLES))
            reports = await conn.fetchrow(CACHE_SQL, list(REPORT_TABLES))
        finally:
            await conn.close()
        return {"customers": {"hit": int(customers["hit"]), "read": int(customers["read"])},
                "reports": {"hit": int(reports["hit"]), "read": int(reports["read"])},
                "customer_tables": list(CUSTOMER_TABLES), "report_tables": list(REPORT_TABLES)}

    # --- the reports ---------------------------------------------------------------------------

    async def start_analytics(self, *, full_table_sessions: int, index_sessions: int, limit_tests: bool,
                              extra_warehouses: tuple[int, int] | None) -> dict[str, Any]:
        login = self.node.analytics
        if login is None:
            return {"supported": False, "error": "the environment names no analytics login (node.analytics)"}
        if index_sessions and extra_warehouses is None:
            return {"supported": False, "error": "index reports need warehouses the workload does not use"}
        await self.stop_analytics()       # reports of an earlier start of this adapter are never left behind
        conn = await self._connect(timeout_s=CONNECT_TIMEOUT_S)
        try:
            rows = int(await conn.fetchval(f"SELECT reltuples::bigint FROM pg_class "
                                           f"WHERE oid = '{S}.order_line'::regclass") or 0)
        finally:
            await conn.close()
        if rows <= 0:
            return {"supported": False, "error": f"{S}.order_line has no rows (or no statistics)"}
        plan = (["full"] * full_table_sessions) + (["index"] * index_sessions)
        self._reports = {"started_mono_ns": time.monotonic_ns(), "stopped": False, "rows": rows,
                         "kinds": {}, "records": [], "failed": [], "limit_tests": {}, "sessions": [], "samples": []}
        self._report_stop = asyncio.Event()
        conns: list[asyncpg.Connection] = []
        try:
            for _ in plan:
                conns.append(await self._connect(endpoint=login, timeout_s=CONNECT_TIMEOUT_S, command_timeout=None,
                                                 server_settings={"application_name": ANALYTICS_APPLICATION_NAME}))
        except Exception as exc:  # noqa: BLE001 -- reported; the fault then did not land
            for c in conns:
                c.terminate()
            self._reports = None
            return {"supported": False, "error": f"report session {len(conns) + 1} of {len(plan)} not opened as "
                                                 f"{login.user}: {type(exc).__name__}: {exc}"}
        self._report_conns = tuple(conns)
        sessions, tasks = [], []
        rng = random.Random(0xA11A)        # seeded: the same index ranges every run
        for i, (kind, c) in enumerate(zip(plan, conns)):
            first = None
            if limit_tests and kind == "full" and i < 2:
                first = "limit_temp" if i == 0 else "limit_timeout"
            pid = int(c.get_server_pid())
            sessions.append({"session": i + 1, "kind": kind, "pid": pid, "first": first})
            tasks.append(asyncio.create_task(
                self._run_session(i + 1, kind, c, first, extra_warehouses, random.Random(rng.random())),
                name=f"report-{i + 1}"))
        self._report_tasks = tuple(tasks)
        self._reports["sessions"] = sessions
        return {"supported": True, "t0_mono_ns": time.monotonic_ns(), "sessions": sessions,
                "application_name": ANALYTICS_APPLICATION_NAME, "login": login.user,
                "full_table_sessions": full_table_sessions, "index_sessions": index_sessions,
                "limit_tests": limit_tests, "extra_warehouses": list(extra_warehouses) if extra_warehouses else None}

    async def _run_session(self, session: int, kind: str, conn: asyncpg.Connection, first: str | None,
                           extra: tuple[int, int] | None, rng: random.Random) -> None:
        assert self._report_stop is not None and self._reports is not None
        queries = itertools.chain([first] if first else [], itertools.cycle(FULL_TABLE_CYCLE if kind == "full" else ("q4",)))
        for query in queries:
            if self._report_stop.is_set():
                return
            await self._one_report(session, query, conn, extra, rng)

    async def _one_report(self, session: int, query: str, conn: asyncpg.Connection,
                          extra: tuple[int, int] | None, rng: random.Random) -> None:
        reports = self._reports
        assert reports is not None
        rows = reports["rows"]
        record: dict[str, Any] = {"session": session, "query": query, "started_mono_ns": time.monotonic_ns()}
        try:
            if query == "limit_temp":
                async with conn.transaction():
                    await conn.execute("SET LOCAL max_parallel_workers_per_gather = 0")   # this transaction only
                    await conn.fetch(LIMIT_TEMP_SQL, max(0, rows - 10))
            elif query == "limit_timeout":
                await conn.fetch(LIMIT_TIMEOUT_SQL)
            else:
                if query == "q1":
                    explain = await conn.fetchval(EXPLAIN + Q1_SQL)
                elif query == "q2":
                    explain = await conn.fetchval(EXPLAIN + Q2_SQL)
                elif query == "q3":
                    explain = await conn.fetchval(EXPLAIN + Q3_SQL, rows // 2)
                else:
                    assert extra is not None
                    explain = await conn.fetchval(EXPLAIN + Q4_SQL, rng.randint(extra[0], extra[1]),
                                                  rng.randint(1, DISTRICTS), rng.randint(1, Q4_FIRST_ORDER_MAX))
                record.update(plan_figures(explain))
            record["outcome"] = "completed"
        except asyncio.CancelledError:
            record["outcome"] = "cancelled"
            self._record(record)
            raise
        except asyncpg.PostgresError as exc:
            record.update(outcome="error", sqlstate=exc.sqlstate, error=str(exc)[:300])
        except Exception as exc:  # noqa: BLE001 -- the connection broke: recorded, the session ends
            record.update(outcome="error", sqlstate=None, error=f"{type(exc).__name__}: {exc}"[:300])
            self._record(record)
            raise
        self._record(record)

    def _record(self, record: dict[str, Any]) -> None:
        reports = self._reports
        if reports is None:
            return
        record["ended_mono_ns"] = time.monotonic_ns()
        record["seconds"] = round((record["ended_mono_ns"] - record["started_mono_ns"]) / 1e9, 3)
        kind = reports["kinds"].setdefault(record["query"], _Kind())
        kind.count += 1
        kind.total_s += record["seconds"]
        kind.max_s = max(kind.max_s, record["seconds"])
        if record["outcome"] == "completed":
            kind.completed += 1
            kind.shared_hit += record.get("shared_hit", 0)
            kind.shared_read += record.get("shared_read", 0)
            kind.temp_written += record.get("temp_written", 0)
            kind.workers_launched_max = max(kind.workers_launched_max, record.get("workers_launched", 0))
        elif record["outcome"] == "cancelled":
            kind.cancelled += 1
        else:
            code = str(record.get("sqlstate"))
            kind.errors[code] = kind.errors.get(code, 0) + 1
        if record["query"].startswith("limit_"):
            reports["limit_tests"][record["query"]] = record
        elif len(reports["records"]) < KEEP_RECORDS:
            reports["records"].append(record)
        if record["outcome"] != "completed" and len(reports["failed"]) < KEEP_FAILED:
            reports["failed"].append(record)

    async def analytics_state(self) -> dict[str, Any]:
        reports = self._reports
        if reports is None:
            return {"started": False}
        return {"started": True, "stopped": reports["stopped"], "started_mono_ns": reports["started_mono_ns"],
                "stopped_mono_ns": reports.get("stopped_mono_ns"), "order_line_rows": reports["rows"],
                "sessions": reports["sessions"], "by_kind": {k: v.as_dict() for k, v in reports["kinds"].items()},
                "limit_tests": reports["limit_tests"], "records": reports["records"], "failed": reports["failed"],
                "samples": reports["samples"]}

    async def stop_analytics(self) -> dict[str, Any]:
        """Each running query cancelled (asyncpg sends the server a cancel request when its task is
        cancelled), then each session closed; a session that does not close is cut off."""
        tasks, self._report_tasks = list(self._report_tasks), ()
        conns, self._report_conns = list(self._report_conns), ()
        if self._report_stop is not None:
            self._report_stop.set()
        if not tasks and not conns:
            return {"stopped": False, "note": "no reports were running in this process"}
        for t in tasks:
            t.cancel()
        done, pending = await asyncio.wait(tasks, timeout=STOP_TIMEOUT_S) if tasks else (set(), set())
        closed = cut = 0
        for c in conns:
            if c.is_closed():
                closed += 1
                continue
            try:
                await asyncio.wait_for(c.close(), STOP_TIMEOUT_S)
                closed += 1
            except Exception:  # noqa: BLE001 -- cut off; the server backstop (revert) ends it
                c.terminate()
                cut += 1
        if self._reports is not None:
            self._reports["stopped"] = True
            self._reports["stopped_mono_ns"] = time.monotonic_ns()
        await self._close_sampler()
        return {"stopped": True, "tasks_ended": len(done), "tasks_not_ended": len(pending),
                "sessions_closed": closed, "sessions_cut_off": cut}

    async def revert_analytics(self) -> dict[str, Any]:
        detail: dict[str, Any] = {"in_process": await self.stop_analytics()}
        remaining = await self._tagged_sessions()
        detail["terminated_on_server"] = 0
        if remaining:
            # another role's sessions: ended as the cluster's OS user (a superuser on the local socket)
            try:
                async with RemoteHost(self.node.ssh) as host:
                    r = await host.run(self._psql_as_os_user(TERMINATE_SQL), timeout_s=15.0, check=False)
                detail["terminated_on_server"] = int(r.stdout.strip() or 0) if r.exit_status == 0 else 0
                if r.exit_status != 0:
                    detail["terminate_error"] = (r.stderr or r.stdout).strip()[:300]
            except Exception as exc:  # noqa: BLE001 -- recorded; what is left is read back below
                detail["terminate_error"] = f"{type(exc).__name__}: {exc}"
            for _ in range(SERVER_GONE_POLLS):
                remaining = await self._tagged_sessions()
                if not remaining:
                    break
                await asyncio.sleep(SERVER_GONE_PAUSE_S)
        detail["sessions_remaining"] = remaining
        if remaining:
            raise RuntimeError(f"{remaining} report session(s) ({ANALYTICS_APPLICATION_NAME}) still open after the "
                               f"revert{'; ' + detail['terminate_error'] if detail.get('terminate_error') else ''}")
        return detail

    async def _tagged_sessions(self) -> int:
        conn = await self._connect(timeout_s=CONNECT_TIMEOUT_S)
        try:
            return int(await conn.fetchval(TAGGED_SQL))
        finally:
            await conn.close()

    async def analytics_leftovers(self) -> dict[str, Any]:
        sessions = await self._tagged_sessions()
        conn = await self._connect(timeout_s=CONNECT_TIMEOUT_S)
        try:
            try:
                temp_files: int | None = int(await conn.fetchval("SELECT count(*) FROM pg_ls_tmpdir()"))
                temp_error = None
            except asyncpg.PostgresError as exc:      # needs pg_monitor; unreadable is None, never 0
                temp_files, temp_error = None, f"{exc.sqlstate}: {exc}"
        finally:
            await conn.close()
        out: dict[str, Any] = {"sessions": sessions, "temp_files": temp_files}
        if temp_error:
            out["temp_files_error"] = temp_error
        return out
