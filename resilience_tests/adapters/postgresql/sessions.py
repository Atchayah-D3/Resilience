"""PostgreSQL side of the faults held in the database's own sessions: an idle transaction
blocking vacuum (NL-M-05) and a connection flood (NL-R-04)."""

from __future__ import annotations

import asyncio
import re
import time
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import asyncpg

from resilience_tests.adapters.base import SessionFaultSupport
from resilience_tests.execution.workload.contract import TPCC_STOCK
from resilience_tests.execution.remote import RemoteHost

if TYPE_CHECKING:
    # what this mixin uses from the adapter's core class (host.py)
    from resilience_tests.adapters.postgresql.host import PostgreSQLCore
else:
    PostgreSQLCore = object

# NL-R-04: flood sessions carry this application_name, so a revert -- even from a kill switch
# in another process -- can find and terminate exactly them and nothing else.
FLOOD_APPLICATION_NAME = "resilience-flood"
FLOOD_CONNECT_TIMEOUT_S = 3.0
FLOOD_RECOVERY_TIMEOUT_S = 10.0

# NL-M-05: the injected idle session carries this application_name, so it -- and only it --
# can be found and ended again, by cleanup or by the kill switch after a harness crash.
IDLE_SESSION_APPLICATION_NAME = "resilience-harness-idle"
TERMINATE_IDLE_SESSIONS_SQL = ("SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity "
                               "WHERE application_name = $1 AND pid <> pg_backend_pid()")
IDLE_CONFIRM_TIMEOUT_S = 5.0
VACUUM_PROBE_TIMEOUT_S = 120.0
_DEAD_NOT_REMOVABLE_RE = re.compile(r"(\d+) are dead but not yet removable")
_REMOVABLE_CUTOFF_RE = re.compile(r"removable cutoff: (\d+)")

# NL-N-I03: the lock pile-up behind a schema change. The contended table is harness-owned and
# kept between runs; a share of the workload reads and updates its rows (the "table users"),
# so a lock on it has someone to block. The pile-up's two sessions carry their own
# application_name, so cleanup and the kill switch end exactly them from any process.
CONTENDED_TABLE = "resilience.lock_target"
CONTENDED_SCHEMA, CONTENDED_RELNAME = "resilience", "lock_target"
# Large enough that a rewriting schema change (run 3c) holds its lock for seconds, not a blink.
CONTENDED_ROWS = 2_000_000
LOCK_READER_APPLICATION_NAME = "resilience-lock-reader"
LOCK_DDL_APPLICATION_NAME = "resilience-lock-ddl"
LOCK_PILEUP_APPLICATION_NAMES = (LOCK_READER_APPLICATION_NAME, LOCK_DDL_APPLICATION_NAME)
# What each schema change adds; both are removed again by revert (and by the next prepare).
LOCK_PROBE_COLUMN = "resilience_probe_col"        # add_column: metadata only, instant once locked
LOCK_REWRITE_COLUMN = "resilience_probe_ts"       # add_column_rewrite: a volatile default rewrites every row
LOCK_PROBE_COLUMNS = (LOCK_PROBE_COLUMN, LOCK_REWRITE_COLUMN)
# The tables a pile-up can form on (the catalog's lock_pileup.table -> the engine's table): the
# harness's own contended table, or TPC-C's stock table, whose users are New-Order and Stock-Level.
LOCK_TABLES = {"contended_table": CONTENDED_TABLE, "tpcc_stock": TPCC_STOCK}
# {table} is filled with one of LOCK_TABLES
LOCK_DDL_SQL = {
    "add_column": f"ALTER TABLE {{table}} ADD COLUMN {LOCK_PROBE_COLUMN} integer",
    "add_column_rewrite": (f"ALTER TABLE {{table}} ADD COLUMN {LOCK_REWRITE_COLUMN} "
                           "timestamptz DEFAULT clock_timestamp()"),
}
# The read takes the table's shared lock and keeps it until its transaction ends; its own cost
# does not matter (TPC-C's stock has 100,000 rows per warehouse, so a full count could take
# minutes before the lock is even confirmed). The Scenario 3 document's "SELECT count(*) FROM
# stock" holds the same lock.
LOCK_READ_SQL = "SELECT count(*) FROM (SELECT 1 FROM {table} LIMIT 1) AS one_row"
# The read's transaction is kept open AND busy: short statements back to back, so neither
# idle_in_transaction_session_timeout nor a server statement_timeout can end it early.
LOCK_HOLD_SQL = "SELECT pg_sleep(5)"
LOCK_CHAIN_WINDOW_S = 3.0        # the chain must be seen this soon after the schema change is issued
LOCK_POLL_S = 0.1           # the chain watcher; a short rewrite is over in well under a second
LOCK_CONFIRM_TIMEOUT_S = 5.0
LOCK_RELEASE_TIMEOUT_S = 15.0    # one hold statement (5 s) plus margin
LOCK_DDL_FINISH_TIMEOUT_S = 30.0
LOCK_COLUMN_DROP_ATTEMPTS = 6
LOCK_COLUMN_DROP_PAUSE_S = 2.0
LOCK_SETTINGS = ["lock_timeout", "statement_timeout", "idle_in_transaction_session_timeout",
                 "deadlock_timeout", "log_lock_waits", "max_connections"]
LOCK_WAITERS_SQL = """
SELECT pid, application_name, pg_blocking_pids(pid) AS blocked_by,
       EXTRACT(EPOCH FROM (now() - query_start)) AS waiting_s
  FROM pg_stat_activity
 WHERE datname = current_database() AND wait_event_type = 'Lock'
"""
LOCK_HELD_SQL = ("SELECT mode, granted FROM pg_locks "
                 "WHERE locktype = 'relation' AND relation = to_regclass($1) AND pid = $2")
# Every lock on the contended table, with its holder or waiter: sampled once a second while the
# schema change is unfinished (who holds what, who waits), kept as evidence with the pile-up.
LOCK_TABLE_LOCKS_SQL = """
SELECT l.pid, a.application_name, l.mode, l.granted
  FROM pg_locks l LEFT JOIN pg_stat_activity a USING (pid)
 WHERE l.locktype = 'relation' AND l.relation = to_regclass($1)
 ORDER BY l.granted DESC, l.pid
"""
LOCK_SAMPLE_S = 1.0          # the per-second evidence of the chain and the table's locks
LOCK_SAMPLES_MAX = 900       # 15 min of samples: longer than any catalogued hold
LOCK_PROBE_COLUMNS_SQL = ("SELECT column_name FROM information_schema.columns "
                          "WHERE table_schema = $1 AND table_name = $2 AND column_name = ANY($3::text[])")


def parse_pg_interval_s(val: str | None) -> float:
    """Parse PostgreSQL interval strings such as '30s', '1min', '2h', '500ms', '0' into seconds."""
    if not val:
        return 0.0
    val = str(val).strip().lower()
    if val in ("0", "disabled", "off", "none"):
        return 0.0
    m = re.match(r"^([0-9]+(?:\.[0-9]+)?)\s*([a-z]+)?$", val)
    if not m:
        return 0.0
    num = float(m.group(1))
    unit = m.group(2) or "s"
    if unit in ("ms", "millisecond", "milliseconds"):
        return num / 1000.0
    elif unit in ("s", "sec", "second", "seconds"):
        return num
    elif unit in ("min", "m", "minute", "minutes"):
        return num * 60.0
    elif unit in ("h", "hr", "hour", "hours"):
        return num * 3600.0
    elif unit in ("d", "day", "days"):
        return num * 86400.0
    return num


class SessionsMixin(SessionFaultSupport, PostgreSQLCore):
    """Idle-transaction and connection-flood methods of PostgreSQLAdapter. Mixed into the class in
    adapter.py; uses the core's `node`, `_connect` and `_psql_as_os_user` (host.py)."""

    idle_timeout_sqlstates = ("25P03",)  # idle_in_transaction_session_timeout

    # the sessions a fault holds open (none until one is)
    _idle_conn: asyncpg.Connection | None = None
    _idle_pid: int | None = None
    _idle_conn_error: str | None = None
    _holding_conns: Sequence[asyncpg.Connection] = ()
    # NL-N-I03: the pile-up's read session, its background statements, and what was observed
    _lock_reader: asyncpg.Connection | None = None
    _lock_stop: asyncio.Event | None = None
    _lock_hold_task: asyncio.Task[None] | None = None
    _lock_ddl_task: asyncio.Task[None] | None = None
    _lock_state: dict[str, Any] | None = None

    async def observe_idle_transaction_settings(self) -> dict[str, str]:
        """Read only -- nothing is written: the server's own timeout (NL-M-05)."""
        conn = await self._connect(timeout_s=10.0)
        try:
            return {"idle_in_transaction_session_timeout": str(await conn.fetchval("SHOW idle_in_transaction_session_timeout"))}
        finally:
            await conn.close()

    def idle_session_timeout_s(self, observed: dict[str, str]) -> float:
        return parse_pg_interval_s(observed.get("idle_in_transaction_session_timeout"))


    async def inject_idle_transaction(self) -> dict[str, Any]:
        """Inject an open idle-in-transaction holding back the vacuum horizon (NL-M-05).

        A dedicated session, tagged with IDLE_SESSION_APPLICATION_NAME so every later step --
        including the kill switch after a harness crash -- can find exactly this session and no
        other, BEGINs and takes a transaction id, then goes idle. The fault is confirmed from a
        SEPARATE connection: the session must be seen as `idle in transaction` holding that
        transaction id. Querying pg_stat_activity from the idle session itself would show it
        `active` (it is running that query) and would move its snapshot.

        No fallback: if the session cannot be established and confirmed, the injection did not
        happen and the run aborts. A second, unconfirmed mechanism would only produce a fault
        the harness cannot see or clean up."""
        conn: Any = None
        try:
            conn = await self._connect(timeout_s=10.0, command_timeout=None,
                                       server_settings={"application_name": IDLE_SESSION_APPLICATION_NAME})
            pid = await conn.fetchval("SELECT pg_backend_pid()")
            await conn.execute("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED")
            await conn.execute("SELECT txid_current()")
            self._idle_conn, self._idle_pid = conn, pid
        except Exception as exc:  # noqa: BLE001 -- reported; the orchestrator aborts the run
            if conn is not None:
                try:
                    if not conn.is_closed():
                        await conn.execute("ROLLBACK")
                    await conn.close()
                except Exception:  # noqa: BLE001
                    pass
            self._idle_conn = self._idle_pid = None
            return {"supported": False, "error": f"{type(exc).__name__}: {exc}"}

        row = None
        stat_conn = await self._connect(timeout_s=5.0)
        try:
            deadline = time.monotonic() + IDLE_CONFIRM_TIMEOUT_S
            while time.monotonic() < deadline:
                row = await stat_conn.fetchrow(
                    "SELECT state, backend_xid, backend_xmin, "
                    "EXTRACT(EPOCH FROM (now() - xact_start)) AS xact_age_s "
                    "FROM pg_stat_activity WHERE pid = $1", pid)
                if row is not None and row["state"] == "idle in transaction":
                    break
                await asyncio.sleep(0.05)
        finally:
            await stat_conn.close()
        state = row["state"] if row is not None else None
        xid = row["backend_xid"] if row is not None else None
        detail = {
            "pid": pid,
            "application_name": IDLE_SESSION_APPLICATION_NAME,
            "state": state,
            "backend_xid": None if xid is None else int(xid),
            "backend_xmin": None if row is None or row["backend_xmin"] is None else str(row["backend_xmin"]),
            "xact_age_s": None if row is None or row["xact_age_s"] is None else float(row["xact_age_s"]),
        }
        # confirmed only when the server itself shows the session idle AND holding an xid
        detail["supported"] = state == "idle in transaction" and xid is not None
        if not detail["supported"]:
            detail["error"] = (f"session {pid} was not seen as 'idle in transaction' holding a transaction id "
                               f"within {IDLE_CONFIRM_TIMEOUT_S} s (state {state!r}, backend_xid {xid!r})")
        return detail

    async def check_idle_transaction(self, pid: int | None = None) -> dict[str, Any]:
        """Check status of the idle-in-transaction backend (NL-M-05): still open, or gone with
        the harness's session closed. Says only WHETHER it ended; why it ended is judged by the
        orchestrator from `termination_sqlstate` here and the server's own log line."""
        target_pid = pid or self._idle_pid
        if target_pid is None:
            # No session was ever established. "The backend is absent" must never be read as
            # "the engine enforced idle_in_transaction_session_timeout" -- that is the
            # fail-open a crashed or failed injection would otherwise report as path A.
            return {
                "pid": None,
                "terminated_by_timeout": False,
                "still_idle": False,
                "conn_closed": False,
                "note": "no idle transaction session was established (injection did not produce a backend pid)",
            }

        # Query pg_stat_activity first without touching the held session!
        # Running SELECT 1 inside an open READ COMMITTED transaction takes a new snapshot
        # and advances backend_xmin, mutating the very state we are measuring.
        # Only probe the connection if the backend row is absent from pg_stat_activity.
        try:
            stat_conn = await self._connect(timeout_s=5.0)
            try:
                row = await stat_conn.fetchrow(
                    "SELECT pid, state, backend_xmin, "
                    "EXTRACT(EPOCH FROM (now() - xact_start)) AS xact_age_s, "
                    "wait_event_type, wait_event "
                    "FROM pg_stat_activity WHERE pid = $1",
                    target_pid,
                )
                if row is not None:
                    return {
                        "pid": target_pid,
                        "terminated_by_timeout": False,
                        "still_idle": "idle" in str(row["state"]).lower(),
                        "state": str(row["state"]),
                        "backend_xmin": str(row["backend_xmin"]) if row.get("backend_xmin") else None,
                        "age_s": float(row["xact_age_s"] or 0.0),
                        "wait_event": row.get("wait_event"),
                    }
            finally:
                await stat_conn.close()
        except Exception as exc:  # noqa: BLE001 -- reported; the caller then cannot tell either way
            # Without the activity row the session's state is unknown. Probing the held session
            # instead would read a crashed server as "terminated", and a SELECT on a session that
            # is still open would advance its xmin -- the state being measured.
            return {
                "pid": target_pid,
                "terminated_by_timeout": False,
                "still_idle": False,
                "error": f"pg_stat_activity could not be read: {type(exc).__name__}: {exc}",
            }

        # Backend is not present in pg_stat_activity. Now probe the held session to confirm
        # whether the server terminated it (Path A), keeping the error code it was closed with.
        conn = self._idle_conn
        conn_closed = False
        sqlstate = None
        if conn is not None:
            if conn.is_closed():
                conn_closed = True
            else:
                try:
                    await asyncio.wait_for(conn.fetchval("SELECT 1"), timeout=0.5)
                except Exception as exc:
                    conn_closed = True
                    sqlstate = getattr(exc, "sqlstate", None)
                    self._idle_conn_error = str(exc)

        if conn_closed:
            return {
                "pid": target_pid,
                "terminated_by_timeout": True,
                "still_idle": False,
                "conn_closed": True,
                "termination_sqlstate": sqlstate,
                "note": "harness session was closed and the backend is no longer present",
            }
        return {
            "pid": target_pid,
            "terminated_by_timeout": False,
            "still_idle": False,
            "conn_closed": False,
            "note": "backend not present but the harness session did not close; "
                    "timeout enforcement NOT confirmed",
        }

    def idle_timeout_log_patterns(self) -> tuple[str, ...]:
        return (r"terminating connection due to idle-in-transaction timeout",)

    async def close_idle_transaction(self) -> dict[str, Any]:
        """Roll back and close the injected idle session (NL-M-05). Idempotent.

        Only the harness's own session is ever touched: the server-side fallback terminates
        backends carrying IDLE_SESSION_APPLICATION_NAME and nothing else -- never every idle
        session on the cluster, and never a signal to an operating-system PID that may since
        have been reused."""
        pid = self._idle_pid
        conn = self._idle_conn
        detail: dict[str, Any] = {"pid": pid}
        if conn is not None:
            try:
                if not conn.is_closed():
                    await conn.execute("ROLLBACK")
                    await conn.close()
                detail["conn_closed"] = True
            except Exception as exc:
                detail["conn_close_error"] = str(exc)
            finally:
                self._idle_conn = None
        try:
            stat_conn = await self._connect(timeout_s=5.0)
            try:
                detail["terminated"] = await stat_conn.fetchval(TERMINATE_IDLE_SESSIONS_SQL, IDLE_SESSION_APPLICATION_NAME)
            finally:
                await stat_conn.close()
        except Exception as exc:  # noqa: BLE001 -- the ledger revert repeats this over SSH
            detail["terminate_error"] = f"{type(exc).__name__}: {exc}"
        self._idle_pid = None
        return detail

    async def evaluate_vacuum_bloat(self) -> dict[str, Any]:
        """Dead-tuple counters on the harness tables (NL-M-05). These are planner statistics:
        context for the report, never the evidence a path is accepted on."""
        conn = await self._connect(timeout_s=5.0)
        try:
            row = await conn.fetchrow(
                "SELECT "
                "COALESCE(SUM(CASE WHEN relname = 'churn' THEN n_live_tup END), SUM(n_live_tup), 0) AS live_tup, "
                "COALESCE(SUM(CASE WHEN relname = 'churn' THEN n_dead_tup END), SUM(n_dead_tup), 0) AS dead_tup, "
                "MAX(last_vacuum)::text AS last_vacuum, "
                "MAX(last_autovacuum)::text AS last_autovacuum, "
                "MAX(last_analyze)::text AS last_analyze, "
                "MAX(last_autoanalyze)::text AS last_autoanalyze "
                "FROM pg_stat_user_tables "
                "WHERE schemaname = 'resilience' AND relname IN ('churn', 'markers')"
            )
            row_dict = dict(row) if row else {}
            live = int(row_dict.get("live_tup", 0))
            dead = int(row_dict.get("dead_tup", 0))
            total = live + dead
            max_age_row = await conn.fetchval(
                "SELECT COALESCE(MAX(EXTRACT(EPOCH FROM (now() - xact_start))), 0) "
                "FROM pg_stat_activity "
                "WHERE state LIKE 'idle in transaction%'"
            )
            return {
                "dead_tuple_ratio": round(dead / total, 4) if total > 0 else 0.0,
                "unvacuumed_dead_tuples": dead,
                "live_tuples": live,
                "tuple_bloat_ratio": round(total / max(live, 1), 4),
                "oldest_transaction_age_s": round(float(max_age_row or 0.0), 2),
                "last_vacuum": row_dict.get("last_vacuum"),
                "last_autovacuum": row_dict.get("last_autovacuum"),
                "last_analyze": row_dict.get("last_analyze"),
                "last_autoanalyze": row_dict.get("last_autoanalyze"),
            }
        finally:
            await conn.close()

    async def probe_vacuum_horizon(self) -> dict[str, Any]:
        """VACUUM (VERBOSE) the churn table while the idle session is still open (NL-M-05 path
        B evidence). PostgreSQL reports, per table, how many dead tuples it could not remove
        because an open transaction might still see them, and the cutoff it was held to:

            tuples: 120 removed, 2000 remain, 5310 are dead but not yet removable
            removable cutoff: 74121, which was 6022 XIDs old when operation ended

        The table is the harness's own, so vacuuming it touches nothing the operator owns."""
        messages: list[str] = []
        conn = await self._connect(timeout_s=10.0, command_timeout=VACUUM_PROBE_TIMEOUT_S)
        try:
            # PostgreSQL 15+ puts the counts in the message itself; older servers in DETAIL
            conn.add_log_listener(lambda _c, msg: messages.append(
                f"{getattr(msg, 'message', '') or ''}\n{getattr(msg, 'detail', '') or ''}"))
            await conn.execute("VACUUM (VERBOSE) resilience.churn")
        finally:
            await conn.close()
        text = "\n".join(messages)
        dead = _DEAD_NOT_REMOVABLE_RE.search(text)
        cutoff = _REMOVABLE_CUTOFF_RE.search(text)
        return {
            "supported": True,
            "dead_not_removable": int(dead.group(1)) if dead else None,
            "removable_cutoff": int(cutoff.group(1)) if cutoff else None,
            "output": text[-2000:],
        }

    async def exhaust_connections(self, hold_s: float) -> dict[str, Any]:
        """NL-R-04 (Framework §10.5): open max_connections + 50% sessions under load, hold them
        for `hold_s`, release them, and confirm the server accepts a new session again.

        Every flood session carries FLOOD_APPLICATION_NAME, so revert_exhaust_connections can
        terminate exactly these from any process. The flood is released here, inside the fault,
        so validate's own connections never compete with it."""
        t0_mono_ns = time.monotonic_ns()
        conn = await self._connect(timeout_s=5.0)
        try:
            max_conn = int(await conn.fetchval("SHOW max_connections"))
            su_reserved = int(await conn.fetchval("SHOW superuser_reserved_connections"))
        finally:
            await conn.close()

        attempted = max(10, int(max_conn * 1.5))
        explicit: list[str] = []      # SQLSTATE 53300: the server said why it refused
        other: list[str] = []         # timeouts, resets: a refusal nobody explained
        holding: list[asyncpg.Connection] = []
        self._holding_conns = holding

        async def one() -> None:
            try:
                holding.append(await self._connect(
                    self.node.client, timeout_s=FLOOD_CONNECT_TIMEOUT_S, command_timeout=None,
                    server_settings={"application_name": FLOOD_APPLICATION_NAME}))
            except asyncpg.TooManyConnectionsError as exc:
                explicit.append(str(exc))
            except Exception as exc:  # noqa: BLE001 -- classified as an unexplained refusal
                other.append(f"{type(exc).__name__}: {exc}")

        await asyncio.gather(*(one() for _ in range(attempted)))
        held = len(self._holding_conns)

        # Reserved slots, probed while the flood is held, as a role that is entitled to them.
        # Only a superuser proves anything here: a non-superuser who gets in found an ordinary
        # free slot, which says nothing about the reserved ones.
        superuser_slot_honoured: bool | None
        superuser_probe: dict[str, Any]
        try:
            async with RemoteHost(self.node.ssh) as host:
                r = await host.run(self._psql_as_os_user("SELECT rolsuper FROM pg_roles WHERE rolname = current_user"),
                                   timeout_s=10.0, check=False)
            out = (r.stdout + r.stderr).strip()
            superuser_probe = {"exit_status": r.exit_status, "output": out[:300]}
            if r.exit_status == 0 and r.stdout.strip() == "t":
                superuser_slot_honoured = True
            elif r.exit_status == 0:
                superuser_slot_honoured = None
                superuser_probe["note"] = "the probing role is not a superuser, so reserved slots were not tested"
            else:
                superuser_slot_honoured = False
        except Exception as exc:  # noqa: BLE001 -- no probe means not measured, never a pass
            superuser_slot_honoured = None
            superuser_probe = {"error": f"{type(exc).__name__}: {exc}"}

        await asyncio.sleep(hold_s)
        released = await self._release_flood()

        # After release a new ordinary session must be accepted again, within a bound
        recovered, recovery_s = False, None
        t_rel = time.monotonic()
        while time.monotonic() - t_rel < FLOOD_RECOVERY_TIMEOUT_S:
            try:
                probe = await self._connect(self.node.client, timeout_s=2.0)
                await probe.close()
                recovered, recovery_s = True, round(time.monotonic() - t_rel, 3)
                break
            except Exception:  # noqa: BLE001 -- retried until the bound
                await asyncio.sleep(0.2)

        return {
            "action": "connection_exhaustion",
            "t0_mono_ns": t0_mono_ns,
            "max_connections": max_conn,
            "superuser_reserved": su_reserved,
            "attempted_connections": attempted,
            "held_connections": held,
            "rejected_explicit": len(explicit),
            "rejected_other": len(other),
            "rejection_samples": (explicit[:2] + other[:3]),
            # explicit only if the limit was actually reached AND every refusal said why
            "rejections_explicit": bool(explicit) and not other,
            "superuser_slot_honoured": superuser_slot_honoured,
            "superuser_probe": superuser_probe,
            "hold_s": hold_s,
            "released_connections": released,
            "connections_recover_after_release": recovered,
            "recovery_after_release_s": recovery_s,
        }

    async def _release_flood(self) -> int:
        released = 0
        for c in self._holding_conns:
            try:
                await c.close(timeout=5.0)
                released += 1
            except Exception:  # noqa: BLE001 -- the server side is terminated by revert if needed
                c.terminate()
        self._holding_conns = []
        return released

    async def revert_exhaust_connections(self) -> dict[str, Any]:
        released = await self._release_flood()
        terminate = ("SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity "
                     f"WHERE application_name = '{FLOOD_APPLICATION_NAME}'")
        remaining_sql = f"SELECT count(*) FROM pg_stat_activity WHERE application_name = '{FLOOD_APPLICATION_NAME}'"
        async with RemoteHost(self.node.ssh) as host:
            t = await host.run(self._psql_as_os_user(terminate), timeout_s=15.0)
            remaining = 0
            for _ in range(25):   # terminated backends take a moment to leave pg_stat_activity
                r = await host.run(self._psql_as_os_user(remaining_sql), timeout_s=15.0)
                remaining = int(r.stdout.strip() or 0)
                if remaining == 0:
                    break
                await asyncio.sleep(0.2)
        if remaining:
            raise RuntimeError(f"{remaining} flood session(s) ({FLOOD_APPLICATION_NAME}) still open after terminate")
        return {"action": "flood sessions terminated", "released_in_process": released,
                "terminated_on_server": int(t.stdout.strip() or 0), "remaining": 0}

    # ------------------------------------------------------------------ NL-N-I03 lock pile-up

    async def observe_lock_settings(self) -> dict[str, str]:
        """Read only -- nothing is written: the server's own timeouts and lock-wait logging."""
        conn = await self._connect(timeout_s=10.0)
        try:
            rows = await conn.fetch("SELECT name, setting FROM pg_settings WHERE name = ANY($1::text[])",
                                    LOCK_SETTINGS)
            return {r["name"]: str(r["setting"]) for r in rows}
        finally:
            await conn.close()

    async def start_lock_pileup(self, *, blocker: str, ddl: str, ddl_lock_timeout_s: float,
                                ddl_after_s: float, table: str = "contended_table") -> dict[str, Any]:
        """NL-N-I03: a read holds the contended table open (blocker "long_read"), and the
        schema change follows `ddl_after_s` later on its own session -- or, with blocker
        "none", the schema change starts at once. Returns as soon as the pile-up is running,
        confirmed from the server: the read's lock granted, or the schema change issued.

        Session settings only, on the pile-up's own sessions: the read may not be ended by any
        timeout, and the schema change waits exactly `ddl_lock_timeout_s` (0: for ever). The
        server's configuration is never touched."""
        if ddl not in LOCK_DDL_SQL:
            return {"supported": False, "error": f"schema change {ddl!r} is not built for PostgreSQL"}
        if table not in LOCK_TABLES:
            return {"supported": False, "error": f"table {table!r} is not built for PostgreSQL"}
        relation = LOCK_TABLES[table]
        await self.end_lock_pileup()   # a previous pile-up of this adapter is never left behind
        state: dict[str, Any] = {
            "blocker": blocker, "ddl": ddl, "ddl_lock_timeout_s": ddl_lock_timeout_s, "ddl_after_s": ddl_after_s,
            "table": relation, "reader_pid": None, "ddl_pid": None, "ddl_issued_mono_ns": None, "ddl_ended_mono_ns": None,
            "ddl_outcome": None, "ddl_sqlstate": None, "ddl_error": None,
            "chain_confirmed": None, "chain": None, "queue_max": 0, "longest_wait_s": 0.0,
            "samples": [],
        }
        self._lock_state = state
        self._lock_stop = asyncio.Event()
        t0_mono_ns: int | None = None
        if blocker == "long_read":
            try:
                reader = await self._connect(timeout_s=10.0, command_timeout=None, server_settings={
                    "application_name": LOCK_READER_APPLICATION_NAME,
                    "statement_timeout": "0", "idle_in_transaction_session_timeout": "0"})
            except Exception as exc:  # noqa: BLE001 -- reported; the fault then did not land
                return {"supported": False, "error": f"read session not opened: {type(exc).__name__}: {exc}"}
            try:
                await self._no_transaction_timeout(reader)
                state["reader_pid"] = int(await reader.fetchval("SELECT pg_backend_pid()"))
                await reader.execute("BEGIN")
                await reader.fetchval(LOCK_READ_SQL.format(table=relation))
                held = await self._lock_held(state["reader_pid"], "AccessShareLock")
            except Exception as exc:  # noqa: BLE001
                reader.terminate()
                return {"supported": False, "error": f"read did not take its lock: {type(exc).__name__}: {exc}"}
            if not held:
                reader.terminate()
                return {"supported": False, "error": f"session {state['reader_pid']} was not seen holding "
                                                     f"AccessShareLock on {relation}"}
            self._lock_reader = reader
            t0_mono_ns = time.monotonic_ns()
            self._lock_hold_task = asyncio.create_task(self._hold_read(reader), name="lock-pileup-read")
        self._lock_ddl_task = asyncio.create_task(self._run_ddl(), name="lock-pileup-ddl")
        if blocker == "none":
            deadline = time.monotonic() + LOCK_CONFIRM_TIMEOUT_S
            while state["ddl_issued_mono_ns"] is None and state["ddl_outcome"] is None and time.monotonic() < deadline:
                await asyncio.sleep(0.05)
            if state["ddl_issued_mono_ns"] is None:
                await self.end_lock_pileup()
                return {"supported": False, "error": state["ddl_error"] or "the schema change was not issued "
                                                     f"within {LOCK_CONFIRM_TIMEOUT_S} s"}
            t0_mono_ns = state["ddl_issued_mono_ns"]
        return {"supported": True, "t0_mono_ns": t0_mono_ns, "table": relation, "blocker": blocker,
                "ddl": ddl, "ddl_lock_timeout_s": ddl_lock_timeout_s, "ddl_after_s": ddl_after_s,
                "reader_pid": state["reader_pid"],
                "application_names": list(LOCK_PILEUP_APPLICATION_NAMES)}

    @staticmethod
    async def _no_transaction_timeout(conn: asyncpg.Connection) -> None:
        """transaction_timeout exists from PostgreSQL 17; on an older server there is none to
        clear."""
        try:
            await conn.execute("SET transaction_timeout = 0")
        except asyncpg.UndefinedObjectError:
            pass

    async def _lock_held(self, pid: int, mode: str) -> bool:
        conn = await self._connect(timeout_s=5.0)
        try:
            rows = await conn.fetch(LOCK_HELD_SQL, (self._lock_state or {}).get("table", CONTENDED_TABLE), pid)
            return any(r["mode"] == mode and r["granted"] for r in rows)
        finally:
            await conn.close()

    async def _hold_read(self, reader: asyncpg.Connection) -> None:
        """Keep the read's transaction open and busy until the pile-up is released."""
        assert self._lock_stop is not None
        while not self._lock_stop.is_set():
            await reader.execute(LOCK_HOLD_SQL)

    async def _run_ddl(self) -> None:
        """Issue the schema change on its own tagged session and record how it ended, while a
        watcher looks at the blocking chain from a third connection."""
        state, stop = self._lock_state, self._lock_stop
        assert state is not None and stop is not None
        try:
            await asyncio.wait_for(stop.wait(), timeout=state["ddl_after_s"])
            return   # released before the schema change was due: it was never issued
        except TimeoutError:
            pass
        timeout_ms = int(round(state["ddl_lock_timeout_s"] * 1000))
        try:
            conn = await self._connect(timeout_s=10.0, command_timeout=None, server_settings={
                "application_name": LOCK_DDL_APPLICATION_NAME,
                "lock_timeout": f"{timeout_ms}ms" if timeout_ms else "0", "statement_timeout": "0"})
        except Exception as exc:  # noqa: BLE001
            state.update(ddl_outcome="error", ddl_error=f"schema-change session not opened: {type(exc).__name__}: {exc}")
            return
        watcher: asyncio.Task[None] | None = None
        try:
            await self._no_transaction_timeout(conn)
            state["ddl_pid"] = int(await conn.fetchval("SELECT pg_backend_pid()"))
            state["ddl_issued_mono_ns"] = time.monotonic_ns()
            watcher = asyncio.create_task(self._watch_lock_chain(), name="lock-pileup-watch")
            try:
                await conn.execute(LOCK_DDL_SQL[state["ddl"]].format(table=state["table"]))
                outcome, sqlstate, error = "completed", None, None
            except asyncpg.LockNotAvailableError as exc:      # 55P03: the lock timeout fired
                outcome, sqlstate, error = "lock_timeout", exc.sqlstate, str(exc)
            except asyncpg.QueryCanceledError as exc:         # 57014: cancelled at release
                outcome, sqlstate, error = "cancelled", exc.sqlstate, str(exc)
            except asyncpg.PostgresError as exc:
                outcome, sqlstate, error = "error", exc.sqlstate, str(exc)
            state.update(ddl_ended_mono_ns=time.monotonic_ns(), ddl_outcome=outcome,
                         ddl_sqlstate=sqlstate, ddl_error=error)
        except Exception as exc:  # noqa: BLE001 -- the connection itself failed
            state.update(ddl_ended_mono_ns=time.monotonic_ns(), ddl_outcome="error",
                         ddl_error=f"{type(exc).__name__}: {exc}")
        finally:
            if watcher is not None:
                watcher.cancel()
                await asyncio.gather(watcher, return_exceptions=True)
            conn.terminate()

    async def _watch_lock_chain(self) -> None:
        """While the schema change is unfinished: who waits for whom. The chain is confirmed
        when, within LOCK_CHAIN_WINDOW_S of the schema change being issued, the server shows
        the schema change blocked by the read (or, with no blocker, holding its exclusive lock)
        AND at least one other session blocked by the schema change."""
        state = self._lock_state
        assert state is not None
        ddl_pid, reader_pid = state["ddl_pid"], state["reader_pid"]
        issued = time.monotonic()
        next_sample = issued
        conn = await self._connect(timeout_s=5.0)
        try:
            while state["ddl_outcome"] is None:
                rows = [dict(r) for r in await conn.fetch(LOCK_WAITERS_SQL)]
                queue = [r for r in rows if ddl_pid in (r["blocked_by"] or [])]
                state["queue_max"] = max(state["queue_max"], len(queue))
                if time.monotonic() >= next_sample and len(state["samples"]) < LOCK_SAMPLES_MAX:
                    next_sample += LOCK_SAMPLE_S
                    locks = await conn.fetch(LOCK_TABLE_LOCKS_SQL, state["table"])
                    state["samples"].append({
                        "t_s": round(time.monotonic() - issued, 3),
                        "queue": len(queue),
                        # [pid, application_name, blocked_by, waiting_s] of every session waiting for a lock
                        "waiting": [[r["pid"], r["application_name"], list(r["blocked_by"] or []),
                                     round(float(r["waiting_s"] or 0.0), 3)] for r in rows],
                        # [pid, application_name, mode, granted] of every lock on the contended table
                        "locks": [[r["pid"], r["application_name"], r["mode"], r["granted"]] for r in locks],
                    })
                if queue:
                    state["longest_wait_s"] = max(state["longest_wait_s"],
                                                  max(float(r["waiting_s"] or 0.0) for r in queue))
                if state["chain_confirmed"] is None:
                    ddl_row = next((r for r in rows if r["pid"] == ddl_pid), None)
                    if reader_pid is not None:
                        ddl_held_up = ddl_row is not None and reader_pid in (ddl_row["blocked_by"] or [])
                    else:
                        ddl_held_up = await self._lock_held(ddl_pid, "AccessExclusiveLock")
                    if ddl_held_up and queue:
                        state["chain_confirmed"] = True
                        state["chain"] = {
                            "ddl": {"pid": ddl_pid, "application_name": LOCK_DDL_APPLICATION_NAME,
                                    "blocked_by": None if ddl_row is None else list(ddl_row["blocked_by"] or [])},
                            "blocker": None if reader_pid is None else
                            {"pid": reader_pid, "application_name": LOCK_READER_APPLICATION_NAME},
                            "queued_sessions": len(queue),
                            "seen_after_s": round(time.monotonic() - issued, 3),
                        }
                    elif time.monotonic() - issued > LOCK_CHAIN_WINDOW_S:
                        state["chain_confirmed"] = False
                await asyncio.sleep(LOCK_POLL_S)
        finally:
            await conn.close()

    async def lock_pileup_state(self) -> dict[str, Any]:
        """The pile-up as it stands (see SessionFaultSupport.lock_pileup_state)."""
        if self._lock_state is None:
            return {"started": False, "ddl_outcome": None, "chain_confirmed": None}
        return {"started": True, **self._lock_state, "chain": dict(self._lock_state["chain"] or {}) or None,
                "samples": list(self._lock_state.get("samples") or [])}

    async def end_lock_pileup(self) -> dict[str, Any]:
        """Release the pile-up from this adapter's sessions: stop the read's statements and
        commit its transaction (a waiting schema change then gets its lock and finishes), and
        cancel a schema change still running past the release. Idempotent."""
        state, stop = self._lock_state, self._lock_stop
        if state is None or stop is None:
            return {}
        detail: dict[str, Any] = {}
        stop.set()
        if self._lock_hold_task is not None:
            await asyncio.wait([self._lock_hold_task], timeout=LOCK_RELEASE_TIMEOUT_S)
            self._lock_hold_task.cancel()
            await asyncio.gather(self._lock_hold_task, return_exceptions=True)
            self._lock_hold_task = None
        reader, self._lock_reader = self._lock_reader, None
        if reader is not None:
            try:
                if not reader.is_closed():
                    await reader.execute("COMMIT")
                detail["read_committed"] = True
            except Exception as exc:  # noqa: BLE001 -- the server side is terminated by revert
                detail["read_commit_error"] = f"{type(exc).__name__}: {exc}"
            finally:
                reader.terminate()
        task = self._lock_ddl_task
        if task is not None:
            done, _ = await asyncio.wait([task], timeout=LOCK_DDL_FINISH_TIMEOUT_S)
            if not done and state.get("ddl_pid"):
                # still running at the end of the hold (a rewrite longer than fault.duration):
                # cancelled on the server, which ends it with 57014
                detail["cancel_requested"] = await self._cancel_backend(state["ddl_pid"])
                await asyncio.wait([task], timeout=LOCK_RELEASE_TIMEOUT_S)
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            self._lock_ddl_task = None
        detail.update(ddl_outcome=state["ddl_outcome"], ddl_sqlstate=state["ddl_sqlstate"])
        return detail

    async def _cancel_backend(self, pid: int) -> bool:
        conn = await self._connect(timeout_s=5.0)
        try:
            return bool(await conn.fetchval("SELECT pg_cancel_backend($1)", pid))
        finally:
            await conn.close()

    async def revert_lock_pileup(self) -> dict[str, Any]:
        """End every session the pile-up tagged, on the server, from any process -- then remove
        the columns its schema change may have added. Raises if a tagged session or a probe
        column remains, so the ledger records the revert as failed and the kill switch retries."""
        detail: dict[str, Any] = {"in_process": await self.end_lock_pileup()}
        names = ", ".join(f"'{n}'" for n in LOCK_PILEUP_APPLICATION_NAMES)
        terminate = ("SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity "
                     f"WHERE application_name IN ({names}) AND pid <> pg_backend_pid()")
        remaining_sql = f"SELECT count(*) FROM pg_stat_activity WHERE application_name IN ({names})"
        async with RemoteHost(self.node.ssh) as host:
            t = await host.run(self._psql_as_os_user(terminate), timeout_s=15.0)
            remaining = 0
            for _ in range(25):   # terminated backends take a moment to leave pg_stat_activity
                r = await host.run(self._psql_as_os_user(remaining_sql), timeout_s=15.0)
                remaining = int(r.stdout.strip() or 0)
                if remaining == 0:
                    break
                await asyncio.sleep(0.2)
        detail.update(terminated_on_server=int(t.stdout.strip() or 0), sessions_remaining=remaining)
        if remaining:
            raise RuntimeError(f"{remaining} lock pile-up session(s) still open after terminate")
        detail["columns_dropped"], columns_left = await self._drop_probe_columns()
        detail["probe_columns_remaining"] = len(columns_left)
        if columns_left:
            raise RuntimeError(f"probe column(s) {columns_left} still on a pile-up table")
        return {"action": "lock pile-up sessions ended, probe columns removed", **detail}

    async def _drop_probe_columns(self) -> tuple[list[str], list[str]]:
        """Drop the pile-up's probe columns from every table a pile-up can form on (a fresh
        adapter -- the kill switch -- does not know which one a crashed run used), with a short
        lock timeout so the drop can never cause a pile-up of its own; retried while the table
        is busy. (dropped, remaining), each as "table.column"."""
        conn = await self._connect(timeout_s=10.0, command_timeout=60.0)
        dropped: list[str] = []
        left_all: list[str] = []
        try:
            await conn.execute("SET lock_timeout = '5s'")
            for relation in dict.fromkeys(LOCK_TABLES.values()):
                schema, relname = relation.split(".", 1)
                present = [r["column_name"] for r in await conn.fetch(
                    LOCK_PROBE_COLUMNS_SQL, schema, relname, list(LOCK_PROBE_COLUMNS))]
                if present:
                    drops = ", ".join(f"DROP COLUMN IF EXISTS {c}" for c in present)
                    for _ in range(LOCK_COLUMN_DROP_ATTEMPTS):
                        try:
                            await conn.execute(f"ALTER TABLE {relation} {drops}")
                            break
                        except asyncpg.LockNotAvailableError:
                            await asyncio.sleep(LOCK_COLUMN_DROP_PAUSE_S)
                left = [r["column_name"] for r in await conn.fetch(
                    LOCK_PROBE_COLUMNS_SQL, schema, relname, list(LOCK_PROBE_COLUMNS))]
                dropped += [f"{relation}.{c}" for c in present if c not in left]
                left_all += [f"{relation}.{c}" for c in left]
            return dropped, left_all
        finally:
            await conn.close()

    async def lock_pileup_leftovers(self) -> dict[str, Any]:
        """Read only, after the run: tagged pile-up sessions still open on the server, and probe
        columns still on a pile-up table (Scenario 3 document, table 20 and check 7)."""
        names = list(LOCK_PILEUP_APPLICATION_NAMES)
        conn = await self._connect(timeout_s=10.0)
        try:
            sessions = int(await conn.fetchval(
                "SELECT count(*) FROM pg_stat_activity WHERE application_name = ANY($1::text[])", names))
            columns: list[str] = []
            for relation in dict.fromkeys(LOCK_TABLES.values()):
                schema, relname = relation.split(".", 1)
                columns += [f"{relation}.{r['column_name']}" for r in await conn.fetch(
                    LOCK_PROBE_COLUMNS_SQL, schema, relname, list(LOCK_PROBE_COLUMNS))]
            return {"sessions": sessions, "probe_columns": columns}
        finally:
            await conn.close()
