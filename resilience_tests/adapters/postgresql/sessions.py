"""PostgreSQL side of the faults held in the database's own sessions: an idle transaction
blocking vacuum (NL-M-05) and a connection flood (NL-R-04)."""

from __future__ import annotations

import asyncio
import re
import time
from typing import Any

import asyncpg

from resilience_tests.execution.remote import RemoteHost

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


class SessionsMixin:
    """Idle-transaction and connection-flood methods of PostgreSQLAdapter. Mixed into the class in
    adapter.py; uses the core's `_connect` and `_psql_as_os_user`."""

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
        target_pid = pid or getattr(self, "_idle_pid", None)
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
        conn = getattr(self, "_idle_conn", None)
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
        pid = getattr(self, "_idle_pid", None)
        conn = getattr(self, "_idle_conn", None)
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
        self._holding_conns: list[asyncpg.Connection] = []

        async def one() -> None:
            try:
                self._holding_conns.append(await self._connect(
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
        for c in getattr(self, "_holding_conns", []):
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
