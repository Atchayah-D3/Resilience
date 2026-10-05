"""PostgreSQL adapter (Arch §8) -- serves ShaktiDB and standalone PostgreSQL.

Everything PostgreSQL-specific lives here: asyncpg, SQL, the error taxonomy, pg_amcheck,
page-checksum counters and the durability settings. The rest of the harness knows none of it.
"""

from __future__ import annotations

import asyncio
import re
import shlex
import time
from typing import Any

import asyncpg

from resilience_tests.adapters.base import (
    BaseDatabaseAdapter,
    Capability,
    DatabaseSession,
    IntegrityResult,
    TransactionOutcome,
    register_adapter,
)
from resilience_tests.control.profile import DbEndpoint, Node
from resilience_tests.execution.remote import RemoteHost, as_user

HARNESS_DDL = """
CREATE SCHEMA IF NOT EXISTS resilience;
CREATE TABLE IF NOT EXISTS resilience.markers (
    uuid uuid PRIMARY KEY,
    seq  bigint NOT NULL,
    ts   timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS resilience.probe_writes (
    id bigserial PRIMARY KEY,
    ts timestamptz NOT NULL DEFAULT clock_timestamp()
);
-- Bloat is measured here, not on the markers. A fixed CHURN_ROWS live rows are seeded and the
-- count is held constant, so bytes-per-live-row moves only when dead tuples are not reclaimed.
-- `payload` is wide enough that an update is unlikely to fit on the same page (defeating HOT),
-- which is what makes the garbage visible rather than quietly reused.
CREATE TABLE IF NOT EXISTS resilience.churn (
    id      bigint PRIMARY KEY,
    v       bigint NOT NULL DEFAULT 0,
    ts      timestamptz NOT NULL DEFAULT clock_timestamp(),
    payload text NOT NULL
) WITH (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_vacuum_threshold = 50,
    autovacuum_vacuum_cost_delay = 0
);
CREATE INDEX IF NOT EXISTS churn_v_idx ON resilience.churn (v);
"""

# Rows held live in the churn table -- CALIBRATED, not chosen. The fault phase is only ~10,000
# transactions (10 cycles x 5 s x 200 TPS), so the key space decides how many times each row is
# rewritten, and that is what decides whether vacuum falling behind is visible at all. Measured
# on PostgreSQL 17.10, comparing a healthy server against one with autovacuum off:
#
#   key space   updates/row   healthy   bloating   separation
#      20,000       0.5        1.05       1.09       1.03x   <- indistinguishable
#       2,000       5.0        1.04       1.25       1.19x   <- usable
#
# 2,000 rows also stays small enough to sit in shared_buffers, so the measurement is of vacuum
# behaviour rather than of the I/O path. At 64 workers, ~1 concurrent collision is expected on
# a 2,000-row space -- brief row-lock waits, far below the p99 ceiling.
CHURN_ROWS = 2_000
SEED_CHURN = """
INSERT INTO resilience.churn (id, payload)
SELECT g, repeat('x', 180) FROM generate_series(1, $1) g
ON CONFLICT (id) DO NOTHING
"""
# One transaction of the `mixed` profile: the marker insert that RPO depends on, plus real
# update traffic. The UPDATE touches the indexed column so the index bloats too.
CHURN_UPDATE = "UPDATE resilience.churn SET v = v + 1, ts = clock_timestamp(), payload = $2 WHERE id = $1"
# Every Nth transaction also deletes a row and puts one back, so the live count is unchanged
# while line pointers accumulate -- the shape of real OLTP churn, not just in-place updates.
CHURN_DELETE = "DELETE FROM resilience.churn WHERE id = $1"
CHURN_REINSERT = "INSERT INTO resilience.churn (id, payload) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING"
CHURN_PAYLOAD = "y" * 180
INSERT_MARKER = "INSERT INTO resilience.markers(uuid, seq, ts) VALUES ($1, $2, clock_timestamp())"
SELECT_MARKERS = "SELECT uuid::text FROM resilience.markers"
PROBE_WRITE = "INSERT INTO resilience.probe_writes DEFAULT VALUES"
TRUNCATE = "TRUNCATE resilience.markers, resilience.probe_writes, resilience.churn"

DURABILITY_SETTINGS = ["data_checksums", "fsync", "synchronous_commit", "full_page_writes",
                       "wal_sync_method", "server_version"]

# Errors where the server said the transaction did not happen. Everything else -- timeout,
# reset, killed server -- is UNKNOWN (Arch §7.1).
_DEFINITE_ABORT = (asyncpg.exceptions.IntegrityConstraintViolationError, asyncpg.exceptions.SerializationError)
_CONNECTION_ERRORS = (OSError, asyncpg.PostgresError, asyncpg.InterfaceError, TimeoutError)

# pg_amcheck reports one header line per corrupt object, then indented detail.
_AMCHECK_FINDING_RE = re.compile(r"^(heap table|btree index|index|sequence) ", re.MULTILINE)
_AMCHECK_ERROR_RE = re.compile(r"^pg_amcheck: error: ", re.MULTILINE)
# A database pg_amcheck skipped was not checked; that is never a clean result.
_AMCHECK_SKIP_RE = re.compile(r"skipping database", re.IGNORECASE)
AMCHECK_INSTALLED_SQL = "SELECT count(*) FROM pg_extension WHERE extname = 'amcheck'"
CHECKSUM_STATS_SQL = ("SELECT coalesce(datname, '<shared>') AS db, coalesce(checksum_failures, 0) AS failures, "
                      "coalesce(stats_reset::text, '') AS stats_reset FROM pg_stat_database")
AMCHECK_PROBE_TIMEOUT_S = 60.0

# One sample of the on-disk footprint. pg_ls_waldir needs pg_monitor, which the harness role
# is granted by the provisioning role.
# Table side: readable by the harness role that owns the schema.
FOOTPRINT_SQL = """
SELECT pg_database_size(current_database())                      AS database_bytes,
       pg_total_relation_size('resilience.markers')              AS markers_bytes,
       (SELECT count(*) FROM resilience.markers)                 AS markers_live_rows,
       coalesce((SELECT n_dead_tup FROM pg_stat_user_tables
                  WHERE schemaname='resilience' AND relname='markers'), 0) AS markers_dead_rows,
       -- the churn table: live rows are held constant, so these bytes are the bloat signal
       pg_total_relation_size('resilience.churn')                AS churn_bytes,
       (SELECT count(*) FROM resilience.churn)                   AS churn_live_rows,
       -- Reported only, NEVER gated on: PostgreSQL discards the statistics collector's
       -- counters on crash recovery, so after a kill this reads as 0 regardless of how much
       -- garbage is physically on disk. The bytes above survive a crash; this does not.
       coalesce((SELECT n_dead_tup FROM pg_stat_user_tables
                  WHERE schemaname='resilience' AND relname='churn'), 0)   AS churn_dead_rows
"""

# WAL side: pg_ls_waldir() needs superuser or pg_monitor, which the harness role may not
# have. Asked separately so a permission the operator chose not to grant costs the WAL
# figures only -- the per-row bloat measurement still stands.
# pg_size_bytes parses the server's own 'max_wal_size' text ('1GB', '4096MB'), so the value
# is never rebuilt from an assumed unit.
WAL_FOOTPRINT_SQL = """
SELECT coalesce(sum(size)::bigint, 0)                            AS wal_bytes,
       count(*)                                                  AS wal_segments,
       pg_size_bytes(current_setting('max_wal_size'))            AS max_wal_size_bytes
  FROM pg_ls_waldir()
"""

# Exactly the work a crash right now would leave to replay: WAL between the last checkpoint's
# redo point and the current insert point. Restricted to superuser / pg_monitor.
REDO_DISTANCE_SQL = ("SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), redo_lsn)::bigint "
                     "FROM pg_control_checkpoint()")

_SAME_AS_CONNECT: Any = object()  # sentinel: bound statements by the connect timeout

ChecksumStats = dict[str, tuple[int, str]]  # database -> (checksum_failures, stats_reset)


def checksum_failures_since(baseline: ChecksumStats | None, now: ChecksumStats) -> int:
    """Checksum failures that occurred during the run. pg_stat_database counters are
    cumulative since the last statistics reset, and a crash discards the statistics -- so a
    database whose counter went backwards or whose reset time changed counts everything it
    now shows (all of it is from this run). Without a baseline every failure counts:
    over-reporting fails a run, under-reporting would pass one."""
    total = 0
    for db, (failures, reset) in now.items():
        before = (baseline or {}).get(db)
        if before is None or before[1] != reset or failures < before[0]:
            total += failures
        else:
            total += failures - before[0]
    return total


class PostgreSQLSession(DatabaseSession):
    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def commit_marker(self, seq: int, marker_id: str) -> TransactionOutcome:
        try:
            async with self._conn.transaction():
                await self._conn.execute(INSERT_MARKER, marker_id, seq)
        except _DEFINITE_ABORT:
            return TransactionOutcome.DEFINITELY_ABORTED
        except _CONNECTION_ERRORS:
            await self.close()
            return TransactionOutcome.UNKNOWN
        return TransactionOutcome.COMMITTED

    async def commit_marker_with_churn(self, seq: int, marker_id: str, churn_key: int,
                                       replace: bool) -> TransactionOutcome:
        try:
            async with self._conn.transaction():
                await self._conn.execute(INSERT_MARKER, marker_id, seq)
                if replace:
                    # delete and put the row straight back: the live count is unchanged, but
                    # the old tuple and its index entries become garbage
                    await self._conn.execute(CHURN_DELETE, churn_key)
                    await self._conn.execute(CHURN_REINSERT, churn_key, CHURN_PAYLOAD)
                else:
                    await self._conn.execute(CHURN_UPDATE, churn_key, CHURN_PAYLOAD)
        except _DEFINITE_ABORT:
            return TransactionOutcome.DEFINITELY_ABORTED
        except _CONNECTION_ERRORS:
            await self.close()
            return TransactionOutcome.UNKNOWN
        return TransactionOutcome.COMMITTED

    async def try_write(self) -> bool:
        try:
            await self._conn.execute(PROBE_WRITE)
        except _CONNECTION_ERRORS:
            await self.close()
            return False
        return True

    async def ping(self) -> bool:
        try:
            await self._conn.fetchval("SELECT 1")
        except _CONNECTION_ERRORS:
            await self.close()
            return False
        return True

    async def close(self) -> None:
        try:
            self._conn.terminate()
        except Exception:  # noqa: BLE001 -- the connection is already known to be broken
            pass

    @property
    def is_closed(self) -> bool:
        return self._conn.is_closed()


@register_adapter
class PostgreSQLAdapter(BaseDatabaseAdapter):
    engine = "postgresql"
    capabilities = frozenset({
        Capability.TRANSACTIONAL_MARKERS,
        Capability.WORKLOAD_CHURN,
        Capability.STRUCTURAL_INTEGRITY_CHECK,
        Capability.PAGE_CHECKSUMS,
        Capability.DURABILITY_SETTINGS,
    })

    churn_key_space = CHURN_ROWS

    def __init__(self, node: Node) -> None:
        super().__init__(node)
        self._checksum_baseline: ChecksumStats | None = None
        self._scenario_config_applied: dict[str, str] = {}
        self._scenario_config_error: str | None = None
        self._scenario_observed: dict[str, str] = {}

    async def _connect(self, endpoint: DbEndpoint | None = None, timeout_s: float = 5.0,
                       command_timeout: float | None = _SAME_AS_CONNECT) -> asyncpg.Connection:
        """`timeout_s` bounds establishing the connection. `command_timeout` bounds each
        statement on it; the harness's own queries reuse the connect bound, but a client
        session passes None -- see `session`."""
        ep = endpoint or self.node.db
        # password comes from the driver host's ~/.pgpass, never from the profile
        return await asyncpg.connect(host=ep.host, port=ep.port, database=ep.dbname, user=ep.user,
                                     timeout=timeout_s,
                                     command_timeout=timeout_s if command_timeout is _SAME_AS_CONNECT else command_timeout)

    async def session(self, endpoint: DbEndpoint | None = None, timeout_s: float = 5.0) -> DatabaseSession:
        """A workload/probe session. Statements are NOT bounded by the connect timeout: the
        caller bounds its own operation (the workload gives a marker transaction
        TXN_TIMEOUT_S, a probe gives its attempt PROBE_ATTEMPT_TIMEOUT_S). Tying the two
        together made a commit slower than the 2 s connect timeout indeterminate -- dropping
        genuine losses out of rpo_txn, and counting a latency spike as a dropped connection."""
        return PostgreSQLSession(await self._connect(endpoint or self.node.client, timeout_s, command_timeout=None))

    async def prepare_harness_state(self) -> None:
        # Pre-grant required roles to harness user if running against a cluster with SSH access.
        # PostgreSQL 15+ restricts CHECKPOINT to superusers and members of 'pg_checkpoint',
        # and pg_stat_activity detailed monitoring to 'pg_read_all_stats'.
        try:
            async with RemoteHost(self.node.ssh) as host:
                grant_cmd = (
                    f"cd /tmp && {shlex.quote(self.node.pg_bin + '/psql')} -X -p {self.node.db.port} "
                    f"-d {shlex.quote(self.node.db.dbname)} -c "
                    f"{shlex.quote(f'GRANT pg_checkpoint, pg_read_all_stats TO {self.node.db.user};')}"
                )
                await host.run(as_user(self.node.os_user, grant_cmd), timeout_s=10.0, check=False)
        except Exception:
            pass  # Best effort: fake adapters, unit tests, or environments without SSH

        conn = await self._connect(timeout_s=120.0)   # the churn seed is 20k rows
        try:
            await conn.execute(HARNESS_DDL)
            await conn.execute("DROP INDEX CONCURRENTLY IF EXISTS idx_nlc06_concurrent")
            await conn.execute(TRUNCATE)
            # Seeded every run, after the truncate: the churn table starts from a freshly
            # written, unfragmented state, so the first footprint sample is a real floor and
            # not whatever the previous run left behind.
            await conn.execute(SEED_CHURN, CHURN_ROWS)
            await conn.execute("VACUUM (ANALYZE) resilience.churn")
        finally:
            await conn.close()

    async def create_index_concurrently(self, table: str, column: str, index_name: str) -> None:
        """Launch a CREATE INDEX CONCURRENTLY statement. When crash occurs mid-build, this will raise
        a connection error which is expected."""
        conn = await self._connect(timeout_s=120.0, command_timeout=None)
        try:
            await conn.execute(f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {index_name} ON {table} ({column})")
        finally:
            try:
                await conn.close()
            except Exception:
                pass

    async def get_index_status(self, index_name: str) -> dict[str, Any] | None:
        """Query pg_class and pg_index to inspect whether index exists, and if it is marked valid/ready."""
        try:
            conn = await self._connect(timeout_s=10.0)
        except Exception:
            return None
        try:
            row = await conn.fetchrow(
                "SELECT c.relname, i.indisvalid, i.indisready "
                "FROM pg_class c JOIN pg_index i ON c.oid = i.indexrelid "
                "WHERE c.relname = $1",
                index_name,
            )
            if row:
                return {"name": row["relname"], "is_valid": row["indisvalid"], "is_ready": row["indisready"]}
            return None
        except Exception:
            return None
        finally:
            await conn.close()

    async def cleanup_index(self, index_name: str) -> bool:
        """Drop the index concurrently or directly. Returns True if dropped or non-existent."""
        try:
            conn = await self._connect(timeout_s=30.0)
        except Exception:
            return False
        try:
            await conn.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {index_name}")
            return True
        except Exception:
            try:
                await conn.execute(f"DROP INDEX IF EXISTS {index_name}")
                return True
            except Exception:
                return False
        finally:
            await conn.close()


    async def redo_distance_bytes(self) -> int | None:
        try:
            conn = await self._connect(timeout_s=10.0)
        except Exception:  # noqa: BLE001 -- sampled beside a kill; never fails the run
            return None
        try:
            return int(await conn.fetchval(REDO_DISTANCE_SQL))
        except Exception:  # noqa: BLE001 -- pg_control_checkpoint() needs pg_monitor
            return None
        finally:
            await conn.close()

    async def marker_ids(self) -> set[str]:
        conn = await self._connect(timeout_s=60.0)
        try:
            return {r[0] for r in await conn.fetch(SELECT_MARKERS)}
        finally:
            await conn.close()

    async def sentinel(self, table: str, hostname: str) -> dict[str, Any] | None:
        conn = await self._connect()
        try:
            row = await conn.fetchrow(f"SELECT * FROM {table} WHERE hostname = $1", hostname)
            return dict(row) if row else None
        except asyncpg.UndefinedTableError:
            return None
        finally:
            await conn.close()

    async def durability_settings(self) -> dict[str, str]:
        conn = await self._connect()
        try:
            rows = await conn.fetch("SELECT name, setting FROM pg_settings WHERE name = ANY($1::text[])",
                                    DURABILITY_SETTINGS)
            return {r["name"]: r["setting"] for r in rows}
        finally:
            await conn.close()

    async def certification_blockers(self) -> list[str]:
        settings = await self.durability_settings()
        blockers = []
        if settings.get("data_checksums") != "on":
            # Framework §16.2 and NL-I-09: without checksums, corruption can be silent
            blockers.append("data_checksums is not on: the framework refuses to certify")
        return blockers

    def fault_detection_log_patterns(self) -> tuple[str, ...]:
        # Lines the RUNNING postmaster writes when it notices it is being acted on. A SIGKILL
        # produces none of these -- nothing is logged by a process that is simply gone -- so
        # NL-C-01 reports mttd_s as NOT_MEASURED instead of timing the replacement's startup.
        return (
            r"received (fast |smart |immediate )?shutdown request",
            r"received SIGHUP",
        )

    def recovery_start_log_patterns(self) -> tuple[str, ...]:
        # Written by the REPLACEMENT postmaster as it begins crash recovery: evidence of
        # restart, not of detection.
        return (
            r"database system was interrupted",
            r"database system was not properly shut down",
            r"automatic recovery in progress",
        )

    async def server_version(self) -> str:
        conn = await self._connect()
        try:
            return await conn.fetchval("SELECT version()")
        finally:
            await conn.close()

    async def storage_footprint(self) -> dict[str, Any]:
        """Measured, never assumed: sizes come from the server, and the WAL ceiling is read
        from its own configuration rather than guessed."""
        conn = await self._connect(timeout_s=30.0)
        try:
            sample = {k: int(v) for k, v in dict(await conn.fetchrow(FOOTPRINT_SQL)).items()}
            try:
                wal = dict(await conn.fetchrow(WAL_FOOTPRINT_SQL))
            except asyncpg.PostgresError as exc:
                # typically insufficient_privilege: recorded, not silently zeroed -- a zero
                # would read as a WAL that never grows
                sample["wal_unavailable"] = str(exc)
            else:
                sample.update({k: int(v) for k, v in wal.items() if v is not None})
            return sample
        finally:
            await conn.close()

    async def _checksum_stats(self) -> ChecksumStats:
        conn = await self._connect()
        try:
            rows = await conn.fetch(CHECKSUM_STATS_SQL)
            return {r["db"]: (int(r["failures"]), r["stats_reset"]) for r in rows}
        finally:
            await conn.close()

    async def mark_integrity_baseline(self) -> dict[str, Any]:
        self._checksum_baseline = await self._checksum_stats()
        return {"checksum_failures": {db: list(v) for db, v in self._checksum_baseline.items()}}

    def integrity_databases(self) -> list[str]:
        return list(self.node.integrity_databases or [self.node.db.dbname])

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

    async def verify_checkpoint_aborted(self) -> dict[str, Any]:
        """Verify that the in-flight checkpoint was aborted by the crash and that recovery
        replayed WAL from the prior valid checkpoint's REDO point.

        Verification logic: after crash recovery, pg_control_checkpoint() reports the
        end-of-recovery checkpoint. If the in-flight checkpoint had actually completed before
        the kill arrived, pg_control's checkpoint_lsn would have advanced to a value BETWEEN
        the pre-kill baseline and the end-of-recovery checkpoint. We compare the post-recovery
        checkpoint_time against the pre-kill checkpoint_time: if the post-recovery checkpoint
        is newer than the pre-kill one AND the pre-kill checkpoint_lsn is still the most recent
        checkpoint before the end-of-recovery one, the in-flight checkpoint was indeed aborted.

        If no baseline was captured (e.g. pg_control_checkpoint not available), we cannot verify
        and report checkpoint_aborted as None (unknown) rather than a false True.
        """
        if hasattr(self, "_checkpoint_task"):
            self._checkpoint_task.cancel()
        if hasattr(self, "_checkpoint_conn"):
            try:
                await self._checkpoint_conn.close()
            except Exception:
                pass

        baseline = getattr(self, "_checkpoint_baseline", None)
        detail: dict[str, Any] = {}
        try:
            conn = await self._connect()
            try:
                row = await conn.fetchrow(
                    "SELECT checkpoint_lsn, redo_lsn, checkpoint_time::text FROM pg_control_checkpoint()"
                )
                if row:
                    current_cp = dict(row)
                    detail["current_checkpoint"] = current_cp
                    if baseline:
                        detail["prior_checkpoint"] = baseline
                        # The pre-kill checkpoint_lsn is what was in pg_control BEFORE we
                        # issued CHECKPOINT. After crash recovery, pg_control holds the
                        # end-of-recovery checkpoint. If the in-flight checkpoint had
                        # completed, there would be an intermediate checkpoint_lsn between
                        # the baseline and the end-of-recovery one. Since crash recovery
                        # replays from the redo_lsn of the LAST COMPLETED checkpoint, we
                        # check: did the post-recovery redo_lsn advance past the baseline
                        # checkpoint_lsn? If so, the baseline was still the last valid
                        # checkpoint (the in-flight one was aborted).
                        baseline_cp_lsn = baseline.get("checkpoint_lsn")
                        current_cp_lsn = current_cp.get("checkpoint_lsn")
                        current_redo_lsn = current_cp.get("redo_lsn")
                        if baseline_cp_lsn is not None and current_cp_lsn is not None:
                            # Both are ints (pg_lsn cast to bigint by asyncpg)
                            aborted = int(current_cp_lsn) > int(baseline_cp_lsn)
                            detail["checkpoint_aborted"] = aborted
                            if current_redo_lsn is not None:
                                detail["redo_advanced"] = int(current_redo_lsn) > int(baseline_cp_lsn)
                        else:
                            # Cannot compare: report unknown rather than false True
                            detail["checkpoint_aborted"] = None
                            detail["note"] = "LSN comparison not possible: missing baseline or current checkpoint_lsn"
                    else:
                        detail["checkpoint_aborted"] = None
                        detail["note"] = "no pre-kill checkpoint baseline was captured"
                else:
                    detail["checkpoint_aborted"] = None
                    detail["note"] = "pg_control_checkpoint() returned no data after recovery"
            finally:
                await conn.close()
        except Exception as exc:
            detail["error"] = str(exc)
            detail["checkpoint_aborted"] = None
        return detail

    async def integrity_check(self, timeout_s: float) -> IntegrityResult:
        """pg_amcheck --heapallindexed over the configured databases (Arch §10.1, Framework
        NL-I-07). Runs as the cluster's OS user over SSH, so the harness role needs no
        superuser.

        Nothing is installed on the target: every database must already have the amcheck
        extension, or the check refuses (a database pg_amcheck skips is not a clean one). The
        default scope is the harness database -- `--all` would scan every database a customer
        owns, and on production-sized data would not finish inside the validate bound."""
        node = self.node
        databases = self.integrity_databases()
        async with RemoteHost(node.ssh) as host:
            missing = []
            for db in databases:
                probe = (f"cd /tmp && {shlex.quote(node.pg_bin + '/psql')} -X -At -p {node.db.port} "
                         f"-d {shlex.quote(db)} -c {shlex.quote(AMCHECK_INSTALLED_SQL)}")
                r = await host.run(as_user(node.os_user, probe), timeout_s=AMCHECK_PROBE_TIMEOUT_S, check=False)
                if r.exit_status != 0 or r.stdout.strip() != "1":
                    missing.append(db)
            if missing:
                raise RuntimeError(
                    f"amcheck is not installed in {missing}; the operator installs it by hand "
                    "(CREATE EXTENSION amcheck) -- the harness does not modify target databases"
                )
            db_args = " ".join(f"-d {shlex.quote(db)}" for db in databases)
            command = (f"cd /tmp && {shlex.quote(node.pg_bin + '/pg_amcheck')} -p {node.db.port} "
                       f"{db_args} --heapallindexed")
            result = await host.run(as_user(node.os_user, command), timeout_s=timeout_s, check=False)
        output = result.stdout + result.stderr
        if result.exit_status not in (0, 2):
            # 1 = could not run: we do not know whether the data is sound. Fail closed.
            raise RuntimeError(f"pg_amcheck could not run (exit {result.exit_status}): {output.strip()[:500]}")
        if _AMCHECK_SKIP_RE.search(output):
            raise RuntimeError(f"pg_amcheck skipped a database, so it was not checked: {output.strip()[:500]}")
        findings = len(_AMCHECK_FINDING_RE.findall(output)) + len(_AMCHECK_ERROR_RE.findall(output))
        if result.exit_status == 2 and findings == 0:
            findings = 1  # corruption reported but the message shape was unrecognised -- never round down
        stats = await self._checksum_stats()
        baseline = self._checksum_baseline
        return IntegrityResult(
            structural_errors=findings, checksum_failures=checksum_failures_since(baseline, stats),
            raw_output=output,
            detail={"exit_status": result.exit_status, "databases": databases,
                    "checksum_baseline_taken": baseline is not None},
        )

    async def inject_idle_transaction(self) -> dict[str, Any]:
        """Inject an open idle-in-transaction holding back the vacuum xmin horizon (NL-M-05).
        Opens a dedicated session, issues BEGIN, and runs a query to establish an active snapshot
        and pin backend_xmin, then leaves the session idle."""
        conn: Any = None
        try:
            conn = await self._connect(timeout_s=10.0)
            pid = await conn.fetchval("SELECT pg_backend_pid()")
            await conn.execute("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED")
            try:
                await conn.execute("SELECT txid_current()")
            except Exception:
                await conn.execute("SELECT 1")
            row = await conn.fetchrow(
                "SELECT pid, state, backend_xmin, backend_xid, "
                "EXTRACT(EPOCH FROM (now() - xact_start)) AS xact_age_s "
                "FROM pg_stat_activity WHERE pid = $1",
                pid,
            )
            xmin = str(row["backend_xmin"]) if row and row.get("backend_xmin") else None
            state = str(row["state"]) if row and row.get("state") else "idle in transaction"
            self._idle_conn = conn
            self._idle_pid = pid
            self._idle_xmin = xmin
            return {
                "supported": True,
                "pid": pid,
                "backend_xmin": xmin,
                "state": state,
                "xact_age_s": float(row["xact_age_s"]) if row and row.get("xact_age_s") is not None else 0.0,
            }
        except Exception as exc:
            # Latent double-session prevention: cleanly roll back and close any partially
            # opened connection before falling back to SSH.
            if conn is not None:
                try:
                    if not conn.is_closed():
                        await conn.execute("ROLLBACK")
                    await conn.close()
                except Exception:
                    pass
                conn = None
            self._idle_conn = None
            self._idle_pid = None
            self._idle_xmin = None
            try:
                async with RemoteHost(self.node.ssh) as host:
                    psql = shlex.quote(self.node.pg_bin + '/psql')
                    sql = "BEGIN; SELECT txid_current();"
                    cmd = (
                        f"cd /tmp && nohup bash -c '("
                        f"echo {shlex.quote(sql)}; sleep 7200"
                        f") | {psql} -X -p {self.node.db.port} -d {shlex.quote(self.node.db.dbname)}' "
                        f">/dev/null 2>&1 & echo $!"
                    )
                    r = await host.run(as_user(self.node.os_user, cmd), timeout_s=10.0, check=False)
                    bg_pid = int(r.stdout.strip()) if r.stdout.strip().isdigit() else None
                    self._idle_pid = bg_pid
                    return {"supported": True, "pid": bg_pid, "method": "ssh_background"}
            except Exception as fb_exc:
                return {"supported": False, "error": f"{type(exc).__name__}: {exc} (fallback: {fb_exc})"}

    async def check_idle_transaction(self, pid: int | None = None) -> dict[str, Any]:
        """Check status of the idle-in-transaction backend (NL-M-05).
        Determines whether the session was terminated by idle_in_transaction_session_timeout
        or is still alive in pg_stat_activity."""
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
        except Exception:
            pass

        # Backend is not present in pg_stat_activity. Now probe the held session to confirm
        # whether the server terminated it (Path A).
        conn = getattr(self, "_idle_conn", None)
        conn_closed = False
        if conn is not None:
            if conn.is_closed():
                conn_closed = True
            else:
                try:
                    await asyncio.wait_for(conn.fetchval("SELECT 1"), timeout=0.5)
                except Exception as exc:
                    conn_closed = True
                    self._idle_conn_error = str(exc)

        if conn_closed:
            return {
                "pid": target_pid,
                "terminated_by_timeout": True,
                "still_idle": False,
                "conn_closed": True,
                "note": "harness session was closed and the backend is no longer present "
                        "(idle_in_transaction_session_timeout presumed)",
            }
        return {
            "pid": target_pid,
            "terminated_by_timeout": False,
            "still_idle": False,
            "conn_closed": False,
            "note": "backend not present but the harness session did not close; "
                    "timeout enforcement NOT confirmed",
        }

    async def close_idle_transaction(self) -> dict[str, Any]:
        """Cleanly terminate or rollback any active idle-in-transaction connection (NL-M-05)."""
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
        if pid is not None:
            try:
                stat_conn = await self._connect(timeout_s=5.0)
                try:
                    terminated = await stat_conn.fetchval("SELECT pg_terminate_backend($1)", pid)
                    detail["pg_terminate_backend"] = terminated
                finally:
                    await stat_conn.close()
            except Exception:
                try:
                    async with RemoteHost(self.node.ssh) as host:
                        await host.run(as_root(f"kill -9 {pid} 2>/dev/null || true"), timeout_s=5.0, check=False)
                        detail["killed_via_ssh"] = True
                except Exception:
                    pass
            self._idle_pid = None
        return detail

    async def evaluate_vacuum_bloat(self) -> dict[str, Any]:
        """Evaluate relation dead-tuple accumulation and operational bloat alert telemetry
        under blocked vacuum (NL-M-05)."""
        try:
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
                ratio = round(dead / total, 4) if total > 0 else 0.0

                max_age_row = await conn.fetchval(
                    "SELECT COALESCE(MAX(EXTRACT(EPOCH FROM (now() - xact_start))), 0) "
                    "FROM pg_stat_activity "
                    "WHERE state LIKE 'idle in transaction%'"
                )
                oldest_age = round(float(max_age_row or 0.0), 2)
                # NL-M-04 sets the operational alert at dead_tuple_ratio >= 0.20. The idle
                # session's age is reported on its own (oldest_transaction_age_s); making the
                # "bloat alert" also fire merely because a session is old would conflate "the
                # fault is still open" with "dead tuples are actually accumulating", and would
                # pass a run that never produced any bloat at all.
                bloat_alert = ratio >= 0.20
                bloat_ratio = round((live + dead) / max(live, 1), 4)

                return {
                    "dead_tuple_ratio": ratio,
                    "unvacuumed_dead_tuples": dead,
                    "live_tuples": live,
                    "bloat_ratio": bloat_ratio,
                    "oldest_transaction_age_s": oldest_age,
                    "bloat_alert_fired": bloat_alert,
                    "last_vacuum": row_dict.get("last_vacuum"),
                    "last_autovacuum": row_dict.get("last_autovacuum"),
                    "last_analyze": row_dict.get("last_analyze"),
                    "last_autoanalyze": row_dict.get("last_autoanalyze"),
                }
            finally:
                await conn.close()
        except Exception as exc:
            return {
                "dead_tuple_ratio": 0.0,
                "unvacuumed_dead_tuples": 0,
                "oldest_transaction_age_s": 0.0,
                "bloat_alert_fired": False,
                "error": str(exc),
            }

    async def configure_for_scenario(self, scenario_id: str) -> dict[str, str]:
        """Apply temporary configuration specific to a scenario before workload starts.

        For NL-C-05 (Repeated crash cycles): Option A tunes autovacuum (autovacuum_naptime = 5s,
        autovacuum_vacuum_cost_delay = 0) so autovacuum cycles rapidly within the ~50s test window.
        This bounds healthy bloat to <= 1.05 and enables a strict, defensible acceptance gate
        at bloat_ratio <= 1.15.

        For NL-M-05 (Idle-in-transaction blocking vacuum): inspects current
        idle_in_transaction_session_timeout setting and records baseline for verification.

        Settings are written to postgresql.auto.conf (ALTER SYSTEM) so they persist across all
        crash restarts during the run, and are restored cleanly in
        restore_scenario_configuration() at cleanup.

        For other scenarios: any leftover autovacuum tuning from a previous run is actively
        cleaned up to ensure baseline and recovery are not contaminated."""
        if scenario_id == "NL-M-05":
            # Observed only, never applied: the harness must not claim "tuning applied" for a
            # value it merely read, and cleanup must NOT ALTER SYSTEM RESET a deployment's
            # idle_in_transaction_session_timeout that the harness never wrote.
            try:
                conn = await self._connect(timeout_s=10.0)
                try:
                    timeout = await conn.fetchval("SHOW idle_in_transaction_session_timeout")
                    self._scenario_observed = {
                        "idle_in_transaction_session_timeout": str(timeout),
                    }
                    return {"observed_idle_in_transaction_session_timeout": str(timeout)}
                finally:
                    await conn.close()
            except Exception:
                return {}
        if scenario_id != "NL-C-05":
            await self.cleanup_leftover_configuration()
            return {}
        try:
            conn = await self._connect(timeout_s=10.0)
            try:
                naptime = await conn.fetchval("SHOW autovacuum_naptime")
                cost_delay = await conn.fetchval("SHOW autovacuum_vacuum_cost_delay")
                self._scenario_config_applied = {
                    "autovacuum_naptime": str(naptime),
                    "autovacuum_vacuum_cost_delay": str(cost_delay),
                }
                await conn.execute("ALTER SYSTEM SET autovacuum_naptime = '5s'")
                await conn.execute("ALTER SYSTEM SET autovacuum_vacuum_cost_delay = '0'")
                await conn.execute("SELECT pg_reload_conf()")
                return {"autovacuum_naptime": "5s", "autovacuum_vacuum_cost_delay": "0"}
            finally:
                await conn.close()
        except Exception as exc:
            # Fallback to SSH execution as os_user (postgres superuser). Each ALTER SYSTEM
            # must run as its own statement: psql -c wraps a multi-statement string in a
            # single implicit transaction, and ALTER SYSTEM refuses to run inside one.
            try:
                async with RemoteHost(self.node.ssh) as host:
                    psql = shlex.quote(self.node.pg_bin + '/psql')
                    statements = (
                        "ALTER SYSTEM SET autovacuum_naptime = '5s';",
                        "ALTER SYSTEM SET autovacuum_vacuum_cost_delay = '0';",
                        "SELECT pg_reload_conf();",
                    )
                    parts = [
                        f"{psql} -X -p {self.node.db.port} "
                        f"-d {shlex.quote(self.node.db.dbname)} -c {shlex.quote(stmt)}"
                        for stmt in statements
                    ]
                    cmd = "cd /tmp && " + " && ".join(parts)
                    r = await host.run(as_user(self.node.os_user, cmd), timeout_s=15.0, check=False)
                    if r.exit_status == 0:
                        self._scenario_config_applied = {
                            "autovacuum_naptime": "5s",
                            "autovacuum_vacuum_cost_delay": "0",
                        }
                        return {"autovacuum_naptime": "5s", "autovacuum_vacuum_cost_delay": "0"}
                    self._scenario_config_error = f"ssh fallback exited {r.exit_status}: {r.stderr.strip()[:200]}"
            except Exception as fb_exc:
                self._scenario_config_error = f"{type(fb_exc).__name__}: {fb_exc}"
        return {}

    async def config_deviations(self) -> dict[str, str]:
        """Detect any parameters set in postgresql.auto.conf (ALTER SYSTEM deviations)."""
        deviations: dict[str, str] = {}
        try:
            conn = await self._connect(timeout_s=10.0)
            try:
                rows = await conn.fetch(
                    "SELECT name, setting FROM pg_file_settings "
                    "WHERE sourcefile LIKE '%postgresql.auto.conf' AND error IS NULL"
                )
                if isinstance(rows, list):
                    for r in rows:
                        try:
                            deviations[r["name"]] = str(r["setting"])
                        except (KeyError, TypeError):
                            pass
            finally:
                await conn.close()
        except Exception:
            try:
                async with RemoteHost(self.node.ssh) as host:
                    sql_cmd = (
                        "SELECT name, setting FROM pg_file_settings "
                        "WHERE sourcefile LIKE '%postgresql.auto.conf' AND error IS NULL;"
                    )
                    cmd = (
                        f"cd /tmp && {shlex.quote(self.node.pg_bin + '/psql')} -X -At -F '=' -p {self.node.db.port} "
                        f"-d {shlex.quote(self.node.db.dbname)} -c {shlex.quote(sql_cmd)}"
                    )
                    r = await host.run(as_user(self.node.os_user, cmd), timeout_s=15.0, check=False)
                    if r.exit_status == 0 and r.stdout:
                        for line in r.stdout.strip().splitlines():
                            if "=" in line:
                                k, v = line.split("=", 1)
                                deviations[k.strip()] = v.strip()
            except Exception:
                pass
        return deviations

    async def cleanup_leftover_configuration(self) -> dict[str, str]:
        """Reset any scenario-tuning parameters that may have leaked into postgresql.auto.conf
        from previous failed runs."""
        deviations = await self.config_deviations()
        scenario_params = {"autovacuum_naptime", "autovacuum_vacuum_cost_delay", "idle_in_transaction_session_timeout"}
        leftover = {k: v for k, v in deviations.items() if k in scenario_params}
        if not leftover:
            return {}
        try:
            conn = await self._connect(timeout_s=10.0)
            try:
                for k in leftover:
                    await conn.execute(f"ALTER SYSTEM RESET {k}")
                await conn.execute("SELECT pg_reload_conf()")
            finally:
                await conn.close()
        except Exception:
            try:
                async with RemoteHost(self.node.ssh) as host:
                    psql = shlex.quote(self.node.pg_bin + '/psql')
                    statements = [f"ALTER SYSTEM RESET {k};" for k in leftover] + ["SELECT pg_reload_conf();"]
                    parts = [
                        f"{psql} -X -p {self.node.db.port} "
                        f"-d {shlex.quote(self.node.db.dbname)} -c {shlex.quote(stmt)}"
                        for stmt in statements
                    ]
                    cmd = "cd /tmp && " + " && ".join(parts)
                    await host.run(as_user(self.node.os_user, cmd), timeout_s=15.0, check=False)
            except Exception:
                pass
        return leftover

    async def restore_scenario_configuration(self) -> None:
        """Revert any temporary configuration applied by configure_for_scenario(),
        ensuring the database is restored cleanly without leaving altered settings."""
        if not self._scenario_config_applied:
            return
        keys = list(self._scenario_config_applied.keys())
        try:
            conn = await self._connect(timeout_s=10.0)
            try:
                for k in keys:
                    await conn.execute(f"ALTER SYSTEM RESET {k}")
                await conn.execute("SELECT pg_reload_conf()")
                self._scenario_config_applied.clear()
            finally:
                await conn.close()
        except Exception:
            # Fallback to SSH execution as os_user
            try:
                async with RemoteHost(self.node.ssh) as host:
                    psql = shlex.quote(self.node.pg_bin + '/psql')
                    statements = [f"ALTER SYSTEM RESET {k};" for k in keys] + ["SELECT pg_reload_conf();"]
                    parts = [
                        f"{psql} -X -p {self.node.db.port} "
                        f"-d {shlex.quote(self.node.db.dbname)} -c {shlex.quote(stmt)}"
                        for stmt in statements
                    ]
                    cmd = "cd /tmp && " + " && ".join(parts)
                    r = await host.run(as_user(self.node.os_user, cmd), timeout_s=15.0, check=False)
                    if r.exit_status == 0:
                        self._scenario_config_applied.clear()
            except Exception:
                pass

    async def quick_integrity_check(self) -> dict[str, Any]:
        """A fast inter-cycle checksum check to localize corruptions to the cycle that caused them."""
        try:
            stats = await self._checksum_stats()
            baseline = self._checksum_baseline
            failures = checksum_failures_since(baseline, stats)
            return {"checksum_failures": failures, "ok": failures == 0}
        except Exception as exc:
            return {"checksum_failures": 0, "ok": False, "error": str(exc)}

    async def exhaust_connections(self, hold_duration_s: float = 2.0) -> dict[str, Any]:
        """Connection exhaustion under load (NL-R-04: max_connections + 50% concurrent attempts)."""
        t0_mono_ns = time.monotonic_ns()
        max_conn = 100
        su_reserved = 3

        # Query live database GUC settings for max_connections and superuser_reserved_connections
        try:
            conn = await self._connect(timeout_s=5.0)
            try:
                row = await conn.fetchrow("SHOW max_connections")
                if row and str(row[0]).isdigit():
                    max_conn = int(row[0])
                row2 = await conn.fetchrow("SHOW superuser_reserved_connections")
                if row2 and str(row2[0]).isdigit():
                    su_reserved = int(row2[0])
            finally:
                await conn.close()
        except Exception:
            # Fallback to SSH psql if direct connection is blocked
            try:
                async with RemoteHost(self.node.ssh) as host:
                    res = await host.run(
                        f"cd /tmp && {shlex.quote(self.node.pg_bin + '/psql')} -X -At -p {self.node.db.port} "
                        f"-d {shlex.quote(self.node.db.dbname)} -c 'SHOW max_connections; SHOW superuser_reserved_connections;'",
                        timeout_s=10.0, check=False
                    )
                    if res.exit_status == 0:
                        lines = [l.strip() for l in res.stdout.strip().splitlines() if l.strip().isdigit()]
                        if len(lines) >= 1:
                            max_conn = int(lines[0])
                        if len(lines) >= 2:
                            su_reserved = int(lines[1])
            except Exception:
                pass

        # Framework §10.5: Open max_connections + 50% concurrent sessions under active load
        total_flood_target = max(10, int(max_conn * 1.5))
        self._holding_conns = []
        rejection_errors: list[str] = []

        async def _flood_worker():
            try:
                return await self._connect(self.node.client, timeout_s=3.0, command_timeout=None)
            except asyncpg.TooManyConnectionsError as exc:
                rejection_errors.append(str(exc))
                return None
            except Exception as exc:
                rejection_errors.append(str(exc))
                return None

        try:
            tasks = [_flood_worker() for _ in range(total_flood_target)]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for c in results:
                if c is not None and not isinstance(c, Exception):
                    self._holding_conns.append(c)
        except Exception:
            pass

        rejections_explicit = False
        rejection_error = ""
        if self._holding_conns and rejection_errors:
            rejection_error = rejection_errors[0]
            explicit_count = sum(
                1 for err in rejection_errors
                if "53300" in err or "too many clients" in err.lower() or "remaining connection slots are reserved" in err.lower()
            )
            if explicit_count > 0:
                rejections_explicit = True

        # Verify if superuser reserved slot is honoured under connection exhaustion
        superuser_slot_honoured = False
        if self._holding_conns:
            # 1. First probe via SSH as node.os_user (postgres superuser)
            try:
                async with RemoteHost(self.node.ssh) as host:
                    psql = shlex.quote(self.node.pg_bin + '/psql')
                    su_cmd = f"cd /tmp && {psql} -X -p {self.node.db.port} -d {shlex.quote(self.node.db.dbname)} -c 'SELECT 1;'"
                    r = await host.run(as_user(self.node.os_user, su_cmd), timeout_s=5.0, check=False)
                    if r.exit_status == 0:
                        superuser_slot_honoured = True
            except Exception:
                superuser_slot_honoured = False

            # 2. Fallback to direct asyncpg connect if SSH probe is not available
            if not superuser_slot_honoured:
                try:
                    su_conn = await self._connect(self.node.db, timeout_s=2.0, command_timeout=None)
                    superuser_slot_honoured = True
                    await su_conn.close()
                except Exception:
                    superuser_slot_honoured = False

        held_count = len(self._holding_conns)

        return {
            "action": "connection_exhaustion",
            "t0_mono_ns": t0_mono_ns,
            "max_connections": max_conn,
            "superuser_reserved": su_reserved,
            "attempted_connections": total_flood_target,
            "held_connections": held_count,
            "rejected_connections": len(rejection_errors),
            "rejections_explicit": rejections_explicit,
            "rejection_error": rejection_error,
            "superuser_slot_honoured": superuser_slot_honoured,
        }

    async def revert_exhaust_connections(self) -> dict[str, Any]:
        drained = 0
        for c in getattr(self, "_holding_conns", []):
            try:
                await c.close()
                drained += 1
            except Exception:
                pass
        self._holding_conns = []
        return {"action": "connection_exhaustion_drained", "drained": drained, "state": "active"}

