"""PostgreSQL adapter (Arch §8) -- serves ShaktiDB and standalone PostgreSQL.

Everything PostgreSQL-specific lives in this package: asyncpg, SQL, the error taxonomy,
pg_amcheck, page-checksum counters and the durability settings. The rest of the harness knows
none of it. This module holds the core (connections, markers, settings, integrity); the
methods for particular faults are mixed in from corruption.py, operations.py and sessions.py.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any

import asyncpg

from resilience_tests.adapters.base import (
    BaseDatabaseAdapter,
    Capability,
    DatabaseSession,
    IntegrityResult,
    MicroOp,
    TransactionOutcome,
    register_adapter,
)
from resilience_tests.adapters.postgresql.corruption import (  # noqa: F401  (re-exported)
    CORRUPTION_TARGET,
    _INVALID_PAGE_RE,
    CorruptionMixin,
)
from resilience_tests.adapters.postgresql.operations import (  # noqa: F401  (re-exported)
    AVAC_ANY_WORKER_SQL,
    AVAC_SETTINGS,
    AVAC_MAKE_DEAD,
    AVAC_RELATIONS_SQL,
    AVAC_TABLE,
    AVAC_WORKERS_SQL,
    CIC_INDEX_QUALIFIED,
    recovery_redo_start_lsn,
    OperationsMixin,
)
from resilience_tests.adapters.postgresql.sessions import (  # noqa: F401  (re-exported)
    CONTENDED_RELNAME,
    CONTENDED_ROWS,
    CONTENDED_SCHEMA,
    CONTENDED_TABLE,
    IDLE_SESSION_APPLICATION_NAME,
    LOCK_PILEUP_APPLICATION_NAMES,
    SessionsMixin,
    parse_pg_interval_s,
)
from resilience_tests.adapters.postgresql.tpcc import TpccMixin, call_tpcc  # TPC-C SQL (build steps 4, 6)
from resilience_tests.control.profile import DbEndpoint, Node
from resilience_tests.execution.remote import RemoteHost, privileged

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
-- Elle's list-append objects (Arch §10.3): one row per list, the list held as an array so an
-- append is a single upsert and a read returns the whole list in commit order.
CREATE TABLE IF NOT EXISTS resilience.elle_lists (
    k bigint PRIMARY KEY,
    v bigint[] NOT NULL
);
-- NL-C-03: the large transaction inserts one parent and millions of children referencing it,
-- so after recovery "no partial rows" and "FK invariants hold" are both checkable.
CREATE TABLE IF NOT EXISTS resilience.bulk_parent (
    id bigint PRIMARY KEY
);
CREATE TABLE IF NOT EXISTS resilience.bulk_child (
    id        bigint NOT NULL,
    parent_id bigint NOT NULL REFERENCES resilience.bulk_parent (id),
    payload   text NOT NULL
);
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
TRUNCATE = ("TRUNCATE resilience.markers, resilience.probe_writes, resilience.churn, "
            "resilience.elle_lists, resilience.bulk_child, resilience.bulk_parent")

# Elle list-append micro-operations (Arch §10.3). Run at SERIALIZABLE, the level Elle is told
# to check: PostgreSQL's REPEATABLE READ permits G2 (write skew) by design, so checking a
# weaker level against a serializable model would report the documented behaviour as a bug.
ELLE_ISOLATION = "serializable"
ELLE_READ = "SELECT v FROM resilience.elle_lists WHERE k = $1"
ELLE_APPEND = ("INSERT INTO resilience.elle_lists AS l (k, v) VALUES ($1, ARRAY[$2::bigint]) "
               "ON CONFLICT (k) DO UPDATE SET v = l.v || $2::bigint")


# A previous version of NL-C-06 built its index on resilience.markers; removed if still there.
LEGACY_CIC_INDEX = "resilience.idx_nlc06_concurrent"


# NL-N-I03: the contended table (sessions.py names it). Created and seeded only when a scenario
# declares workload.contended_share, and kept between runs. Every column a pile-up's schema
# change adds and drops again keeps its slot (PostgreSQL never reuses one; a table has at most
# 1,600), so the table is rebuilt once the slots used pass CONTENDED_MAX_ATTRIBUTES.
CONTENDED_DDL = f"""
CREATE SCHEMA IF NOT EXISTS {CONTENDED_SCHEMA};
CREATE TABLE IF NOT EXISTS {CONTENDED_TABLE} (
    id         bigint PRIMARY KEY,
    val        bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
)
"""
SEED_CONTENDED = (f"INSERT INTO {CONTENDED_TABLE} (id) SELECT g FROM generate_series(1, $1) g "
                  "ON CONFLICT (id) DO NOTHING")
CONTENDED_ATTRIBUTES_SQL = ("SELECT count(*) FROM pg_attribute "
                            f"WHERE attrelid = '{CONTENDED_TABLE}'::regclass AND attnum > 0")
CONTENDED_MAX_ATTRIBUTES = 1_000
# One transaction of a table user: the marker insert, plus a read and an update of one row. The
# transaction's own lock and statement timeouts are cleared, so a wait it meets is measured as
# latency rather than ended by whatever timeout the role or server carries.
CONTENDED_NO_TIMEOUTS = ("SET LOCAL lock_timeout = 0", "SET LOCAL statement_timeout = 0")
CONTENDED_READ = f"SELECT val FROM {CONTENDED_TABLE} WHERE id = $1"
CONTENDED_UPDATE = f"UPDATE {CONTENDED_TABLE} SET val = val + 1, updated_at = clock_timestamp() WHERE id = $1"


DURABILITY_SETTINGS = ["data_checksums", "fsync", "synchronous_commit", "full_page_writes",
                       "wal_sync_method", "server_version"]

# Errors where the server said the transaction did not happen. Everything else -- timeout,
# reset, killed server -- is UNKNOWN (Arch §7.1).
_DEFINITE_ABORT = (asyncpg.exceptions.IntegrityConstraintViolationError, asyncpg.exceptions.SerializationError)
# A serializable list-append transaction can also be chosen as a deadlock victim; the server
# rolled it back and said so, which is a definite abort (:fail), not an unknown outcome.
_LIST_APPEND_ABORT = _DEFINITE_ABORT + (asyncpg.exceptions.DeadlockDetectedError,)
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


# Database activity, one snapshot per call (Resilience Testing document, section 2.3: "wait events,
# locks, connections, WAL and checkpoints, dead rows, temporary files ... sampled every second").
# Read only; counters are cumulative and the analysis takes their differences. {checkpoints} is
# filled per server version: pg_stat_checkpointer from PostgreSQL 17, pg_stat_bgwriter before.
ACTIVITY_SQL = """
SELECT json_build_object(
  'connections_by_state', (SELECT coalesce(json_object_agg(st, n), '{{}}'::json) FROM
      (SELECT coalesce(state, 'background') AS st, count(*) AS n FROM pg_stat_activity GROUP BY 1) s),
  'client_connections', (SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend'),
  'max_connections', current_setting('max_connections')::int,
  'wait_events', (SELECT coalesce(json_object_agg(k, n), '{{}}'::json) FROM
      (SELECT wait_event_type || ':' || wait_event AS k, count(*) AS n FROM pg_stat_activity
        WHERE state = 'active' AND wait_event IS NOT NULL AND pid <> pg_backend_pid() GROUP BY 1) w),
  'lock_waiters', (SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock'),
  'longest_transaction_s', (SELECT coalesce(extract(epoch FROM max(clock_timestamp() - xact_start)), 0)
      FROM pg_stat_activity WHERE xact_start IS NOT NULL AND pid <> pg_backend_pid()
        AND backend_type = 'client backend'),
  'autovacuum_workers', (SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'autovacuum worker'),
  'wal_lsn_bytes', CASE WHEN pg_is_in_recovery() THEN NULL
                        ELSE pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0') END,
  'checkpoints', {checkpoints},
  'database', (SELECT json_build_object('xact_commit', xact_commit, 'xact_rollback', xact_rollback,
      'blks_read', blks_read, 'blks_hit', blks_hit, 'temp_files', temp_files, 'temp_bytes', temp_bytes,
      'deadlocks', deadlocks) FROM pg_stat_database WHERE datname = current_database()),
  'dead_tuples', (SELECT coalesce(sum(n_dead_tup), 0) FROM pg_stat_user_tables),
  'live_tuples', (SELECT coalesce(sum(n_live_tup), 0) FROM pg_stat_user_tables)
)::text
"""
ACTIVITY_CHECKPOINTS_17 = ("(SELECT json_build_object('timed', num_timed, 'requested', num_requested, "
                           "'write_time_ms', write_time, 'sync_time_ms', sync_time, "
                           "'buffers_written', buffers_written) FROM pg_stat_checkpointer)")
ACTIVITY_CHECKPOINTS_OLD = ("(SELECT json_build_object('timed', checkpoints_timed, 'requested', checkpoints_req, "
                            "'write_time_ms', checkpoint_write_time, 'sync_time_ms', checkpoint_sync_time, "
                            "'buffers_written', buffers_checkpoint) FROM pg_stat_bgwriter)")
ACTIVITY_TIMEOUT_S = 3.0


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

    async def commit_marker_list_append(self, seq: int, marker_id: str, read_key: int,
                                        append_key: int) -> tuple[TransactionOutcome, list[MicroOp]]:
        try:
            async with self._conn.transaction(isolation=ELLE_ISOLATION):
                await self._conn.execute(INSERT_MARKER, marker_id, seq)
                first = await self._conn.fetchval(ELLE_READ, read_key)
                await self._conn.execute(ELLE_APPEND, append_key, seq)
                after = await self._conn.fetchval(ELLE_READ, append_key)
        except _LIST_APPEND_ABORT:
            return TransactionOutcome.DEFINITELY_ABORTED, []
        except _CONNECTION_ERRORS:
            await self.close()
            return TransactionOutcome.UNKNOWN, []
        return TransactionOutcome.COMMITTED, [
            ("r", read_key, None if first is None else list(first)),
            ("append", append_key, seq),
            ("r", append_key, None if after is None else list(after)),
        ]

    async def commit_marker_with_contended_row(self, seq: int, marker_id: str, key: int) -> TransactionOutcome:
        try:
            async with self._conn.transaction():
                for statement in CONTENDED_NO_TIMEOUTS:
                    await self._conn.execute(statement)
                await self._conn.execute(INSERT_MARKER, marker_id, seq)
                await self._conn.fetchval(CONTENDED_READ, key)
                await self._conn.execute(CONTENDED_UPDATE, key)
        except _DEFINITE_ABORT:
            return TransactionOutcome.DEFINITELY_ABORTED
        except _CONNECTION_ERRORS:
            await self.close()
            return TransactionOutcome.UNKNOWN
        return TransactionOutcome.COMMITTED

    async def tpcc_execute(self, txn_type: str, inputs: Mapping[str, Any],
                           marker: tuple[int, str] | None) -> tuple[str, dict[str, Any], str | None]:
        outcome, journal, sqlstate = await call_tpcc(self._conn, txn_type, inputs, marker)
        if outcome == "unknown":
            await self.close()          # no longer trustworthy: the workload reconnects
        return outcome, journal, sqlstate

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
class PostgreSQLAdapter(CorruptionMixin, OperationsMixin, SessionsMixin, TpccMixin, BaseDatabaseAdapter):
    engine = "postgresql"
    capabilities = frozenset({
        Capability.TPCC_WORKLOAD,          # tables, loader, the five functions, consistency (TPC-C steps 4, 6)
        Capability.CONTENDED_TABLE,        # NL-N-I03: the harness-owned contended table
        Capability.TRANSACTIONAL_MARKERS,
        Capability.WORKLOAD_CHURN,
        Capability.STRUCTURAL_INTEGRITY_CHECK,
        Capability.PAGE_CHECKSUMS,
        Capability.DURABILITY_SETTINGS,
        Capability.LIST_APPEND_HISTORY,
    })

    churn_key_space = CHURN_ROWS
    contended_key_space = CONTENDED_ROWS

    def __init__(self, node: Node) -> None:
        super().__init__(node)
        self._checksum_baseline: ChecksumStats | None = None
        # activity_snapshot(): one connection kept for the sampler, and its query for this server version
        self._activity_conn: asyncpg.Connection | None = None
        self._activity_sql: str | None = None

    async def _connect(self, endpoint: DbEndpoint | None = None, timeout_s: float = 5.0,
                       command_timeout: float | None = _SAME_AS_CONNECT,
                       server_settings: dict[str, str] | None = None) -> asyncpg.Connection:
        """`timeout_s` bounds establishing the connection. `command_timeout` bounds each
        statement on it; the harness's own queries reuse the connect bound, but a client
        session passes None -- see `session`."""
        ep = endpoint or self.node.db
        # password comes from the driver host's ~/.pgpass, never from the profile
        return await asyncpg.connect(host=ep.host, port=ep.port, database=ep.dbname, user=ep.user,
                                     timeout=timeout_s, server_settings=server_settings,
                                     command_timeout=timeout_s if command_timeout is _SAME_AS_CONNECT else command_timeout)

    def _psql_as_os_user(self, sql: str, *, tuples_only: bool = True) -> str:
        """psql over SSH as the cluster's OS user: a superuser on the local socket, which is
        what reads and acts on other roles' sessions."""
        flags = ["-X", "-At", "-v", "ON_ERROR_STOP=1"] if tuples_only else ["-X", "-v", "ON_ERROR_STOP=1"]
        return self._as_os_user(["psql", *flags, "-p", str(self.node.db.port), "-d", self.node.db.dbname, "-c", sql])

    def _as_os_user(self, argv: list[str]) -> str:
        """One of the engine's own programs (argv[0] in pg_bin), run on the target as the
        cluster's OS user from /tmp, in the form the environment allows (remote.privileged)."""
        return privileged(self.node.ssh, [f"{self.node.pg_bin}/{argv[0]}", *argv[1:]], user=self.node.os_user, cwd="/tmp")

    async def session(self, endpoint: DbEndpoint | None = None, timeout_s: float = 5.0,
                      settings: Mapping[str, str] | None = None) -> DatabaseSession:
        """A workload/probe session. Statements are NOT bounded by the connect timeout: the
        caller bounds its own operation (the workload gives a marker transaction
        TXN_TIMEOUT_S, a probe gives its attempt PROBE_ATTEMPT_TIMEOUT_S). Tying the two
        together made a commit slower than the 2 s connect timeout indeterminate -- dropping
        genuine losses out of rpo_txn, and counting a latency spike as a dropped connection."""
        # `settings` travel in the connection's startup message, so they apply to this session
        # only, from its first statement (no SET round trip, nothing changed for other sessions).
        return PostgreSQLSession(await self._connect(endpoint or self.node.client, timeout_s, command_timeout=None,
                                                     server_settings=dict(settings) if settings else None))

    async def data_fs_free_bytes(self) -> int | None:
        """Free bytes on the filesystem of the data directory (df, as the disk prober reads it), or
        None when it cannot be read -- never a guess."""
        try:
            async with RemoteHost(self.node.ssh) as host:
                r = await host.run(privileged(self.node.ssh, ["df", "-P", "-B1", self.node.pgdata]),
                                   timeout_s=15.0, check=False)
        except Exception:  # noqa: BLE001 -- unreadable is None; the caller refuses to load
            return None
        lines = r.stdout.strip().splitlines() if r.exit_status == 0 else []
        fields = lines[-1].split() if len(lines) >= 2 else []
        return int(fields[3]) if len(fields) >= 4 and fields[3].isdigit() else None

    async def prepare_harness_state(self) -> None:
        # Pre-grant required roles to harness user if running against a cluster with SSH access.
        # PostgreSQL 15+ restricts CHECKPOINT to superusers and members of 'pg_checkpoint',
        # and pg_stat_activity detailed monitoring to 'pg_read_all_stats'. The grant persists
        # (it is not revoked at cleanup), so it is recorded for the report's disclosures.
        self.harness_grants = []
        try:
            async with RemoteHost(self.node.ssh) as host:
                grant_cmd = self._as_os_user([
                    "psql", "-X", "-p", str(self.node.db.port), "-d", self.node.db.dbname, "-c",
                    f"GRANT pg_checkpoint, pg_read_all_stats TO {self.node.db.user};"])
                r = await host.run(grant_cmd, timeout_s=10.0, check=False)
                if r.exit_status == 0:
                    self.harness_grants = ["pg_checkpoint", "pg_read_all_stats"]
        except Exception:
            pass  # Best effort: fake adapters, unit tests, or environments without SSH

        conn = await self._connect(timeout_s=120.0)   # the churn seed is 20k rows
        try:
            await conn.execute(HARNESS_DDL)
            # Schema-qualified: unqualified, the drop resolves through search_path, misses the
            # resilience schema, and silently leaves the index behind.
            await conn.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {LEGACY_CIC_INDEX}")
            await conn.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {CIC_INDEX_QUALIFIED}")
            # a corrupted relation left by an earlier run would fail every later integrity check;
            # DROP never reads its pages, so it is safe even when they are damaged
            await conn.execute(f"DROP TABLE IF EXISTS {CORRUPTION_TARGET}")
            await conn.execute(TRUNCATE)
            # Seeded every run, after the truncate: the churn table starts from a freshly
            # written, unfragmented state, so the first footprint sample is a real floor and
            # not whatever the previous run left behind.
            await conn.execute(SEED_CHURN, CHURN_ROWS)
            await conn.execute("VACUUM (ANALYZE) resilience.churn")
        finally:
            await conn.close()

    async def amcheck_relation(self, relation: str, timeout_s: float, *,
                               heapallindexed: bool = False) -> dict[str, Any]:
        """pg_amcheck on the one relation (Arch §10.1 names it for NL-I-01). A checksum failure
        surfaces either as a reported finding or as the check's own read failing with the
        invalid-page error; both mean the checker noticed. A clean exit means it did not.
        `corrupt_pages` lists every page the checker could not read, so the caller can tell
        whether it found THE damaged page rather than merely something."""
        node = self.node
        command = self._as_os_user(["pg_amcheck", "-p", str(node.db.port), "-d", node.db.dbname,
                                    f"--relation={relation}"] + (["--heapallindexed"] if heapallindexed else []))
        async with RemoteHost(node.ssh) as host:
            result = await host.run(command, timeout_s=timeout_s, check=False)
        output = (result.stdout + result.stderr).strip()
        noticed = bool(_AMCHECK_FINDING_RE.search(output) or _INVALID_PAGE_RE.search(output)
                       or "checksum" in output.lower())
        if result.exit_status == 0 and not noticed:
            detected: bool | None = False
        elif result.exit_status != 0 and noticed:
            detected = True
        else:
            detected = None   # e.g. could not connect, or exited non-zero for another reason
        pages = [{"block": int(b), "relation_path": p} for b, p in _INVALID_PAGE_RE.findall(output)]
        return {"detected": detected, "exit_status": result.exit_status, "corrupt_pages": pages,
                "output": output[:2000], "command": command}

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

    async def prepare_contended_table(self) -> dict[str, Any]:
        """NL-N-I03: the contended table, created once and kept; rebuilt when its column slots
        run low, and cleared of any probe column a crashed run left (sessions.py). Runs before
        the workload starts, so nothing waits on the drop."""
        conn = await self._connect(timeout_s=120.0, command_timeout=300.0)
        try:
            await conn.execute(CONTENDED_DDL)
            attributes = int(await conn.fetchval(CONTENDED_ATTRIBUTES_SQL))
            rebuilt = attributes > CONTENDED_MAX_ATTRIBUTES
            if rebuilt:
                await conn.execute(f"DROP TABLE {CONTENDED_TABLE}")
                await conn.execute(CONTENDED_DDL)
                attributes = int(await conn.fetchval(CONTENDED_ATTRIBUTES_SQL))
            rows = int(await conn.fetchval(f"SELECT count(*) FROM {CONTENDED_TABLE}"))
            seeded = rows < CONTENDED_ROWS
            if seeded:   # once, or after a rebuild: a kept table is not re-inserted every run
                await conn.execute(SEED_CONTENDED, CONTENDED_ROWS)
                await conn.execute(f"VACUUM (ANALYZE) {CONTENDED_TABLE}")
                rows = int(await conn.fetchval(f"SELECT count(*) FROM {CONTENDED_TABLE}"))
        finally:
            await conn.close()
        dropped, left = await self._drop_probe_columns()
        if left:
            raise RuntimeError(f"probe column(s) {left} left on {CONTENDED_TABLE} could not be removed")
        return {"table": CONTENDED_TABLE, "rows": rows, "attribute_slots": attributes,
                "rebuilt": rebuilt, "seeded": seeded, "leftover_columns_dropped": dropped}

    def crash_restart_log_patterns(self) -> tuple[str, ...]:
        # A backend that dies abnormally makes the postmaster end every other session and
        # reinitialize, while it keeps running itself; its start time does not change.
        return (
            r"server process \(PID \d+\) was terminated by signal",
            r"terminating any other active server processes",
            r"all server processes terminated; reinitializing",
            r"\bPANIC:",
        )

    def lock_wait_log_patterns(self) -> tuple[str, ...]:
        # log_lock_waits: "process 4711 still waiting for AccessExclusiveLock on relation 16384
        # of database 5 after 1000.081 ms"
        return (r"process \d+ still waiting for \w+ on relation",)

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

    async def activity_snapshot(self) -> dict[str, Any]:
        """One read-only snapshot of database activity (ACTIVITY_SQL) over one connection kept
        for the purpose; a broken connection is dropped and the error raised, and the next call
        connects again (the target may be restarting)."""
        conn = self._activity_conn
        if conn is None or conn.is_closed() or self._activity_sql is None:
            conn = await self._connect(timeout_s=ACTIVITY_TIMEOUT_S, server_settings={
                "application_name": "resilience-activity"})
            version = int(await conn.fetchval("SHOW server_version_num"))
            self._activity_conn = conn
            self._activity_sql = ACTIVITY_SQL.format(
                checkpoints=ACTIVITY_CHECKPOINTS_17 if version >= 170000 else ACTIVITY_CHECKPOINTS_OLD)
        try:
            return json.loads(await conn.fetchval(self._activity_sql, timeout=ACTIVITY_TIMEOUT_S))
        except Exception:
            await self.close_activity()
            raise

    async def close_activity(self) -> None:
        conn, self._activity_conn = self._activity_conn, None
        if conn is not None:
            conn.terminate()

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
                probe = self._as_os_user(["psql", "-X", "-At", "-p", str(node.db.port), "-d", db,
                                          "-c", AMCHECK_INSTALLED_SQL])
                r = await host.run(probe, timeout_s=AMCHECK_PROBE_TIMEOUT_S, check=False)
                if r.exit_status != 0 or r.stdout.strip() != "1":
                    missing.append(db)
            if missing:
                raise RuntimeError(
                    f"amcheck is not installed in {missing}; the operator installs it by hand "
                    "(CREATE EXTENSION amcheck) -- the harness does not modify target databases"
                )
            db_args = [a for db in databases for a in ("-d", db)]
            exclude = [f"--exclude-relation={r}" for r in self.integrity_exclusions]
            command = self._as_os_user(["pg_amcheck", "-p", str(node.db.port), *db_args, "--heapallindexed", *exclude])
            result = await host.run(command, timeout_s=timeout_s, check=False)
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
            detail={"command": command, "exit_status": result.exit_status, "databases": databases,
                    "excluded_relations": list(self.integrity_exclusions),
                    "checksum_baseline_taken": baseline is not None},
        )

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
                    cmd = self._as_os_user(["psql", "-X", "-At", "-F", "=", "-p", str(self.node.db.port),
                                            "-d", self.node.db.dbname, "-c", sql_cmd])
                    r = await host.run(cmd, timeout_s=15.0, check=False)
                    if r.exit_status == 0 and r.stdout:
                        for line in r.stdout.strip().splitlines():
                            if "=" in line:
                                k, v = line.split("=", 1)
                                deviations[k.strip()] = v.strip()
            except Exception:
                pass
        return deviations

    async def quick_integrity_check(self) -> dict[str, Any]:
        """A fast inter-cycle checksum check to localize corruptions to the cycle that caused them."""
        try:
            stats = await self._checksum_stats()
            baseline = self._checksum_baseline
            failures = checksum_failures_since(baseline, stats)
            return {"checksum_failures": failures, "ok": failures == 0}
        except Exception as exc:
            return {"checksum_failures": 0, "ok": False, "error": str(exc)}


