"""PostgreSQL adapter (Arch §8) -- serves ShaktiDB and standalone PostgreSQL.

Everything PostgreSQL-specific lives here: asyncpg, SQL, the error taxonomy, pg_amcheck,
page-checksum counters and the durability settings. The rest of the harness knows none of it.
"""

from __future__ import annotations

import asyncio
import re
import shlex
import time
from collections.abc import Sequence
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
# A previous version of NL-C-06 built its index on resilience.markers; removed if still there.
LEGACY_CIC_INDEX = "resilience.idx_nlc06_concurrent"

# NL-R-04: flood sessions carry this application_name, so a revert -- even from a kill switch
# in another process -- can find and terminate exactly them and nothing else.
FLOOD_APPLICATION_NAME = "resilience-flood"
FLOOD_CONNECT_TIMEOUT_S = 3.0
FLOOD_RECOVERY_TIMEOUT_S = 10.0

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

# NL-M-05: the injected idle session carries this application_name, so it -- and only it --
# can be found and ended again, by cleanup or by the kill switch after a harness crash.
IDLE_SESSION_APPLICATION_NAME = "resilience-harness-idle"
TERMINATE_IDLE_SESSIONS_SQL = ("SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity "
                               "WHERE application_name = $1 AND pid <> pg_backend_pid()")
IDLE_CONFIRM_TIMEOUT_S = 5.0
VACUUM_PROBE_TIMEOUT_S = 120.0
_DEAD_NOT_REMOVABLE_RE = re.compile(r"(\d+) are dead but not yet removable")
_REMOVABLE_CUTOFF_RE = re.compile(r"removable cutoff: (\d+)")

# NL-I (fault type data_corruption): the harness-owned relation the fault corrupts. Its own
# table, so a byte is only ever flipped in data the harness created; never markers, never
# operator data. autovacuum is off on it so nothing in the background reads the damaged page
# before the harness does -- the first read is the one that is measured.
CORRUPTION_TARGET = "resilience.corruption_target"
CORRUPTION_TARGET_ROWS = 2_000          # ~35 pages: a page in the middle is surely populated
CORRUPTION_BYTE_FROM_PAGE_END = 64      # inside the tuple area, far from the page header
# PostgreSQL: "invalid page in block 17 of relation base/16384/16400" (SQLSTATE XX001)
_INVALID_PAGE_RE = re.compile(r"invalid page in block (\d+) of relation (\S+)")

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


# Written by the replacement postmaster at the start of crash recovery, e.g.
# "LOG:  redo starts at 0/3000108". The LSN is the redo point recovery replays from.
_REDO_STARTS_RE = re.compile(r"redo starts at ([0-9A-Fa-f]+)/([0-9A-Fa-f]+)")


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


def recovery_redo_start_lsn(log_lines: Sequence[str]) -> int | None:
    """The LSN crash recovery started replaying from, from the first matching log line."""
    for line in log_lines:
        m = _REDO_STARTS_RE.search(line)
        if m:
            return (int(m.group(1), 16) << 32) | int(m.group(2), 16)
    return None


def _format_lsn(lsn: int | None) -> str | None:
    return None if lsn is None else f"{int(lsn) >> 32:X}/{int(lsn) & 0xFFFFFFFF:X}"


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
        Capability.LIST_APPEND_HISTORY,
    })

    churn_key_space = CHURN_ROWS

    def __init__(self, node: Node) -> None:
        super().__init__(node)
        self._checksum_baseline: ChecksumStats | None = None

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
        flags = "-X -At -v ON_ERROR_STOP=1" if tuples_only else "-X -v ON_ERROR_STOP=1"
        return as_user(self.node.os_user,
                       f"cd /tmp && {shlex.quote(self.node.pg_bin + '/psql')} {flags} -p {self.node.db.port} "
                       f"-d {shlex.quote(self.node.db.dbname)} -c {shlex.quote(sql)}")

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
        # and pg_stat_activity detailed monitoring to 'pg_read_all_stats'. The grant persists
        # (it is not revoked at cleanup), so it is recorded for the report's disclosures.
        self.harness_grants: list[str] = []
        try:
            async with RemoteHost(self.node.ssh) as host:
                grant_cmd = (
                    f"cd /tmp && {shlex.quote(self.node.pg_bin + '/psql')} -X -p {self.node.db.port} "
                    f"-d {shlex.quote(self.node.db.dbname)} -c "
                    f"{shlex.quote(f'GRANT pg_checkpoint, pg_read_all_stats TO {self.node.db.user};')}"
                )
                r = await host.run(as_user(self.node.os_user, grant_cmd), timeout_s=10.0, check=False)
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

    corruption_sqlstates = ("XX001",)  # data_corrupted

    async def prepare_corruption_target(self) -> dict[str, Any]:
        """Recreate the corruption target and locate a populated page in its data file."""
        conn = await self._connect(timeout_s=60.0)
        try:
            await conn.execute(f"DROP TABLE IF EXISTS {CORRUPTION_TARGET}")
            await conn.execute(f"CREATE TABLE {CORRUPTION_TARGET} (id int NOT NULL, payload text NOT NULL) "
                               "WITH (autovacuum_enabled = false)")
            await conn.execute(f"INSERT INTO {CORRUPTION_TARGET} SELECT g, repeat('c', 100) "
                               "FROM generate_series(1, $1) g", CORRUPTION_TARGET_ROWS)
            row = await conn.fetchrow(
                "SELECT pg_relation_filepath($1::regclass) AS path, "
                "pg_relation_filenode($1::regclass)::bigint AS filenode, "
                "current_setting('block_size')::int AS block_size, "
                "pg_relation_size($1::regclass)::bigint AS bytes", CORRUPTION_TARGET)
        finally:
            await conn.close()
        pages = int(row["bytes"]) // int(row["block_size"])
        return {
            "relation": CORRUPTION_TARGET,
            "relation_path": str(row["path"]),
            "filenode": int(row["filenode"]),
            "block": pages // 2,
            "pages": pages,
            "block_size": int(row["block_size"]),
            "byte_in_page": int(row["block_size"]) - CORRUPTION_BYTE_FROM_PAGE_END,
            "rows": CORRUPTION_TARGET_ROWS,
        }

    async def read_corruption_target(self, attempts: int = 2) -> dict[str, Any]:
        """A full sequential read (count(*) over a table with no index reads every page),
        on a fresh connection each time so no attempt is answered from a session cache."""
        out = []
        for _ in range(attempts):
            conn = await self._connect(timeout_s=10.0, command_timeout=60.0)
            try:
                rows = await conn.fetchval(f"SELECT count(*) FROM {CORRUPTION_TARGET}")
                out.append({"error": None, "rows": int(rows)})
            except asyncpg.PostgresError as exc:
                message = str(exc)
                where = _INVALID_PAGE_RE.search(message)
                out.append({"error": type(exc).__name__, "sqlstate": getattr(exc, "sqlstate", None),
                            "message": message[:500],
                            "block": int(where.group(1)) if where else None,
                            "relation_path": where.group(2) if where else None})
            finally:
                await conn.close()
        return {"attempts": out}

    async def amcheck_relation(self, relation: str, timeout_s: float) -> dict[str, Any]:
        """pg_amcheck on the one relation (Arch §10.1 names it for NL-I-01). A checksum failure
        surfaces either as a reported finding or as the check's own read failing with the
        invalid-page error; both mean the checker noticed. A clean exit means it did not."""
        node = self.node
        command = (f"cd /tmp && {shlex.quote(node.pg_bin + '/pg_amcheck')} -p {node.db.port} "
                   f"-d {shlex.quote(node.db.dbname)} --relation={shlex.quote(relation)}")
        async with RemoteHost(node.ssh) as host:
            result = await host.run(as_user(node.os_user, command), timeout_s=timeout_s, check=False)
        output = (result.stdout + result.stderr).strip()
        noticed = bool(_AMCHECK_FINDING_RE.search(output) or _INVALID_PAGE_RE.search(output)
                       or "checksum" in output.lower())
        if result.exit_status == 0 and not noticed:
            detected: bool | None = False
        elif result.exit_status != 0 and noticed:
            detected = True
        else:
            detected = None   # e.g. could not connect, or exited non-zero for another reason
        return {"detected": detected, "exit_status": result.exit_status, "output": output[:2000]}

    def corruption_log_locations(self, lines: Sequence[str]) -> list[tuple[int, str]]:
        found = []
        for line in lines:
            m = _INVALID_PAGE_RE.search(line)
            if m:
                found.append((int(m.group(1)), m.group(2)))
        return found

    def idle_session_timeout_s(self, observed: dict[str, str]) -> float:
        return parse_pg_interval_s(observed.get("idle_in_transaction_session_timeout"))

    # ------------------------------------------------------------------ fault.during

    async def prepare_scenario_objects(self, during: str | None) -> dict[str, Any]:
        """Objects a `during` operation needs, created in init -- before the baseline, so
        seeding them never disturbs the steady state being measured."""
        if during != "concurrent_index_build":
            return {}
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
        task = getattr(self, "_bg_task", None)
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
        task = getattr(self, "_bg_task", None)
        conn = getattr(self, "_bg_conn", None)
        pid = getattr(self, "_bg_pid", None)
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
            return {"dropped_index": CIC_INDEX_QUALIFIED, "truncated": ["resilience.bulk_child", "resilience.bulk_parent"]}
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
        if hasattr(self, "_checkpoint_task"):
            self._checkpoint_task.cancel()
        if hasattr(self, "_checkpoint_conn"):
            try:
                await self._checkpoint_conn.close()
            except Exception:  # noqa: BLE001 -- the server it talked to was killed
                pass

        prior = (getattr(self, "_checkpoint_baseline", None) or {}).get("redo_lsn")
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
            exclude = "".join(f" --exclude-relation={shlex.quote(r)}" for r in self.integrity_exclusions)
            command = (f"cd /tmp && {shlex.quote(node.pg_bin + '/pg_amcheck')} -p {node.db.port} "
                       f"{db_args} --heapallindexed{exclude}")
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
            detail={"command": command, "exit_status": result.exit_status, "databases": databases,
                    "excluded_relations": list(self.integrity_exclusions),
                    "checksum_baseline_taken": baseline is not None},
        )

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
        except Exception:
            pass

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

    idle_timeout_sqlstates = ("25P03",)  # idle_in_transaction_session_timeout

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

    async def observe_fault_settings(self, fault_type: str) -> dict[str, str]:
        """SHOW only -- nothing is written. For the idle-in-transaction fault the deciding
        setting is the server's own timeout."""
        if fault_type != "idle_in_transaction":
            return {}
        conn = await self._connect(timeout_s=10.0)
        try:
            return {"idle_in_transaction_session_timeout": str(await conn.fetchval("SHOW idle_in_transaction_session_timeout"))}
        finally:
            await conn.close()

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

    async def quick_integrity_check(self) -> dict[str, Any]:
        """A fast inter-cycle checksum check to localize corruptions to the cycle that caused them."""
        try:
            stats = await self._checksum_stats()
            baseline = self._checksum_baseline
            failures = checksum_failures_since(baseline, stats)
            return {"checksum_failures": failures, "ok": failures == 0}
        except Exception as exc:
            return {"checksum_failures": 0, "ok": False, "error": str(exc)}

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
