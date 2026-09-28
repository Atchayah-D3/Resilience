"""PostgreSQL adapter (Arch §8) -- serves ShaktiDB and standalone PostgreSQL.

Everything PostgreSQL-specific lives here: asyncpg, SQL, the error taxonomy, pg_amcheck,
page-checksum counters and the durability settings. The rest of the harness knows none of it.
"""

from __future__ import annotations

import re
import shlex
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
        conn = await self._connect(timeout_s=120.0)   # the churn seed is 20k rows
        try:
            await conn.execute(HARNESS_DDL)
            await conn.execute(TRUNCATE)
            # Seeded every run, after the truncate: the churn table starts from a freshly
            # written, unfragmented state, so the first footprint sample is a real floor and
            # not whatever the previous run left behind.
            await conn.execute(SEED_CHURN, CHURN_ROWS)
            await conn.execute("VACUUM (ANALYZE) resilience.churn")
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

    async def quick_integrity_check(self) -> dict[str, Any]:
        """Query pg_stat_database.checksum_failures to verify no block corruptions occurred
        during this cycle, taking < 1 ms without the cost of a full pg_amcheck."""
        try:
            stats = await self._checksum_stats()
            failures = checksum_failures_since(self._checksum_baseline, stats)
            return {"checksum_failures": failures, "ok": failures == 0}
        except Exception as exc:  # noqa: BLE001
            return {"error": str(exc), "ok": False}

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
