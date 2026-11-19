"""The end checks of a long run, compared in the database (specs/007-common-oltp-workload).

The common workload writes millions of journal lines (one hour at 2,500 TPS: about 8 million
writing transactions). Held in the harness's memory, the comparison with the database needed
more than 7 GB and the process was killed. Here every journal line is copied into a temporary
table on ONE connection, in batches (the harness keeps only one batch in memory), and the
database compares them with resilience.markers and the TPC-C tables. Only numbers, and a short
list of lost or phantom markers, come back.

The rules are those of the in-memory comparison, unchanged (markers.diff_markers,
tpcc/checks.py): a marker's distinct uuids count once; where one uuid has several lines, the
LAST line counts (later file, then later line), as the in-memory dicts kept the last line. The
per-order and per-payment queries are the adapter's own (tpcc.ORDER_CHECK_SQL,
tpcc.PAYMENT_CHECK_SQL), over the same rows the in-memory version sent.

Temporary tables belong to this connection only and disappear when it closes: the target is
left as found. They take room on the target's disk while the check runs (about 2 x the journals'
size; service/endchecks.check_space refuses to start without room to spare); `facts()` reports
how much they took.
"""

from __future__ import annotations

import datetime
import gzip
import hashlib
from array import array
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import asyncpg

from resilience_tests.adapters.postgresql.tpcc import ORDER_CHECK_SQL, PAYMENT_CHECK_SQL

MARKER, ACKED, ABORTED, DB = "marker", "acked", "aborted", "db"

# column lists of the rows the harness sends (service/endchecks.py builds them from the lines)
COLUMNS: dict[str, tuple[str, ...]] = {
    MARKER: ("fileno", "lineno", "uuid", "type"),
    ACKED: ("fileno", "lineno", "uuid", "t_ack", "w", "d", "o_id", "ol_cnt", "c_w", "c_d", "c", "amt"),
    ABORTED: ("fileno", "lineno", "uuid", "t_abort", "type", "sqlstate", "business_rollback"),
    DB: ("uuid",),
}

DDL = """
CREATE TEMP TABLE jc_marker  (fileno int NOT NULL, lineno bigint NOT NULL, uuid uuid NOT NULL, type text);
CREATE TEMP TABLE jc_acked   (fileno int NOT NULL, lineno bigint NOT NULL, uuid uuid NOT NULL, t_ack float8,
                              w int, d int, o_id int, ol_cnt int, c_w int, c_d int, c int, amt bigint);
CREATE TEMP TABLE jc_aborted (fileno int NOT NULL, lineno bigint NOT NULL, uuid uuid NOT NULL, t_abort float8,
                              type text, sqlstate text, business_rollback boolean);
CREATE TEMP TABLE jc_db      (uuid uuid NOT NULL);
"""

DATABASE_MARKERS = "resilience.markers"
CURSOR_PREFETCH = 10_000


class PgJournalCheck:
    """One connection with the run's journals in temporary tables. Use: load() every batch of
    every kind, use_database_markers() (or load DB rows read from markers_db.txt.gz), prepare(),
    then the queries; close() at the end."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self.conn = conn
        self._d = "jc_db_u"                   # the database's markers: the live table, or the file's copy
        self._prepared = False
        self.duplicates: dict[str, int] = {}

    @classmethod
    async def open(cls, conn: asyncpg.Connection) -> PgJournalCheck:
        await conn.execute(DDL)
        return cls(conn)

    async def load(self, kind: str, rows: Sequence[tuple[Any, ...]]) -> None:
        if rows:
            await self.conn.copy_records_to_table(f"jc_{kind}", records=rows, columns=list(COLUMNS[kind]))

    async def use_database_markers(self) -> None:
        """Compare with resilience.markers as it is now (the workload has stopped: nothing writes it)."""
        self._d = DATABASE_MARKERS

    async def prepare(self) -> None:
        """One row per uuid (the last line where there are several), with an index each."""
        for kind in (MARKER, ACKED, ABORTED) + ((DB,) if self._d != DATABASE_MARKERS else ()):
            raw, uniq = f"jc_{kind}", f"jc_{kind}_u"
            dup = await self.conn.fetchval(f"SELECT count(*) - count(DISTINCT uuid) FROM {raw}")
            self.duplicates[kind] = int(dup)
            if dup == 0:
                await self.conn.execute(f"ALTER TABLE {raw} RENAME TO {uniq}")
            elif kind == DB:
                await self.conn.execute(f"CREATE TEMP TABLE {uniq} AS SELECT DISTINCT uuid FROM {raw}; DROP TABLE {raw}")
            else:
                await self.conn.execute(f"CREATE TEMP TABLE {uniq} AS SELECT DISTINCT ON (uuid) * FROM {raw} "
                                        f"ORDER BY uuid, fileno DESC, lineno DESC; DROP TABLE {raw}")
            await self.conn.execute(f"CREATE UNIQUE INDEX ON {uniq} (uuid); ANALYZE {uniq}")
        self._prepared = True

    def _in_d(self, alias: str) -> str:
        return f"EXISTS (SELECT 1 FROM {self._d} m WHERE m.uuid = {alias}.uuid)"

    async def counts(self, new_order_type: str) -> dict[str, int]:
        assert self._prepared
        in_w = "EXISTS (SELECT 1 FROM jc_marker_u w WHERE w.uuid = {a}.uuid)"
        in_a = "EXISTS (SELECT 1 FROM jc_acked_u a2 WHERE a2.uuid = {a}.uuid)"
        in_b = "EXISTS (SELECT 1 FROM jc_aborted_u b2 WHERE b2.uuid = {a}.uuid)"
        row = await self.conn.fetchrow(f"""
            SELECT
              (SELECT count(*) FROM jc_marker_u) AS written,
              (SELECT count(*) FROM jc_acked_u) AS acked,
              (SELECT count(*) FROM jc_aborted_u) AS aborted,
              (SELECT count(*) FROM {self._d}) AS in_db,
              (SELECT count(*) FROM jc_acked_u a WHERE NOT {self._in_d('a')}) AS lost,
              (SELECT count(*) FROM {self._d} m WHERE NOT EXISTS (SELECT 1 FROM jc_marker_u w WHERE w.uuid = m.uuid))
                  AS phantom,
              (SELECT count(*) FROM jc_acked_u a WHERE NOT {in_w.format(a='a')}) AS unjournalled_ack,
              (SELECT count(*) FROM jc_marker_u x WHERE NOT {in_a.format(a='x')} AND NOT {in_b.format(a='x')})
                  AS unknown,
              (SELECT count(*) FROM jc_marker_u x WHERE NOT {in_a.format(a='x')} AND NOT {in_b.format(a='x')}
                                                   AND {self._in_d('x')}) AS unknown_committed,
              (SELECT count(*) FROM jc_aborted_u b WHERE {in_w.format(a='b')} AND NOT {in_a.format(a='b')})
                  AS rolled_back,
              (SELECT count(*) FROM jc_aborted_u b WHERE {in_w.format(a='b')} AND NOT {in_a.format(a='b')}
                                                     AND {self._in_d('b')}) AS rolled_back_kept,
              (SELECT count(*) FROM jc_aborted_u b WHERE {self._in_d('b')}) AS aborted_kept,
              (SELECT count(*) FROM {self._d} m JOIN jc_marker_u w ON w.uuid = m.uuid WHERE w.type = $1)
                  AS new_orders_with_marker""", new_order_type)
        return {k: int(v) for k, v in dict(row).items()}

    async def lost(self, limit: int) -> list[str]:
        rows = await self.conn.fetch(f"""SELECT a.uuid::text AS u FROM jc_acked_u a WHERE NOT {self._in_d('a')}
                                         ORDER BY a.uuid::text COLLATE "C" LIMIT $1""", limit)
        return [r["u"] for r in rows]

    async def phantom(self, limit: int) -> list[str]:
        rows = await self.conn.fetch(f"""SELECT m.uuid::text AS u FROM {self._d} m
                                          WHERE NOT EXISTS (SELECT 1 FROM jc_marker_u w WHERE w.uuid = m.uuid)
                                         ORDER BY m.uuid::text COLLATE "C" LIMIT $1""", limit)
        return [r["u"] for r in rows]

    async def lost_t_ack(self) -> array:
        """When each lost commit was acknowledged (0.0 when its line has no time), as the in-memory
        version read it. An array of floats: 8 bytes each, even if every commit was lost."""
        out = array("d")
        async with self.conn.transaction():
            async for r in self.conn.cursor(f"SELECT coalesce(a.t_ack, 0.0) FROM jc_acked_u a WHERE NOT {self._in_d('a')}",
                                            prefetch=CURSOR_PREFETCH):
                out.append(float(r[0]))
        return out

    async def write_db_markers(self, path: Path) -> dict[str, Any]:
        """The database's markers into markers_db.txt.gz exactly as markers.write_db_markers wrote
        them (sorted as text, one per line, gzip; SHA-256 of the uncompressed text), streamed."""
        sha, count = hashlib.sha256(), 0
        with gzip.open(path, "wb") as fh:
            async with self.conn.transaction():
                async for r in self.conn.cursor(f'SELECT uuid::text AS u FROM {self._d} ORDER BY uuid::text COLLATE "C"',
                                                prefetch=CURSOR_PREFETCH):
                    line = (r[0] + "\n").encode()
                    fh.write(line)
                    sha.update(line)
                    count += 1
        return {"file": path.name, "count": count, "sha256": sha.hexdigest()}

    async def order_check(self, lost: bool) -> dict[str, int]:
        """The per-order check of the acknowledged New-Orders whose marker is in the database
        (lost=False) or is not (lost=True): the rows the in-memory version sent, the same query."""
        where = ("NOT " if lost else "") + self._in_d("a")
        async with self.conn.transaction():
            await self.conn.execute("CREATE TEMP TABLE tpcc_acked (w int, d int, o_id int, ol_cnt int) ON COMMIT DROP")
            n = await self.conn.fetchval(f"""WITH ins AS (INSERT INTO tpcc_acked
                                              SELECT a.w, a.d, a.o_id, a.ol_cnt FROM jc_acked_u a
                                               WHERE a.o_id IS NOT NULL AND {where} RETURNING 1)
                                             SELECT count(*) FROM ins""")
            if not n:
                return {"checked": 0, "missing": 0, "incomplete": 0}
            row = await self.conn.fetchrow(ORDER_CHECK_SQL)
            return {"checked": int(n), "missing": int(row["missing"]), "incomplete": int(row["incomplete"])}

    async def payment_check(self, lost: bool, since: datetime.datetime) -> dict[str, int]:
        """The per-payment check of the acknowledged Payments whose marker is (not) in the database."""
        where = ("NOT " if lost else "") + self._in_d("a")
        async with self.conn.transaction():
            await self.conn.execute("CREATE TEMP TABLE tpcc_paid (w int, d int, c_w int, c_d int, c int, amt int) "
                                    "ON COMMIT DROP")
            n = await self.conn.fetchval(f"""WITH ins AS (INSERT INTO tpcc_paid
                                              SELECT a.w, a.d, a.c_w, a.c_d, a.c, a.amt FROM jc_acked_u a
                                               WHERE a.amt IS NOT NULL AND {where} RETURNING 1)
                                             SELECT count(*) FROM ins""")
            if not n:
                return {"checked": 0, "missing": 0}
            missing = await self.conn.fetchval(PAYMENT_CHECK_SQL, since)
            return {"checked": int(n), "missing": int(missing)}

    async def database_markers_digest(self) -> dict[str, Any]:
        """Count and SHA-256 of resilience.markers now, computed as markers_db.txt.gz's (sorted
        text, one per line), streamed: has the table changed since the file was saved?"""
        sha, count = hashlib.sha256(), 0
        async with self.conn.transaction():
            async for r in self.conn.cursor(f'SELECT uuid::text FROM {DATABASE_MARKERS} ORDER BY uuid::text COLLATE "C"',
                                            prefetch=CURSOR_PREFETCH):
                sha.update((r[0] + "\n").encode())
                count += 1
        return {"count": count, "sha256": sha.hexdigest()}

    async def newest_database_marker_wall(self) -> float | None:
        """The time (seconds since 1970) of the newest row in resilience.markers, None if empty."""
        v = await self.conn.fetchval(f"SELECT extract(epoch FROM max(ts)) FROM {DATABASE_MARKERS}")
        return None if v is None else float(v)

    async def sessions_named(self, application_name: str) -> int:
        """Connections with this application_name now (the clerks of a workload still running)."""
        return int(await self.conn.fetchval("SELECT count(*) FROM pg_stat_activity WHERE application_name = $1",
                                            application_name))

    async def facts(self) -> dict[str, Any]:
        temp = await self.conn.fetchval("SELECT coalesce(sum(pg_total_relation_size(c.oid)), 0) FROM pg_class c "
                                        "WHERE c.relnamespace = pg_my_temp_schema()")
        return {"compared_in": "database (temporary tables)", "temp_bytes": int(temp or 0),
                "duplicate_lines": dict(self.duplicates),
                "database_markers": "resilience.markers" if self._d == DATABASE_MARKERS else "markers_db.txt.gz"}

    async def close(self) -> None:
        await self.conn.close()
