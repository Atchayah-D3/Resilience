"""PostgreSQL side of data-file corruption (Framework NL-I): the harness-owned target
relation, locating the byte to damage, and reading the damaged relation back."""

from __future__ import annotations

import json
import re
import struct
import time
from collections.abc import Sequence
from typing import Any

import asyncpg


# NL-I (fault type data_corruption): the harness-owned relation the fault corrupts. Its own
# table, so a byte is only ever flipped in data the harness created; never markers, never
# operator data. autovacuum is off on it so nothing in the background reads the damaged page
# before the harness does -- the first read is the one that is measured.
CORRUPTION_TARGET = "resilience.corruption_target"
CORRUPTION_TARGET_ROWS = 2_000          # ~35 pages: a page in the middle is surely populated
CORRUPTION_BYTE_FROM_PAGE_END = 64      # inside the tuple area, far from the page header

# PostgreSQL: "invalid page in block 17 of relation base/16384/16400" (SQLSTATE XX001)
_INVALID_PAGE_RE = re.compile(r"invalid page in block (\d+) of relation (\S+)")

# NL-I-02 (relation_kind btree_index): a btree index on the same harness-owned table. Built
# after the rows are loaded, so the build lays the leaves out in key order from block 1; an
# internal page is allocated whenever a level above first needs one (the first one early, at
# block 3), so a block a third of the way in is normally a leaf. That is proved, not assumed:
# with the database down the driver reads the page's own flags first (page_check), then the
# whole page, from which locate_corruption_byte picks the byte (a data entry's heap pointer).
CORRUPTION_INDEX_NAME = "corruption_target_idx"
CORRUPTION_INDEX = f"resilience.{CORRUPTION_INDEX_NAME}"
CORRUPTION_INDEX_ROWS = 20_000         # ~55 leaf pages of int keys

# btree page layout: the last 16 bytes are the special space (BTPageOpaqueData); btpo_flags is
# the uint16 at offset 12 of it, so its low byte (little-endian) is 4 bytes from the page end,
# and BTP_LEAF is bit 0. A big-endian target fails the check and the run aborts -- never guesses.
BTREE_LEAF_FLAG_FROM_PAGE_END = 4
BTREE_LEAF_FLAG = 0x01

# Page header (PageHeaderData, 24 bytes): pd_lower, pd_upper, pd_special are uint16 at 12, 14,
# 16; the line pointer array starts at 24, 4 bytes each (lp_off:15, lp_flags:2, lp_len:15).
PAGE_HEADER_BYTES = 24
PAGE_BOUNDS_AT = 12
LINE_POINTER_BYTES = 4
LP_NORMAL = 1
BTREE_SPECIAL_BYTES = 16              # btpo_prev, btpo_next, btpo_level (uint32), btpo_flags, cycleid

# An index entry (IndexTupleData): t_tid = {bi_hi, bi_lo, ip_posid} (uint16 each), t_info
# (uint16: size in the low 13 bits); the int key follows at byte 8. INDEX_ALT_TID_MASK marks a
# posting-list or pivot entry, whose t_tid is not a heap row pointer -- such an entry is refused.
INDEX_TUPLE_HEADER_BYTES = 8
INDEX_SIZE_MASK = 0x1FFF
INDEX_ALT_TID_MASK = 0x2000
TID_OFFSET_NUMBER_AT = 4              # ip_posid; its low byte (little-endian) is the byte flipped

# A full-range ordered read walks every leaf of the index, so it must reach the damaged page.
# The scan types that would avoid the index are switched off for that session only.
# It must be a plain Index Scan: that returns each row's id FROM THE TABLE ROW the index entry
# points at, so a corrupted row pointer read unverified shows up as a wrong id. An Index Only
# Scan returns the key stored in the index and follows the pointer only to check visibility --
# a redirected pointer to another visible row would still print the right id. payload is not
# in the index, so an Index Only Scan cannot answer this query; it is also switched off.
INDEX_READ_SQL = f"SELECT id, payload FROM {CORRUPTION_TARGET} WHERE id >= 1 ORDER BY id"
INDEX_READ_SETTINGS = {"enable_seqscan": "off", "enable_bitmapscan": "off", "enable_sort": "off",
                       "enable_indexonlyscan": "off"}
_INDEX_SCAN_NODE = "Index Scan"

def _plan_nodes(plan: Any) -> list[dict[str, Any]]:
    """Every node of an EXPLAIN (FORMAT JSON) plan, depth first."""
    stack = [entry["Plan"] for entry in plan] if isinstance(plan, list) else [plan.get("Plan", plan)]
    nodes = []
    while stack:
        node = stack.pop()
        nodes.append(node)
        stack.extend(node.get("Plans") or [])
    return nodes


class CorruptionMixin:
    """Corruption-target methods of PostgreSQLAdapter. Mixed into the class in
    adapter.py; uses the core's `_connect` and `_psql_as_os_user`."""

    async def prepare_corruption_target(self, kind: str = "heap") -> dict[str, Any]:
        """Recreate the corruption target and locate a populated page in its data file."""
        if kind == "btree_index":
            return await self._prepare_index_corruption_target()
        if kind != "heap":
            raise ValueError(f"no corruption target of kind {kind!r} for PostgreSQL")
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

    async def _prepare_index_corruption_target(self) -> dict[str, Any]:
        """The table plus a btree index on it; the target is a leaf page of the index file."""
        conn = await self._connect(timeout_s=60.0)
        try:
            await conn.execute(f"DROP TABLE IF EXISTS {CORRUPTION_TARGET}")
            # autovacuum (and with it autoanalyze) off on the table and its index: nothing in the
            # background reads the damaged page first, and without statistics the planner does
            # not probe the index's end pages either
            await conn.execute(f"CREATE TABLE {CORRUPTION_TARGET} (id int NOT NULL, payload text NOT NULL) "
                               "WITH (autovacuum_enabled = false)")
            await conn.execute(f"INSERT INTO {CORRUPTION_TARGET} SELECT g, repeat('i', 16) "
                               "FROM generate_series(1, $1) g", CORRUPTION_INDEX_ROWS)
            await conn.execute(f"CREATE INDEX {CORRUPTION_INDEX_NAME} ON {CORRUPTION_TARGET} (id)")
            row = await conn.fetchrow(
                "SELECT pg_relation_filepath($1::regclass) AS path, "
                "pg_relation_filenode($1::regclass)::bigint AS filenode, "
                "current_setting('block_size')::int AS block_size, "
                "pg_relation_size($1::regclass)::bigint AS bytes", CORRUPTION_INDEX)
        finally:
            await conn.close()
        block_size = int(row["block_size"])
        pages = int(row["bytes"]) // block_size
        if pages < 4:
            raise RuntimeError(f"{CORRUPTION_INDEX} has only {pages} pages; no leaf page to corrupt")
        return {
            "relation": CORRUPTION_INDEX,
            "relation_kind": "btree_index",
            "table": CORRUPTION_TARGET,
            "relation_path": str(row["path"]),
            "filenode": int(row["filenode"]),
            "block": pages // 3,
            "pages": pages,
            "block_size": block_size,
            # no fixed byte: the driver reads the page with the database down and asks
            # locate_corruption_byte, which finds a real entry from the page's own line pointers
            "locate": "the low byte of the heap row pointer (offset number) of a data entry",
            "page_check": {"byte_in_page": block_size - BTREE_LEAF_FLAG_FROM_PAGE_END,
                           "mask": BTREE_LEAF_FLAG, "equals": BTREE_LEAF_FLAG,
                           "meaning": "btree leaf page (BTP_LEAF set in btpo_flags)"},
            "rows": CORRUPTION_INDEX_ROWS,
            # pg_amcheck checks a table's indexes along with it, so the run-wide check leaves out
            # both; they are checked on their own after REINDEX (heapallindexed)
            "integrity_exclusions": [CORRUPTION_TARGET, CORRUPTION_INDEX],
        }

    def locate_corruption_byte(self, target: dict[str, Any], page: bytes) -> dict[str, Any]:
        """The byte to flip in a btree leaf page, from the page's own bytes: the low byte of the
        heap row pointer's offset number of a data entry in the middle of the page. With that
        byte changed, an index scan that used the page unverified would fetch a different heap
        slot -- another row, or none -- so a wrong result is visible to the read. Every layout
        fact used is checked against the page first; anything unexpected raises ValueError and
        no byte is written."""
        bs = int(target["block_size"])
        if len(page) != bs:
            raise ValueError(f"read {len(page)} bytes of the page, expected {bs}")
        lower, upper, special = struct.unpack_from("<HHH", page, PAGE_BOUNDS_AT)
        if special != bs - BTREE_SPECIAL_BYTES:
            raise ValueError(f"pd_special is {special}, not {bs - BTREE_SPECIAL_BYTES}: not a btree page")
        if not PAGE_HEADER_BYTES <= lower <= upper <= special:
            raise ValueError(f"page bounds are inconsistent (lower {lower}, upper {upper}, special {special})")
        _prev, right, level, flags = struct.unpack_from("<IIIH", page, special)
        if not flags & BTREE_LEAF_FLAG or level != 0:
            raise ValueError(f"not a leaf page (btpo_flags {flags:#06x}, level {level})")
        items = (lower - PAGE_HEADER_BYTES) // LINE_POINTER_BYTES
        first = 1 if right == 0 else 2      # a page with a right sibling starts with its high key
        if items < first:
            raise ValueError(f"the page has {items} line pointers and no data entry")
        item = first + (items - first) // 2
        (lp,) = struct.unpack_from("<I", page, PAGE_HEADER_BYTES + LINE_POINTER_BYTES * (item - 1))
        off, lp_flags, length = lp & 0x7FFF, (lp >> 15) & 0x3, lp >> 17
        if lp_flags != LP_NORMAL or length < INDEX_TUPLE_HEADER_BYTES + 4 or off < upper or off + length > special:
            raise ValueError(f"line pointer {item} is not a normal entry inside the tuple area "
                             f"(off {off}, flags {lp_flags}, len {length})")
        bi_hi, bi_lo, posid, t_info = struct.unpack_from("<HHHH", page, off)
        if t_info & INDEX_ALT_TID_MASK or (t_info & INDEX_SIZE_MASK) != length or posid == 0:
            raise ValueError(f"entry {item} is not a plain index entry pointing at a heap row "
                             f"(t_info {t_info:#06x}, len {length}, offset number {posid})")
        (key,) = struct.unpack_from("<i", page, off + INDEX_TUPLE_HEADER_BYTES)
        if not 1 <= key <= int(target.get("rows", CORRUPTION_INDEX_ROWS)):
            raise ValueError(f"entry {item} holds key {key}, not one of the rows written")
        heap_block = (bi_hi << 16) | bi_lo
        return {"byte_in_page": off + TID_OFFSET_NUMBER_AT, "field": target.get("locate"),
                "item": item, "items": items, "entry_offset": off, "entry_length": length, "key": key,
                "heap_tid": [heap_block, posid], "heap_tid_after": [heap_block, posid ^ 0xFF]}

    @staticmethod
    def _corruption_error(exc: asyncpg.PostgresError) -> dict[str, Any]:
        message = str(exc)
        where = _INVALID_PAGE_RE.search(message)
        return {"error": type(exc).__name__, "sqlstate": getattr(exc, "sqlstate", None),
                "message": message[:500],
                "block": int(where.group(1)) if where else None,
                "relation_path": where.group(2) if where else None}

    async def read_index_target(self, attempts: int = 2) -> dict[str, Any]:
        """The table read through its index, on a fresh connection each time. The plan is
        checked first: a read the planner answered without that index -- or with an Index Only
        Scan, which never returns a table row -- says nothing about it."""
        out = []
        for _ in range(attempts):
            conn = await self._connect(timeout_s=10.0, command_timeout=60.0, server_settings=INDEX_READ_SETTINGS)
            attempt: dict[str, Any] = {"used_index": None}
            try:
                plan = await conn.fetchval(f"EXPLAIN (FORMAT JSON) {INDEX_READ_SQL}")
                nodes = _plan_nodes(json.loads(plan) if isinstance(plan, str) else plan)
                attempt["plan"] = [n.get("Node Type") for n in nodes]
                attempt["used_index"] = any(n.get("Node Type") == _INDEX_SCAN_NODE
                                            and n.get("Index Name") == CORRUPTION_INDEX_NAME for n in nodes)
                ids = [r[0] for r in await conn.fetch(INDEX_READ_SQL)]
                attempt.update(error=None, rows=len(ids),
                               matches_written=ids == list(range(1, CORRUPTION_INDEX_ROWS + 1)))
            except asyncpg.PostgresError as exc:
                attempt.update(self._corruption_error(exc))
            finally:
                await conn.close()
            out.append(attempt)
        return {"attempts": out}

    async def rebuild_corruption_target(self) -> dict[str, Any]:
        """REINDEX builds new index storage from the table and never reads the damaged pages."""
        conn = await self._connect(timeout_s=10.0, command_timeout=300.0)
        started = time.monotonic()
        try:
            await conn.execute(f"REINDEX INDEX {CORRUPTION_INDEX}")
            duration = round(time.monotonic() - started, 3)
            path = await conn.fetchval("SELECT pg_relation_filepath($1::regclass)", CORRUPTION_INDEX)
            return {"ok": True, "duration_s": duration, "error": None, "relation_path_after": str(path)}
        except asyncpg.PostgresError as exc:
            return {"ok": False, "duration_s": None, "error": f"{type(exc).__name__}: {str(exc)[:500]}",
                    "sqlstate": getattr(exc, "sqlstate", None)}
        finally:
            await conn.close()

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
                out.append(self._corruption_error(exc))
            finally:
                await conn.close()
        return {"attempts": out}

    def corruption_log_locations(self, lines: Sequence[str]) -> list[tuple[int, str]]:
        found = []
        for line in lines:
            m = _INVALID_PAGE_RE.search(line)
            if m:
                found.append((int(m.group(1)), m.group(2)))
        return found
