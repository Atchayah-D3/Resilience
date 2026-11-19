"""The common workload's end checks, for runs of any length (specs/007-common-oltp-workload).

After the clerks stop, from the evidence alone -- the journals on the harness host and what the
database holds -- the same checks as a scenario's validate phase:

    lost commits     every acknowledged marker is in resilience.markers (rpo_txn)
    phantoms         no marker in the database that the journals never wrote
    unknown          written, neither acknowledged nor surely rolled back (reported)
    TPC-C            per-order check (every acknowledged New-Order exists with all its lines),
                     per-payment check (every acknowledged Payment has its history row), the
                     orders created against the New-Orders with a marker, rolled-back
                     transactions kept, and the consistency rules (1-9 on all data, 10 and 12 on
                     a sample of districts) -- tpcc/checks.py decides the labels

How: every line of every journal (marker, acked, aborted) is read one at a time and copied into
temporary tables on one database connection, in batches of BATCH_ROWS; the database compares
(adapters/postgresql/journal_check.py). The harness never holds more than one batch, so an hour
at 2,500 TPS (8 million transactions, killed at 7 GB by the old in-memory comparison) and a run
of days need the same memory. An adapter without `journal_check` (the unit tests' fake engines)
is compared in memory by InMemoryJournalCheck, with the same rules.

Nothing is weakened: every line is read and sent; a line that cannot be read is counted as torn
(the lost-commit count is then not trusted); a part that cannot run leaves its measure NOT
MEASURED with the reason, never 0.
"""

from __future__ import annotations

import datetime
import gzip
import hashlib
import json
import random
from array import array
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from resilience_tests.execution.workload import contract
from resilience_tests.execution.workload.markers import (
    ABORTED_JOURNAL,
    ACKED_JOURNAL,
    DB_MARKERS_FILE,
    MARKER_JOURNAL,
    diff_markers,
    journal_paths,
    write_db_markers,
)
from resilience_tests.execution.workload.tpcc import checks, data

MARKER, ACKED, ABORTED, DB = "marker", "acked", "aborted", "db"
BATCH_ROWS = 20_000
# Room the temporary tables take on the target's disk, per byte of journal on the harness host:
# 1.8 measured on the lab's 15-minute run of 18 Nov 2026 (183 MB of journals, 333 MB of tables
# and indexes); the database's markers read from markers_db.txt.gz take about 4 x the file.
TEMP_BYTES_PER_JOURNAL_BYTE = 2.0
TEMP_BYTES_PER_GZ_MARKER_BYTE = 4.0
SPACE_MARGIN = 1.5            # the target's data filesystem must have 1.5 x that free, or no check starts
LOST_LIST_MAX = 100           # lost and phantom markers named in the summary (all are in the evidence files)


class NotEnoughSpace(RuntimeError):
    """The target's data filesystem has too little free room for the temporary tables."""


def temp_space_needed(folder: Path, markers_from_file: bool) -> int:
    """Bytes the temporary tables will take on the target for this folder's journals."""
    journals = sum(p.stat().st_size for name in (MARKER_JOURNAL, ACKED_JOURNAL, ABORTED_JOURNAL)
                   for p in journal_paths(folder, name))
    extra = (folder / DB_MARKERS_FILE).stat().st_size * TEMP_BYTES_PER_GZ_MARKER_BYTE if markers_from_file else 0
    return int(journals * TEMP_BYTES_PER_JOURNAL_BYTE + extra)


async def check_space(adapter: Any, folder: Path, markers_from_file: bool) -> dict[str, Any]:
    """Refuse (NotEnoughSpace) when the target's data filesystem cannot take the temporary tables
    with the margin: the check must never fill the database's disk. Free space that cannot be read
    is recorded and the check goes on (the tables are removed when the check ends)."""
    need = temp_space_needed(folder, markers_from_file)
    reader = getattr(adapter, "data_fs_free_bytes", None)
    free = await reader() if reader is not None else None
    fact = {"temp_bytes_expected": need, "data_fs_free_bytes": free, "margin": SPACE_MARGIN}
    if free is None:
        fact["note"] = "the free space on the target's data filesystem could not be read: not checked"
    elif free < need * SPACE_MARGIN:
        raise NotEnoughSpace(f"the end checks need about {need / 1e9:.1f} GB of temporary tables on the target "
                             f"({SPACE_MARGIN} x that must be free), but its data filesystem has {free / 1e9:.1f} GB free")
    return fact


class JournalLineError(ValueError):
    """A journal line that is valid JSON but not a journal record (no uuid): a harness defect."""


@dataclass
class TpccInputs:
    """What the TPC-C checks need besides the files: from the run's start (service.json)."""

    orders_before: int | None             # sum(D_NEXT_O_ID - 1) when the data was checked at the start
    history_since: datetime.datetime | None   # the database's time at the start: this run's history rows
    warehouses: int                       # for the sample of districts (rules 10 and 12)


@dataclass
class EndCheckResult:
    end: dict[str, Any]                   # as summary.json "end_checks"
    facts: dict[str, Any] = field(default_factory=dict)       # markers_db_file, journal_check
    lost_t_ack: array = field(default_factory=lambda: array("d"))   # when each lost commit was acknowledged


# --- reading the journals, one line at a time ------------------------------------------------------


@dataclass
class _Read:
    lines: int = 0
    torn: int = 0
    files: list[str] = field(default_factory=list)


def _int(v: Any) -> int | None:
    return None if v is None else int(v)


def _float(v: Any) -> float | None:
    return None if v is None else float(v)


def _row(kind: str, fileno: int, lineno: int, r: Any, where: str) -> tuple[Any, ...]:
    if not isinstance(r, dict) or not isinstance(r.get("uuid"), str):
        raise JournalLineError(f"{where}: a journal line without a uuid")
    u = r["uuid"]
    if kind == MARKER:
        return (fileno, lineno, u, str(r.get("type", "")))
    if kind == ACKED:
        return (fileno, lineno, u, _float(r.get("t_ack")), _int(r.get("w")), _int(r.get("d")), _int(r.get("o_id")),
                _int(r.get("ol_cnt")), _int(r.get("c_w")), _int(r.get("c_d")), _int(r.get("c")), _int(r.get("amt")))
    return (fileno, lineno, u, _float(r.get("t_abort")), r.get("type"), r.get("sqlstate"),
            None if r.get("business_rollback") is None else bool(r.get("business_rollback")))


def journal_batches(folder: Path, kind: str, read: _Read) -> Iterator[list[tuple[Any, ...]]]:
    """Every line of every journal of one kind, as rows, BATCH_ROWS at a time. A line that is not
    JSON is torn (counted, as markers.read_journal counts it)."""
    name = {MARKER: MARKER_JOURNAL, ACKED: ACKED_JOURNAL, ABORTED: ABORTED_JOURNAL}[kind]
    batch: list[tuple[Any, ...]] = []
    for fileno, path in enumerate(journal_paths(folder, name)):
        read.files.append(path.name)
        with path.open("rb") as fh:
            for lineno, raw in enumerate(fh, 1):
                read.lines += 1
                try:
                    r = json.loads(raw)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    read.torn += 1
                    continue
                batch.append(_row(kind, fileno, lineno, r, f"{path.name} line {lineno}"))
                if len(batch) >= BATCH_ROWS:
                    yield batch
                    batch = []
    if batch:
        yield batch


def db_marker_batches(path: Path) -> Iterator[list[tuple[Any, ...]]]:
    """The database's markers as saved in markers_db.txt.gz, BATCH_ROWS at a time."""
    batch: list[tuple[Any, ...]] = []
    with gzip.open(path, "rt") as fh:
        for line in fh:
            u = line.strip()
            if u:
                batch.append((u,))
                if len(batch) >= BATCH_ROWS:
                    yield batch
                    batch = []
    if batch:
        yield batch


def file_digest(path: Path) -> dict[str, Any]:
    """Count and SHA-256 (of the uncompressed text) of markers_db.txt.gz, as written_db_markers reports."""
    sha, count = hashlib.sha256(), 0
    with gzip.open(path, "rb") as fh:
        for line in fh:
            sha.update(line)
            count += line.strip() != b""
    return {"file": path.name, "count": count, "sha256": sha.hexdigest()}


# --- the comparison in memory (fake engines; the same rules as the database's) ---------------------


class InMemoryJournalCheck:
    """For an adapter without journal_check: the rows kept in dicts and sets, the last line of a
    uuid winning, as markers.diff_markers and tpcc/checks.py have always read them. Small runs only."""

    def __init__(self, adapter: Any) -> None:
        self.adapter = adapter
        self.written: dict[str, str] = {}
        self.acked: dict[str, dict[str, Any]] = {}
        self.aborted: dict[str, dict[str, Any]] = {}
        self.db: set[str] = set()
        self._db_rows: list[str] | None = []
        self.lines = {MARKER: 0, ACKED: 0, ABORTED: 0, DB: 0}

    async def load(self, kind: str, rows: list[tuple[Any, ...]]) -> None:
        self.lines[kind] += len(rows)
        if kind == MARKER:
            self.written.update({r[2]: r[3] for r in rows})
        elif kind == ACKED:
            keys = ("t_ack", "w", "d", "o_id", "ol_cnt", "c_w", "c_d", "c", "amt")
            self.acked.update({r[2]: {"uuid": r[2], **{k: v for k, v in zip(keys, r[3:]) if v is not None}}
                               for r in rows})
        elif kind == ABORTED:
            self.aborted.update({r[2]: {"uuid": r[2], "t_abort": r[3]} for r in rows})
        else:
            assert self._db_rows is not None
            self._db_rows.extend(r[0] for r in rows)

    async def use_database_markers(self) -> None:
        self._db_rows = None

    async def prepare(self) -> None:
        self.db = set(await self.adapter.marker_ids()) if self._db_rows is None else set(self._db_rows)

    async def counts(self, new_order_type: str) -> dict[str, int]:
        d = diff_markers(self.written, self.acked, self.db, self.aborted)
        aborted = set(self.aborted)
        return {"written": d.written, "acked": d.acked, "aborted": len(aborted), "in_db": d.in_db,
                "lost": len(d.lost), "phantom": len(d.phantom), "unjournalled_ack": len(d.unjournalled_ack),
                "unknown": len(d.indeterminate), "unknown_committed": len(d.indeterminate_committed),
                "rolled_back": len(d.rolled_back), "rolled_back_kept": len(d.rolled_back_kept),
                "aborted_kept": len(aborted & self.db),
                "new_orders_with_marker": sum(1 for u in self.db if self.written.get(u) == new_order_type)}

    async def lost(self, limit: int) -> list[str]:
        return sorted(set(self.acked) - self.db)[:limit]

    async def phantom(self, limit: int) -> list[str]:
        return sorted(self.db - set(self.written))[:limit]

    async def lost_t_ack(self) -> array:
        return array("d", (float(r.get("t_ack") or 0.0) for u, r in self.acked.items() if u not in self.db))

    async def write_db_markers(self, path: Path) -> dict[str, Any]:
        return write_db_markers(path.parent, self.db)

    async def order_check(self, lost: bool) -> dict[str, int]:
        uuids = set(self.acked) - self.db if lost else set(self.acked) & self.db
        return await self.adapter.tpcc_order_check(checks.new_order_lines(self.acked, uuids))

    async def payment_check(self, lost: bool, since: datetime.datetime) -> dict[str, int]:
        uuids = set(self.acked) - self.db if lost else set(self.acked) & self.db
        return await self.adapter.tpcc_payment_check(checks.payment_lines(self.acked, uuids), since)

    async def database_markers_digest(self) -> dict[str, Any]:
        lines = "".join(u + "\n" for u in sorted(await self.adapter.marker_ids())).encode()
        return {"count": lines.count(b"\n"), "sha256": hashlib.sha256(lines).hexdigest()}

    async def newest_database_marker_wall(self) -> float | None:
        return None                       # a fake engine keeps no times

    async def sessions_named(self, application_name: str) -> int:
        return 0

    async def facts(self) -> dict[str, Any]:
        return {"compared_in": "memory (engine without journal_check)",
                "database_markers": "resilience.markers" if self._db_rows is None else DB_MARKERS_FILE}

    async def close(self) -> None:
        return None


async def open_check(adapter: Any) -> Any:
    make = getattr(adapter, "journal_check", None)
    return await make() if make is not None else InMemoryJournalCheck(adapter)


# --- the end checks --------------------------------------------------------------------------------


async def run_end_checks(adapter: Any, folder: Path, *, tpcc: TpccInputs | None,
                         markers_from_file: bool = False,
                         step: Callable[[str], None] | None = None) -> EndCheckResult:
    """All end checks of a run folder. `markers_from_file`: compare with the database's markers
    as saved in markers_db.txt.gz right after the run (a run checked again later, when the live
    table may have changed) instead of resilience.markers now. `tpcc`: run the TPC-C checks."""
    say = step or (lambda _s: None)
    end: dict[str, Any] = {"_errors": []}
    result = EndCheckResult(end=end)
    reads = {MARKER: _Read(), ACKED: _Read(), ABORTED: _Read()}
    counts: dict[str, int] | None = None
    load_error: str | None = None
    session = None
    try:
        _require_journals(folder)
        if getattr(adapter, "journal_check", None) is not None:
            result.facts["temp_space"] = await check_space(adapter, folder, markers_from_file)
        session = await open_check(adapter)
        for kind in (MARKER, ACKED, ABORTED):
            say(f"end checks: sending the {kind} journals to the database")
            for batch in journal_batches(folder, kind, reads[kind]):
                await session.load(kind, batch)
        if markers_from_file:
            say("end checks: the database's markers from markers_db.txt.gz")
            for batch in db_marker_batches(folder / DB_MARKERS_FILE):
                await session.load(DB, batch)
        else:
            await session.use_database_markers()
        say("end checks: comparing journals and database")
        await session.prepare()
        if markers_from_file:
            result.facts["markers_db_file"] = {**file_digest(folder / DB_MARKERS_FILE), "read_from_file": True}
        else:
            result.facts["markers_db_file"] = await session.write_db_markers(folder / DB_MARKERS_FILE)
        counts = await session.counts(contract.NEW_ORDER)
    except Exception as exc:  # noqa: BLE001 -- recorded; the measures stay unknown (the baseline fails)
        load_error = f"{type(exc).__name__}: {exc}"
        end["_errors"].append(f"lost-commit check: {load_error}")

    try:
        if counts is not None and session is not None:
            torn = sum(r.torn for r in reads.values())
            trusted = not torn and not counts["unjournalled_ack"]
            if counts["lost"]:
                result.lost_t_ack = await session.lost_t_ack()
            end.update({"rpo_txn": counts["lost"] if trusted else None, "phantom": counts["phantom"],
                        "markers": {"written": counts["written"], "acked": counts["acked"], "in_db": counts["in_db"],
                                    "lost": await session.lost(LOST_LIST_MAX), "lost_count": counts["lost"],
                                    "indeterminate": counts["unknown"],
                                    "indeterminate_committed": counts["unknown_committed"],
                                    "rolled_back": counts["rolled_back"],
                                    "phantom": await session.phantom(LOST_LIST_MAX),
                                    "unjournalled_ack": counts["unjournalled_ack"], "torn_journal_lines": torn}})
            if not trusted:
                end["_errors"].append("the journals disagree with themselves (torn lines or acknowledgements "
                                      "without a marker line): the lost-commit count is not trusted")
        if tpcc is not None:
            say("end checks: TPC-C checks")
            await _tpcc_checks(adapter, session if counts is not None else None, folder, tpcc, counts, reads,
                               load_error, end)
        if session is not None:
            try:
                result.facts["journal_check"] = {**await session.facts(),
                                                 "lines": {k: r.lines for k, r in reads.items()},
                                                 "torn_lines": {k: r.torn for k, r in reads.items()},
                                                 "files": {k: r.files for k, r in reads.items()}}
            except Exception as exc:  # noqa: BLE001 -- facts only
                result.facts["journal_check"] = {"error": f"{type(exc).__name__}: {exc}"}
    finally:
        if session is not None:
            await session.close()
    return result


def _require_journals(folder: Path) -> None:
    """As diff_from_journals: no marker journal is an error, never a perfect RPO; every marker
    journal must have its acknowledgement journal."""
    marker_paths = journal_paths(folder, MARKER_JOURNAL)
    if not marker_paths:
        raise FileNotFoundError(folder / MARKER_JOURNAL)
    stem_m, stem_a = MARKER_JOURNAL.rsplit(".", 1)[0], ACKED_JOURNAL.rsplit(".", 1)[0]
    for mp in marker_paths:
        expected = mp.with_name(mp.name.replace(stem_m, stem_a, 1))
        if not expected.exists():
            raise FileNotFoundError(expected)


async def _tpcc_checks(adapter: Any, session: Any, folder: Path, t: TpccInputs, counts: dict[str, int] | None,
                       reads: dict[str, _Read], load_error: str | None, end: dict[str, Any]) -> None:
    """TpccProfile.consistency_checks, with the journal comparison done by `session`."""
    from resilience_tests.execution.workload.tpcc.profile import _driver_load

    problems: list[str] = []
    if load_error:
        problems.append(f"markers: {load_error}")

    async def attempt(what: str, call: Any) -> Any:
        try:
            return await call
        except Exception as exc:  # noqa: BLE001 -- recorded; the measure is then not measured
            problems.append(f"{what}: {type(exc).__name__}: {exc}")
            return None

    acked_check = lost_check = acked_pay = lost_pay = None
    if session is not None:
        acked_check = await attempt("per-order check", session.order_check(lost=False))
        lost_check = await attempt("per-order check of lost New-Orders", session.order_check(lost=True))
        if t.history_since is None:
            problems.append("per-payment check: the run's start time was not recorded at prepare")
        else:
            acked_pay = await attempt("per-payment check", session.payment_check(lost=False, since=t.history_since))
            lost_pay = await attempt("per-payment check of lost Payments",
                                     session.payment_check(lost=True, since=t.history_since))
    status = await attempt("order count", adapter.tpcc_data_status())
    rules: dict[int, int | None] = {}
    full = await attempt("consistency rules", adapter.tpcc_consistency(checks.RULES_ALL_DATA))
    # rules 10 and 12: a new sample of districts every run, recorded so the run can be checked again
    sample_seed = random.SystemRandom().randrange(1, 2**31)
    w_list, d_list = data.sample_districts(random.Random(sample_seed), t.warehouses, checks.SAMPLE_DISTRICTS)
    sample = {"seed": sample_seed, "districts": [[w, d] for w, d in zip(w_list, d_list)]}
    sampled = await attempt("consistency rules (sample)", adapter.tpcc_consistency(
        checks.RULES_SAMPLED, district_sample=checks.SAMPLE_DISTRICTS, seed=sample_seed))
    rules.update(full or {})
    rules.update(sampled or {})

    c = counts or {}
    ev = checks.EvidenceCounts(
        written=c.get("written", 0), acked=c.get("acked", 0), aborted=c.get("aborted", 0), db_read=counts is not None,
        lost=c.get("lost", 0), phantom=c.get("phantom", 0), rolled_back_but_kept=c.get("aborted_kept", 0),
        unknown=c.get("unknown", 0), unknown_committed=c.get("unknown_committed", 0),
        new_orders_with_marker=c.get("new_orders_with_marker", 0),
        acked_order_check=acked_check, lost_order_check=lost_check,
        orders_before=t.orders_before, orders_after=(status or {}).get("current_orders"),
        acked_payment_check=acked_pay, lost_payment_check=lost_pay,
        rules=rules, sample=sample, torn_aborted_lines=reads[ABORTED].torn)
    measures, facts, reasons = checks.evaluate_counts(ev)
    facts["driver_load"] = _driver_load(folder / "events.jsonl") or "the run's events could not be read"
    if problems:
        facts["errors"] = problems
        reasons = {m: f"{why} ({'; '.join(problems)})" for m, why in reasons.items()}
    end["_not_measured"] = reasons
    end["tpcc_facts"] = facts
    end.update(measures)
