"""Shared fixtures for the pgbench workload tests: the fake pgbench's file-backed database, an
adapter that hands out the real PostgreSQL adapter's pgbench transactions, and a driver
factory. See tests/fakes/fake_pgbench.py for what the fake database can be told to do."""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any

from catalog.schema import Workload
from resilience_tests.adapters.base import BaseDatabaseAdapter, Capability, DatabaseSession, IntegrityResult
from resilience_tests.adapters.postgresql.adapter import PostgreSQLAdapter
from resilience_tests.control.profile import load_profile
from resilience_tests.execution.workload.history_writer import HistoryWriter
from resilience_tests.execution.workload.markers import MarkerJournals
from resilience_tests.execution.workload.pgbench_driver import PgbenchWorkloadDriver
from resilience_tests.observability.event_stream import EventStream

FAKE_PGBENCH = str(Path(__file__).parent / "fake_pgbench.py")
SHAPE_PROFILE = {"marker": ("oltp_write_heavy", "none"), "churn": ("mixed", "none"),
                 "list_append": ("oltp_write_heavy", "list_append")}


class FakeDB:
    """The fake pgbench's database directory."""

    def __init__(self, path: Path) -> None:
        self.path = path
        path.mkdir(parents=True, exist_ok=True)

    def flag(self, name: str, on: bool = True, value: str = "") -> None:
        p = self.path / name
        if on:
            p.write_text(value)
        elif p.exists():
            p.unlink()

    @property
    def down(self) -> bool:
        return (self.path / "down").exists()

    def committed(self) -> dict[str, float]:
        p = self.path / "committed"
        if not p.exists():
            return {}
        return {u: float(t) for u, t in (line.split() for line in p.read_text().splitlines())}

    def lists(self) -> dict[int, list[int]]:
        p = self.path / "lists.json"
        return {int(k): v for k, v in json.loads(p.read_text()).items()} if p.exists() else {}

    def churn_ops(self) -> int:
        p = self.path / "churn"
        return len(p.read_text().splitlines()) if p.exists() else 0


class _Session(DatabaseSession):
    def __init__(self, db: FakeDB) -> None:
        self.db = db

    async def commit_marker(self, seq, marker_id):
        raise NotImplementedError

    async def try_write(self) -> bool:
        return not self.db.down

    async def ping(self) -> bool:
        return not self.db.down

    async def close(self) -> None:
        pass

    @property
    def is_closed(self) -> bool:
        return False


class FakePgbenchAdapter(BaseDatabaseAdapter):
    engine = "fake-pgbench"
    capabilities = frozenset({Capability.TRANSACTIONAL_MARKERS, Capability.WORKLOAD_CHURN,
                              Capability.LIST_APPEND_HISTORY, Capability.PGBENCH_WORKLOAD})
    churn_key_space = 50
    # the PostgreSQL adapter's own transactions: the fake pgbench interprets exactly those
    pgbench_launch = PostgreSQLAdapter.pgbench_launch
    pgbench_marker_uuid = PostgreSQLAdapter.pgbench_marker_uuid

    def __init__(self, node, db: FakeDB) -> None:
        super().__init__(node)
        self.db = db
        self.open_sessions = 0

    async def session(self, endpoint=None, timeout_s: float = 5.0) -> DatabaseSession:
        if self.db.down:
            raise ConnectionRefusedError("the database system is starting up")
        return _Session(self.db)

    async def prepare_harness_state(self) -> None:
        pass

    async def marker_ids(self) -> set[str]:
        return set(self.db.committed())

    async def sentinel(self, table: str, hostname: str):
        return {}

    async def durability_settings(self) -> dict[str, str]:
        return {}

    async def certification_blockers(self) -> list[str]:
        return []

    async def integrity_check(self, timeout_s: float) -> IntegrityResult:
        return IntegrityResult(structural_errors=0, checksum_failures=0)

    async def server_version(self) -> str:
        return "PostgreSQL 17.11.1.0 on x86_64"

    async def sessions_with_application_name(self, name: str) -> int | None:
        return self.open_sessions


def make_driver(tmp_path: Path, monkeypatch, shape: str = "marker", concurrency: int = 2,
                rate_tps: float = 50) -> tuple[PgbenchWorkloadDriver, FakePgbenchAdapter, FakeDB]:
    db = FakeDB(tmp_path / "fakedb")
    monkeypatch.setenv("FAKE_PGBENCH_DB", str(db.path))
    adapter = FakePgbenchAdapter(load_profile("e2-dedicated-vm").nodes[0], db)
    profile, history_kind = SHAPE_PROFILE[shape]
    workload = Workload(profile=profile, concurrency=concurrency, rate_tps=rate_tps,
                        transaction_markers=True, history=history_kind)
    run_dir = tmp_path / "run"
    history = HistoryWriter(run_dir / "history.edn") if shape == "list_append" else None
    driver = PgbenchWorkloadDriver(adapter, workload, MarkerJournals(run_dir), EventStream(run_dir / "events.jsonl"),
                                   pgbench_bin=FAKE_PGBENCH, pgbench_version="pgbench (PostgreSQL) 17.11.1.0",
                                   history=history)
    return driver, adapter, db


async def until(condition, timeout_s: float = 15.0, what: str = "condition") -> None:
    t0 = time.monotonic()
    while not condition():
        if time.monotonic() - t0 > timeout_s:
            raise AssertionError(f"timed out waiting for {what}")
        await asyncio.sleep(0.05)


def close_evidence(driver: PgbenchWorkloadDriver) -> None:
    driver.journals.close()
    if driver.history is not None:
        driver.history.close()


def history_ops(driver: PgbenchWorkloadDriver) -> list[dict[str, Any]]:
    """history.edn as (type, process, value text) -- enough to check what the tests need."""
    import re
    ops = []
    for line in (driver.journals.run_dir / "history.edn").read_text().splitlines():
        ops.append({"type": re.search(r":type :(\w+)", line).group(1),
                    "process": int(re.search(r":process (\d+)", line).group(1)),
                    "value": re.search(r":value (\[.*\])\}$", line).group(1)})
    return ops
