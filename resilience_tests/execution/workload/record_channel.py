"""Mechanism-neutral transaction record channel for pgbench workload driver (data-model.md)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Literal

RecordKind = Literal["pre", "ack"]


@dataclass(frozen=True)
class TransactionRecord:
    identity: str
    kind: RecordKind
    t_wall: float
    value: Any = None  # Optional payload (e.g. Elle list-append read values)


class RecordChannel(ABC):
    """Abstract interface for receiving transaction records during pgbench execution."""

    @abstractmethod
    def drain(self) -> list[TransactionRecord]:
        """Drain and return newly arrived transaction records."""

    @property
    @abstractmethod
    def complete(self) -> bool:
        """Whether the channel captured records reliably without missing data."""

    @property
    @abstractmethod
    def incomplete_reason(self) -> str | None:
        """Reason why records may be incomplete if complete is False."""

    async def open(self) -> None:
        """Open or initialize the channel."""

    async def close(self) -> None:
        """Flush and close the channel."""


class InMemoryRecordChannel(RecordChannel):
    """In-memory channel used for unit tests, fakes, and decoupled testing."""

    def __init__(self) -> None:
        self._records: list[TransactionRecord] = []
        self._all_records: list[TransactionRecord] = []
        self._complete: bool = True
        self._incomplete_reason: str | None = None

    def push(self, record: TransactionRecord) -> None:
        self._records.append(record)
        self._all_records.append(record)

    def record(self, identity: str, kind: RecordKind, t_wall: float, value: Any = None) -> None:
        self.push(TransactionRecord(identity=identity, kind=kind, t_wall=t_wall, value=value))

    def mark_incomplete(self, reason: str) -> None:
        self._complete = False
        self._incomplete_reason = reason

    def drain(self) -> list[TransactionRecord]:
        batch = list(self._records)
        self._records.clear()
        return batch

    @property
    def all_records(self) -> list[TransactionRecord]:
        return list(self._all_records)

    @property
    def complete(self) -> bool:
        return self._complete

    @property
    def incomplete_reason(self) -> str | None:
        return self._incomplete_reason


class JournalRecordChannel(RecordChannel):
    """Production/journal channel writing to MarkerJournals (marker.jrnl / acked.jrnl).

    Translates TransactionRecord to MarkerJournals entries using identity_uuid(identity).
    Validates TR-1..TR-7 invariants:
    - Pre records must precede commit.
    - Ack records must follow commit.
    - If a pre or ack record fails, marks incomplete.
    """

    def __init__(self, journals: Any) -> None:
        from resilience_tests.execution.workload.identity import identity_uuid

        self.journals = journals
        self._identity_uuid = identity_uuid
        self._buffered: list[TransactionRecord] = []
        self._all_records: list[TransactionRecord] = []
        self._complete: bool = True
        self._incomplete_reason: str | None = None
        self._pre_identities: set[str] = set()

    async def record_pre(self, identity: str, t_wall: float, seq: int = 0) -> None:
        u = self._identity_uuid(identity)
        try:
            await self.journals.written(seq, u, t_wall)
            self._pre_identities.add(identity)
            rec = TransactionRecord(identity=identity, kind="pre", t_wall=t_wall)
            self._buffered.append(rec)
            self._all_records.append(rec)
        except BaseException as e:
            self._complete = False
            self._incomplete_reason = f"failed writing pre-commit record: {e}"
            raise

    async def record_ack(self, identity: str, t_wall: float) -> None:
        if identity not in self._pre_identities:
            self._complete = False
            self._incomplete_reason = f"ack without pre record for identity {identity}"
            raise RuntimeError(self._incomplete_reason)
        u = self._identity_uuid(identity)
        try:
            await self.journals.acknowledged(u, t_wall)
            rec = TransactionRecord(identity=identity, kind="ack", t_wall=t_wall)
            self._buffered.append(rec)
            self._all_records.append(rec)
        except BaseException as e:
            self._complete = False
            self._incomplete_reason = f"failed writing ack record: {e}"
            raise

    async def push(self, record: TransactionRecord, seq: int = 0) -> None:
        if record.kind == "pre":
            await self.record_pre(record.identity, record.t_wall, seq=seq)
        elif record.kind == "ack":
            await self.record_ack(record.identity, record.t_wall)

    def drain(self) -> list[TransactionRecord]:
        b = list(self._buffered)
        self._buffered.clear()
        return b

    @property
    def all_records(self) -> list[TransactionRecord]:
        return list(self._all_records)

    @property
    def complete(self) -> bool:
        return self._complete

    @property
    def incomplete_reason(self) -> str | None:
        return self._incomplete_reason

