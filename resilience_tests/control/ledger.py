"""Injection ledger (Arch §15; Arch §4.3 for compound scenarios).

Every injection is journalled BEFORE execution, so a crashed harness can still be cleaned
up -- the harness must not be able to strand a node in a faulted state. The kill switch
reads the ledger and reverts everything still outstanding.

The ledger lives on the driver host, outside any run directory, and is fsync'd per record.
"""

from __future__ import annotations

import json
import os
import uuid
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Literal

from resilience_tests.observability.event_stream import HarnessClock

State = Literal["intent", "applied", "reverted", "revert_failed"]


@dataclass(frozen=True)
class LedgerEntry:
    injection_id: str
    state: State
    t_wall: float
    run_id: str
    env_profile: str
    fault_type: str
    driver: str
    node: str
    detail: dict[str, Any]


class InjectionLedger:
    def __init__(self, path: Path, clock: HarnessClock | None = None) -> None:
        self.path = path
        self.clock = clock or HarnessClock()

    def _append(self, entry: LedgerEntry) -> None:
        # created on first write, never on a read: a tool that only inspects the ledger must
        # not leave a directory behind that makes a mistyped --env look like a clean site
        self.path.parent.mkdir(parents=True, exist_ok=True)
        line = (json.dumps(asdict(entry), separators=(",", ":"), sort_keys=True) + "\n").encode()
        fd = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o640)
        try:
            os.write(fd, line)
            os.fsync(fd)
        finally:
            os.close(fd)

    def intent(
        self, *, run_id: str, env_profile: str, fault_type: str, driver: str, node: str, detail: dict[str, Any]
    ) -> LedgerEntry:
        entry = LedgerEntry(
            str(uuid.uuid4()), "intent", self.clock.wall(), run_id, env_profile, fault_type, driver, node, detail
        )
        self._append(entry)
        return entry

    def transition(self, entry: LedgerEntry, state: State, **detail: Any) -> LedgerEntry:
        new = LedgerEntry(
            entry.injection_id, state, self.clock.wall(), entry.run_id, entry.env_profile,
            entry.fault_type, entry.driver, entry.node, {**entry.detail, **detail},
        )
        self._append(new)
        return new

    def entries(self) -> list[LedgerEntry]:
        if not self.path.exists():
            return []
        out = []
        with self.path.open() as fh:
            for line in fh:
                if line.strip():
                    out.append(LedgerEntry(**json.loads(line)))
        return out

    def outstanding(self) -> list[LedgerEntry]:
        """Latest state per injection that is not `reverted` -- what the kill switch must undo.
        An `intent` with no `applied` is outstanding too: the harness may have died mid-call."""
        latest: dict[str, LedgerEntry] = {}
        for e in self.entries():
            latest[e.injection_id] = e
        return [e for e in latest.values() if e.state != "reverted"]
