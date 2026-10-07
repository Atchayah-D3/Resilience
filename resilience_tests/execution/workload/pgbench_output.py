"""Pure parsers for PostgreSQL / ShaktiDB 17 pgbench output (spec US3, research R5, R8).

Fail closed: unparseable input raises ValueError / ParseError, never returns fake zeros.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any


class PgbenchParseError(ValueError):
    """Raised when pgbench output cannot be parsed into expected schema."""


@dataclass(frozen=True)
class PgbenchProgressLine:
    interval_s: float
    tps: float
    lat_ms: float
    stddev_ms: float
    failed: int


@dataclass(frozen=True)
class PgbenchAbortLine:
    client: int
    command_idx: int | None
    script: str | None
    message: str


@dataclass(frozen=True)
class CommandLatency:
    latency_ms: float
    failures: int
    command: str


@dataclass(frozen=True)
class PgbenchSummary:
    transaction_type: str
    clients: int
    threads: int
    duration_s: float
    transactions_processed: int
    failed_transactions: int
    serialization_failures: int
    deadlock_failures: int
    latency_avg_ms: float
    latency_stddev_ms: float
    initial_connection_time_ms: float | None
    tps: float
    command_latencies: list[CommandLatency] = field(default_factory=list)


# Pattern: progress: 1.0 s, 100.0 tps, lat 10.000 ms stddev 1.000, 0 failed
_PROGRESS_RE = re.compile(
    r"^progress:\s+([0-9.]+)\s+s,\s+([0-9.]+)\s+tps,\s+lat\s+([0-9.]+)\s+ms\s+stddev\s+([0-9.]+),\s+(\d+)\s+failed"
)

# Pattern: client 0 aborted in command 2 (SQL) of script foo.sql: ERROR: ...
_ABORT_RE = re.compile(
    r"^client\s+(\d+)\s+aborted(?:\s+in\s+command\s+(\d+)(?:\s+\([^)]+\))?)?(?:\s+of\s+script\s+([^:]+))?:\s*(.*)$"
)

_CMD_LATENCY_RE = re.compile(
    r"^\s*([0-9.]+)\s+(\d+)\s+(.+)$"
)


def parse_progress_line(line: str) -> PgbenchProgressLine:
    line = line.strip()
    m = _PROGRESS_RE.match(line)
    if not m:
        raise PgbenchParseError(f"unparseable progress line: {line!r}")
    return PgbenchProgressLine(
        interval_s=float(m.group(1)),
        tps=float(m.group(2)),
        lat_ms=float(m.group(3)),
        stddev_ms=float(m.group(4)),
        failed=int(m.group(5)),
    )


def parse_abort_line(line: str) -> PgbenchAbortLine:
    line = line.strip()
    m = _ABORT_RE.match(line)
    if not m:
        raise PgbenchParseError(f"unparseable abort line: {line!r}")
    return PgbenchAbortLine(
        client=int(m.group(1)),
        command_idx=int(m.group(2)) if m.group(2) else None,
        script=m.group(3) if m.group(3) else None,
        message=m.group(4).strip(),
    )


def is_abort_line(line: str) -> bool:
    return bool(_ABORT_RE.match(line.strip()))


def parse_summary(text: str) -> PgbenchSummary:
    lines = [ln.rstrip() for ln in text.splitlines()]
    if not lines:
        raise PgbenchParseError("empty pgbench summary text")

    tx_type: str | None = None
    clients: int | None = None
    threads: int | None = None
    duration_s: float | None = None
    processed: int | None = None
    failed_tx: int = 0
    serial_failures: int = 0
    deadlock_failures: int = 0
    lat_avg: float | None = None
    lat_stddev: float | None = None
    conn_time: float | None = None
    tps: float | None = None
    cmd_latencies: list[CommandLatency] = []

    in_statement_latencies = False

    for line in lines:
        s = line.strip()
        if not s:
            continue
        if s.startswith("transaction type:"):
            tx_type = s.split(":", 1)[1].strip()
        elif s.startswith("number of clients:"):
            clients = int(s.split(":", 1)[1].strip())
        elif s.startswith("number of threads:"):
            threads = int(s.split(":", 1)[1].strip())
        elif s.startswith("duration:"):
            duration_s = float(s.split(":", 1)[1].strip().split()[0])
        elif s.startswith("number of transactions actually processed:"):
            processed = int(s.split(":", 1)[1].strip())
        elif s.startswith("number of failed transactions:"):
            # e.g. "number of failed transactions: 0 (0.000%)"
            val_part = s.split(":", 1)[1].strip().split()[0]
            failed_tx = int(val_part)
        elif s.startswith("number of transactions failed due to serialization failure:"):
            serial_failures = int(s.split(":", 1)[1].strip())
        elif s.startswith("number of transactions failed due to deadlock:"):
            deadlock_failures = int(s.split(":", 1)[1].strip())
        elif s.startswith("latency average ="):
            lat_avg = float(s.split("=")[1].strip().split()[0])
        elif s.startswith("latency stddev ="):
            lat_stddev = float(s.split("=")[1].strip().split()[0])
        elif s.startswith("initial connection time ="):
            conn_time = float(s.split("=")[1].strip().split()[0])
        elif s.startswith("tps ="):
            tps = float(s.split("=")[1].strip().split()[0])
        elif s.startswith("statement latencies in milliseconds and failures:"):
            in_statement_latencies = True
        elif in_statement_latencies:
            m = _CMD_LATENCY_RE.match(line)
            if m:
                cmd_latencies.append(
                    CommandLatency(
                        latency_ms=float(m.group(1)),
                        failures=int(m.group(2)),
                        command=m.group(3).strip(),
                    )
                )

    if tx_type is None or clients is None or duration_s is None or processed is None or tps is None or lat_avg is None:
        raise PgbenchParseError(
            f"incomplete summary; parsed: tx_type={tx_type}, clients={clients}, duration_s={duration_s}, "
            f"processed={processed}, tps={tps}, lat_avg={lat_avg}"
        )

    return PgbenchSummary(
        transaction_type=tx_type,
        clients=clients,
        threads=threads or 1,
        duration_s=duration_s,
        transactions_processed=processed,
        failed_transactions=failed_tx,
        serialization_failures=serial_failures,
        deadlock_failures=deadlock_failures,
        latency_avg_ms=lat_avg,
        latency_stddev_ms=lat_stddev if lat_stddev is not None else 0.0,
        initial_connection_time_ms=conn_time,
        tps=tps,
        command_latencies=cmd_latencies,
    )
