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
    # "sql": a statement failed -- the database (or the way to it) ended the client.
    # "meta": a meta-command (\\shell, \\setshell, ...) failed -- never the database's doing.
    kind: str | None = None


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

# ShaktiDB / PostgreSQL 17 pgbench (recorded on the lab build), with or without the
# "pgbench: error: " prefix:
#   client 0 aborted in command 3 (SQL) of script 0; perhaps the backend died while processing
#   client 0 script 0 aborted in command 3 query 0: FATAL:  terminating connection due to ...
#   client 0 aborted in command 0 (shell) of script 0; execution of meta-command failed
_ABORT_RE = re.compile(
    r"^(?:pgbench:\s+error:\s+)?client\s+(\d+)\s+(?:script\s+(\S+)\s+)?aborted"
    r"(?:\s+in\s+command\s+(\d+))?(?:\s+\(([^)]+)\))?(?:\s+(query)\s+\d+)?"
    r"(?:\s+of\s+script\s+([^:;]+))?\s*[:;]?\s*(.*)$"
)
# pgbench could not open its connection at all (the server is down or refusing): it exits
# before running a transaction, with no abort line.
_CONNECT_FAILURE_RE = re.compile(r"connection to server .* failed|could not create connection", re.IGNORECASE)

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
    tag = (m.group(4) or "").lower()
    kind = "sql" if tag == "sql" or m.group(5) else ("meta" if tag else None)
    return PgbenchAbortLine(
        client=int(m.group(1)),
        command_idx=int(m.group(3)) if m.group(3) else None,
        script=m.group(2) or (m.group(6).strip() if m.group(6) else None),
        message=m.group(7).strip(),
        kind=kind,
    )


def is_abort_line(line: str) -> bool:
    return bool(_ABORT_RE.match(line.strip()))


def is_connect_failure(stderr_text: str) -> bool:
    """pgbench exited because it could not connect (no transaction was attempted)."""
    return bool(_CONNECT_FAILURE_RE.search(stderr_text))


# The server refused who we are or what we may touch: no retry will change that, and retrying
# would only spin until the phase bound. ("too many clients", "starting up" and "connection
# refused" are not here: those are fault conditions the run must ride through.)
_AUTH_FAILURE_RE = re.compile(
    r"authentication failed|no password supplied|pg_hba\.conf|"
    r"role \"[^\"]*\" does not exist|database \"[^\"]*\" does not exist|permission denied",
    re.IGNORECASE)


def is_auth_failure(stderr_text: str) -> bool:
    """pgbench was refused for its credentials or privileges."""
    return bool(_AUTH_FAILURE_RE.search(stderr_text))


def is_statement_error(abort: PgbenchAbortLine) -> bool:
    """The server answered a statement with ERROR on a live connection (a FATAL, or a connection
    that died, is the connection being lost instead)."""
    return abort.kind == "sql" and abort.message.upper().startswith("ERROR")


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
