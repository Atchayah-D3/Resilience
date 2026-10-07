"""Tests for pure parsers of PostgreSQL / ShaktiDB 17 pgbench output (T027)."""

import pytest

from resilience_tests.execution.workload.pgbench_output import (
    PgbenchParseError,
    is_abort_line,
    parse_abort_line,
    parse_progress_line,
    parse_summary,
)


def test_parse_progress_line():
    line = "progress: 1.0 s, 100.0 tps, lat 10.000 ms stddev 1.000, 0 failed"
    p = parse_progress_line(line)
    assert p.interval_s == 1.0
    assert p.tps == 100.0
    assert p.lat_ms == 10.0
    assert p.stddev_ms == 1.0
    assert p.failed == 0


def test_parse_progress_line_unparseable_raises():
    with pytest.raises(PgbenchParseError):
        parse_progress_line("invalid progress output line")
    with pytest.raises(PgbenchParseError):
        parse_progress_line("progress: foo s, bar tps")


def test_parse_abort_line():
    line1 = "client 0 aborted in command 2 (SQL) of script bench.sql: ERROR: terminating connection due to administrator command"
    assert is_abort_line(line1)
    a1 = parse_abort_line(line1)
    assert a1.client == 0
    assert a1.command_idx == 2
    assert a1.script == "bench.sql"
    assert "terminating connection" in a1.message

    line2 = "client 3 aborted: server closed the connection unexpectedly"
    assert is_abort_line(line2)
    a2 = parse_abort_line(line2)
    assert a2.client == 3
    assert a2.command_idx is None
    assert a2.script is None
    assert a2.message == "server closed the connection unexpectedly"


def test_parse_abort_line_unparseable_raises():
    assert not is_abort_line("LOG: statement: SELECT 1")
    with pytest.raises(PgbenchParseError):
        parse_abort_line("LOG: statement: SELECT 1")


def test_parse_summary_with_per_command_latencies():
    summary_text = """
transaction type: custom.sql
scaling factor: 1
query mode: simple
number of clients: 1
number of threads: 1
maximum number of tries: 1
duration: 10.000000 s
number of transactions actually processed: 1000
number of failed transactions: 5 (0.500%)
number of transactions failed due to serialization failure: 3
number of transactions failed due to deadlock: 2
latency average = 1.234 ms
latency stddev = 0.456 ms
initial connection time = 0.500 ms
tps = 99.500000 (without initial connection time)
statement latencies in milliseconds and failures:
         0.123           0  \\set aid random(1, 100000)
         0.800           3  UPDATE accounts SET abalance = abalance + 1 WHERE aid = :aid;
         0.311           2  COMMIT;
"""
    s = parse_summary(summary_text)
    assert s.transaction_type == "custom.sql"
    assert s.clients == 1
    assert s.threads == 1
    assert s.duration_s == 10.0
    assert s.transactions_processed == 1000
    assert s.failed_transactions == 5
    assert s.serialization_failures == 3
    assert s.deadlock_failures == 2
    assert s.latency_avg_ms == 1.234
    assert s.latency_stddev_ms == 0.456
    assert s.initial_connection_time_ms == 0.500
    assert s.tps == 99.500000
    assert len(s.command_latencies) == 3
    assert s.command_latencies[0].command == "\\set aid random(1, 100000)"
    assert s.command_latencies[0].latency_ms == 0.123
    assert s.command_latencies[1].failures == 3


def test_parse_summary_incomplete_raises():
    incomplete = """
transaction type: test.sql
number of clients: 1
tps = 100.0
"""
    with pytest.raises(PgbenchParseError):
        parse_summary(incomplete)
