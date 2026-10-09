"""Tests for pure parsers of PostgreSQL / ShaktiDB 17 pgbench output (T027)."""

import pytest

from resilience_tests.execution.workload.pgbench_output import (
    PgbenchParseError,
    is_abort_line,
    is_auth_failure,
    is_connect_failure,
    is_statement_error,
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


def test_parse_abort_lines_of_the_lab_pgbench():
    """Recorded from ShaktiDB 17 pgbench: a terminated backend, a crashed server, and a failed
    record step. The first two are the database's doing; the third never is."""
    term = parse_abort_line("pgbench: error: client 0 script 0 aborted in command 3 query 0: FATAL:  "
                            "terminating connection due to administrator command")
    assert (term.client, term.command_idx, term.kind) == (0, 3, "sql")
    assert "terminating connection" in term.message
    crash = parse_abort_line("pgbench: error: client 0 aborted in command 3 (SQL) of script 0; "
                             "perhaps the backend died while processing")
    assert (crash.command_idx, crash.kind, crash.script) == (3, "sql", "0")
    meta = parse_abort_line("pgbench: error: client 0 aborted in command 0 (shell) of script 0; "
                            "execution of meta-command failed")
    assert meta.kind == "meta"
    assert parse_abort_line("pgbench: error: client 0 aborted in command 1 (setshell) of script 0; "
                            "execution of meta-command failed").kind == "meta"
    assert not is_abort_line("pgbench: error: Run was aborted; the above results are incomplete.")


def test_connect_failure_is_recognised():
    assert is_connect_failure('pgbench: error: connection to server at "127.0.0.1", port 55433 failed: '
                              "Connection refused\npgbench: error: could not create connection for setup")
    assert not is_connect_failure("pgbench: error: client 0 aborted in command 3 (SQL) of script 0")


def test_auth_failures_are_told_apart_from_fault_conditions():
    assert is_auth_failure('connection to server at "10.11.21.112", port 5433 failed: FATAL:  '
                           'password authentication failed for user "harness"')
    assert is_auth_failure('FATAL:  no pg_hba.conf entry for host "10.11.21.111", user "harness"')
    assert is_auth_failure('FATAL:  role "harness" does not exist')
    # what a fault looks like at connect time is retried, never fatal
    assert not is_auth_failure("FATAL:  sorry, too many clients already")
    assert not is_auth_failure("FATAL:  the database system is starting up")
    assert not is_auth_failure("FATAL:  remaining connection slots are reserved for roles with the SUPERUSER attribute")
    assert not is_auth_failure("Connection refused")


def test_statement_errors_are_told_apart_from_connection_loss():
    error = parse_abort_line('pgbench: error: client 0 script 0 aborted in command 11 query 0: ERROR:  '
                             'column "ts" of relation "churn" does not exist')
    assert is_statement_error(error)
    for lost in ("pgbench: error: client 0 script 0 aborted in command 3 query 0: FATAL:  "
                 "terminating connection due to administrator command",
                 "pgbench: error: client 0 aborted in command 3 (SQL) of script 0; "
                 "perhaps the backend died while processing",
                 "pgbench: error: client 0 aborted in command 0 (shell) of script 0; "
                 "execution of meta-command failed"):
        assert not is_statement_error(parse_abort_line(lost))


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
