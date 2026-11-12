"""The log tailer must say when it cannot read the database log, and never record empty lines.

Was: on a target where `sudo -n tail` is refused (new lab target BDB-QA-U22-3, dry run of
NL-C-01 on 11 November), the tailer recorded "connected" plus one empty "log_line" per attempt
and reconnected every second -- 100 SSH logins in two minutes, and evidence claiming a
connection while no log was read.
"""

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from types import SimpleNamespace

from resilience_tests.execution.probes import probers
from resilience_tests.observability.event_stream import EventStream
from tests.test_adapter_seam import NODE
from tests.test_orchestrator import env, outcomes, run  # noqa: F401  (pytest fixture)


class FakeStdout:
    def __init__(self, lines):
        self._lines = list(lines)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._lines:
            raise StopAsyncIteration
        return self._lines.pop(0)


class FakeProcess:
    def __init__(self, lines, exit_status, stderr):
        self.stdout = FakeStdout(lines)
        self._done = SimpleNamespace(exit_status=exit_status, stderr=stderr)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def wait(self):
        return self._done


def fake_remote(lines, exit_status, stderr, connects):
    class FakeRemoteHost:
        def __init__(self, endpoint, connect_timeout_s=10.0):
            pass

        async def __aenter__(self):
            connects.append(1)
            return self

        async def __aexit__(self, *exc):
            return False

        def process(self, command):
            assert command.startswith("sudo -n")
            return FakeProcess(lines, exit_status, stderr)
    return FakeRemoteHost


def _tail(monkeypatch, lines, exit_status, stderr, seconds=0.5):
    connects: list[int] = []
    monkeypatch.setattr(probers, "RemoteHost", fake_remote(lines, exit_status, stderr, connects))
    monkeypatch.setattr(probers, "LOG_RECONNECT_PAUSE_S", 0.05)
    monkeypatch.setattr(probers, "LOG_UNAVAILABLE_PAUSE_S", 10.0)

    async def go():
        with tempfile.TemporaryDirectory() as tmp:
            stream = EventStream(Path(tmp) / "events.jsonl")
            tailer = probers.LogTailer(NODE, stream)
            tailer.start()
            await asyncio.sleep(seconds)
            await tailer.stop()
            stream.close()
            return [e for e in stream.events() if e.source == "log_tailer"]
    return asyncio.run(go()), connects


def test_a_refused_tail_is_recorded_as_unavailable_with_its_reason_and_retried_slowly(monkeypatch):
    events, connects = _tail(monkeypatch, [""], 1, "sudo: a password is required\n")
    kinds = [e.kind for e in events]
    assert "log_line" not in kinds                                         # no empty "log lines"
    unavailable = [e for e in events if e.kind == "unavailable"]
    assert unavailable and unavailable[0].data["error"] == "sudo: a password is required"
    assert unavailable[0].data["exit_status"] == 1
    assert len(connects) == 1                                              # not one SSH login per second


def test_real_log_lines_still_come_through_and_empty_reads_are_dropped(monkeypatch):
    lines = ["2026-11-11 LOG:  database system is ready to accept connections\n", "", "\n"]
    events, _ = _tail(monkeypatch, lines, 0, "", seconds=0.2)
    logged = [e.data["line"] for e in events if e.kind == "log_line"]
    assert logged and all(logged) and logged[0].endswith("ready to accept connections")


def test_a_run_whose_log_could_not_be_read_discloses_it(env, monkeypatch):  # noqa: F811
    from resilience_tests.control import orchestrator as orch

    class RefusedTail:
        def __init__(self, node, stream):
            self.stream = stream

        def start(self):
            self.stream.emit("log_tailer", "unavailable", node="n", exit_status=1,
                             error="sudo: a password is required")

        async def stop(self):
            pass

    monkeypatch.setattr(orch, "LogTailer", RefusedTail)
    results = run(env, "NL-C-01")
    assert any("The database log could not be read during this run (sudo: a password is required)" in d
               for d in results["disclosures"]), results["disclosures"]
