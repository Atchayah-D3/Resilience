"""Remote execution over SSH (Arch §12: asyncssh -- async, key-based, precise timing control;
Ansible ad-hoc was rejected because its startup latency ruins injection timing)."""

from __future__ import annotations

import asyncio
import shlex
from dataclasses import dataclass
from types import TracebackType

import asyncssh

from resilience_tests.control.profile import SSHEndpoint


class RemoteCommandError(RuntimeError):
    def __init__(self, host: str, command: str, result: RemoteResult) -> None:
        super().__init__(f"{host}: `{command}` exited {result.exit_status}: {result.stderr.strip()[:500]}")
        self.result = result


@dataclass(frozen=True)
class RemoteResult:
    exit_status: int | None
    stdout: str
    stderr: str


def as_user(user: str, command: str) -> str:
    """Run `command` as another OS user via non-interactive sudo."""
    return f"sudo -n -u {shlex.quote(user)} -- sh -c {shlex.quote(command)}"


def as_root(command: str) -> str:
    return f"sudo -n -- sh -c {shlex.quote(command)}"


class RemoteHost:
    """A persistent SSH connection. Keep one open across an injection so the fault call does
    not pay connection set-up latency at T0."""

    def __init__(self, endpoint: SSHEndpoint, connect_timeout_s: float = 10.0) -> None:
        self.endpoint = endpoint
        self.connect_timeout_s = connect_timeout_s
        self._conn: asyncssh.SSHClientConnection | None = None

    async def __aenter__(self) -> RemoteHost:
        await self.connect()
        return self

    async def __aexit__(self, et: type[BaseException] | None, e: BaseException | None, tb: TracebackType | None) -> None:
        await self.close()

    async def connect(self) -> None:
        self._conn = await asyncio.wait_for(
            asyncssh.connect(
                self.endpoint.host,
                port=self.endpoint.port,
                username=self.endpoint.user,
                # host keys verified against ~/.ssh/known_hosts on the driver host (asyncssh default)
                keepalive_interval=5,
                keepalive_count_max=2,
            ),
            timeout=self.connect_timeout_s,
        )

    async def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            try:
                await asyncio.wait_for(self._conn.wait_closed(), timeout=5)
            except (TimeoutError, OSError, asyncssh.Error):
                pass
            self._conn = None

    async def run(self, command: str, *, timeout_s: float, check: bool = True) -> RemoteResult:
        if self._conn is None:
            await self.connect()
        assert self._conn is not None
        result = await asyncio.wait_for(self._conn.run(command, check=False), timeout=timeout_s)
        out = RemoteResult(result.exit_status, _text(result.stdout), _text(result.stderr))
        if check and out.exit_status != 0:
            raise RemoteCommandError(self.endpoint.host, command, out)
        return out


    def process(self, command: str) -> asyncssh.SSHClientProcess:
        """Start a long-running command whose output is streamed (use as an async context)."""
        if self._conn is None:
            raise RuntimeError("not connected")
        return self._conn.create_process(command)


async def run_once(endpoint: SSHEndpoint, command: str, *, timeout_s: float, check: bool = True) -> RemoteResult:
    async with RemoteHost(endpoint, connect_timeout_s=min(timeout_s, 10.0)) as host:
        return await host.run(command, timeout_s=timeout_s, check=check)


def _text(value: str | bytes | None) -> str:
    if value is None:
        return ""
    return value.decode(errors="replace") if isinstance(value, bytes) else value
