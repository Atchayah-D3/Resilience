"""What the PostgreSQL adapter's topic mixins (corruption.py, operations.py, sessions.py) use
from its core class (adapter.py).

The mixins are only ever combined into PostgreSQLAdapter, which provides these members. Naming
them here makes the dependency explicit: a type checker reads each mixin against this protocol,
and tests/test_adapter_structure.py fails when a mixin uses a member of the core that is not
declared here, or when the core stops providing one -- so a rename in adapter.py cannot leave a
mixin calling a method that no longer exists.
"""

from __future__ import annotations

from typing import Any, Protocol

import asyncpg

from resilience_tests.control.profile import DbEndpoint, Node


class PostgreSQLCore(Protocol):
    node: Node

    async def _connect(self, endpoint: DbEndpoint | None = None, timeout_s: float = 5.0,
                       command_timeout: Any = ...,
                       server_settings: dict[str, str] | None = None) -> asyncpg.Connection:
        """An asyncpg connection to the node (adapter.py: PostgreSQLAdapter._connect)."""
        ...

    def _psql_as_os_user(self, sql: str, *, tuples_only: bool = True) -> str:
        """A psql command run over SSH as the cluster's OS user (PostgreSQLAdapter._psql_as_os_user)."""
        ...
