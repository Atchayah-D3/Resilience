"""Workload transaction identity encoding and UUID mapping (data-model.md)."""

from __future__ import annotations

import hashlib
import uuid


def identity_text(launch: int, client: int, seq: int) -> str:
    """Format launch, client, and sequence into canonical identity string."""
    return f"L{launch}-C{client}-S{seq}"


def identity_uuid(text: str) -> str:
    """Derive deterministic UUID string matching PostgreSQL's `md5(text)::uuid`."""
    digest = hashlib.md5(text.encode("utf-8")).hexdigest()
    return str(uuid.UUID(digest))
