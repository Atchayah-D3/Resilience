"""JSON over HTTP for the L4 observability clients (Prometheus, Grafana).

The standard library only (urllib), run off the event loop, so the harness gains no dependency
for context it never judges by. Every call is bounded by its own timeout.
"""

from __future__ import annotations

import asyncio
import json
import urllib.request
from collections.abc import Mapping
from typing import Any


class HttpError(RuntimeError):
    """An HTTP call failed: unreachable, timed out, or answered with an error status."""


async def request(method: str, url: str, *, headers: Mapping[str, str] | None = None,
                  body: Any = None, timeout_s: float = 5.0) -> bytes:
    def call() -> bytes:
        data = json.dumps(body).encode() if body is not None else None
        hdrs = {"Accept": "application/json", **({"Content-Type": "application/json"} if data else {}),
                **(headers or {})}
        req = urllib.request.Request(url, data=data, method=method, headers=hdrs)
        try:
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                return resp.read()
        except Exception as exc:  # noqa: BLE001 -- one error type for every way a call can fail
            raise HttpError(f"{method} {url.split('?')[0]}: {type(exc).__name__}: {exc}") from exc
    return await asyncio.to_thread(call)


async def request_json(method: str, url: str, **kwargs: Any) -> Any:
    raw = await request(method, url, **kwargs)
    try:
        return json.loads(raw) if raw else None
    except ValueError as exc:
        raise HttpError(f"{method} {url.split('?')[0]}: the answer is not JSON") from exc
