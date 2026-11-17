"""Prometheus: the store of the environment's metrics (Arch §9, L4). The harness only reads it,
to copy a run's window into the run folder -- context, never a pass/fail value (Arch §7.1)."""

from __future__ import annotations

import urllib.parse
from typing import Any

from resilience_tests.observability.http_client import HttpError, request, request_json


class PrometheusClient:
    def __init__(self, url: str, timeout_s: float = 10.0) -> None:
        self.url, self.timeout_s = url.rstrip("/"), timeout_s

    async def ready(self) -> None:
        """Raises HttpError when Prometheus does not answer that it is ready."""
        await request("GET", f"{self.url}/-/ready", timeout_s=self.timeout_s)

    async def query_range(self, query: str, start_s: float, end_s: float, step_s: float) -> list[dict[str, Any]]:
        """[{"metric": {labels}, "values": [[unix_s, "value"], ...]}] for the window."""
        params = urllib.parse.urlencode({"query": query, "start": f"{start_s:.3f}", "end": f"{end_s:.3f}",
                                         "step": f"{step_s:g}"})
        answer = await request_json("GET", f"{self.url}/api/v1/query_range?{params}", timeout_s=self.timeout_s)
        if not isinstance(answer, dict) or answer.get("status") != "success":
            raise HttpError(f"query failed: {(answer or {}).get('error', 'no answer')}")
        return list(answer["data"]["result"])
