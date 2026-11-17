"""Grafana: the dashboards people review a run on (Arch §9, L4: "human review; per-run annotated
timeline"). The harness writes annotations -- its own events on the graphs -- and builds the
link a run report opens. It never reads a value back from Grafana."""

from __future__ import annotations

import urllib.parse
from collections.abc import Mapping, Sequence
from typing import Any

from resilience_tests.observability.http_client import HttpError, request_json


class GrafanaClient:
    def __init__(self, url: str, token: str, timeout_s: float = 5.0) -> None:
        self.url, self.timeout_s = url.rstrip("/"), timeout_s
        self._headers = {"Authorization": f"Bearer {token}"}

    async def health(self) -> dict[str, Any]:
        answer = await request_json("GET", f"{self.url}/api/health", timeout_s=self.timeout_s)
        if not isinstance(answer, dict) or answer.get("database") != "ok":
            raise HttpError(f"Grafana is not healthy: {answer}")
        return answer

    async def annotate(self, *, time_s: float, text: str, tags: Sequence[str], end_s: float | None = None) -> int:
        """An organisation-wide annotation (shown on every dashboard that asks for its tags).
        A region when `end_s` is given. Returns its id."""
        body: dict[str, Any] = {"time": int(time_s * 1000), "text": text, "tags": list(tags)}
        if end_s is not None:
            body["timeEnd"] = int(end_s * 1000)
        answer = await request_json("POST", f"{self.url}/api/annotations", headers=self._headers, body=body,
                                    timeout_s=self.timeout_s)
        if not isinstance(answer, dict) or "id" not in answer:
            raise HttpError(f"annotation not stored: {answer}")
        return int(answer["id"])

    def dashboard_link(self, uid: str, start_s: float, end_s: float, variables: Mapping[str, str]) -> str:
        query = {"from": int(start_s * 1000), "to": int(end_s * 1000),
                 **{f"var-{k}": v for k, v in variables.items()}}
        return f"{self.url}/d/{uid}?{urllib.parse.urlencode(query)}"
