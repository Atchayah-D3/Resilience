"""The common workload's lifecycle for a pytest scenario session (specs/007, attach story): ONE
workload for the whole session, the scenarios one after another against it.

    pytest resilience_tests/test_scenarios.py --env e2-u22-3 --common-workload workloads/attach-check.yaml \\
        --scenario NL-N-I03a --scenario NL-N-I03b -q -rA

    session start     start the workload once (or, with --attach-workload, use the one running)
                      and wait until it runs past its warm-up
    before a scenario cooldown (not before the first), then wait until the workload is healthy again
                      (alive, running, doing its work); every scenario attaches to it
    session end       stop the workload once -- also after a failure or Ctrl-C -- only if this
                      session started it; its end summary goes into the session's index.html

A scenario never starts or stops the workload. A workload that does not recover between scenarios
fails that scenario and skips the rest with the reason -- never a pass.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from resilience_tests.control.profile import EnvProfile, Node
from resilience_tests.execution.workload.attached import AttachedWorkload, find_folder, wait_for_health
from resilience_tests.execution.workload.service import client

# where the session's CommonWorkload is kept for the report plugin (config.stash)
try:
    import pytest
    COMMON_WORKLOAD_KEY: Any = pytest.StashKey()
except ImportError:  # pragma: no cover -- pytest is how scenarios run
    COMMON_WORKLOAD_KEY = None

COOLDOWN_S = 60.0              # between scenarios: the previous fault's after-effects settle
HEALTH_WAIT_S = 600.0          # then at most this long for the workload to be healthy again
START_WAIT_S = 3 * 3600.0      # the workload's start may load the TPC-C data before its warm-up


@dataclass
class CommonWorkload:
    profile: EnvProfile
    node: Node
    config_path: str | None = None          # start a workload with these settings; None: use the running one
    cooldown_s: float = COOLDOWN_S
    health_wait_s: float = HEALTH_WAIT_S
    start_wait_s: float = START_WAIT_S
    say: Any = print
    folder: Path | None = None
    started_here: bool = False
    scenarios_run: int = 0
    unhealthy: str | None = None             # set once the workload did not recover: the rest are skipped
    stopped: dict[str, Any] | None = None
    started_wall: float | None = None
    ended_wall: float | None = None
    problems: list[str] = field(default_factory=list)

    def start(self) -> Path:
        """Start the workload (or find the running one) and wait until it runs past its warm-up."""
        self.started_wall = time.time()
        if self.config_path is None:
            self.folder = find_folder(self.profile, self.node)
            self.say(f"using the running common workload {self.folder}")
        else:
            self.folder = client.start(self.profile.name, self.config_path, node=self.node.name)
            self.started_here = True
            self.say(f"common workload started: {self.folder}; waiting until it runs past its warm-up")
        client.wait_until_running(self.folder, self.start_wait_s)
        return self.folder

    def before_scenario(self, scenario_id: str) -> list[str]:
        """Cooldown (not before the first scenario), then the health check. Empty list: go."""
        assert self.folder is not None, "start() first"
        if self.scenarios_run and self.cooldown_s:
            self.say(f"cooldown {self.cooldown_s:g} s before {scenario_id}")
            time.sleep(self.cooldown_s)
        self.scenarios_run += 1
        problems = wait_for_health(AttachedWorkload(self.folder, None), self.health_wait_s)
        if problems:
            self.unhealthy = f"the common workload is not healthy before {scenario_id}: " + "; ".join(problems)
        return problems

    def stop(self) -> None:
        """Stop the workload once, if this session started it, and keep its end summary."""
        self.ended_wall = time.time()
        if self.folder is None or not self.started_here:
            return                                  # a workload this session did not start is left running
        self.say("stopping the common workload (its end checks may take minutes)")
        try:
            self.stopped = client.stop(self.folder, by="pytest session") or {"note": "still stopping"}
        except Exception as exc:  # noqa: BLE001 -- reported; the kill switch can stop it (--stop-workload)
            self.stopped = {"error": f"{type(exc).__name__}: {exc}"}
            self.problems.append(self.stopped["error"])

    def summary(self) -> dict[str, Any]:
        """For the session's index.html: the workload, what it ran, and its own end verdict."""
        info = (client.info(self.folder) or {}) if self.folder else {}
        used = (info.get("settings") or {}).get("workload_used") or {}
        end = self.stopped or {}
        return {
            "folder": str(self.folder) if self.folder else None, "started_here": self.started_here,
            "profile": used.get("profile"), "clerks": used.get("concurrency"), "rate_tps": used.get("rate_tps"),
            "warehouses": (used.get("tpcc") or {}).get("warehouses"),
            "state": end.get("state"), "baseline_valid": end.get("baseline_valid"),
            "end_checks": {k: (end.get("end_checks") or {}).get(k)
                           for k in ("rpo_txn", "phantom", "partial_commits", "tpcc_consistency_violations")},
            "unhealthy": self.unhealthy, "problems": list(self.problems),
            "window_wall": [self.started_wall, self.ended_wall],
            "grafana_link": self._grafana_link(),
        }

    def _grafana_link(self) -> str | None:
        """The whole session on the dashboard: from the workload's start to its stop."""
        obs = self.profile.observability
        if obs is None or self.started_wall is None:
            return None
        from resilience_tests.observability.grafana import GrafanaClient
        return GrafanaClient(obs.grafana_url, token="").dashboard_link(
            obs.grafana_dashboard_uid, self.started_wall - 30, (self.ended_wall or time.time()) + 30,
            {"env": self.profile.name, "node": self.node.name, "db": self.node.db.dbname})
