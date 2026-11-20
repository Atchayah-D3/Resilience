"""The common workload's settings file (workloads/<id>.yaml), checked before anything starts.

The workload section uses the scenarios' own rules (catalog.schema.Workload), so a workload is
described one way everywhere, and any profile a scenario uses can run as the common workload
(oltp_write_heavy, mixed with the churn table, tpcc, ...). Not yet: Elle history (list_append). The settings actually used -- with `by_cache` sizes and a share of
capacity turned into numbers -- are written into the run's summary.json at the end.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from catalog.schema import Workload


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class BaselineRules(_Model):
    """When the measured part of a run (after the warm-up) counts as a valid baseline.
    Every value is NEEDS SIGN-OFF until the lead agrees it (constitution: cite or sign off)."""

    # throughput: every full window of this length keeps at least this share of the rate
    tps_window_s: int = Field(default=60, gt=0)
    tps_share_min: float = Field(default=0.95, gt=0, le=1)
    # latency: the p99 of all the transactions of each window of this length stays at or
    # under the limit (histograms summed: a stall of a second or two inside a window shows)
    p99_window_s: int = Field(default=10, gt=0)
    p99_limit_ms: float = Field(default=3000.0, gt=0)
    # errors (server rollbacks other than TPC-C's business rollbacks, and unknown outcomes) as a
    # share of all transactions; dropped connections are reported separately and must be 0
    error_share_max: float = Field(default=0.0001, ge=0)
    # the harness's memory may grow at most this share between `memory_from_s` and the end
    memory_growth_max: float = Field(default=0.10, ge=0)
    memory_from_s: float = Field(default=600.0, ge=0)


class ServiceConfig(_Model):
    id: str = Field(pattern=r"^[A-Za-z0-9_.-]+$")
    description: str = ""
    # how long the customers run in total, the warm-up included; null: until the stop command (a
    # suite of scenarios that takes as long as it takes; the runner stops the workload at its end)
    duration_s: float | None = Field(gt=0)
    # the first part of the run, not measured (caches fill, the workload settles)
    warmup_s: float = Field(default=300.0, ge=0)
    # one summary line per this many seconds (intervals-<n>s.jsonl), next to the 1-second lines
    metrics_interval_s: int = Field(default=10, ge=1)
    # status.json is rewritten this often
    status_interval_s: float = Field(default=5.0, gt=0)
    # after a scenario's run_end mark, this long is still counted as the scenario's (its effects
    # settling) and not judged as the workload's baseline
    scenario_settle_s: float = Field(default=0.0, ge=0)
    # no transaction committed for this long while running: an alert in the status and events;
    # the workload keeps trying (a fault may keep the database down that long)
    no_commit_alert_s: float = Field(default=600.0, gt=0)
    # always reload the TPC-C data at the start (otherwise only when it is missing, of another
    # size, grown past the limit or inconsistent). Never during the run.
    fresh_load: bool = False
    workload: Workload
    baseline: BaselineRules = BaselineRules()

    @model_validator(mode="after")
    def _common_workload(self) -> ServiceConfig:
        w = self.workload
        if w.profile == "idle":
            raise ValueError("workload.profile idle sends no transactions; the common workload needs a load")
        if not w.transaction_markers:
            raise ValueError("workload.transaction_markers must be true: without the journals no lost "
                             "commit could be found (Arch §6.2)")
        if w.history != "none":
            raise ValueError("workload.history list_append: the common workload does not record Elle history yet "
                             "(NL-C-03 keeps its own workload until it does)")
        if self.duration_s is not None and self.warmup_s >= self.duration_s:
            raise ValueError(f"warmup_s ({self.warmup_s:g}) must be shorter than duration_s ({self.duration_s:g})")
        return self

    @property
    def until_stopped(self) -> bool:
        return self.duration_s is None

    def as_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


def load_config(path: str | Path) -> ServiceConfig:
    p = Path(path).expanduser()
    try:
        raw = yaml.safe_load(p.read_text())
    except OSError as exc:
        raise ValueError(f"cannot read {p}: {exc.strerror}") from None
    if not isinstance(raw, dict):
        raise ValueError(f"{p}: expected a mapping of settings")
    return ServiceConfig.model_validate(raw)
