"""What a run left behind, as the reports read it. Tolerant by design: a run whose harness died
leaves no results.json, and the report must say so rather than skip the run."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

RESULTS_FILE = "results.json"
EVENTS_FILE = "events.jsonl"


@dataclass
class RunEvidence:
    run_dir: Path
    results: dict[str, Any] | None              # None: no result was recorded
    events: list[dict[str, Any]] = field(default_factory=list)
    malformed_event_lines: int = 0
    events_missing: bool = False


def load_run(run_dir: Path | str) -> RunEvidence:
    run_dir = Path(run_dir)
    results = None
    path = run_dir / RESULTS_FILE
    if path.is_file():
        try:
            results = json.loads(path.read_text())
        except (OSError, ValueError):
            results = None
    events: list[dict[str, Any]] = []
    bad = 0
    ev_path = run_dir / EVENTS_FILE
    if ev_path.is_file():
        with ev_path.open(encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    events.append(json.loads(line))
                except ValueError:
                    bad += 1                      # a torn last line after a crash, say
    return RunEvidence(run_dir=run_dir, results=results, events=events,
                       malformed_event_lines=bad, events_missing=not ev_path.is_file())
