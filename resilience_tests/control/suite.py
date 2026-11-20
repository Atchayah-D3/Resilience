"""The sequential runner (specs/007, attach story): ONE common workload for the whole resilience run,
the scenarios one after another against it.

    python -m resilience_tests.control.suite --env e2-u22-3 --workload workloads/oltp-common.yaml \\
        --scenario NL-N-I03a --scenario NL-N-I03b [--cooldown 60] [--use-running]

    1. start the common workload once (or, with --use-running, use the one already running)
    2. wait until it is running past its warm-up
    3. for each scenario in the run plan: cooldown, then wait until the workload is healthy again
       (alive, running, doing its work), then the scenario's nine phases, attached to it
    4. after the last scenario -- or on a failure or Ctrl-C -- stop the workload once (only if this
       runner started it), and print each scenario's result

A scenario never starts or stops the workload. A workload that does not recover between scenarios
ends the suite: the remaining scenarios are reported as not run, never as passed.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from catalog.schema import load_catalog
from resilience_tests.control.matrix import RunPlanItem, expand
from resilience_tests.control.orchestrator import RunOptions, run_item
from resilience_tests.control.profile import EnvProfile, load_profile
from resilience_tests.execution.workload.attached import AttachedWorkload, AttachRefused, find_folder, wait_for_health
from resilience_tests.execution.workload.service import client

COOLDOWN_S = 60.0              # between scenarios: the previous fault's after-effects settle
HEALTH_WAIT_S = 600.0          # then at most this long for the workload to be healthy again
START_WAIT_S = 3 * 3600.0      # the workload's start may load the TPC-C data before its warm-up
SUITE_FILE = "suite.json"


@dataclass
class SuiteResult:
    workload_folder: str | None = None
    started_here: bool = False
    runs: list[dict[str, Any]] = field(default_factory=list)
    stopped: dict[str, Any] | None = None
    error: str | None = None

    @property
    def passed(self) -> bool:
        return self.error is None and bool(self.runs) and all(r["status"] == "passed" for r in self.runs)


class SuiteRunner:
    """Start the workload once, run the plan against it, stop it once."""

    def __init__(self, profile: EnvProfile, plan: list[RunPlanItem], *, workload_config: str | None,
                 use_running: bool = False, cooldown_s: float = COOLDOWN_S, health_wait_s: float = HEALTH_WAIT_S,
                 start_wait_s: float = START_WAIT_S, options: RunOptions | None = None, say: Any = print) -> None:
        if not use_running and not workload_config:
            raise ValueError("give the workload's settings file (--workload), or --use-running")
        self.profile = profile
        self.plan = plan
        self.workload_config = workload_config
        self.use_running = use_running
        self.cooldown_s = cooldown_s
        self.health_wait_s = health_wait_s
        self.start_wait_s = start_wait_s
        self.options = RunOptions(**{**(options.__dict__ if options else {}), "attach_workload": True})
        self.say = say
        self.result = SuiteResult()

    def run(self) -> SuiteResult:
        node = self.plan[0].node if self.plan else self.profile.nodes[0]
        try:
            folder = self._workload(node)
            for i, item in enumerate(self.plan):
                if i > 0 and self.cooldown_s:
                    self.say(f"cooldown {self.cooldown_s:g} s before {item.scenario.id}")
                    time.sleep(self.cooldown_s)
                problems = self._wait_healthy(folder)
                if problems:
                    self.result.error = (f"the common workload is not healthy before {item.scenario.id}: "
                                         + "; ".join(problems))
                    self._not_run(self.plan[i:], self.result.error)
                    break
                self.say(f"== {item.scenario.id}: attached to {folder.name}")
                results = asyncio.run(run_item(item, self.profile, self.options))
                self._record(item, results)
        except (AttachRefused, client.ServiceFailed, TimeoutError) as exc:
            self.result.error = f"{type(exc).__name__}: {exc}"
            self._not_run(self.plan[len(self.result.runs):], self.result.error)
        finally:
            self._stop_workload()
        return self.result

    def _workload(self, node: Any) -> Path:
        if self.use_running:
            folder = find_folder(self.profile, node)
            self.say(f"using the running common workload {folder}")
        else:
            assert self.workload_config is not None
            folder = client.start(self.profile.name, self.workload_config, node=node.name)
            self.say(f"common workload started: {folder}; waiting until it runs past its warm-up")
        # recorded at once: an interrupt during the (long) start-up must still stop what was started
        self.result.workload_folder, self.result.started_here = str(folder), not self.use_running
        client.wait_until_running(folder, self.start_wait_s)
        return folder

    def _wait_healthy(self, folder: Path) -> list[str]:
        # read only: the health check reads the workload's own files and copies nothing
        return wait_for_health(AttachedWorkload(folder, None), self.health_wait_s)

    def _record(self, item: RunPlanItem, results: dict[str, Any]) -> None:
        run = {"scenario": item.scenario.id, "run_id": results.get("run_id"), "status": results.get("status"),
               "error": results.get("error"), "evidence_dir": results.get("evidence_dir")}
        try:
            from resilience_tests.reporting.html import render_run
            run["report"] = str(render_run(results["evidence_dir"]))
        except Exception as exc:  # noqa: BLE001 -- the verdict stands; the page is a view of it
            run["report_error"] = f"{type(exc).__name__}: {exc}"
        self.result.runs.append(run)
        self.say(f"== {item.scenario.id}: {str(run['status']).upper()}"
                 + (f" ({run['error']})" if run.get("error") else "") + f"  {run.get('evidence_dir')}")

    def _not_run(self, items: list[RunPlanItem], why: str) -> None:
        for item in items:
            self.result.runs.append({"scenario": item.scenario.id, "run_id": None, "status": "not_run", "error": why})

    def _stop_workload(self) -> None:
        folder = self.result.workload_folder
        if not folder or not self.result.started_here:
            return          # a workload this runner did not start is left running
        self.say("stopping the common workload (its end checks may take minutes)")
        try:
            self.result.stopped = client.stop(folder, by="suite runner") or {"note": "still stopping"}
        except Exception as exc:  # noqa: BLE001 -- reported; the kill switch can stop it (--stop-workload)
            self.result.stopped = {"error": f"{type(exc).__name__}: {exc}"}


def build_plan(profile: EnvProfile, scenarios: list[str], reference_class: str = "E2") -> tuple[list[RunPlanItem], list[Any]]:
    catalog = load_catalog()
    if catalog.errors:
        raise SystemExit("catalog does not load: " + "; ".join(f"{e.path.name}: {e.message}" for e in catalog.errors))
    plan, skipped = expand(catalog, profile, reference_env_class=reference_class, only=set(scenarios) or None)
    order = {sid: i for i, sid in enumerate(scenarios)}
    plan.sort(key=lambda it: order.get(it.scenario.id, len(order)))    # the order given on the command line
    return plan, skipped


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env", required=True, help="environment profile (envs/<name>.yaml)")
    ap.add_argument("--scenario", action="append", default=[], help="scenario id, in run order (repeatable)")
    ap.add_argument("--workload", help="the common workload's settings file (e.g. workloads/oltp-common.yaml)")
    ap.add_argument("--use-running", action="store_true", help="use the workload already running; never stop it")
    ap.add_argument("--cooldown", type=float, default=COOLDOWN_S, help=f"seconds between scenarios (default {COOLDOWN_S:g})")
    ap.add_argument("--health-wait", type=float, default=HEALTH_WAIT_S,
                    help=f"seconds to wait for a healthy workload before each scenario (default {HEALTH_WAIT_S:g})")
    ap.add_argument("--start-wait", type=float, default=START_WAIT_S, help="seconds for the workload to start and warm up")
    ap.add_argument("--reference-class", default="E2")
    args = ap.parse_args(argv)
    if not args.scenario:
        ap.error("give at least one --scenario")
    profile = load_profile(args.env)
    plan, skipped = build_plan(profile, args.scenario, args.reference_class)
    for s in skipped:
        print(f"skipped {s.scenario_id}: {s.reason}")
    if not plan:
        print("nothing to run")
        return 2
    runner = SuiteRunner(profile, plan, workload_config=args.workload, use_running=args.use_running,
                         cooldown_s=args.cooldown, health_wait_s=args.health_wait, start_wait_s=args.start_wait)
    try:
        result = runner.run()
    except KeyboardInterrupt:
        print("interrupted: the workload was asked to stop if this runner started it", file=sys.stderr)
        return 130
    out = Path(profile.driver_host.run_dir).expanduser() / f"suite-{time.strftime('%Y%m%d-%H%M%S')}.json"
    out.write_text(json.dumps(result.__dict__, indent=2, default=str) + "\n")
    print("\nscenario          status       evidence")
    for r in result.runs:
        print(f"{r['scenario']:<17} {str(r['status']).upper():<12} {r.get('evidence_dir') or r.get('error') or ''}")
    if result.error:
        print(f"suite stopped: {result.error}")
    print(f"suite record: {out}")
    return 0 if result.passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
