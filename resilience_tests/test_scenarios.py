"""Scenario execution: one pytest case per run-plan item (catalog x environment x target).

    pytest resilience_tests/test_scenarios.py --env e2-dedicated-vm --scenario NL-D-02 \
        --target-is-disposable --junitxml=reports/junit.xml

The pass/fail of each case is the mechanically evaluated verdict (Arch §10.2).
"""

from __future__ import annotations

import asyncio
import json

import pytest

from catalog.schema import load_catalog
from resilience_tests.control.matrix import RunPlanItem, expand
from resilience_tests.control.orchestrator import RunOptions, run_item
from resilience_tests.control.profile import load_profile


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if "plan_item" not in metafunc.fixturenames:
        return
    env = metafunc.config.getoption("--env")
    if not env:
        metafunc.parametrize("plan_item", [pytest.param(None, marks=pytest.mark.skip(reason="--env not given"))])
        return
    catalog = load_catalog()
    if catalog.errors:
        raise pytest.UsageError("catalog does not load: " + "; ".join(f"{e.path.name}: {e.message}" for e in catalog.errors))
    profile = load_profile(env)
    only = set(metafunc.config.getoption("--scenario")) or None
    plan, skipped = expand(catalog, profile, reference_env_class=metafunc.config.getoption("--reference-class"), only=only)
    params = [pytest.param(item, id=item.run_key) for item in plan]
    params += [pytest.param(None, id=s.scenario_id, marks=pytest.mark.skip(reason=s.reason)) for s in skipped]
    metafunc.parametrize("plan_item", params)


def test_scenario(plan_item: RunPlanItem, request: pytest.FixtureRequest) -> None:
    cfg = request.config
    profile = load_profile(cfg.getoption("--env"))
    options = RunOptions(
        target_is_disposable=cfg.getoption("--target-is-disposable"),
        stop_before_fault=cfg.getoption("--stop-before-fault"),
    )
    results = asyncio.run(run_item(plan_item, profile, options))
    request.node.user_properties.append(("results", results["evidence_dir"]))
    summary = json.dumps({k: results[k] for k in ("run_id", "status", "error", "measured", "evidence_dir")}, indent=2, default=str)
    if results["status"] == "stopped_before_fault" and options.stop_before_fault:
        pytest.skip(f"dry run completed (no fault, no verdict):\n{summary}")
    assert results["status"] == "passed", f"{plan_item.run_key} {results['status'].upper()}\n{summary}"
