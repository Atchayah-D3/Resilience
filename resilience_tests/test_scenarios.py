"""Scenario execution: one pytest case per run-plan item (catalog x environment x target).

    pytest resilience_tests/test_scenarios.py --env e2-dedicated-vm --scenario NL-D-02 \
        --target-is-disposable --junitxml=reports/junit.xml

With the common workload (specs/007) -- one workload for the whole session, every scenario
attached to it, in the order given (control/common_workload.py):

    pytest resilience_tests/test_scenarios.py --env e2-u22-3 --common-workload workloads/attach-check.yaml \
        --scenario NL-N-I03a --scenario NL-N-I03b -q -rA

The pass/fail of each case is the mechanically evaluated verdict (Arch §10.2).
"""

from __future__ import annotations

import asyncio
import json

import pytest

from catalog.schema import load_catalog
from resilience_tests.control.common_workload import COMMON_WORKLOAD_KEY, CommonWorkload
from resilience_tests.control.matrix import RunPlanItem, expand
from resilience_tests.control.orchestrator import RunOptions, run_item
from resilience_tests.control.profile import load_profile
from resilience_tests.execution.workload.hooks import tpcc_override_note, with_tpcc_workload
from resilience_tests.reporting.html import render_run


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
    warehouses = metafunc.config.getoption("--tpcc-warehouses")
    if warehouses:
        for sid, sc in list(catalog.scenarios.items()):
            if only is None or sid in only:
                catalog.scenarios[sid] = with_tpcc_workload(sc, warehouses)
    plan, skipped = expand(catalog, profile, reference_env_class=metafunc.config.getoption("--reference-class"), only=only)
    order = {sid: i for i, sid in enumerate(metafunc.config.getoption("--scenario"))}
    plan.sort(key=lambda it: order.get(it.scenario.id, len(order)))     # the order given on the command line
    params = [pytest.param(item, id=item.run_key) for item in plan]
    params += [pytest.param(None, id=s.scenario_id, marks=pytest.mark.skip(reason=s.reason)) for s in skipped]
    metafunc.parametrize("plan_item", params)


@pytest.fixture(scope="session")
def common_workload(request: pytest.FixtureRequest):
    """The session's common workload (--common-workload / --attach-workload), started once before
    the first scenario and stopped once after the last -- also after a failure or Ctrl-C. None
    when the scenarios run their own workloads."""
    cfg = request.config
    settings, attach = cfg.getoption("--common-workload"), cfg.getoption("--attach-workload")
    if not settings and not attach:
        yield None
        return
    profile = load_profile(cfg.getoption("--env"))
    workload = CommonWorkload(profile, profile.nodes[0], config_path=settings,
                              cooldown_s=cfg.getoption("--cooldown"),
                              health_wait_s=cfg.getoption("--workload-health-wait"))
    cfg.stash[COMMON_WORKLOAD_KEY] = workload        # the report plugin puts its summary on index.html
    try:
        workload.start()
        yield workload
    finally:
        workload.stop()



def test_scenario(plan_item: RunPlanItem, request: pytest.FixtureRequest, common_workload: CommonWorkload | None) -> None:
    cfg = request.config
    profile = load_profile(cfg.getoption("--env"))
    warehouses = cfg.getoption("--tpcc-warehouses")
    if common_workload is not None:
        if common_workload.unhealthy:
            pytest.skip(f"not run: {common_workload.unhealthy}")
        problems = common_workload.before_scenario(plan_item.scenario.id)
        if problems:
            pytest.fail(common_workload.unhealthy or "the common workload is not healthy", pytrace=False)
    options = RunOptions(
        target_is_disposable=cfg.getoption("--target-is-disposable"),
        stop_before_fault=cfg.getoption("--stop-before-fault"),
        workload_note=tpcc_override_note(warehouses) if warehouses else None,
        tpcc_fresh_load=cfg.getoption("--tpcc-fresh-load"),
        attach_workload=common_workload is not None,
    )
    results = asyncio.run(run_item(plan_item, profile, options))
    props = request.node.user_properties
    props.append(("results", results["evidence_dir"]))
    props += [("scenario_id", plan_item.scenario.id), ("priority", plan_item.scenario.priority),
              ("status", results["status"]), ("run_id", results["run_id"])]
    # The run's own HTML page (Arch §10.4). A report that fails to render is recorded and
    # printed; it never changes the run's status (spec FR-011).
    try:
        props.append(("report", str(render_run(results["evidence_dir"]))))
    except Exception as exc:  # noqa: BLE001 -- the verdict stands; the page is a view of it
        props.append(("report_error", f"{type(exc).__name__}: {exc}"))
        print(f"run report not rendered for {results['run_id']}: {type(exc).__name__}: {exc}")
    summary = json.dumps({k: results[k] for k in ("run_id", "status", "error", "measured", "evidence_dir")}, indent=2, default=str)
    if results["status"] == "stopped_before_fault" and options.stop_before_fault:
        pytest.skip(f"dry run completed (no fault, no verdict):\n{summary}")
    assert results["status"] == "passed", f"{plan_item.run_key} {results['status'].upper()}\n{summary}"
