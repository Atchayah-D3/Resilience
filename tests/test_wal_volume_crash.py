"""NL-C-04 -- Recovery time versus WAL volume (Framework §10.2, Tier 1, P0).

Blocked by infrastructure: 16 GB of un-checkpointed WAL needs a dedicated pg_wal volume
(docs/infra-requirements.md, Request 1). The run plan must skip it and the orchestrator must
refuse it until a profile declares `infra: [dedicated_wal_volume]`.

The negative tests run it against a profile that declares the volume, and check that each
failure is caused by the predicate under test -- not by a harness error that fails the run
for some other reason.
"""

from __future__ import annotations

import pytest

from catalog.schema import load_catalog
from resilience_tests.adapters.base import IntegrityResult
from resilience_tests.analysis.rto_decomposer import RtoDecomposition
from resilience_tests.control import orchestrator as orch_mod
from resilience_tests.control.matrix import expand
from tests.test_orchestrator import OutageAdapter, env, outcomes, run, why  # noqa: F401 (fixture)


def test_nl_c_04_catalog_specification():
    sc = load_catalog().scenarios["NL-C-04"]
    assert sc.priority == "P0" and sc.fault.type == "process_kill"
    assert sc.needs_infra == ["dedicated_wal_volume"]
    # the unsourced 120 s is gone; the placeholder is NL-C-01's Platinum budget
    assert "rto_first_write_s <= 60" in sc.accept
    assert not any("120" in a for a in sc.accept)


def test_run_plan_skips_nl_c_04_without_a_wal_volume(env):
    plan, skipped = expand(load_catalog(), env, reference_env_class=env.env_class, only={"NL-C-04"})
    assert plan == []
    (skip,) = skipped
    assert "blocked by infrastructure" in skip.reason and "dedicated_wal_volume" in skip.reason


def test_orchestrator_refuses_nl_c_04_without_a_wal_volume(env):
    results = run(env, "NL-C-04")
    assert results["status"] == "aborted", why(results)
    assert "dedicated_wal_volume" in results["error"]
    assert results["timing"]["t0_mono_ns"] is None     # nothing was killed


@pytest.fixture
def wal_env(env):
    return env.model_copy(update={"infra": ["dedicated_wal_volume"]})


def failed_predicates(results):
    return {p for p, o in outcomes(results).items() if o != "pass"}


def test_nl_c_04_runs_once_the_volume_is_declared(wal_env):
    results = run(wal_env, "NL-C-04")
    assert results["status"] == "passed", why(results)


def test_nl_c_04_fails_on_excessive_rto(wal_env, monkeypatch):
    orig = orch_mod.decompose

    def slow(*args, **kwargs):
        d = orig(*args, **kwargs)
        return RtoDecomposition(t0_ns=d.t0_ns, rto_first_write_s=135.0, rto_to_slo_s=d.rto_to_slo_s,
                                slo_window_end_s=d.slo_window_end_s, components=d.components,
                                outage_observed=d.outage_observed)

    monkeypatch.setattr(orch_mod, "decompose", slow)
    results = run(wal_env, "NL-C-04")
    assert results["status"] == "failed", why(results)
    # Was: filtered on outcome != "passed" while the evaluator says "pass", so every
    # predicate matched and the assertion could not fail.
    assert failed_predicates(results) == {"rto_first_write_s <= 60"}, why(results)


def test_nl_c_04_fails_on_structural_corruption(wal_env, monkeypatch):
    async def corrupt(self, timeout_s):
        # Was: IntegrityResult(checked=..., errors=...) -- fields that do not exist, so a
        # TypeError failed the run and the "corruption" was never evaluated.
        return IntegrityResult(structural_errors=3, checksum_failures=0)

    monkeypatch.setattr(OutageAdapter, "integrity_check", corrupt)
    results = run(wal_env, "NL-C-04")
    assert results["status"] == "failed", why(results)
    assert results["measured"]["structural_integrity_errors"] == 3
    assert failed_predicates(results) == {"corruption_count == 0", "structural_integrity_errors == 0"}, why(results)
