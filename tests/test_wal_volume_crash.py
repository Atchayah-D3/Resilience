"""NL-C-04 -- Recovery time versus WAL volume (Framework §10.2, Tier 1, P0).

Verifies database crash recovery when a large volume of uncheckpointed WAL records
has accumulated under sustained heavy write load (rate_tps: 500).

Crash recovery must:
1. Replay all accumulated WAL records within the RTO budget (rto_first_write_s <= 120).
2. Restore cleanly and unattended (starts_unattended == True).
3. Preserve all acknowledged transactions (rpo_txn == 0).
4. Suffer zero structural integrity corruptions (structural_integrity_errors == 0, corruption_count == 0).
"""

from __future__ import annotations

import pytest

from catalog.schema import load_catalog
from resilience_tests.analysis.rto_decomposer import RtoDecomposition
from resilience_tests.control import orchestrator as orch_mod
from tests.test_orchestrator import OUTAGE_S, env, run, why


def test_nl_c_04_catalog_specification():
    """Verify that NL-C-04 conforms strictly to Framework §10.2 / Arch §5 catalog requirements."""
    catalog = load_catalog()
    assert "NL-C-04" in catalog.scenarios
    sc = catalog.scenarios["NL-C-04"]

    assert sc.id == "NL-C-04"
    assert sc.name == "Recovery time versus WAL volume"
    assert sc.tier == "node_local"
    assert sc.category == "NL-C"
    assert sc.priority == "P0"
    assert sc.workload.rate_tps == 500
    assert sc.workload.concurrency == 64
    assert sc.workload.profile == "oltp_write_heavy"
    assert sc.fault.type == "process_kill"
    assert sc.fault.driver == "os_ssh"
    assert sc.fault.target == "primary"

    # Capability requirements
    assert "transactional_markers" in sc.requires
    assert "structural_integrity_check" in sc.requires

    # Acceptance criteria per Framework §10.2 table
    accept_text = " ".join(sc.accept)
    assert "rpo_txn == 0" in accept_text
    assert "rto_first_write_s <= 120" in accept_text
    assert "structural_integrity_errors == 0" in accept_text
    assert "corruption_count == 0" in accept_text
    assert "starts_unattended == true" in accept_text


def test_nl_c_04_orchestrator_execution(env):
    """End-to-end execution of NL-C-04 through orchestrator validating all acceptance gates."""
    results = run(env, "NL-C-04")
    assert results["status"] == "passed", why(results)

    measured = results["measured"]
    assert measured["rpo_txn"] == 0
    assert measured["rto_first_write_s"] is not None
    assert measured["rto_first_write_s"] <= 120
    assert measured["starts_unattended"] is True
    assert measured["structural_integrity_errors"] == 0
    assert measured["corruption_count"] == 0


def test_nl_c_04_fails_on_excessive_rto(env, monkeypatch):
    """Negative test: if recovery time exceeds the 120s RTO envelope, verdict must fail."""
    orig_decompose = orch_mod.decompose

    def slow_decompose(*args, **kwargs):
        decomp = orig_decompose(*args, **kwargs)
        return RtoDecomposition(
            t0_ns=decomp.t0_ns,
            rto_first_write_s=135.0,  # Violates rto_first_write_s <= 120
            rto_to_slo_s=decomp.rto_to_slo_s,
            slo_window_end_s=decomp.slo_window_end_s,
            components=decomp.components,
            outage_observed=decomp.outage_observed,
        )

    monkeypatch.setattr(orch_mod, "decompose", slow_decompose)
    results = run(env, "NL-C-04")
    assert results["status"] == "failed", why(results)
    verdict = results.get("verdict", {})
    failed_results = [r for r in verdict.get("results", []) if r.get("outcome") != "passed"]
    assert any("rto_first_write_s" in r.get("predicate", "") for r in failed_results)


def test_nl_c_04_fails_on_structural_corruption(env, monkeypatch):
    """Negative test: if amcheck finds integrity corruption after recovery, verdict must fail."""
    from resilience_tests.adapters.base import IntegrityResult
    from tests.test_orchestrator import OutageAdapter

    orig_integrity_check = OutageAdapter.integrity_check

    async def corrupt_integrity_check(self, timeout_s):
        return IntegrityResult(
            checked=True,
            errors=3,
            detail={"errors": ["heap page corrupt: line pointer 12 offset out of range"]},
        )

    monkeypatch.setattr(OutageAdapter, "integrity_check", corrupt_integrity_check)
    results = run(env, "NL-C-04")
    assert results["status"] == "failed", why(results)
    verdict = results.get("verdict", {})
    failed_results = [r for r in verdict.get("results", []) if r.get("outcome") != "passed"]
    assert any("structural_integrity_errors" in r.get("predicate", "") for r in failed_results)
