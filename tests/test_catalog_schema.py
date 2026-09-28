import copy
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from catalog.schema import CATALOG_ROOT, Catalog, Scenario, load_catalog, load_reference, run_checks

NLC01 = yaml.safe_load((CATALOG_ROOT / "NL" / "NL-C-01.yaml").read_text())


def variant(**changes):
    d = copy.deepcopy(NLC01)
    for dotted, value in changes.items():
        cur = d
        *path, last = dotted.split("__")
        for p in path:
            cur = cur[p]
        cur[last] = value
    return d


def test_nlc01_loads():
    sc = Scenario.model_validate(NLC01)
    assert sc.id == "NL-C-01" and sc.fault.type == "process_kill" and not sc.environment.env_sensitive
    assert set(sc.accept) >= {"rpo_txn == 0", "rto_first_write_s <= 60", "structural_integrity_errors == 0"}
    assert "transactional_markers" in sc.requires


@pytest.mark.parametrize("leak", ["virsh destroy", "sysrq", "10.11.21.112", "ipmitool power off", "libvirt"])
def test_scenario_must_not_name_hosts_or_drivers(leak):
    with pytest.raises(ValidationError, match="envs/"):
        Scenario.model_validate(variant(description=f"power off via {leak}"))


def test_fault_driver_must_be_profile_section():
    # the scenario names an env-profile SECTION, never a driver
    with pytest.raises(ValidationError, match="os_ssh"):
        Scenario.model_validate(variant(fault__driver="systemd"))


def test_blast_radius_above_one_outside_clx_fails_closed():
    with pytest.raises(ValidationError, match="max_nodes_affected must be 1"):
        Scenario.model_validate(variant(blast_radius__max_nodes_affected=2))


def test_compound_only_in_clx():
    compound = {"fault_b": {"trigger": "on_state", "condition": "patroni_state == 1", "timeout_s": 120,
                            "type": "process_kill", "target": "sync_standby"},
                "blast_radius_override": {"max_nodes_affected": 2}}
    with pytest.raises(ValidationError, match="only permitted in category CL-X"):
        Scenario.model_validate(variant(compound=compound))


def test_measures_must_declare_the_capability_they_need():
    with pytest.raises(ValidationError, match="structural_integrity_check"):
        Scenario.model_validate(variant(requires=["transactional_markers"]))


def test_rpo_requires_transaction_markers():
    with pytest.raises(ValidationError, match="transaction_markers"):
        Scenario.model_validate(variant(workload__transaction_markers=False))


def test_accept_must_use_declared_measures():
    with pytest.raises(ValidationError, match="undeclared measures"):
        Scenario.model_validate(variant(accept=["rpo_txn == 0", "made_up_s <= 1"]))


def test_accept_must_be_falsifiable():
    with pytest.raises(ValidationError):
        Scenario.model_validate(variant(accept=["graceful degradation"]))


def test_production_is_never_a_permitted_environment():
    with pytest.raises(ValidationError):
        Scenario.model_validate(variant(blast_radius__environments=["production"]))


def test_file_name_must_match_id(tmp_path: Path):
    (tmp_path / "NL").mkdir()
    (tmp_path / "CL").mkdir()
    (tmp_path / "DX").mkdir()
    (tmp_path / "NL" / "NL-C-09.yaml").write_text(yaml.safe_dump(NLC01))
    cat = load_catalog(tmp_path)
    assert not cat.scenarios and "must be NL-C-01.yaml" in cat.errors[0].message


def test_six_checks_on_current_catalog():
    results = {r.number: r for r in run_checks(load_catalog(), load_reference())}
    # the checks that must always hold, however much of the catalog is authored
    assert results[1].passed and results[3].passed and results[4].passed and results[5].passed
    # only part of the framework catalog is authored here, so contiguity and reconciliation
    # report exactly what is missing rather than passing quietly
    assert not results[6].passed
    if not results[2].passed:
        assert all("missing" in d for d in results[2].detail)


def test_check5_flags_unknown_section():
    sc = Scenario.model_validate(variant(description="see §99.9 for details"))
    results = {r.number: r for r in run_checks(Catalog({sc.id: sc}), load_reference())}
    assert not results[5].passed


def test_check4_flags_dangling_reference():
    sc = Scenario.model_validate(variant(see_also=["CL-R-04"]))
    results = {r.number: r for r in run_checks(Catalog({sc.id: sc}), load_reference())}
    assert not results[4].passed and "CL-R-04" in results[4].detail[0]


def test_reference_is_internally_consistent():
    results = {r.number: r for r in run_checks(Catalog(), load_reference())}
    assert not [d for d in results[6].detail if d.startswith("reference")]
