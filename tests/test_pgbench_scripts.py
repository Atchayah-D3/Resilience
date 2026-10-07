"""Tests for pgbench scripts and shape derivation (spec US4, tasks T040, T041)."""

import pytest

from catalog.schema import Workload, load_catalog
from resilience_tests.adapters.postgresql.adapter import PostgreSQLAdapter
from resilience_tests.control.profile import load_profile
from resilience_tests.execution.workload.driver import UnsupportedWorkload
from resilience_tests.execution.workload.interface import derive_shape


@pytest.fixture
def adapter():
    profile = load_profile("e2-dedicated-vm")
    return PostgreSQLAdapter(profile.nodes[0])


def test_marker_script(adapter):
    spec = adapter.pgbench_launch(shape="marker", launch=1, client=2)
    assert "\\set seq :seq + 1" in spec.script
    assert "md5('L'||:launch||'-C'||:client||'-S'||:seq)::uuid" in spec.script
    assert "INSERT INTO resilience.markers" in spec.script
    assert spec.variables["launch"] == 1
    assert spec.variables["client"] == 2
    assert spec.variables["seq"] == 0
    # No passwords or host literals in script
    assert "password" not in spec.script.lower()
    assert adapter.node.client.host not in spec.script


def test_churn_script(adapter):
    spec = adapter.pgbench_launch(shape="churn", launch=3, client=4)
    assert "\\set seq :seq + 1" in spec.script
    assert "random(1, :churn_keys)" in spec.script
    assert "resilience.churn" in spec.script
    assert "\\if" in spec.script
    assert "DELETE FROM resilience.churn" in spec.script
    assert "UPDATE resilience.churn" in spec.script
    assert spec.variables["churn_keys"] == adapter.churn_key_space
    assert spec.variables["replace_every"] == 10
    assert "password" not in spec.script.lower()
    assert adapter.node.client.host not in spec.script


def test_list_append_script(adapter):
    spec = adapter.pgbench_launch(shape="list_append", launch=5, client=0)
    assert "\\set seq :seq + 1" in spec.script
    assert "resilience.lists" in spec.script
    assert "\\gset" in spec.script
    assert "SELECT v AS read_v" in spec.script
    assert "RETURNING v AS append_v \\gset" in spec.script
    assert "password" not in spec.script.lower()
    assert adapter.node.client.host not in spec.script


def test_shape_derivation():
    catalog = load_catalog()

    # NL-C-01 is oltp_write_heavy -> marker
    wl_c01 = catalog.scenarios["NL-C-01"].workload
    assert derive_shape(wl_c01) == "marker"

    # NL-C-05 is mixed -> churn
    wl_c05 = catalog.scenarios["NL-C-05"].workload
    assert wl_c05.profile == "mixed"
    assert derive_shape(wl_c05) == "churn"

    # NL-C-03 has history: list_append -> list_append
    wl_c03 = catalog.scenarios["NL-C-03"].workload
    assert wl_c03.history == "list_append"
    assert derive_shape(wl_c03) == "list_append"


def test_shape_derivation_refusals():
    # Unsupported profile (oltp_read is not supported by driver yet)
    with pytest.raises(UnsupportedWorkload, match="not built yet"):
        derive_shape(Workload(profile="oltp_read", concurrency=1, rate_tps=10, transaction_markers=True))

    # Missing transaction markers
    with pytest.raises(UnsupportedWorkload, match="not built yet"):
        derive_shape(Workload(profile="oltp_write_heavy", concurrency=1, rate_tps=10, transaction_markers=False))

    # Combined churn and list_append is refused
    with pytest.raises(UnsupportedWorkload, match="not combined"):
        derive_shape(Workload(profile="mixed", history="list_append", concurrency=1, rate_tps=10, transaction_markers=True))
