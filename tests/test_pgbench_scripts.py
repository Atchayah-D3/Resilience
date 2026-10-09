"""Tests for pgbench scripts and shape derivation (spec US4, tasks T040, T041)."""

import pytest

from catalog.schema import Workload, load_catalog
from resilience_tests.adapters.postgresql.adapter import PostgreSQLAdapter
from resilience_tests.control.profile import load_profile
from resilience_tests.execution.workload.driver import UnsupportedWorkload
from resilience_tests.execution.workload.interface import derive_shape


RUN = "NL-C-01-20261008T092214Z-bbe290"


@pytest.fixture
def adapter():
    profile = load_profile("e2-dedicated-vm")
    return PostgreSQLAdapter(profile.nodes[0])


def _clean(script, adapter):
    assert "password" not in script.lower()
    assert adapter.node.client.host not in script


def test_marker_script_is_the_built_in_marker_transaction(adapter):
    spec = adapter.pgbench_launch("marker", RUN)
    assert spec.script.splitlines() == [
        "BEGIN;",
        "INSERT INTO resilience.markers(uuid, seq, ts) "
        f"VALUES (md5('resilience-pgbench-{RUN}-' || :seq)::uuid, :seq, clock_timestamp());",
        "COMMIT;",
    ]
    assert spec.connection == adapter.node.client and spec.application_name == "resilience-pgbench"
    _clean(spec.script, adapter)


def test_marker_uuid_is_derived_as_the_script_derives_it(adapter):
    import hashlib
    import uuid
    expected = str(uuid.UUID(hashlib.md5(f"resilience-pgbench-{RUN}-42".encode()).hexdigest()))
    assert adapter.pgbench_marker_uuid(42, RUN) == expected


def test_marker_uuids_differ_between_runs(adapter):
    """seq restarts at 1 every run: without the run id, a marker an earlier run left behind would
    carry the same id as this run's, and a commit this run lost would look present."""
    other = "NL-C-01-20261009T000000Z-000000"
    assert adapter.pgbench_marker_uuid(1, RUN) != adapter.pgbench_marker_uuid(1, other)
    assert other not in adapter.pgbench_launch("marker", RUN).script


def test_a_run_id_that_is_not_a_safe_literal_is_refused(adapter):
    for bad in ("x'); DROP TABLE resilience.markers; --", "with space", ""):
        with pytest.raises(ValueError):
            adapter.pgbench_launch("marker", bad)
        with pytest.raises(ValueError):
            adapter.pgbench_marker_uuid(1, bad)


def test_churn_script_is_one_transaction_with_the_built_in_statements(adapter):
    lines = adapter.pgbench_launch("churn", RUN).script.splitlines()
    assert lines[0] == "BEGIN;" and lines[-1] == "COMMIT;"
    assert lines[1].startswith("INSERT INTO resilience.markers")      # the marker first, as built-in
    assert lines[2:] == [
        "\\if :creplace",
        "DELETE FROM resilience.churn WHERE id = :ckey;",
        "INSERT INTO resilience.churn (id, payload) VALUES (:ckey, repeat('y', 180)) ON CONFLICT (id) DO NOTHING;",
        "\\else",
        "UPDATE resilience.churn SET v = v + 1, ts = clock_timestamp(), payload = repeat('y', 180) WHERE id = :ckey;",
        "\\endif",
        "COMMIT;",
    ]


def test_list_append_script_reads_append_and_reads_at_serializable(adapter):
    script = adapter.pgbench_launch("list_append", RUN, read_chunks=3, chunk_chars=10).script
    lines = script.splitlines()
    assert lines[0] == "BEGIN ISOLATION LEVEL SERIALIZABLE;" and lines[-1] == "COMMIT;"
    assert lines[1].startswith("INSERT INTO resilience.markers")
    assert "WHERE k = :rk" in lines[2] and lines[2].endswith("\\gset")
    assert lines[3].startswith("INSERT INTO resilience.elle_lists AS l (k, v) VALUES (:ak, ARRAY[:seq::bigint])")
    assert "WHERE k = :ak" in lines[4] and lines[4].endswith("\\gset")
    for var in ("r1len", "r1c1", "r1c3", "r2len", "r2c3"):
        assert f"AS {var}" in script
    assert "substr(x, 21, 10) AS r1c3" in script
    assert "ORDER BY i" in script and "e - :vbase" in script and "'nil'" in script
    _clean(script, adapter)
    with pytest.raises(ValueError):
        adapter.pgbench_launch("list_append", RUN)


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
