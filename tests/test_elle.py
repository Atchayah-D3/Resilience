"""Elle history export and checker (Arch §6.3, §10.3)."""

from pathlib import Path

import pytest

from resilience_tests.analysis.elle_checker import ELLE_JAR_PATH, ElleChecker, parse_output
from resilience_tests.execution.workload.history_writer import HistoryWriter, edn_ops

INVOKED = [("r", 1, None), ("append", 2, 100), ("r", 2, None)]


def test_history_writer_records_the_reads_it_is_given(tmp_path: Path):
    history_file = tmp_path / "history.edn"
    writer = HistoryWriter(history_file)
    try:
        writer.record("invoke", 0, INVOKED)
        writer.record("ok", 0, [("r", 1, [7, 9]), ("append", 2, 100), ("r", 2, [3, 100])])
        writer.record("invoke", 1, INVOKED)
        writer.record("info", 1, INVOKED, error="indeterminate")
    finally:
        writer.close()
    lines = history_file.read_text().splitlines()
    assert ":type :invoke" in lines[0] and "[:r 1 nil]" in lines[0]
    assert ":value [[:r 1 [7 9]] [:append 2 100] [:r 2 [3 100]]]" in lines[1]
    assert ":type :info" in lines[3] and ":error :indeterminate" in lines[3]


def test_edn_rejects_unknown_micro_ops():
    with pytest.raises(ValueError):
        edn_ops([("w", 1, 2)])
    assert edn_ops([("r", 3, [])]) == "[[:r 3 []]]"


def test_missing_jar_is_no_verdict_never_valid(tmp_path: Path):
    """Was: an in-process 'checker' that could find nothing reported valid: True."""
    history = tmp_path / "history.edn"
    history.write_text("{:index 0, :type :ok, :f :txn, :process 0, :value [[:append 1 1] [:r 1 [1]]]}\n")
    res = ElleChecker.check(history, tmp_path / "elle", jar=tmp_path / "missing.jar")
    assert res.valid is None and res.anomalies_count is None
    assert "setup_elle.sh" in (res.error or "")


def test_empty_history_is_no_verdict(tmp_path: Path):
    res = ElleChecker.check(tmp_path / "absent.edn", tmp_path / "elle")
    assert res.valid is None and "empty" in (res.error or "")


def test_parse_valid_and_invalid_output(tmp_path: Path):
    ok = parse_output("history.edn\ttrue\n", "", 0, tmp_path / "none", "serializable", 10)
    assert ok.valid is True and ok.anomalies_count == 0
    out = tmp_path / "elle"
    out.mkdir()
    (out / "G-single-item.txt").write_text("cycle")
    (out / "G2-item.txt").write_text("cycle")
    bad = parse_output("history.edn\tfalse\n", "", 1, out, "serializable", 10)
    assert bad.valid is False and bad.anomalies_count == 2
    assert bad.anomaly_types == ["G-single-item", "G2-item"]


def test_false_without_explanations_still_counts_one(tmp_path: Path):
    bad = parse_output("h.edn\tfalse\n", "", 1, tmp_path / "empty", "serializable", 1)
    assert bad.valid is False and bad.anomalies_count == 1


def test_unknown_or_garbled_output_is_no_verdict(tmp_path: Path):
    assert parse_output("h.edn\tunknown\n", "", 0, tmp_path, "serializable", 1).valid is None
    assert parse_output("", "Exception in thread main", 1, tmp_path, "serializable", 1).valid is None


@pytest.mark.skipif(not ELLE_JAR_PATH.exists(), reason="elle-cli.jar not installed (vendor/elle/setup_elle.sh)")
def test_real_elle_finds_a_dependency_cycle(tmp_path: Path):
    """End to end against the pinned jar: two transactions that each read the other's
    append form a write-read cycle (G1c), which serializability forbids.

    Not G1a: elle-cli 0.1.11 does not return on a history whose read observed an aborted
    append (it hangs). The harness then times out and reports NOT_MEASURED -- fail-closed,
    never a pass -- but a hanging test is no test, so the cycle is used here."""
    history = tmp_path / "history.edn"
    history.write_text(
        "{:index 0, :type :invoke, :f :txn, :process 0, :value [[:append 1 1] [:r 2 nil]]}\n"
        "{:index 1, :type :invoke, :f :txn, :process 1, :value [[:append 2 2] [:r 1 nil]]}\n"
        "{:index 2, :type :ok, :f :txn, :process 0, :value [[:append 1 1] [:r 2 [2]]]}\n"
        "{:index 3, :type :ok, :f :txn, :process 1, :value [[:append 2 2] [:r 1 [1]]]}\n"
    )
    res = ElleChecker.check(history, tmp_path / "elle")
    assert res.valid is False, res.raw_output
    assert res.anomalies_count >= 1


@pytest.mark.skipif(not ELLE_JAR_PATH.exists(), reason="elle-cli.jar not installed (vendor/elle/setup_elle.sh)")
def test_real_elle_accepts_a_clean_history(tmp_path: Path):
    history = tmp_path / "history.edn"
    history.write_text(
        "{:index 0, :type :invoke, :f :txn, :process 0, :value [[:append 1 10] [:r 1 nil]]}\n"
        "{:index 1, :type :ok, :f :txn, :process 0, :value [[:append 1 10] [:r 1 [10]]]}\n"
        "{:index 2, :type :invoke, :f :txn, :process 1, :value [[:append 1 20] [:r 1 nil]]}\n"
        "{:index 3, :type :ok, :f :txn, :process 1, :value [[:append 1 20] [:r 1 [10 20]]]}\n"
    )
    res = ElleChecker.check(history, tmp_path / "elle")
    assert res.valid is True, res.raw_output
