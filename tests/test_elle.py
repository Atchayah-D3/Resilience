"""Unit tests for Elle history export and anomaly checker (Arch §6.3, §10.3)."""

from pathlib import Path
from resilience_tests.execution.workload.history_writer import HistoryWriter
from resilience_tests.analysis.elle_checker import ElleChecker


def test_history_writer_emits_edn(tmp_path: Path):
    history_file = tmp_path / "history.edn"
    writer = HistoryWriter(history_file)
    try:
        writer.record_invoke(process_id=0, key=1, value=100)
        writer.record_ok(process_id=0, key=1, value=100, observed_values=[100])
        writer.record_invoke(process_id=1, key=2, value=101)
        writer.record_info(process_id=1, key=2, value=101, error="indeterminate")
    finally:
        writer.close()

    assert history_file.exists()
    content = history_file.read_text()
    assert ":type :invoke" in content
    assert ":type :ok" in content
    assert ":type :info" in content
    assert ":error :indeterminate" in content


def test_elle_checker_validates_clean_history(tmp_path: Path):
    history_file = tmp_path / "history.edn"
    history_file.write_text(
        "{:index 0, :type :invoke, :f :txn, :process 0, :time 1000, :value [[:append 1 10] [:r 1 nil]]}\n"
        "{:index 1, :type :ok, :f :txn, :process 0, :time 1010, :value [[:append 1 10] [:r 1 [10]]]}\n"
        "{:index 2, :type :invoke, :f :txn, :process 1, :time 1020, :value [[:append 1 20] [:r 1 nil]]}\n"
        "{:index 3, :type :ok, :f :txn, :process 1, :time 1030, :value [[:append 1 20] [:r 1 [10 20]]]}\n"
    )
    res = ElleChecker.check(history_file)
    assert res.valid is True
    assert res.anomalies_count == 0


def test_elle_checker_detects_aborted_read_g1a(tmp_path: Path):
    history_file = tmp_path / "history.edn"
    history_file.write_text(
        "{:index 0, :type :invoke, :f :txn, :process 0, :time 1000, :value [[:append 1 99] [:r 1 nil]]}\n"
        "{:index 1, :type :fail, :f :txn, :process 0, :time 1010, :value [[:append 1 99]], :error :aborted}\n"
        "{:index 2, :type :invoke, :f :txn, :process 1, :time 1020, :value [[:append 1 100] [:r 1 nil]]}\n"
        "{:index 3, :type :ok, :f :txn, :process 1, :time 1030, :value [[:append 1 100] [:r 1 [99 100]]]}\n"
    )
    res = ElleChecker.check(history_file)
    assert res.valid is False
    assert res.anomalies_count > 0
    assert "G1a" in res.anomalies
