"""TPC-C build step 8: merging the numbers of several worker processes, the driver-limited
guard, and how many processes a rate needs. Pure functions, no database."""

from __future__ import annotations

import pytest

from resilience_tests.execution.workload import contract
from resilience_tests.execution.workload.driver import p99
from resilience_tests.execution.workload.tpcc import samples
from resilience_tests.execution.workload.tpcc.samples import (
    DriverLoadWatch,
    choose_processes,
    merge_raws,
    merge_window,
    split_workers,
)


def raw(process, latencies, commits=None, elapsed=1.0, second=100, group="all", cpu=0.3, lag=0.0):
    commits = len(latencies) if commits is None else commits
    types = {t: {"commits": 0, "errors": 0, "business_rollbacks": 0, "latencies": []} for t in contract.TPCC_TYPES}
    types[contract.NEW_ORDER] = {"commits": commits, "errors": 0, "business_rollbacks": 1, "latencies": list(latencies)}
    return {"process": process, "second": second, "seq": 1, "t_end_ns": second * 10**9, "elapsed": elapsed,
            "cpu_share": cpu, "loop_lag_s": lag,
            "totals": {"commits": commits, "errors": 0, "indeterminate": 0, "drops": 0, "reconnects": 0,
                       "connect_failures": 0},
            "latencies": list(latencies), "journal_ms": [0.5] * len(latencies), "types": types,
            "groups": {group: {"commits": commits, "errors": 0, "indeterminate": 0, "drops": 0,
                               "latencies": list(latencies)}}}


def test_percentiles_come_from_all_latencies_together_never_from_averaging():
    fast = [1.0] * 99 + [2.0]              # process 0: p99 = 1 ms
    slow = [100.0] * 100                   # process 1: p99 = 100 ms
    s = merge_raws([raw(0, fast), raw(1, slow)])
    assert s["p99_ms"] == p99(fast + slow) == 100.0
    assert s["p99_ms"] != (p99(fast) + p99(slow)) / 2                # the average (50.5) would be wrong
    assert s["p50_ms"] == 2.0                                        # the 100th of 200 values
    assert s[contract.BY_TYPE][contract.NEW_ORDER]["p99_ms"] == 100.0
    assert s[contract.BY_TYPE][contract.NEW_ORDER]["p50_ms"] == 2.0          # step 9: per-type p50
    assert s[contract.BY_TYPE][contract.NEW_ORDER]["max_ms"] == 100.0        # step 9: the slowest response
    assert s[contract.BY_TYPE][contract.PAYMENT]["max_ms"] is None           # no Payment in this interval


def test_totals_add_up_and_tps_uses_each_process_own_interval():
    s = merge_raws([raw(0, [1.0] * 100, elapsed=1.0), raw(1, [1.0] * 100, elapsed=1.25)])
    assert s["commits"] == 200
    assert s["tps"] == pytest.approx(100 / 1.0 + 100 / 1.25)
    assert s[contract.BY_TYPE][contract.NEW_ORDER]["tps"] == pytest.approx(180.0)
    assert s[contract.BY_TYPE][contract.NEW_ORDER]["business_rollbacks"] == 2
    assert s[contract.BY_GROUP]["all"]["commits"] == 200
    assert set(contract.SAMPLE_FIELDS) <= set(s)                     # the contract's sample fields


def test_groups_are_merged_separately():
    s = merge_raws([raw(0, [5.0] * 10, group="oltp"), raw(1, [50.0] * 10, group="analytics"),
                    raw(2, [6.0] * 10, group="oltp")])
    g = s[contract.BY_GROUP]
    assert g["oltp"]["commits"] == 20 and g["analytics"]["commits"] == 10
    assert g["oltp"]["p99_ms"] == 6.0 and g["analytics"]["p99_ms"] == 50.0


def test_a_window_counts_each_process_over_the_time_it_really_covered():
    raws = [raw(0, [1.0] * 100, second=s) for s in (1, 2, 3)] + [raw(1, [1.0] * 50, second=s) for s in (1, 2, 3)]
    w = merge_window(raws, duration_s=2.6)        # the window opened 0.4 s into the first interval
    assert w["commits"] == 450
    assert w["tps"] == pytest.approx(150.0)        # not 450 / 2.6 = 173
    with pytest.raises(ValueError):
        merge_raws([])


def test_processes_are_chosen_by_rate_and_limited_by_clerks_and_cores():
    assert choose_processes(200, 64, 8) == 1                         # today's scenarios: one process
    assert choose_processes(None, 64, 8) == 1
    assert choose_processes(5_000, 64, 8) == 3                       # one per 2,000 TPS
    assert choose_processes(50_000, 64, 8) == 7                      # one core left for the coordinator
    assert choose_processes(50_000, 4, 32) == 4                      # never more processes than clerks
    assert choose_processes(5_000, 64, 1) == 1


def test_every_clerk_runs_in_exactly_one_process_and_groups_are_shared_fairly():
    parts = split_workers(64, 3)
    assert sorted(i for p in parts for i in p) == list(range(64))
    assert [len(p) for p in parts] == [22, 21, 21]
    # groups take clerks in declaration order (0-15 analytics, 16-63 oltp): every process gets some of each
    assert all(any(i < 16 for i in p) and any(i >= 16 for i in p) for p in parts)


def test_the_guard_fires_only_after_several_busy_seconds_in_a_row():
    watch = DriverLoadWatch()
    for _ in range(samples.DRIVER_LIMIT_SECONDS - 1):
        assert watch.update([0.3, 0.97], [0.0, 0.0]) is None
    assert watch.update([0.3, 0.2], [0.0, 0.0]) is None              # one calm second resets the count
    for _ in range(samples.DRIVER_LIMIT_SECONDS - 1):
        assert watch.update([0.99], [0.0]) is None
    why = watch.update([0.99], [0.0])
    assert why and why.startswith("driver limited")
    assert watch.max_cpu == 0.99


def test_a_late_event_loop_also_counts_as_driver_limited():
    watch = DriverLoadWatch()
    results = [watch.update([0.4], [samples.DRIVER_LOOP_LAG_LIMIT_S]) for _ in range(samples.DRIVER_LIMIT_SECONDS)]
    assert results[-1] is not None and all(r is None for r in results[:-1])


def test_a_slow_database_is_not_driver_limited():
    # clerks waiting for a slow database use no CPU and the loop stays on time
    watch = DriverLoadWatch()
    assert all(watch.update([0.05, 0.04], [0.01, 0.0]) is None for _ in range(60))


def test_a_stall_also_counts_the_transactions_it_held_back():
    """Coordinated omission (audit issue 4): a clerk due every 300 ms that waits 1,000 ms for one
    answer also missed the starts at 300, 600 and 900 ms; they are recorded as 700, 400, 100 ms."""
    assert samples.corrected_latencies(1000.0, 300.0) == [1000.0, 700.0, 400.0, 100.0]
    assert samples.corrected_latencies(5.0, 300.0) == [5.0]                 # on time: nothing hidden
    assert samples.corrected_latencies(5.0, None) == [5.0]                  # no rate: nothing to correct
    long = samples.corrected_latencies(1e9, 1.0)
    assert len(long) == samples.MAX_CORRECTION_SAMPLES + 1                  # bounded


def test_the_corrected_p99_is_reported_next_to_the_measured_one():
    r = raw(0, [2.0] * 99 + [2000.0])
    r["corrected"] = [2.0] * 99 + samples.corrected_latencies(2000.0, 320.0)
    s = merge_raws([r])
    assert s["p99_ms"] == 2.0 or s["p99_ms"] == 2000.0
    assert s["p99_corrected_ms"] > s["p99_ms"]                              # the stall shows
    assert merge_raws([raw(0, [3.0] * 10)])["p99_corrected_ms"] == 3.0      # raw numbers without the field
    w = merge_window([r], duration_s=1.0)
    assert w["p99_corrected_ms"] > w["p99_ms"]
