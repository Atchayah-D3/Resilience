"""Trial run of the TPC-C engine on a target, without a fault (TPC-C build step 6 checkpoint).

    python -m resilience_tests.execution.workload.tpcc.trial --env e2-dedicated-vm --clerks 16 --rate 200 --seconds 60

Before: the harness's safety checks (load.check_target), the harness state (as a run's init:
empty marker table), and the tpcc profile's prepare (data loaded and consistent, functions
installed, run constants). Then the engine runs for --seconds. After, from the evidence alone:

    rate      the TPS reached against the declared rate
    mix       each type's share against TPC-C's minimums (Clause 5.2.3)
    rollbacks business rollbacks as a share of New-Orders (about 1%, Clause 2.4.1.4)
    RPO       every acknowledged write transaction's marker is in the database; no phantom
    aborted   no rolled-back transaction left a marker
    orders    every acknowledged New-Order exists with exactly its lines (the per-order check)
    rules     TPC-C consistency rules 1-10 and 12 hold

The trial adds orders, so the data grows (the growth check before every run watches this).
Evidence goes to <driver_host.run_dir>/tpcc-trial-<time>/ (or --out): events.jsonl, the
journals, summary.json.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

from catalog.schema import Workload
from resilience_tests.adapters import postgresql  # noqa: F401  (registers the adapter)
from resilience_tests.adapters.base import adapter_for
from resilience_tests.control.profile import load_profile
from resilience_tests.execution.workload import contract
from resilience_tests.execution.workload.markers import (
    ABORTED_JOURNAL,
    ACKED_JOURNAL,
    MarkerJournals,
    diff_from_journals,
    journal_paths,
    read_journal,
)
from resilience_tests.execution.workload.tpcc import inputs
from resilience_tests.execution.workload.tpcc.load import Refused, check_target
from resilience_tests.execution.workload.tpcc.profile import TpccProfile
from resilience_tests.observability.event_stream import EventStream

RATE_KEPT_SHARE = 0.95          # the trial "keeps the rate" at 95% or more of the declared TPS
ROLLBACK_SHARE_RANGE = (0.002, 0.03)   # about 1%; wide, because a short trial has few New-Orders
ROLLBACK_MIN_ORDERS = 500


def summarize(samples: list[dict[str, Any]], seconds: float, rate: float) -> dict[str, Any]:
    """Pure: totals per type from the 1-second samples, the mix and the business-rollback share."""
    commits, business, errors = Counter(), Counter(), Counter()
    for s in samples:
        for t, st in s.get(contract.BY_TYPE, {}).items():
            commits[t] += st["commits"]
            business[t] += st["business_rollbacks"]
            errors[t] += st["errors"]
    attempts = {t: commits[t] + business[t] + errors[t] for t in contract.TPCC_TYPES}
    total = sum(attempts.values())
    mix = {t: (attempts[t] / total if total else 0.0) for t in contract.TPCC_TYPES}
    new_orders = attempts[contract.NEW_ORDER]
    tps = sum(s["commits"] for s in samples) / seconds if seconds > 0 else 0.0
    return {
        "tps": tps, "rate": rate, "rate_kept": tps >= RATE_KEPT_SHARE * rate,
        "commits": dict(commits), "business_rollbacks": dict(business), "errors": dict(errors),
        "mix": mix, "mix_ok": total > 0 and all(mix[t] >= m for t, m in inputs.MIX_MINIMUM.items()),
        "rollback_share": business[contract.NEW_ORDER] / new_orders if new_orders else None,
        "indeterminate": sum(s["indeterminate"] for s in samples),
        "drops": sum(s["drops"] for s in samples),
    }


def verdicts(summary: dict[str, Any]) -> list[tuple[str, bool, str]]:
    """Pure: each check of the trial with its result and detail."""
    out = [
        ("rate kept", summary["rate_kept"], f"{summary['tps']:.0f} of {summary['rate']:.0f} TPS"),
        ("mix meets TPC-C minimums", summary["mix_ok"],
         ", ".join(f"{t} {v:.1%}" for t, v in summary["mix"].items())),
        ("no errors, no unknown outcomes", not any(summary["errors"].values()) and summary["indeterminate"] == 0,
         f"errors {sum(summary['errors'].values())}, unknown {summary['indeterminate']}"),
        ("RPO 0, no phantom", summary.get("rpo_txn") == 0 and summary.get("phantom") == 0,
         f"lost {summary.get('rpo_txn')}, phantom {summary.get('phantom')}"),
        ("no rolled-back transaction kept", summary.get("aborted_kept") == 0, f"{summary.get('aborted_kept')}"),
        ("every acknowledged New-Order complete",
         summary.get("orders_incomplete") == 0
         and (summary.get("orders_checked") or 0) >= summary["commits"].get(contract.NEW_ORDER, 0) > 0,
         f"{summary.get('orders_checked')} checked, {summary.get('orders_incomplete')} incomplete"),
        ("consistency rules 1-10, 12 hold", bool(summary.get("rules")) and not any(summary["rules"].values()),
         json.dumps(summary.get("rules"))),
    ]
    new_orders = summary["commits"].get(contract.NEW_ORDER, 0) + summary["business_rollbacks"].get(contract.NEW_ORDER, 0)
    if new_orders >= ROLLBACK_MIN_ORDERS:
        lo, hi = ROLLBACK_SHARE_RANGE
        share = summary["rollback_share"] or 0.0
        out.append(("business rollbacks about 1%", lo <= share <= hi, f"{share:.2%} of {new_orders} New-Orders"))
    return out


def _args(argv: list[str]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(prog="python -m resilience_tests.execution.workload.tpcc.trial",
                                 description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env", required=True)
    ap.add_argument("--node")
    ap.add_argument("--clerks", type=int, default=16)
    ap.add_argument("--rate", type=float, default=200.0, help="declared TPS (all types together)")
    ap.add_argument("--seconds", type=float, default=60.0)
    ap.add_argument("--lock-timeout", help="session setting for every clerk, e.g. 5s")
    ap.add_argument("--processes", type=int, help="worker processes (default: by rate, one per 2,000 TPS)")
    ap.add_argument("--out", help="evidence folder (default: <driver_host.run_dir>/tpcc-trial-<time>)")
    return ap.parse_args(argv)


async def _main(a: argparse.Namespace) -> int:
    profile_env = load_profile(a.env)
    node = profile_env.node(a.node) if a.node else profile_env.nodes[0]
    adapter = adapter_for(profile_env.database.engine, node)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    out = Path(a.out).expanduser() if a.out else Path(profile_env.driver_host.run_dir) / f"tpcc-trial-{stamp}"
    try:
        out.mkdir(parents=True, exist_ok=False)
    except OSError as exc:
        raise Refused(f"cannot create {out} ({exc.strerror}); choose a folder with --out") from None
    hostname = await check_target(profile_env, node, adapter)
    status = await adapter.tpcc_data_status()
    if not status.get("loaded"):
        raise Refused(f"no TPC-C data: {status.get('reason')}")
    session = {"lock_timeout": a.lock_timeout} if a.lock_timeout else {}
    w = Workload(profile="tpcc", concurrency=a.clerks, transaction_markers=True, rate_tps=a.rate,
                 session=session, tpcc={"warehouses": status["warehouses"]})
    print(f"target {hostname}: {status['warehouses']} warehouses; {a.clerks} clerks at {a.rate:.0f} TPS for "
          f"{a.seconds:.0f} s; evidence in {out}", flush=True)

    await adapter.prepare_harness_state()          # as a run's init: empty marker table
    profile = TpccProfile(w)
    prepared = await profile.prepare(adapter)
    stream, journals = EventStream(out / "events.jsonl"), MarkerJournals(out)
    drv = profile.make_driver(adapter, journals, stream, processes=a.processes)
    await drv.start()
    await asyncio.wait_for(drv.wait_until_ready(), 30)
    drv.begin_window()
    await asyncio.sleep(a.seconds)
    window = drv.end_window()
    await drv.stop()
    journals.close()
    stream.close()
    if drv.failure:
        print(f"ENGINE FAILED: {drv.failure}")
        return 1

    samples = [e.data for e in stream.events() if e.kind == "sample"]
    summary = summarize(samples, window.duration_s, a.rate)
    summary.update({"p50_ms": window.p50_ms, "p99_ms": window.p99_ms, "prepare": prepared,
                    "processes": getattr(drv, "processes", 1)})
    db = await adapter.marker_ids()
    diff, torn = diff_from_journals(out, db)
    # every journal file: one per worker process when the engine ran in several processes
    aborted = [r for p in journal_paths(out, ABORTED_JOURNAL) for r in read_journal(p).records]
    orders = [r for p in journal_paths(out, ACKED_JOURNAL) for r in read_journal(p).records if "o_id" in r]
    incomplete = await adapter.tpcc_orders_incomplete(orders)
    summary.update({"rpo_txn": diff.rpo_txn if not torn else None, "phantom": len(diff.phantom),
                    "indeterminate_txn": len(diff.indeterminate), "aborted_kept": len({r["uuid"] for r in aborted} & db),
                    "orders_checked": len(orders), "orders_incomplete": incomplete,
                    "rules": await adapter.tpcc_consistency([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12])})
    checks = verdicts(summary)
    (out / "summary.json").write_text(json.dumps({"summary": summary, "checks": checks}, indent=2, default=str) + "\n")
    load = [e.data for e in stream.events() if e.kind == "driver_load"]
    summary["max_cpu_share"] = max((c for d in load for c in d["cpu_share"]), default=None)
    print(f"\n{window.tps:.0f} TPS, p50 {summary['p50_ms']:.1f} ms, p99 {summary['p99_ms']:.1f} ms, "
          f"{summary['processes']} worker process(es), busiest at {summary['max_cpu_share'] or 0:.0%} of a core")
    for name, ok, detail in checks:
        print(f"  {'PASS' if ok else 'FAIL'}  {name}: {detail}")
    passed = all(ok for _, ok, _ in checks)
    print("\ntrial passed" if passed else "\ntrial FAILED")
    return 0 if passed else 1


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(_main(_args(sys.argv[1:] if argv is None else argv)))


if __name__ == "__main__":
    raise SystemExit(main())
