"""The capacity test: the target's maximum TPS for the TPC-C workload (TPC-C build step 8).

    python -m resilience_tests.execution.workload.tpcc.capacity --env e2-u22-3 --clerks 64

Steps of rising rate (start, x factor, ...), each held for --step-seconds after a short warm-up,
with as many worker processes as the rate needs. A step is "kept up" when the database
delivered at least 95% of the asked rate AND TPC-C's response-time rule held (90% of New-Order,
Payment, Order-Status and Delivery within 5 s, Stock-Level within 20 s; Clause 5.2.5.3). The
test stops at the first step that was not kept up; the capacity is the highest TPS of a step
that was. If the harness itself became the limit (driver_limited), the test stops and reports
only a lower bound: the harness, not the database, ran out.

The result is stored in resilience_tpcc.meta (capacity_tps), and the evidence of every step goes
to <driver_host.run_dir>/tpcc-capacity-<time>/ (or --out).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from catalog.schema import Workload
from resilience_tests.adapters import postgresql  # noqa: F401  (registers the adapter)
from resilience_tests.adapters.base import adapter_for
from resilience_tests.control.profile import load_profile
from resilience_tests.execution.workload import contract
from resilience_tests.execution.workload.markers import MarkerJournals
from resilience_tests.execution.workload.tpcc.load import Refused, check_target
from resilience_tests.execution.workload.tpcc.profile import TpccProfile
from resilience_tests.execution.workload.tpcc.samples import choose_processes
from resilience_tests.observability.event_stream import EventStream

KEPT_UP_SHARE = 0.95
RESPONSE_LIMIT_MS = {contract.NEW_ORDER: 5_000, contract.PAYMENT: 5_000, contract.ORDER_STATUS: 5_000,
                     contract.DELIVERY: 5_000, contract.STOCK_LEVEL: 20_000}      # Clause 5.2.5.3
WARMUP_S = 5.0


@dataclass
class Step:
    rate: float
    processes: int
    tps: float
    p99_ms: float | None
    worst_p90_ms: dict[str, float | None]
    response_ok: bool
    errors: int
    driver_limited: bool
    failure: str | None = None

    @property
    def kept_up(self) -> bool:
        return self.failure is None and self.response_ok and self.tps >= KEPT_UP_SHARE * self.rate


def response_ok(samples: list[dict[str, Any]]) -> tuple[bool, dict[str, float | None]]:
    """TPC-C's response-time rule over a step, judged conservatively: the worst one-second p90
    of each type must be within its limit (a step passes only if every second did)."""
    worst: dict[str, float | None] = {}
    for t, limit in RESPONSE_LIMIT_MS.items():
        values = [s[contract.BY_TYPE][t]["p90_ms"] for s in samples if s[contract.BY_TYPE][t]["p90_ms"] is not None]
        worst[t] = max(values) if values else None
    ok = all(v is None or v <= RESPONSE_LIMIT_MS[t] for t, v in worst.items())
    return ok, worst


def decide(steps: list[Step], max_rate: float) -> tuple[bool, float | None, str]:
    """(stop?, capacity or None, why). Pure."""
    last = steps[-1]
    kept = [s.tps for s in steps if s.kept_up]
    best = max(kept) if kept else None
    if last.driver_limited:
        bound = max(s.tps for s in steps)
        return True, None, (f"the harness was the limit at {last.rate:.0f} TPS asked: the capacity is at least "
                            f"{bound:.0f} TPS; add worker processes or harness machines to measure it")
    if last.failure:
        return True, best, f"the step failed: {last.failure}"
    if not last.response_ok:
        return True, best, "TPC-C's response-time rule broke (90% within 5 s / 20 s)"
    if last.tps < KEPT_UP_SHARE * last.rate:
        return True, best, f"the database delivered {last.tps:.0f} of {last.rate:.0f} TPS asked: saturated"
    if last.rate >= max_rate:
        return True, best, f"reached --max-rate {max_rate:.0f} without saturating: the capacity is at least this"
    return False, None, "kept up; trying a higher rate"


def _args(argv: list[str]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(prog="python -m resilience_tests.execution.workload.tpcc.capacity",
                                 description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env", required=True)
    ap.add_argument("--node")
    ap.add_argument("--clerks", type=int, default=64, help="clerks (connections), as in the scenarios (default 64)")
    ap.add_argument("--start-rate", type=float, default=200.0)
    ap.add_argument("--factor", type=float, default=1.5, help="rate increase per step (default 1.5)")
    ap.add_argument("--step-seconds", type=float, default=30.0)
    ap.add_argument("--max-rate", type=float, default=100_000.0)
    ap.add_argument("--processes", type=int, help="fixed number of worker processes (default: by rate)")
    ap.add_argument("--no-store", action="store_true", help="do not store the result in resilience_tpcc.meta")
    ap.add_argument("--out", help="evidence folder (default: <driver_host.run_dir>/tpcc-capacity-<time>)")
    return ap.parse_args(argv)


async def _run_step(adapter: Any, profile: TpccProfile, warehouses: int, clerks: int, rate: float, seconds: float,
                    processes: int, out: Path) -> Step:
    out.mkdir(parents=True)
    w = Workload(profile="tpcc", concurrency=clerks, transaction_markers=True, rate_tps=rate,
                 tpcc={"warehouses": warehouses})
    step_profile = TpccProfile(w, seed=profile.seed)
    step_profile.constants = profile.constants
    stream, journals = EventStream(out / "events.jsonl"), MarkerJournals(out)
    drv = step_profile.make_driver(adapter, journals, stream, processes=processes)
    await drv.start()
    try:
        await asyncio.wait_for(drv.wait_until_ready(), 60)
        await asyncio.sleep(WARMUP_S)
        t0 = time.monotonic_ns()
        drv.begin_window()
        await asyncio.sleep(seconds)
        window = drv.end_window()
    finally:
        await drv.stop()
        journals.close()
        stream.close()
    samples = [e.data for e in stream.events() if e.kind == "sample" and e.t_mono_ns >= t0]
    ok, worst = response_ok(samples)
    limited = any(e.kind == "fatal" and e.data.get("driver_limited") for e in stream.events())
    return Step(rate=rate, processes=processes, tps=window.tps, p99_ms=window.p99_ms, worst_p90_ms=worst,
                response_ok=ok, errors=window.errors, driver_limited=limited,
                failure=None if limited else drv.failure)


async def _main(a: argparse.Namespace) -> int:
    env = load_profile(a.env)
    node = env.node(a.node) if a.node else env.nodes[0]
    adapter = adapter_for(env.database.engine, node)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    out = Path(a.out).expanduser() if a.out else Path(env.driver_host.run_dir) / f"tpcc-capacity-{stamp}"
    try:
        out.mkdir(parents=True, exist_ok=False)
    except OSError as exc:
        raise Refused(f"cannot create {out} ({exc.strerror}); choose a folder with --out") from None
    hostname = await check_target(env, node, adapter)
    status = await adapter.tpcc_data_status()
    if not status.get("loaded"):
        raise Refused(f"no TPC-C data: {status.get('reason')}")
    warehouses = status["warehouses"]
    await adapter.prepare_harness_state()
    base = TpccProfile(Workload(profile="tpcc", concurrency=a.clerks, transaction_markers=True,
                                rate_tps=a.start_rate, tpcc={"warehouses": warehouses}))
    await base.prepare(adapter)
    print(f"target {hostname}: {warehouses} warehouses, {a.clerks} clerks; steps of {a.step_seconds:.0f} s from "
          f"{a.start_rate:.0f} TPS x {a.factor}; evidence in {out}", flush=True)

    steps: list[Step] = []
    rate, capacity, why = a.start_rate, None, ""
    while True:
        n = a.processes or choose_processes(rate, a.clerks, os.cpu_count() or 1)
        step = await _run_step(adapter, base, warehouses, a.clerks, rate, a.step_seconds, n, out / f"step-{len(steps) + 1}")
        steps.append(step)
        worst = max((v for v in step.worst_p90_ms.values() if v is not None), default=None)
        print(f"  asked {rate:7.0f}  got {step.tps:7.0f} TPS  processes {n}  p99 {step.p99_ms or 0:7.1f} ms  "
              f"worst p90 {worst or 0:7.1f} ms  errors {step.errors}  {'kept up' if step.kept_up else 'NOT kept up'}",
              flush=True)
        stop, capacity, why = decide(steps, a.max_rate)
        if stop:
            break
        rate = min(rate * a.factor, a.max_rate)

    rules = await adapter.tpcc_consistency([1, 2, 3])
    print(f"\n{why}")
    if capacity is not None:
        # NOPM: New-Orders per minute; New-Order is 10 of the deck's 23 cards (about 1% of them roll back)
        print(f"CAPACITY: {capacity:.0f} TPS (about {capacity * 10 / 23 * 0.99 * 60:.0f} NOPM)")
        if not a.no_store:
            await adapter.tpcc_record_capacity(round(capacity, 1))
            print("stored in resilience_tpcc.meta (capacity_tps)")
    print("consistency rules 1-3 after the test:", "hold" if not any(rules.values()) else f"FAIL {rules}")
    (out / "summary.json").write_text(json.dumps({"steps": [asdict(s) | {"kept_up": s.kept_up} for s in steps],
                                                  "capacity_tps": capacity, "why": why, "rules": rules}, indent=2) + "\n")
    return 0 if capacity is not None and not any(rules.values()) else 1


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(_main(_args(sys.argv[1:] if argv is None else argv)))


if __name__ == "__main__":
    raise SystemExit(main())
