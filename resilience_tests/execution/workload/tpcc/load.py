"""Load, inspect or remove the TPC-C data on a target (TPC-C build step 4). Run once per target.

    python -m resilience_tests.execution.workload.tpcc.load --env e2-dedicated-vm --warehouses 320
    python -m resilience_tests.execution.workload.tpcc.load --env e2-dedicated-vm --status
    python -m resilience_tests.execution.workload.tpcc.load --env e2-dedicated-vm --check
    python -m resilience_tests.execution.workload.tpcc.load --env e2-dedicated-vm --drop

Standard size: 5 warehouses per clerk (docs/tpcc-workload-proposal.md 2.2), e.g. 64 clerks ->
320 warehouses, about 100 MB per warehouse.

Safety, as the harness does before every run (Arch §15): the environment must not be
production, the target's hostname (read over SSH) must match the profile's allowlist, and
the sentinel table must hold a row for that hostname with the profile's inventory tag.
Only schema resilience_tpcc is created or dropped; no server or role setting is changed.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from typing import Any

from resilience_tests.adapters import postgresql  # noqa: F401  (registers the adapter)
from resilience_tests.adapters.base import BaseDatabaseAdapter, adapter_for
from resilience_tests.control.profile import EnvProfile, Node, load_profile
from resilience_tests.execution.remote import run_once
from resilience_tests.execution.workload.tpcc import data


class Refused(SystemExit):
    def __init__(self, why: str) -> None:
        super().__init__(f"refused: {why}")


def _args(argv: list[str]) -> argparse.Namespace:
    ap = argparse.ArgumentParser(prog="python -m resilience_tests.execution.workload.tpcc.load",
                                 description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env", required=True, help="environment profile, e.g. e2-dedicated-vm")
    ap.add_argument("--node", help="node name in the profile (default: the first node)")
    what = ap.add_mutually_exclusive_group(required=True)
    what.add_argument("--warehouses", type=int, help="load this many warehouses")
    what.add_argument("--status", action="store_true", help="show what is loaded")
    what.add_argument("--check", action="store_true", help="run every consistency rule except 11 on all data")
    what.add_argument("--drop", action="store_true", help="remove all TPC-C data (schema resilience_tpcc)")
    ap.add_argument("--jobs", type=int, default=4, help="warehouses loaded in parallel (default 4)")
    ap.add_argument("--replace", action="store_true", help="drop existing TPC-C data before loading")
    ap.add_argument("--seed", type=int, help="random seed of the loader (default: random)")
    return ap.parse_args(argv)


async def check_target(profile: EnvProfile, node: Node, adapter: BaseDatabaseAdapter) -> str:
    """The harness's fingerprint check (control/safety.py), for a command that is not a scenario."""
    if profile.environment == "production":
        raise Refused(f"profile {profile.name} is a production environment (Framework §7.1)")
    hostname = (await run_once(node.ssh, "hostname", timeout_s=15)).stdout.strip()
    allow = profile.safety.allowlist
    if not re.fullmatch(allow.hostname_pattern, hostname):
        raise Refused(f"target hostname {hostname!r} does not match the allowlist {allow.hostname_pattern!r}")
    sentinel = await adapter.sentinel(profile.safety.sentinel_table, hostname)
    if sentinel is None:
        raise Refused(f"no row for {hostname!r} in {profile.safety.sentinel_table} (default deny)")
    if sentinel.get("inventory_tag") != allow.inventory_tag:
        raise Refused(f"sentinel inventory_tag {sentinel.get('inventory_tag')!r} != {allow.inventory_tag!r}")
    return hostname


def _gb(n: int | None) -> str:
    return "-" if n is None else f"{n / 1e9:.1f} GB"


async def _main(a: argparse.Namespace) -> int:
    profile = load_profile(a.env)
    node = profile.node(a.node) if a.node else profile.nodes[0]
    adapter = adapter_for(profile.database.engine, node)
    hostname = await check_target(profile, node, adapter)
    print(f"target {hostname} ({profile.name}, {node.db.host}:{node.db.port}/{node.db.dbname})")

    if a.status:
        status = await adapter.tpcc_data_status()
        print(json.dumps(status, indent=2, default=str))
        if status.get("loaded"):
            print(f"growth since loading: {data.growth(status):.2f}x (limit {data.GROWTH_LIMIT}), "
                  f"size {_gb(status.get('size_bytes'))}")
        return 0
    if a.drop:
        gone = await adapter.tpcc_drop()
        print("TPC-C data removed" if gone else "ERROR: schema resilience_tpcc still exists")
        return 0 if gone else 1
    if a.check:
        rules = [r for r in data.LOAD_RULES if r != 11]          # rule 11 holds only right after loading
        results = await adapter.tpcc_consistency(rules)
        problems = data.consistency_problems(results)
        print(json.dumps(results, indent=2))
        print("all consistency rules hold" if not problems else "\n".join(problems))
        return 0 if not problems else 1

    if a.warehouses < 1:
        raise Refused("--warehouses must be at least 1")
    print(f"loading {a.warehouses} warehouses with {a.jobs} parallel jobs "
          f"(about {a.warehouses * 0.1:.0f} GB)...", flush=True)
    result: dict[str, Any] = await adapter.tpcc_load(a.warehouses, jobs=a.jobs, replace=a.replace,
                                                     progress=lambda m: print("  " + m, flush=True), seed=a.seed)
    print(f"loaded {result['warehouses']} warehouses in {result['seconds']:.0f} s, size {_gb(result['size_bytes'])}")
    print("rows: " + ", ".join(f"{t} {n}" for t, n in result["rows"].items()))
    print("all 12 consistency rules hold; row counts as TPC-C Clause 4.2.2 expects")
    return 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(_main(_args(sys.argv[1:] if argv is None else argv)))


if __name__ == "__main__":
    raise SystemExit(main())
