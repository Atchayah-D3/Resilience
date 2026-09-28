"""Kill switch (Arch §15): a single command reverses all active injections across all hosts.

    python -m resilience_tests.control.killswitch --env e2-dedicated-vm

Works from the injection ledger, so it also cleans up after a crashed harness.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from resilience_tests.control.ledger import InjectionLedger, LedgerEntry
from resilience_tests.control.profile import EnvProfile, load_profile
from resilience_tests.execution.injectors.base import resolve_by_name

LEDGER_FILE = "injection-ledger.jsonl"
# Per-injection revert budget. It must exceed the drivers' own budgets (see
# injectors/process.REVERT_TOTAL_BUDGET_S) plus SSH set-up, or a revert that is progressing
# normally would be cancelled here and journalled as revert_failed. The cleanup phase bound
# in each env profile must in turn exceed this.
REVERT_TIMEOUT_S = 300.0


def ledger_for(profile: EnvProfile) -> InjectionLedger:
    return InjectionLedger(Path(profile.driver_host.run_dir) / LEDGER_FILE)


async def revert_outstanding(profile: EnvProfile, ledger: InjectionLedger, *, run_id: str | None = None,
                             timeout_s: float = REVERT_TIMEOUT_S) -> list[tuple[LedgerEntry, str]]:
    results: list[tuple[LedgerEntry, str]] = []
    for entry in ledger.outstanding():
        if run_id is not None and entry.run_id != run_id:
            continue
        section = entry.detail.get("section", "")
        try:
            injector = resolve_by_name(section, entry.driver, profile, entry.fault_type)
            async with asyncio.timeout(timeout_s):
                detail = await injector.revert(profile.node(entry.node), entry.detail)
            ledger.transition(entry, "reverted", revert=detail, by="killswitch")
            results.append((entry, "reverted"))
        except Exception as exc:  # noqa: BLE001 -- every failure is recorded; none is swallowed
            ledger.transition(entry, "revert_failed", error=f"{type(exc).__name__}: {exc}", by="killswitch")
            results.append((entry, f"revert_failed: {exc}"))
    return results


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--env", required=True, help="environment profile name or path")
    ap.add_argument("--run-id", help="only this run's injections")
    args = ap.parse_args(argv)
    profile = load_profile(args.env)
    ledger = ledger_for(profile)
    if not ledger.path.exists():
        # "no ledger" is not "nothing to revert": the run may have journalled somewhere else.
        # Saying "clean" here would be the recovery tool lying to an operator at a customer site.
        print(f"NO LEDGER at {ledger.path}\n"
              f"Nothing could be reverted, and nothing has been verified as clean. The harness "
              f"journals every injection here before applying it, so either no run has ever used "
              f"profile {profile.name!r} on this host, or this is not the driver host for that run "
              f"(check --env and driver_host.run_dir).", file=sys.stderr)
        return 2
    results = asyncio.run(revert_outstanding(profile, ledger, run_id=args.run_id))
    if not results:
        print(f"no outstanding injections in {ledger.path}")
    for entry, outcome in results:
        print(f"{entry.injection_id} {entry.fault_type} {entry.node}: {outcome}")
    return 0 if all(o == "reverted" for _, o in results) else 1


if __name__ == "__main__":
    sys.exit(main())
