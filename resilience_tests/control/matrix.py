"""Matrix expander (Arch §4.2, Fig. 2): scenario x environment class x target role -> run plan.

The run plan is derived, never hand-maintained. env_sensitive scenarios expand across every
supported class; the rest run once on the reference environment (Framework §7.3).
"""

from __future__ import annotations

from dataclasses import dataclass

from catalog.schema import Catalog, Scenario
from resilience_tests.adapters import postgresql  # noqa: F401  (registers the adapter)
from resilience_tests.adapters.base import capabilities_of
from resilience_tests.adapters.target_resolver import ResolutionError, TargetResolver
from resilience_tests.control.faults import has_fault_behaviour
from resilience_tests.control.profile import EnvProfile, Node


@dataclass(frozen=True)
class RunPlanItem:
    scenario: Scenario
    env_class: str
    role: str
    node: Node

    @property
    def run_key(self) -> str:
        return f"{self.scenario.id}[{self.env_class}:{self.role}:{self.node.name}]"


@dataclass(frozen=True)
class Skipped:
    scenario_id: str
    reason: str


def missing_infra(scenario: Scenario, profile: EnvProfile) -> list[str]:
    """Infrastructure the scenario needs that this environment does not provide."""
    return sorted(set(scenario.needs_infra) - set(profile.infra))


def not_built_reason(scenario: Scenario) -> str | None:
    """Why the harness cannot run this scenario to specification yet, or None. Checked by the
    run plan (skipped, with the reason) and again in init, before anything is applied.

    - A compound scenario (CL-X) declares a second fault, `compound.fault_b`, that nothing
      injects yet: run anyway, it would apply fault A alone and could pass without being tested.
      Refused until compound injection is built with the Tier-2 work (README: "Design note:
      tests on several machines").
    - A fault type with no behaviour in control/faults: the harness does not know how its
      recovery begins or what it must measure, so the fault would be applied and then left
      unjudged."""
    if scenario.compound is not None:
        return ("compound faults are not built yet: compound.fault_b would never be injected, "
                "so the scenario cannot be run to specification")
    if not has_fault_behaviour(scenario.fault.type):
        return (f"fault type {scenario.fault.type!r} is not built yet: no behaviour for it is registered "
                "in control/faults, so its recovery and measures are not defined")
    return None


def expand(catalog: Catalog, profile: EnvProfile, *, reference_env_class: str,
           only: set[str] | None = None) -> tuple[list[RunPlanItem], list[Skipped]]:
    """Run plan for ONE environment profile. Scenarios whose classes exclude this profile's
    class, or that are env-insensitive while this is not the reference class, are skipped
    with a reason (reported, not silently dropped)."""
    plan: list[RunPlanItem] = []
    skipped: list[Skipped] = []
    resolver = TargetResolver(profile)
    engine = profile.database.engine
    engine_can = {c.value for c in capabilities_of(engine)}
    for sid, sc in sorted(catalog.scenarios.items()):
        if only and sid not in only:
            continue
        if profile.env_class not in sc.environment.classes:
            skipped.append(Skipped(sid, f"class {profile.env_class} not in {sc.environment.classes}"))
            continue
        if not sc.environment.env_sensitive and profile.env_class != reference_env_class:
            skipped.append(Skipped(sid, f"env-insensitive: runs once on reference class {reference_env_class}"))
            continue
        # A scenario is only meaningful against an engine that can support it. Skipping with a
        # reason beats measuring something the engine cannot do (Arch §8).
        missing = sorted(set(sc.requires) - engine_can)
        if missing:
            skipped.append(Skipped(sid, f"engine {engine!r} lacks {', '.join(missing)}"))
            continue
        not_built = not_built_reason(sc)
        if not_built:
            skipped.append(Skipped(sid, not_built))
            continue
        lacking = missing_infra(sc, profile)
        if lacking:
            skipped.append(Skipped(sid, f"blocked by infrastructure: profile {profile.name!r} "
                                        f"does not provide {', '.join(lacking)}"))
            continue
        try:
            targets = resolver.resolve(sc)
        except ResolutionError as exc:
            skipped.append(Skipped(sid, str(exc)))
            continue
        plan.extend(RunPlanItem(sc, profile.env_class, t.role, t.node) for t in targets)
    return plan, skipped
