"""TargetResolver (Arch §8, Fig. 7; Framework §4.2).

`applies_to` is expanded to concrete hosts by a strategy per topology. StaticStrategy reads
the inventory from the environment profile (air-gapped / E1 inventory). PatroniStrategy and
DistDBStrategy query the live topology; they arrive with the clustered tiers (Phases 4-5).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from catalog.schema import Scenario
from resilience_tests.control.profile import EnvProfile, Node


class ResolutionError(RuntimeError):
    pass


@dataclass(frozen=True)
class ResolvedTarget:
    """One (scenario, node) execution: the node whose role matches `applies_to` and whose
    topology role matches `fault.target`."""

    role: str
    node: Node


class ResolverStrategy(ABC):
    @abstractmethod
    def nodes(self, profile: EnvProfile) -> list[Node]: ...


class StaticStrategy(ResolverStrategy):
    def nodes(self, profile: EnvProfile) -> list[Node]:
        return list(profile.nodes)


class TargetResolver:
    def __init__(self, profile: EnvProfile, strategy: ResolverStrategy | None = None) -> None:
        self.profile = profile
        self.strategy = strategy or StaticStrategy()

    def resolve(self, scenario: Scenario) -> list[ResolvedTarget]:
        targets = [
            ResolvedTarget(role=n.role, node=n)
            for n in self.strategy.nodes(self.profile)
            if n.role in scenario.applies_to and n.topology_role == scenario.fault.target
        ]
        if not targets:
            raise ResolutionError(
                f"{scenario.id}: no node in profile {self.profile.name} has a role in {scenario.applies_to} "
                f"with topology role {scenario.fault.target!r}"
            )
        return targets
