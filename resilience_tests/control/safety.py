"""SafetyController (Arch §15; Framework §7.2 "safety is enforced by the harness, not by
convention").

  Environment allowlist   refuse unless the target fingerprint (hostname pattern, sentinel
                          table, inventory tag) is on the allowlist. Default deny.
  Destructive gate        NL-D (and EQS-D) additionally require --target-is-disposable
                          plus an operator-created sentinel marked disposable.
  Blast radius cap        enforced before injection. Ceiling 1 for every category except
                          CL-X (2). Category-scoped, not scenario-settable. Fails closed.
  Abort conditions        abort_if predicates evaluated continuously against the probe stream.
  Kill switch / ledger    control/killswitch.py, control/ledger.py.
  Timeouts                every phase bounded (orchestrator).
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from catalog.schema import COMPOUND_CATEGORY, Scenario
from resilience_tests.analysis import threshold_eval
from resilience_tests.control.profile import EnvProfile, Node

DESTRUCTIVE_CATEGORIES = frozenset({"NL-D"})
NODE_CEILING_DEFAULT = 1
NODE_CEILING_COMPOUND = 2

SENTINEL_DDL_HINT = """\
-- run BY HAND on the target, in database {dbname}, as a role that owns schema resilience:
CREATE SCHEMA IF NOT EXISTS resilience;
CREATE TABLE IF NOT EXISTS {table} (
    hostname      text PRIMARY KEY,
    inventory_tag text NOT NULL,
    disposable    boolean NOT NULL,
    created_by    text NOT NULL,
    created_at    timestamptz NOT NULL DEFAULT now()
);
INSERT INTO {table} (hostname, inventory_tag, disposable, created_by)
VALUES ('<hostname>', '{tag}', true, '<your name>');"""


class SafetyViolation(RuntimeError):
    """Refuse to run. Never downgraded to a warning."""


@dataclass(frozen=True)
class AbortCheck:
    predicate: str
    outcome: str  # pass (condition true -> abort) | fail | not_applicable | missing | error
    values: dict[str, Any]

    @property
    def triggered(self) -> bool:
        return self.outcome == "pass"


class SafetyController:
    def __init__(self, profile: EnvProfile, scenario: Scenario, *, target_is_disposable: bool) -> None:
        self.profile = profile
        self.scenario = scenario
        self.target_is_disposable = target_is_disposable

    @property
    def destructive(self) -> bool:
        return self.scenario.category in DESTRUCTIVE_CATEGORIES

    def node_ceiling(self) -> int:
        return NODE_CEILING_COMPOUND if self.scenario.category == COMPOUND_CATEGORY else NODE_CEILING_DEFAULT

    def check_static(self, targets: list[Node]) -> None:
        sc, prof = self.scenario, self.profile
        if prof.environment not in sc.blast_radius.environments:
            raise SafetyViolation(
                f"{sc.id} may run only in {sc.blast_radius.environments}; profile {prof.name} is {prof.environment!r}"
            )
        requested = (
            sc.compound.blast_radius_override.max_nodes_affected
            if sc.compound is not None
            else sc.blast_radius.max_nodes_affected
        )
        ceiling = self.node_ceiling()
        if requested > ceiling:
            raise SafetyViolation(f"{sc.id} requests {requested} nodes; ceiling for {sc.category} is {ceiling}")
        affected = len(targets) if sc.compound is None else len(targets) * 2
        if affected > ceiling:
            raise SafetyViolation(f"{sc.id} would affect {affected} nodes in one run; ceiling is {ceiling}")
        if self.destructive and not self.target_is_disposable:
            raise SafetyViolation(
                f"{sc.id} is destructive ({sc.category}); pass --target-is-disposable to confirm the target is a scratch instance"
            )

    def check_fingerprint(self, node: Node, observed_hostname: str, sentinel: Mapping[str, Any] | None) -> dict[str, Any]:
        allow = self.profile.safety.allowlist
        if not re.fullmatch(allow.hostname_pattern, observed_hostname):
            raise SafetyViolation(
                f"{node.name}: observed hostname {observed_hostname!r} does not match allowlist {allow.hostname_pattern!r}"
            )
        if sentinel is None:
            raise SafetyViolation(
                f"{node.name}: no sentinel row for {observed_hostname!r} in {self.profile.safety.sentinel_table} "
                "(default deny). The operator creates it by hand:\n"
                + SENTINEL_DDL_HINT.format(dbname=node.db.dbname, table=self.profile.safety.sentinel_table,
                                           tag=allow.inventory_tag)
            )
        if sentinel.get("inventory_tag") != allow.inventory_tag:
            raise SafetyViolation(
                f"{node.name}: sentinel inventory_tag {sentinel.get('inventory_tag')!r} != allowlist {allow.inventory_tag!r}"
            )
        if self.destructive and sentinel.get("disposable") is not True:
            raise SafetyViolation(f"{node.name}: sentinel does not mark this target disposable; {self.scenario.id} refuses")
        return {"hostname": observed_hostname, "inventory_tag": sentinel["inventory_tag"],
                "disposable": sentinel.get("disposable"), "created_by": sentinel.get("created_by")}

    def check_abort(self, signals: Mapping[str, Any]) -> list[AbortCheck]:
        """An abort predicate whose inputs do not apply to this target (e.g. replication lag on a
        standalone node) is recorded as not_applicable -- visible in the report, never silent."""
        out = []
        for text in self.scenario.abort_if:
            r = threshold_eval.evaluate_one(text, signals)
            out.append(AbortCheck(text, r.outcome, r.values))
        return out
