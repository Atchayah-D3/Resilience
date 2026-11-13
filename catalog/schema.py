"""Scenario catalog schema (Pydantic v2) and the six mechanical catalog checks.

Arch §4.1: the catalog is the source of truth; the same file drives documents, the CSV
tracker, pytest parametrization and validation, so scenario and test cannot drift.
Framework §7.1: the scenario record. Framework §13.7: the six checks, run in CI on every
catalog change.

A scenario is environment-agnostic: it names a fault TYPE and what to measure. Hosts,
ports, paths and injection drivers live only in envs/ (Arch §3.1, §5).
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Any, Literal

import yaml
from pydantic import AliasChoices, BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from resilience_tests.analysis import predicates
from resilience_tests.execution.workload import contract as workload_contract

CATALOG_ROOT = Path(__file__).resolve().parent

# --- vocabularies taken from the documents -----------------------------------------------

Tier = Literal["node_local", "cluster", "distributed"]
TIER_OF_PREFIX: dict[str, Tier] = {"NL": "node_local", "CL": "cluster", "DX": "distributed"}

# What an engine must be able to do for a scenario to be meaningful against it. A scenario
# lists what it needs; an engine whose adapter lacks it is skipped with a reason, never
# measured with a fabricated zero (Arch §8).
Capability = Literal[
    "transactional_markers",        # durable, acknowledged writes -- the RPO evidence (Arch §6.2)
    "workload_churn",               # update/delete traffic on a bounded table, so dead rows
                                    # accumulate and cumulative bloat can actually be measured
    "structural_integrity_check",   # a checker for table/index structure (e.g. pg_amcheck)
    "page_checksums",               # corruption detected on read
    "durability_settings",          # flush/commit settings are inspectable
    "list_append_history",          # list-append reads and appends in the marker transaction,
                                    # so Elle can check the recorded history (Arch §10.3)
    "tpcc_workload",                # the TPC-C-like workload: tables, loader, functions, checks
                                    # (docs/tpcc-workload-proposal.md)
    "contended_table",              # a share of marker transactions also reads and updates one
                                    # row of a shared harness table, so a lock held on that table
                                    # has users to block (workload.contended_share)
]

# Infrastructure a scenario cannot be run to specification without (docs/infra-requirements.md).
# An environment profile lists what it provides; a scenario needing anything else is skipped
# with the reason, never run against a weaker substitute.
InfraFeature = Literal[
    "dedicated_wal_volume",         # pg_wal on its own volume, large enough for 16 GB of WAL
]

# Framework §9 -- the 21 categories.
Category = Literal[
    "NL-D", "NL-E", "NL-C", "NL-W", "NL-I", "NL-R", "NL-N", "NL-M",
    "CL-F", "CL-S", "CL-R", "CL-N", "CL-C", "CL-B", "CL-X",
    "DX-T", "DX-M", "DX-S", "DX-Q", "DX-P", "DX-D",
]
COMPOUND_CATEGORY = "CL-X"

# Framework §4.1-4.2 / Arch Fig. 7 -- node roles a scenario applies to.
Role = Literal["standalone", "distdb.pc", "distdb.qc", "distdb.worker"]
EnvClass = Literal["E1", "E2", "E3", "E4"]
Priority = Literal["P0", "P1", "P2"]
# `tpcc`: the TPC-C-like workload (docs/tpcc-workload-proposal.md). Accepted by the catalog so scenarios can
# be written against it; refused as "not built yet" by the run plan and init until the profile exists.
WorkloadProfile = Literal["idle", "oltp_read", "oltp_write_heavy", "bulk_load", "long_analytic", "mixed", "tpcc"]
Environment = Literal["lab", "staging"]  # Framework §7.1: never production

# Arch §5 -- fault types (one FaultInjector interface; the env profile picks the driver).
FaultType = Literal[
    "process_kill", "service_restart", "config_reload", "connection_exhaustion", "idle_in_transaction", "resource_limit", "resource_stress", "network_delay", "network_loss",
    "network_rate", "network_partition", "proxy_fault", "storage_write_loss", "storage_latency",
    "filesystem_full", "host_power_loss", "clock_skew", "data_corruption", "counter_preseed",
    "lock_pileup",
]
# Env-profile section that resolves each fault type to a driver (Arch §3.1: power_control,
# noisy_neighbour, storage_fault; the OS/SSH driver serves the rest, Arch §5).
FAULT_DRIVER_SECTION: dict[str, str] = {
    "host_power_loss": "power_control",
    "storage_write_loss": "storage_fault",
    "storage_latency": "storage_fault",
}
DEFAULT_DRIVER_SECTION = "os_ssh"

FaultTarget = Literal["primary", "sync_standby"]

# A Framework row is `NL-C-01`. An internal scenario -- ours, not a row of the Framework --
# carries an `I` before its number (`NL-N-I03`): it belongs to a category but is not one of
# the scenarios the Framework declares, so the reconciliation checks leave it out. An internal
# scenario run in several variants names each with a letter (`NL-N-I03a`, `NL-N-I03b`).
SCENARIO_ID_RE = re.compile(
    r"^(?P<category>(?P<tier>NL|CL|DX)-[A-Z])-(?P<internal>I)?(?P<num>\d{2})(?P<variant>[a-z])?$")
SCENARIO_REF_RE = re.compile(r"\b(?:NL|CL|DX)-[A-Z]-(?:\d{2}|I\d{2}[a-z]?)\b")
# "Framework §7.3" / "Arch §4.2" / a bare "§13.1". The document matters: the two documents
# have different section trees, so a citation without one cannot be checked (Arch §4.1: a
# documentation defect must be a build failure, not a finding at the next review).
SECTION_REF_RE = re.compile(r"(?:(Arch|Architecture|Framework)\s*)?§\s?(\d+(?:\.\d+)*[a-z]?)")  # §10.1a
IDENT_RE = re.compile(r"^[a-z][a-z0-9_]*$")

# User hard rule + Arch §3.1: a scenario never names a host or a driver.
_IPV4_RE = re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}\b")
FORBIDDEN_ENV_TOKENS = frozenset({
    "virsh", "libvirt", "ipmi", "ipmitool", "redfish", "aws", "azure", "gcp", "sysrq",
    "lvm_snapshot", "zfs", "dmsetup", "chaosd", "toxiproxy", "qemu",
})

ScenarioId = Annotated[str, Field(pattern=SCENARIO_ID_RE.pattern)]


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class EnvironmentAxis(_Model):
    classes: list[EnvClass] = Field(min_length=1)
    env_sensitive: bool

    @field_validator("classes")
    @classmethod
    def _unique(cls, v: list[str]) -> list[str]:
        if len(set(v)) != len(v):
            raise ValueError("duplicate environment class")
        return v


class SteadyState(_Model):
    tps_min: float = Field(gt=0)
    p99_latency_ms_max: float = Field(gt=0)
    replication_lag_s_max: float | None = Field(default=None, ge=0)
    duration_s: int = Field(gt=0)
    # Load runs this long, unmeasured, before the baseline window opens (caches fill, the
    # workload settles). 0: the window opens once every worker is connected, as before.
    warmup_s: float = Field(default=0.0, ge=0)


# A PostgreSQL duration or plain number, as a session setting takes it: 5s, 500ms, 2min, 30000.
SESSION_VALUE_RE = re.compile(r"^\d+(us|ms|s|min|h|d)?$")


class WorkloadGroup(_Model):
    """A named group of clerks (docs/tpcc-contract.md §1). Every sample reports it separately."""

    name: str = Field(pattern=workload_contract.GROUP_NAME_PATTERN)
    clerks: int = Field(gt=0)


class TpccOptions(_Model):
    """Options of the `tpcc` profile (docs/tpcc-contract.md §1)."""

    warehouses: int = Field(gt=0)
    # group name -> the warehouse every clerk of that group works on
    home_warehouse: dict[str, int] = Field(default_factory=dict)


class Workload(_Model):
    profile: WorkloadProfile
    concurrency: int = Field(gt=0)
    transaction_markers: bool
    rate_tps: float | None = Field(default=None, gt=0)  # Framework §10.1 "at 1000 TPS"
    # `list_append`: every marker transaction also appends to and reads back Elle's lists, and
    # the operation history records what was actually read (Arch §10.3). `none`: no history.
    history: Literal["none", "list_append"] = "none"
    # Optional (docs/tpcc-contract.md §1). `session`: settings each clerk sets on its own
    # connection -- never ALTER ROLE, never a server setting. `groups`: named groups of clerks.
    # `tpcc`: options of the tpcc profile. Absent, a scenario behaves exactly as before.
    session: dict[str, str] = Field(default_factory=dict)
    groups: tuple[WorkloadGroup, ...] = ()
    tpcc: TpccOptions | None = None
    # The share of marker transactions that also read and update one row of the shared
    # contended table (the "table users"); the rest touch nothing else (the "others"). Absent:
    # every transaction is the plain marker transaction. Results are also kept per group.
    # (NL-N-I03: the custom marker workload only; under the tpcc profile the table is TPC-C's own.)
    contended_share: Annotated[float, Field(gt=0, lt=1)] | None = None
    # Where a transaction's latency is timed from. `send`: when it is sent (as before).
    # `intended_start`: when the fixed-rate schedule said it should start -- a stall that keeps
    # transactions from starting is then counted in their latency instead of hidden, and the
    # schedule is kept, so transactions missed during a stall start as soon as clerks are free.
    latency_from: Literal["send", "intended_start"] = "send"

    @model_validator(mode="after")
    def _declared_rate(self) -> Workload:
        # Framework §7.5: the workload is a declared parameter of every scenario, never chosen at
        # execution time. Without a rate the load would be "as fast as this machine can go".
        if self.rate_tps is None and self.profile != "idle":
            raise ValueError(f"workload.rate_tps must be declared for profile {self.profile!r} (Framework §7.5)")
        return self

    @model_validator(mode="after")
    def _contract_fields(self) -> Workload:
        unknown = sorted(set(self.session) - workload_contract.SESSION_SETTINGS)
        if unknown:
            raise ValueError(f"workload.session: {', '.join(unknown)} not allowed; only "
                             f"{', '.join(sorted(workload_contract.SESSION_SETTINGS))}")
        bad = sorted(k for k, v in self.session.items() if not SESSION_VALUE_RE.match(str(v)))
        if bad:
            raise ValueError(f"workload.session: {', '.join(bad)} must be a number with an optional unit "
                             "(us, ms, s, min, h, d), e.g. 5s")
        problems = workload_contract.group_problems(
            self.concurrency, [{"name": g.name, "clerks": g.clerks} for g in self.groups])
        if problems:
            raise ValueError("workload.groups: " + "; ".join(problems))
        if self.profile == "tpcc" and self.tpcc is None:
            raise ValueError("workload.tpcc (warehouses, ...) must be given for profile 'tpcc'")
        if self.profile != "tpcc" and self.tpcc is not None:
            raise ValueError("workload.tpcc is only allowed with profile 'tpcc'")
        if self.tpcc is not None:
            names = {g.name for g in self.groups}
            for group, warehouse in self.tpcc.home_warehouse.items():
                if group not in names:
                    raise ValueError(f"workload.tpcc.home_warehouse: group {group!r} is not declared in workload.groups")
                if not 1 <= warehouse <= self.tpcc.warehouses:
                    raise ValueError(f"workload.tpcc.home_warehouse: warehouse {warehouse} of group {group!r} "
                                     f"is not between 1 and {self.tpcc.warehouses}")
        if self.contended_share is not None and (self.profile != "oltp_write_heavy" or self.history != "none"):
            raise ValueError("workload.contended_share is only defined for the plain oltp_write_heavy marker workload")
        return self
        return self


# A database state the fault must land inside (Framework §10.2 NL-C-02 "while the checkpointer
# is active", NL-C-03 "during a 10M-row INSERT", NL-C-06 "mid-build"; Framework §10.7 NL-M-03
# "kill -9 an autovacuum worker"). The orchestrator establishes the state, confirms it is in
# progress, then injects; which scenario asked is never consulted -- only this field.
FaultDuring = Literal["checkpoint", "large_transaction", "concurrent_index_build", "autovacuum_worker"]

# What a data_corruption fault damages (Framework §10.4): NL-I-01 a heap page, NL-I-02 a btree
# file. Absent means heap. The adapter owns the relation; the catalog only says which kind.
CorruptionRelationKind = Literal["heap", "btree_index"]


class LockPileup(_Model):
    """A lock pile-up behind a schema change (fault type lock_pileup): what holds the table,
    which schema change queues behind it, and the schema-change session's own lock timeout.

    `blocker: long_read` -- a session opens a read on the contended table and holds its
    transaction open; `ddl_after_s` later the schema change is issued and must wait for it, so
    every new query on the table queues behind the schema change. `blocker: none` -- nothing
    holds the table; a rewriting schema change gets its lock at once and holds it while it
    rewrites the table. `ddl_lock_timeout_s` is set on the schema-change session only (0: wait
    for ever). fault.duration bounds the whole pile-up."""

    blocker: Literal["long_read", "none"]
    ddl: Literal["add_column", "add_column_rewrite"]
    ddl_lock_timeout_s: float = Field(ge=0)
    ddl_after_s: float = Field(default=0.0, ge=0)
    # The table the pile-up forms on: the harness's own contended table (marker workload with
    # contended_share), or the TPC-C stock table (workload profile tpcc; its users are
    # New-Order and Stock-Level). The adapter maps the name to the engine's table.
    table: Literal["contended_table", "tpcc_stock"] = "contended_table"

    @model_validator(mode="after")
    def _consistent(self) -> LockPileup:
        if self.blocker == "none" and self.ddl_after_s:
            raise ValueError("lock_pileup.ddl_after_s needs a blocker to wait behind (blocker: long_read)")
        if self.blocker == "long_read" and not self.ddl_after_s:
            raise ValueError("lock_pileup.ddl_after_s must be set with blocker: long_read, so the read "
                             "is seen blocking nobody on its own before the schema change arrives")
        return self


class Fault(_Model):
    type: FaultType
    # The env-profile section that serves this fault type (os_ssh, power_control, ...); the
    # profile's <section>.driver then names the driver. Written `section:`; the former key
    # `driver:` is still accepted, so existing scenario files read unchanged.
    section: str = Field(validation_alias=AliasChoices("section", "driver"))
    target: FaultTarget
    duration: Literal["permanent"] | Annotated[float, Field(gt=0)] = "permanent"
    during: FaultDuring | None = None
    relation_kind: CorruptionRelationKind | None = None
    lock_pileup: LockPileup | None = None

    @model_validator(mode="after")
    def _driver_section(self) -> Fault:
        expected = FAULT_DRIVER_SECTION.get(self.type, DEFAULT_DRIVER_SECTION)
        if self.section != expected:
            raise ValueError(
                f"fault.section for {self.type} must be the env-profile section {expected!r}, got {self.section!r}"
            )
        if self.relation_kind is not None and self.type != "data_corruption":
            raise ValueError("fault.relation_kind is only defined for data_corruption")
        if (self.lock_pileup is not None) != (self.type == "lock_pileup"):
            raise ValueError("fault.lock_pileup is required for, and only defined for, lock_pileup")
        if self.lock_pileup is not None:
            if self.duration == "permanent":
                raise ValueError("lock_pileup is held and released by the harness: fault.duration must be seconds")
            if self.lock_pileup.ddl_after_s >= self.duration:
                raise ValueError("lock_pileup.ddl_after_s must fall inside fault.duration")
        return self


class BlastRadius(_Model):
    max_nodes_affected: int = Field(ge=1)
    environments: list[Environment] = Field(min_length=1)


class CompoundFaultB(_Model):
    trigger: Literal["on_state", "on_metric", "after_phase"]
    condition: str
    timeout_s: float = Field(gt=0)
    type: FaultType
    target: FaultTarget


class BlastRadiusOverride(_Model):
    max_nodes_affected: int = Field(ge=2, le=2)  # Arch §4.3 / §15: CL-X hard ceiling is 2


class Repeat(_Model):
    """Framework NL-C-05: a scenario that applies its fault repeatedly, measuring each cycle.

    `interval_s` is the settle time between one cycle's recovery and the next fault, so every
    cycle starts from a comparable state rather than from the tail of the previous recovery."""

    cycles: int = Field(ge=2, le=100)
    interval_s: float = Field(gt=0)


class Compound(_Model):
    fault_b: CompoundFaultB
    blast_radius_override: BlastRadiusOverride


class Scenario(_Model):
    id: ScenarioId
    name: str = Field(min_length=1)
    tier: Tier
    category: Category
    applies_to: list[Role] = Field(min_length=1)
    environment: EnvironmentAxis
    steady_state: SteadyState
    workload: Workload
    fault: Fault
    blast_radius: BlastRadius
    abort_if: list[str] = Field(default_factory=list)
    compound: Compound | None = None
    repeat: Repeat | None = None
    requires: list[Capability] = Field(default_factory=list)
    needs_infra: list[InfraFeature] = Field(default_factory=list)
    measure: list[str] = Field(min_length=1)
    accept: list[str] = Field(min_length=1)
    priority: Priority
    description: str | None = None
    see_also: list[ScenarioId] = Field(default_factory=list)

    @field_validator("measure")
    @classmethod
    def _measure_names(cls, v: list[str]) -> list[str]:
        bad = [m for m in v if not IDENT_RE.match(m)]
        if bad:
            raise ValueError(f"measure names must be snake_case identifiers: {bad}")
        if len(set(v)) != len(v):
            raise ValueError("duplicate measure name")
        return v

    @field_validator("accept", "abort_if")
    @classmethod
    def _predicates(cls, v: list[str]) -> list[str]:
        for text in v:
            predicates.parse(text)  # raises PredicateError -> ValidationError
        return v

    @model_validator(mode="after")
    def _cross_field(self) -> Scenario:
        m = SCENARIO_ID_RE.match(self.id)
        assert m is not None
        if m["category"] != self.category:
            raise ValueError(f"id {self.id} does not belong to category {self.category}")
        if TIER_OF_PREFIX[m["tier"]] != self.tier:
            raise ValueError(f"id {self.id} is tier {TIER_OF_PREFIX[m['tier']]}, not {self.tier}")
        if m["variant"] and not m["internal"]:
            raise ValueError(f"id {self.id}: only an internal scenario (`-I`) is run in lettered variants")

        # Arch §4.3 / §15: the node ceiling is 1 everywhere; CL-X alone carries an override of 2,
        # and it is category-scoped -- no other scenario may set it.
        if self.blast_radius.max_nodes_affected != 1:
            raise ValueError("blast_radius.max_nodes_affected must be 1 (CL-X uses compound.blast_radius_override)")
        if self.category == COMPOUND_CATEGORY and self.compound is None:
            raise ValueError("CL-X scenarios must define compound.fault_b")
        if self.category != COMPOUND_CATEGORY and self.compound is not None:
            raise ValueError("compound is only permitted in category CL-X (fails closed)")
        if self.repeat is not None and self.compound is not None:
            raise ValueError("a scenario is either repeated or compound, not both")

        # Framework §6.4: any RPO-measuring scenario declares transaction_markers: true.
        if "rpo_txn" in self.measure and not self.workload.transaction_markers:
            raise ValueError("rpo_txn is measured, so workload.transaction_markers must be true")
        if self.workload.transaction_markers and "transactional_markers" not in self.requires:
            raise ValueError("transaction_markers needs requires: [transactional_markers]")
        if "structural_integrity_errors" in self.measure and "structural_integrity_check" not in self.requires:
            raise ValueError("structural_integrity_errors needs requires: [structural_integrity_check]")
        if self.workload.history == "list_append" and "list_append_history" not in self.requires:
            raise ValueError("workload.history list_append needs requires: [list_append_history]")
        if "elle_anomalies_count" in self.measure and self.workload.history != "list_append":
            raise ValueError("elle_anomalies_count is measured, so workload.history must be list_append")
        if self.fault.during is not None and self.fault.type != "process_kill":
            raise ValueError("fault.during is only defined for process_kill")
        if self.workload.contended_share is not None and "contended_table" not in self.requires:
            raise ValueError("workload.contended_share needs requires: [contended_table]")
        pileup = self.fault.lock_pileup
        if pileup is not None and pileup.table == "contended_table" and self.workload.contended_share is None:
            # a pile-up on a table nobody uses blocks nobody: the fault would test nothing
            raise ValueError("lock_pileup needs workload.contended_share, so the table it locks has users")
        if pileup is not None and pileup.table == "tpcc_stock" and self.workload.profile != "tpcc":
            raise ValueError("lock_pileup.table tpcc_stock needs workload.profile tpcc, so the table it locks has users")
        if self.internal and self.environment.env_sensitive:
            raise ValueError("an internal scenario is not a Framework §7.3 environment-sensitive row")

        # The TPC-C workload is judged on what it exists to prove (docs/tpcc-workload-guide.md 10.3):
        # a scenario file that selects it must gate those measures, so a partial commit can never
        # be reported while the run passes. (--tpcc-warehouses adds them: hooks.with_tpcc_workload.)
        if self.workload.profile == "tpcc":
            missing_gates = [g for g in workload_contract.TPCC_GATES if g not in self.accept]
            if missing_gates:
                raise ValueError(f"profile tpcc needs these accept predicates: {missing_gates}")

        # Every accept predicate must be computed from declared measures.
        declared = set(self.measure)
        for text in self.accept:
            missing = predicates.parse(text).names - declared
            if missing:
                raise ValueError(f"accept {text!r} references undeclared measures {sorted(missing)}")

        _reject_environment_leak(self)
        return self

    @property
    def accept_predicates(self) -> list[predicates.Predicate]:
        return [predicates.parse(t) for t in self.accept]

    @property
    def abort_predicates(self) -> list[predicates.Predicate]:
        return [predicates.parse(t) for t in self.abort_if]

    @property
    def number(self) -> int:
        m = SCENARIO_ID_RE.match(self.id)
        assert m is not None
        return int(m["num"])

    @property
    def internal(self) -> bool:
        """Ours, not a row of the Framework (`NL-N-I03`): kept out of check 2 and check 6."""
        m = SCENARIO_ID_RE.match(self.id)
        return bool(m and m["internal"])


def _strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, BaseModel):
        value = value.model_dump()
    if isinstance(value, dict):
        return [s for v in value.values() for s in _strings(v)]
    if isinstance(value, (list, tuple)):
        return [s for v in value for s in _strings(v)]
    return []


def _reject_environment_leak(scenario: Scenario) -> None:
    for text in _strings(scenario.model_dump(exclude={"fault": {"driver"}})):
        if _IPV4_RE.search(text):
            raise ValueError(f"scenario contains an address ({text!r}); hosts belong in envs/")
        tokens = set(re.findall(r"[a-z0-9_]+", text.lower()))
        leaked = tokens & FORBIDDEN_ENV_TOKENS
        if leaked:
            raise ValueError(f"scenario names driver/environment {sorted(leaked)}; drivers belong in envs/")


# --- loading -----------------------------------------------------------------------------


@dataclass
class LoadError:
    path: Path
    message: str


@dataclass
class Catalog:
    scenarios: dict[str, Scenario] = field(default_factory=dict)
    errors: list[LoadError] = field(default_factory=list)


def load_scenario(path: Path) -> Scenario:
    with path.open() as fh:
        data = yaml.safe_load(fh)
    scenario = Scenario.model_validate(data)
    if path.stem != scenario.id:
        raise ValueError(f"file name {path.name} must be {scenario.id}.yaml")
    return scenario


def load_catalog(root: Path = CATALOG_ROOT) -> Catalog:
    catalog = Catalog()
    for tier_dir in ("NL", "CL", "DX"):
        for path in sorted((root / tier_dir).glob("*.yaml")):
            try:
                scenario = load_scenario(path)
            except (ValidationError, ValueError, yaml.YAMLError) as exc:
                catalog.errors.append(LoadError(path, str(exc)))
                continue
            if scenario.id in catalog.scenarios:
                catalog.errors.append(LoadError(path, f"duplicate id {scenario.id}"))
                continue
            catalog.scenarios[scenario.id] = scenario
    return catalog


# --- the six checks (Framework §13.7) ----------------------------------------------------


class Reference(_Model):
    """Declared figures from the Framework document that the catalog must reconcile to."""

    arch_sections: list[str]
    categories: dict[Category, dict[str, int]]  # {count, P0, P1, P2}
    tiers: dict[Tier, dict[str, int]]
    total: dict[str, int]
    env_sensitive: list[ScenarioId]
    runs: int
    framework_sections: list[str]


def load_reference(path: Path = CATALOG_ROOT / "reference.yaml") -> Reference:
    with path.open() as fh:
        return Reference.model_validate(yaml.safe_load(fh))


@dataclass
class CheckResult:
    number: int
    name: str
    passed: bool
    detail: list[str]


def run_checks(catalog: Catalog, reference: Reference, *, complete: bool = True) -> list[CheckResult]:
    """The six checks. `complete=False` is for the build-out period (Arch §16), while only part
    of the Framework catalog is authored: checks 1, 3, 4 and 5 are unchanged; check 2 reports
    gaps without failing; check 6 still fails on anything authored that CONTRADICTS the
    Framework (more scenarios or P0s than declared, a wrong env_sensitive flag, too many runs)
    but not on what is merely not written yet. The release gate always uses complete=True.

    Internal scenarios (`NL-N-I03`) are checked like any other by checks 1, 3, 4 and 5; checks
    2 and 6 compare the catalog with the Framework's own rows, so they see Framework rows only."""
    s = catalog.scenarios
    framework_rows = {sid: sc for sid, sc in s.items() if not sc.internal}
    return [
        _check1_wellformed(catalog),
        _check2_contiguity(framework_rows, complete=complete),
        _check3_falsifiability(s),
        _check4_scenario_refs(s),
        _check5_section_refs(s, reference),
        _check6_reconciliation(framework_rows, reference, complete=complete),
    ]


def _check1_wellformed(catalog: Catalog) -> CheckResult:
    detail = [f"{e.path.name}: {e.message}" for e in catalog.errors]
    return CheckResult(1, "Row well-formedness", not detail, detail)


def _check2_contiguity(s: dict[str, Scenario], *, complete: bool = True) -> CheckResult:
    by_cat: dict[str, list[int]] = defaultdict(list)
    for sc in s.values():
        by_cat[sc.category].append(sc.number)
    detail = []
    for cat, nums in sorted(by_cat.items()):
        expected = list(range(1, max(nums) + 1))
        missing = sorted(set(expected) - set(nums))
        if missing:
            detail.append(f"{cat}: missing {', '.join(f'{cat}-{n:02d}' for n in missing)}")
    return CheckResult(2, "ID contiguity", not detail or not complete, detail)


def _check3_falsifiability(s: dict[str, Scenario]) -> CheckResult:
    detail = []
    for sc in s.values():
        for text in sc.accept:
            try:
                predicates.parse(text)
            except predicates.PredicateError as exc:
                detail.append(f"{sc.id}: {exc}")
    return CheckResult(3, "Criterion falsifiability", not detail, detail)


def _check4_scenario_refs(s: dict[str, Scenario]) -> CheckResult:
    detail = []
    for sc in s.values():
        refs = set(sc.see_also) | set(SCENARIO_REF_RE.findall(sc.description or ""))
        for ref in sorted(refs - set(s)):
            detail.append(f"{sc.id}: references undefined scenario {ref}")
    return CheckResult(4, "Scenario cross-references", not detail, detail)


def _check5_section_refs(s: dict[str, Scenario], ref: Reference) -> CheckResult:
    known = {"Framework": set(ref.framework_sections), "Arch": set(ref.arch_sections)}
    detail = []
    for sc in s.values():
        for document, sec in SECTION_REF_RE.findall(sc.description or ""):
            if not document:
                detail.append(f"{sc.id}: §{sec} does not say which document -- "
                              f"cite it as 'Framework §{sec}' or 'Arch §{sec}'")
                continue
            name = "Framework" if document == "Framework" else "Arch"
            if sec not in known[name]:
                detail.append(f"{sc.id}: {name} §{sec} does not resolve to a heading in that document")
    return CheckResult(5, "Section cross-references", not detail, detail)


def _check6_reconciliation(s: dict[str, Scenario], ref: Reference, *, complete: bool = True) -> CheckResult:
    detail = []
    pending: list[str] = []  # not yet authored: a failure only for the complete catalog
    # The reference must be internally consistent first. Framework §9 gives count and P0 per
    # category; P1/P2 only per tier (§5).
    prefix_of = {v: k for k, v in TIER_OF_PREFIX.items()}
    for tier, figures in ref.tiers.items():
        cats = [v for c, v in ref.categories.items() if c.startswith(prefix_of[tier])]
        for key in ("count", "P0"):
            summed = sum(v[key] for v in cats)
            if summed != figures[key]:
                detail.append(f"reference: {tier} {key} categories sum {summed} != declared {figures[key]}")
        if figures["P0"] + figures["P1"] + figures["P2"] != figures["count"]:
            detail.append(f"reference: {tier} priority split does not sum to {figures['count']}")
    for key in ("count", "P0", "P1", "P2"):
        if sum(t[key] for t in ref.tiers.values()) != ref.total[key]:
            detail.append(f"reference: tier {key} sum != total {ref.total[key]}")

    # catalog rows against the declared figures
    cat_count = Counter(sc.category for sc in s.values())
    cat_p0 = Counter(sc.category for sc in s.values() if sc.priority == "P0")
    for cat, fig in sorted(ref.categories.items()):
        if cat_count[cat] > fig["count"] or cat_p0[cat] > fig["P0"]:
            detail.append(f"{cat}: {cat_count[cat]} scenarios / {cat_p0[cat]} P0 authored, "
                          f"framework declares {fig['count']} / {fig['P0']}")
        elif cat_count[cat] != fig["count"]:
            pending.append(f"{cat}: {cat_count[cat]} scenarios authored, framework declares {fig['count']}")
        elif cat_p0[cat] != fig["P0"]:
            detail.append(f"{cat}: {cat_p0[cat]} P0, framework declares {fig['P0']}")
    prio = Counter(sc.priority for sc in s.values())
    for key in ("P0", "P1", "P2"):
        if prio[key] > ref.total[key]:
            detail.append(f"total {key}: {prio[key]} authored, framework declares {ref.total[key]}")
        elif prio[key] != ref.total[key]:
            pending.append(f"total {key}: {prio[key]} authored, framework declares {ref.total[key]}")

    sensitive = {sc.id for sc in s.values() if sc.environment.env_sensitive}
    declared_sensitive = set(ref.env_sensitive)
    for sid in sorted(sensitive - declared_sensitive):
        detail.append(f"{sid}: marked env_sensitive but Framework §7.3 does not list it")
    for sid in sorted((declared_sensitive & set(s)) - sensitive):
        detail.append(f"{sid}: Framework §7.3 lists it env_sensitive but the catalog does not")

    runs = sum(len(sc.environment.classes) if sc.environment.env_sensitive else 1 for sc in s.values())
    if runs > ref.runs:
        detail.append(f"run plan: {runs} environment-qualified runs, framework declares {ref.runs}")
    elif runs != ref.runs:
        pending.append(f"run plan: {runs} environment-qualified runs, framework declares {ref.runs}")
    passed = not detail and (not pending or not complete)
    return CheckResult(6, "Numeric reconciliation", passed, detail + pending)


def main(argv: list[str] | None = None) -> int:
    """CI entry point (Arch §4.1, §14: the checks run on every catalog change and block merge).

        python -m catalog.schema             # release gate: the complete Framework catalog
        python -m catalog.schema --partial   # build-out: authored rows must not contradict it
    """
    import argparse

    ap = argparse.ArgumentParser(description="Run the six catalog checks (Framework §13.7).")
    ap.add_argument("--partial", action="store_true",
                    help="catalog is still being authored: report, but do not fail on, rows not yet written")
    args = ap.parse_args(argv)
    results = run_checks(load_catalog(), load_reference(), complete=not args.partial)
    for r in results:
        print(f"{r.number} {r.name:<28} {'PASS' if r.passed else 'FAIL'}")
        for d in r.detail:
            print(f"    {d}")
    return 0 if all(r.passed for r in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
