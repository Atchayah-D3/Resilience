"""Environment profile (Arch §3.1).

Each environment class is described by a profile the harness consumes, so scenarios stay
environment-agnostic: a scenario names a fault type; the profile decides which driver serves
it and where every host, port and path is. Secrets are never stored here -- database
passwords come from the driver host's ~/.pgpass, SSH is key-based (Arch §12).
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from catalog.schema import EnvClass, InfraFeature, Role

ENVS_ROOT = Path(__file__).resolve().parents[2] / "envs"

StorageClass = Literal["local_nvme", "bbu_raid", "san", "network_block", "nfs"]  # Arch §3.1
PowerDriver = Literal["libvirt", "ipmi", "redfish", "aws", "azure", "gcp"]  # Arch §3.1
ResetDriver = Literal["lvm_snapshot", "zfs_snapshot", "none"]  # Arch §3 / Arch §18 decision 3
TopologyRole = Literal["primary", "sync_standby", "async_standby"]
PHASES = ("reset", "init", "baseline", "pre_fault", "fault_inject", "recovery", "validate", "report", "cleanup")


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SSHEndpoint(_Model):
    host: str
    port: int = Field(gt=0, lt=65536)
    user: str


class DbEndpoint(_Model):
    host: str
    port: int = Field(gt=0, lt=65536)
    dbname: str
    user: str


class Database(_Model):
    """Which adapter serves this environment (Arch §8). The harness supports any engine with
    an adapter; nothing above the adapter layer is engine-specific."""

    engine: str


class Node(_Model):
    name: str
    role: Role
    topology_role: TopologyRole
    ssh: SSHEndpoint
    db: DbEndpoint
    # Where clients (workload + write prober) connect: the pooler if the deployment has one,
    # otherwise the database directly (Arch §7.2 T_reconnect = first write through the pooler).
    client: DbEndpoint
    pgdata: str
    pg_bin: str
    os_user: str  # OS account owning the database (adapters run local tools as this user)
    service: str
    log_file: str
    power_domain: str | None = None  # the node's identity at the power-control layer
    # The address range the driver host appears to come from, as seen by this node. Usually
    # the driver host itself, but not when traffic is translated on the way (NAT, a bastion).
    client_source_cidr: str | None = None
    # Databases the post-fault integrity check covers. Default: the harness database only. On
    # customer infrastructure every database listed here must already carry the integrity
    # extension -- the harness never installs anything -- and must be checkable within the
    # validate phase bound.
    integrity_databases: list[str] | None = Field(default=None, min_length=1)


class DriverHost(_Model):
    """Arch §6.2: the workload driver and its marker journals run on a separate host."""

    host: str
    run_dir: str


class PowerControl(_Model):
    driver: PowerDriver
    uri: str | None = None  # e.g. qemu+ssh://<hypervisor>/system for libvirt


class NoisyNeighbour(_Model):
    injectable: bool
    driver: str | None = None


class OsSsh(_Model):
    """Driver for faults applied through the OS over SSH (Arch §5: process kill, systemd
    properties, filesystem). Always available; the reference implementation."""

    driver: Literal["os_ssh"]


class StorageFault(_Model):
    dm_shim: bool


class Reset(_Model):
    driver: ResetDriver
    snapshot: str | None = None

    @model_validator(mode="after")
    def _snapshot(self) -> Reset:
        if self.driver != "none" and not self.snapshot:
            raise ValueError(f"reset.driver {self.driver} requires reset.snapshot")
        return self


class Allowlist(_Model):
    hostname_pattern: str
    inventory_tag: str


class Safety(_Model):
    """Arch §15: environment allowlist (default deny) and the destructive-scenario gate.

    The sentinel table is created BY HAND by the operator on each target, never by the
    harness. Its row carries the fingerprint the allowlist checks (hostname, inventory tag)
    and the `disposable` flag the destructive gate requires in addition to
    --target-is-disposable.
    """

    allowlist: Allowlist
    sentinel_table: str = Field(pattern=r"^[a-z_][a-z0-9_]*\.[a-z_][a-z0-9_]*$")
    # Standing abort (Arch §15 "abort conditions evaluated continuously"), for every scenario on
    # this environment: stop the run once the filesystem holding the data directory is fuller
    # than this. A crash loop or blocked vacuum can fill it, and where PGDATA shares the root
    # filesystem a full disk takes the host down with it. Operator-owned; None = no such abort.
    max_data_fs_used_pct: float | None = Field(default=None, gt=0, le=100)


class EnvProfile(_Model):
    name: str
    env_class: EnvClass = Field(alias="class")
    environment: Literal["lab", "staging", "production"]
    database: Database
    storage_class: StorageClass | None
    power_control: PowerControl | None = None  # only environments that provide power control
    noisy_neighbour: NoisyNeighbour
    os_ssh: OsSsh | None = None
    storage_fault: StorageFault
    # Infrastructure this environment provides (docs/infra-requirements.md). A scenario that
    # needs something not listed is skipped with the reason -- never run on a substitute.
    infra: list[InfraFeature] = Field(default_factory=list)
    reset: Reset
    sla_tier_target: Literal["platinum", "gold", "silver"]
    driver_host: DriverHost
    nodes: list[Node] = Field(min_length=1)
    safety: Safety
    phase_timeouts_s: dict[str, float]
    # Framework §19 decision 2 / Arch §18 decision 2: evidence limitations must be stated in
    # every report produced against this environment.
    disclosures: list[str] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)

    @model_validator(mode="after")
    def _checks(self) -> EnvProfile:
        missing = set(PHASES) - set(self.phase_timeouts_s)
        extra = set(self.phase_timeouts_s) - set(PHASES)
        if missing or extra:
            raise ValueError(f"phase_timeouts_s must cover exactly {PHASES} (missing {sorted(missing)}, extra {sorted(extra)})")
        if any(v <= 0 for v in self.phase_timeouts_s.values()):
            raise ValueError("every phase timeout must be > 0 -- an unbounded wait is a defect (Arch §15)")
        names = [n.name for n in self.nodes]
        if len(set(names)) != len(names):
            raise ValueError("duplicate node name")
        re.compile(self.safety.allowlist.hostname_pattern)
        if self.driver_host.host in {n.ssh.host for n in self.nodes} | {n.db.host for n in self.nodes}:
            raise ValueError("driver_host must not be a node under test (Arch §6.2)")
        return self

    def node(self, name: str) -> Node:
        for n in self.nodes:
            if n.name == name:
                return n
        raise KeyError(name)


def load_profile(name_or_path: str | Path) -> EnvProfile:
    path = Path(name_or_path)
    if not path.suffix:
        path = ENVS_ROOT / f"{name_or_path}.yaml"
    with path.open() as fh:
        profile = EnvProfile.model_validate(yaml.safe_load(fh))
    if profile.name != path.stem:
        raise ValueError(f"profile name {profile.name!r} must match file name {path.stem!r}")
    return profile
