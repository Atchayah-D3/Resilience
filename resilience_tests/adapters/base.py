"""Target adapters (Arch §8): the only place in the harness that knows what a database is.

Everything above this line -- orchestrator, workload driver, probes, markers, analysis --
speaks to a database only through these two interfaces, so a new engine is a new adapter
rather than a change to the framework.

An adapter declares what it can do. A capability it does not have is reported as
NOT_APPLICABLE, never as zero: a scenario whose acceptance criteria depend on a missing
capability fails rather than passing on a measurement nobody took.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, ClassVar

from resilience_tests.control.profile import DbEndpoint, Node


class Capability(str, Enum):
    """What an engine can offer. The names are the catalog's vocabulary (catalog/schema.py):
    a scenario lists what it requires, and the run plan skips engines that lack it."""

    TRANSACTIONAL_MARKERS = "transactional_markers"
    WORKLOAD_CHURN = "workload_churn"
    STRUCTURAL_INTEGRITY_CHECK = "structural_integrity_check"
    PAGE_CHECKSUMS = "page_checksums"
    DURABILITY_SETTINGS = "durability_settings"


class TransactionOutcome(str, Enum):
    """Arch §6.2, §7.1 -- the :fail-versus-:info rule.

    DEFINITELY_ABORTED is only for an error the server returned to say the transaction did
    not happen. A timeout, a reset connection or a killed server is UNKNOWN: the database is
    permitted to have committed it or not, and treating that as a failure produces confident,
    wrong results.
    """

    COMMITTED = "committed"
    DEFINITELY_ABORTED = "definitely_aborted"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class IntegrityResult:
    structural_errors: int | None      # None when the engine has no structural checker
    checksum_failures: int | None      # None when the engine has no page checksums
    raw_output: str = ""
    detail: dict[str, Any] = field(default_factory=dict)


class DatabaseSession(ABC):
    """One client connection. The workload and the probes use nothing else."""

    @abstractmethod
    async def commit_marker(self, seq: int, marker_id: str) -> TransactionOutcome:
        """One marker transaction: begin, record the marker, commit. Returns how it ended --
        the caller must not have to interpret engine-specific exceptions."""

    async def commit_marker_with_churn(self, seq: int, marker_id: str, churn_key: int,
                                       replace: bool) -> TransactionOutcome:
        """The marker transaction of `commit_marker`, plus UPDATE/DELETE traffic against a
        bounded side table, in the SAME transaction (workload profile `mixed`).

        The marker table stays append-only -- the RPO arithmetic is `acked - in_db`, which only
        holds if markers are never rewritten. The churn table is what produces dead tuples, and
        it is the one bloat is measured on: its live row count is held constant, so growth in
        its bytes is garbage that was never reclaimed and nothing else.

        Engines without Capability.WORKLOAD_CHURN inherit this refusal."""
        raise NotImplementedError(f"{type(self).__name__} does not implement the churn workload")

    @abstractmethod
    async def try_write(self) -> bool:
        """A single small write, for the write prober. True when it succeeded."""

    @abstractmethod
    async def ping(self) -> bool:
        """A single read, for the read prober."""

    @abstractmethod
    async def close(self) -> None: ...

    @property
    @abstractmethod
    def is_closed(self) -> bool: ...


class BaseDatabaseAdapter(ABC):
    """Per-engine implementation. Registered by engine name; selected by the env profile."""

    engine: ClassVar[str]
    capabilities: ClassVar[frozenset[Capability]]

    def __init__(self, node: Node) -> None:
        self.node = node

    def has(self, capability: Capability) -> bool:
        return capability in self.capabilities

    # --- client sessions -------------------------------------------------------------

    @abstractmethod
    async def session(self, endpoint: DbEndpoint | None = None, timeout_s: float = 5.0) -> DatabaseSession:
        """Open a session. `endpoint` defaults to the node's client endpoint."""

    # --- state the harness needs ------------------------------------------------------

    @abstractmethod
    async def prepare_harness_state(self) -> None:
        """Create whatever the marker protocol needs, and clear state from previous runs."""

    @abstractmethod
    async def marker_ids(self) -> set[str]:
        """Every marker present after recovery -- the `db` set of the RPO arithmetic."""

    @abstractmethod
    async def sentinel(self, table: str, hostname: str) -> dict[str, Any] | None:
        """The operator-created sentinel row (Arch §15). Adapters never create it."""

    # --- what the harness reports and gates on ----------------------------------------

    @abstractmethod
    async def durability_settings(self) -> dict[str, str]:
        """Engine-specific settings recorded as evidence in every run."""

    @abstractmethod
    async def certification_blockers(self) -> list[str]:
        """Conditions under which this engine must refuse to certify a result -- e.g. page
        checksums disabled (Framework §16.2, NL-I-09). Empty list means nothing blocks."""

    async def storage_footprint(self) -> dict[str, Any]:
        """On-disk footprint right now: bytes, live and dead rows, write-ahead log size, and
        whatever bound the engine itself declares for that log. Sampled before and after each
        crash cycle so growth the workload does not explain becomes visible (Framework
        NL-C-05, "no cumulative bloat"). An engine that cannot report this returns {}."""
        return {}

    # Live rows the churn table holds, and therefore the key space the workload may address.
    # The driver reads it from here rather than keeping its own copy: a mismatch would mean
    # UPDATEs that match no row, leaving the bloat measurement silently flat -- the exact
    # failure the churn workload exists to prevent.
    churn_key_space: int = 0

    async def redo_distance_bytes(self) -> int | None:
        """Write-ahead log bytes between the last checkpoint's redo point and now: exactly the
        work a crash at this instant would leave to replay. Sampled immediately before each
        kill so a recovery time can be read against the work it actually did, rather than
        compared with another cycle that happened to crash with less outstanding.

        None when the engine cannot report it, or the role may not read it."""
        return None

    async def mark_integrity_baseline(self) -> dict[str, Any]:
        """Record integrity counters before the run, so `integrity_check` reports what happened
        during THIS run rather than since the counters were last reset. Returns the baseline as
        evidence. Engines with no cumulative counters need not override it."""
        return {}

    @abstractmethod
    async def integrity_check(self, timeout_s: float) -> IntegrityResult:
        """Structural and checksum verification, as far as the engine supports it. Counters
        are reported relative to `mark_integrity_baseline` when it was called."""

    async def trigger_checkpoint_and_await_active(self, timeout_s: float = 10.0) -> dict[str, Any]:
        """Trigger a checkpoint under the current workload and wait until the checkpointer is
        actively writing/syncing buffers (not idle), returning checkpointer evidence (PID, wait
        event, active state).

        Used by NL-C-02 to guarantee fault injection lands precisely while the checkpointer
        is active, replacing arbitrary sleep guessing with deterministic state synchronization.

        Default: returns checkpointer_active=False, which causes the orchestrator to abort
        (fail closed). Subclasses must override with engine-specific synchronization."""
        return {"checkpointer_active": False, "method": "default (not implemented)"}

    async def checkpoint_in_flight_at_kill(self, log_lines: Sequence[str]) -> dict[str, Any]:
        """Whether the checkpoint started by `trigger_checkpoint_and_await_active` was still
        unfinished when the process was killed, judged from what the REPLACEMENT process logged
        as it recovered (`log_lines`: the engine's log lines received after T0).

        Returns `in_flight`: True (proven unfinished), False (proven finished before the kill --
        the fault did not land) or None (no evidence either way). Default: None."""
        return {"in_flight": None, "note": "this engine cannot show where crash recovery started"}

    async def inject_idle_transaction(self) -> dict[str, Any]:
        """Inject an open idle-in-transaction holding back the vacuum xmin horizon (NL-M-05).
        Default returns supported=False; subclasses must override."""
        return {"supported": False, "method": "default (not implemented)"}

    async def check_idle_transaction(self, pid: int | None = None) -> dict[str, Any]:
        """Check status of the idle-in-transaction backend: whether it is gone and the harness's
        own session closed (`terminated_by_timeout`), or still open and holding xmin (NL-M-05).
        When the closed session reported why, its error code is `termination_sqlstate`."""
        return {"terminated_by_timeout": False, "still_idle": False}

    def idle_timeout_log_patterns(self) -> tuple[str, ...]:
        """Regexes matching the line this engine logs when it ends a session because of its
        idle-in-transaction timeout. Evidence of WHY a session ended (NL-M-05 path A)."""
        return ()

    idle_timeout_sqlstates: tuple[str, ...] = ()  # error codes with the same meaning

    async def probe_vacuum_horizon(self) -> dict[str, Any]:
        """Run the engine's vacuum on the churn table while the idle session is still open and
        report whether it found dead tuples it was NOT ALLOWED to remove (`dead_not_removable`)
        and the cutoff it used (`removable_cutoff`). This is direct evidence that the vacuum
        horizon is pinned. Default: not supported."""
        return {"supported": False}

    async def close_idle_transaction(self) -> dict[str, Any]:
        """Cleanly terminate or rollback any active idle-in-transaction connection injected
        during fault testing (NL-M-05). Must be idempotent."""
        return {}

    async def evaluate_vacuum_bloat(self) -> dict[str, Any]:
        """Dead-tuple counters on the churn table, as context for NL-M-05. Default: none."""
        return {}

    async def observe_fault_settings(self, fault_type: str) -> dict[str, str]:
        """Read -- never write -- the engine settings that decide how `fault_type` plays out
        (e.g. the idle-in-transaction timeout), so the run can say what it was measured
        against. Default: nothing to observe."""
        return {}

    async def config_deviations(self) -> dict[str, str]:
        """Detect any configuration deviations currently active on the database that diverge
        from standard baseline (e.g. settings in postgresql.auto.conf).
        Default: returns empty dict."""
        return {}

    @abstractmethod
    async def server_version(self) -> str: ...

    def fault_detection_log_patterns(self) -> tuple[str, ...]:
        """Regexes matching this engine's own log lines that evidence THIS PROCESS noticing a
        fault -- the signal MTTD is measured from (Framework §6.2: detection is unconditional).

        Only lines the faulted process itself can still write belong here. A process killed
        with SIGKILL writes nothing, so for that fault there is no detection signal and MTTD
        is NOT_MEASURED -- which is the honest answer. Lines written by the REPLACEMENT
        process as it starts recovering belong in `recovery_start_log_patterns`; counting
        those as detection would report a restart as if it were detection."""
        return ()

    def recovery_start_log_patterns(self) -> tuple[str, ...]:
        """Regexes matching the lines a replacement process writes when it begins recovering
        (e.g. crash recovery after an unclean stop). Reported as `recovery_started_s`."""
        return ()

    # --- data-file corruption (fault type data_corruption, Framework NL-I) -------------------

    # Relations deliberately corrupted by the fault, left out of the run's whole-database
    # integrity check so that check still answers "is everything ELSE intact?". The corrupted
    # relation is checked on its own (amcheck_relation), where a finding is the expected result.
    integrity_exclusions: tuple[str, ...] = ()
    # Error codes this engine raises when a read finds a corrupted page.
    corruption_sqlstates: tuple[str, ...] = ()

    async def prepare_corruption_target(self) -> dict[str, Any]:
        """Create (or recreate) the harness-owned relation the fault will corrupt, and say
        where one of its populated pages lives on disk: `relation`, `relation_path` (relative
        to the data directory), `filenode`, `block`, `block_size`, `byte_in_page`, `rows`."""
        raise NotImplementedError(f"{type(self).__name__} cannot host a corruption target")

    async def read_corruption_target(self, attempts: int = 2) -> dict[str, Any]:
        """Read every page of the corruption target, `attempts` times on fresh connections.
        Each attempt records either the rows read or the error (`sqlstate`, `message`, and the
        `block` / `relation_path` the error names)."""
        raise NotImplementedError(f"{type(self).__name__} cannot read a corruption target")

    async def amcheck_relation(self, relation: str, timeout_s: float) -> dict[str, Any]:
        """The engine's structural checker on ONE relation. `detected`: True when it reported
        the relation damaged, False when it ran clean, None when it could not tell."""
        return {"detected": None, "note": f"{type(self).__name__} has no structural checker"}

    def corruption_log_locations(self, lines: Sequence[str]) -> list[tuple[int, str]]:
        """(block, relation_path) for every log line in which the engine reported reading a
        corrupted page. Used to show no OTHER relation was damaged."""
        return []

    async def start_concurrent_index_build(self) -> None:
        """Run an online index build on the harness's own table, for a fault that must land
        mid-build (fault.during: concurrent_index_build). The engine chooses the table, column
        and index name. Returns when the build ends -- normally cut short by the fault."""
        raise NotImplementedError(f"{type(self).__name__} has no online index build")

    async def concurrent_index_outcome(self) -> dict[str, Any]:
        """After recovery: the state the interrupted build left (`status`), then the cleanup
        or rebuild result (`cleanup_or_rebuild_succeeded`)."""
        return {}

    def idle_session_timeout_s(self, observed: dict[str, str]) -> float:
        """The engine's idle-in-transaction timeout in seconds, from the settings
        `observe_fault_settings` returned; 0.0 when it is disabled or unknown."""
        return 0.0

    async def create_index_concurrently(self, table: str, column: str, index_name: str) -> None:
        """Launch a concurrent index build. Used by NL-C-06."""
        pass

    async def get_index_status(self, index_name: str) -> dict[str, Any] | None:
        """Check index existence and validity in catalog. Returns {'is_valid': bool, 'is_ready': bool} or None."""
        return None

    async def cleanup_index(self, index_name: str) -> bool:
        """Drop or rebuild an index. Returns True if cleanup succeeded."""
        return True

    async def quick_integrity_check(self) -> dict[str, Any]:
        """A fast inter-cycle checksum check to localize corruptions to the cycle that caused them.
        Default: returns empty dict."""
        return {}

    async def exhaust_connections(self, hold_duration_s: float = 2.0) -> dict[str, Any]:
        """Exhaust the connection pool to test connection saturation handling (NL-R-04).
        Default: returns empty dict."""
        return {}

    async def revert_exhaust_connections(self) -> dict[str, Any]:
        """Drain and release any connections held during connection exhaustion.
        Default: returns empty dict."""
        return {}



# --- registry ----------------------------------------------------------------------------

_ADAPTERS: dict[str, type[BaseDatabaseAdapter]] = {}


class EngineNotSupported(RuntimeError):
    pass


def register_adapter(cls: type[BaseDatabaseAdapter]) -> type[BaseDatabaseAdapter]:
    _ADAPTERS[cls.engine] = cls
    return cls


def adapter_for(engine: str, node: Node) -> BaseDatabaseAdapter:
    cls = _ADAPTERS.get(engine)
    if cls is None:
        raise EngineNotSupported(f"no adapter for engine {engine!r}; registered: {sorted(_ADAPTERS)}")
    return cls(node)


def registered_engines() -> list[str]:
    return sorted(_ADAPTERS)


def capabilities_of(engine: str) -> frozenset[Capability]:
    """What this engine can do -- read from the adapter class, no instance needed."""
    cls = _ADAPTERS.get(engine)
    if cls is None:
        raise EngineNotSupported(f"no adapter for engine {engine!r}; registered: {sorted(_ADAPTERS)}")
    return cls.capabilities
