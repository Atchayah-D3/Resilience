"""NL-N-I01: noisy analytics against priority customer work -- an internal noisy neighbour whose
shared resources are the CPU, the memory cache and the disk.

Report sessions of a separate login, limited by the operator, read the TPC-C tables back to back
for fault.duration while the customers (the TPC-C workload) keep their planned rate. Judged
(the 3 November document, section 9): the customers keep at least 70% of their TPS and at most 2x
their p99 (Framework NL-N-07), 90% of their New-Order, Payment and Order-Status within 5 s (TPC-C
Clause 5.2.5.3), at least 80% of their cache hit ratio (Framework NL-N-07); each report gets no
more helpers than the role allows; the role's temp_file_limit and statement_timeout stop the two
limit tests, and no customer is stopped by a limit; back to normal within 60 s; nothing lost or
damaged.

The run is INVALID (stopped, never a pass) when the test could not be valid on this target: the
login cannot log in or allows fewer sessions than the plan needs, a limit the limit tests need is
not set, statement_timeout does not end inside the hold, pg_stat_statements is not there to count
the customers' own pages, the customers' baseline cache hit ratio is below 99% (a loss caused by
the reports could not be seen), the baseline missed the planned rate, or the reports with their
helpers cannot occupy every core.

Run 1b, the misbehaving tenant (NL-N-I01b; Resilience_Testing_ShaktiDB_v2, section 5.1): the same
reports, but each report session first changes some of its own settings for the session
(fault.analytics_load.session_overrides) and one session tries the hard limits
(hard_limit_attempts). The database's answers are the finding: which role limits a tenant can
switch off by itself (overridden) and which the database enforces. The role is never altered. A
memory guard (memory_guard), armed before the first change, ends the reports early if the shared
target runs short of memory. 1b is judged only on what must still hold; the customers' figures
are recorded. Every 1b measure is produced only when the scenario declares it.

The customers' cache hit ratio counts the pages of their own TPC-C calls on every table they read
(pg_stat_statements, the clerks' login), the report tables included: the reports flood exactly
those, so a ratio over the other tables only would barely move (the 21 November run: 1.00019 of
the baseline while the report tables fell to 0.707).
"""

from __future__ import annotations

import asyncio
import contextlib
import re
import time
from collections import Counter
from typing import Any

from resilience_tests.analysis import rto_decomposer
from resilience_tests.analysis.predicates import NOT_MEASURED
from resilience_tests.control.faults.base import HeldFault, PhaseAbort, Sampler, register_fault
from resilience_tests.execution.injectors.analytics import CONFIRM_AT_S, SessionPlan, as_int, session_plan
from resilience_tests.execution.probes.memory_guard import GuardRules, MemoryGuard
from resilience_tests.execution.workload import contract
from resilience_tests.execution.workload.markers import ABORTED_JOURNAL, journal_paths, read_journal

# Back to normal (document section 9): TPS at least 95% of the baseline and p99 at most 1.5x for 60 s
# straight -- stricter than the Framework §6.2 80% -- within RECOVERY_LIMIT_S of the reports stopping.
NORMAL_TPS_FRACTION = 0.95
RECOVERY_LIMIT_S = 60.0
RECOVERY_WATCH_SLACK_S = 10.0
# How long the recovery is watched after the reports stop. Judging needs only RECOVERY_LIMIT_S + one
# window (+ slack); watching longer lets a late return be measured (95 s, say) instead of being only
# "not reached". The phase ends as soon as the customers are back, so only a failing run waits longer.
RECOVERY_WATCH_S = 300.0
# Validity (document sections 1 and 2): the customers' data must be in the memory cache normally,
# and the baseline must have run at the planned rate. NEEDS SIGN-OFF.
BASELINE_HIT_RATIO_MIN = 0.99
RATE_KEPT_FRACTION = 0.95
# limit test 2 must end inside the hold, with room to see it (seconds before the hold ends)
TIMEOUT_MARGIN_S = 30.0
TIMEOUT_TOLERANCE_S = 1.0            # document check 8: within 1 s of the limit (NEEDS SIGN-OFF)
CUSTOMER_LIMIT_SQLSTATES = ("53300", "53400", "57014")   # too many connections, temp_file_limit, statement_timeout
P90_TYPES = (contract.NEW_ORDER, contract.PAYMENT, contract.ORDER_STATUS)
SAMPLE_INTERVAL_S = 1.0
SAMPLE_RETRY_S = 5.0
# What the sessions waited on (evidence only, never judged): the wait events of the 1-second database
# samples, grouped. Row locks and the write-ahead log are waited on by writing sessions (the
# customers; the reports only read); disk reads by everyone, the reports included.
WORST_SECONDS = 10                   # the customers' slowest seconds of the hold, for the main wait
# Run 1b: the server's answer to a session that may not change a setting (temp_file_limit: superuser only)
INSUFFICIENT_PRIVILEGE = "42501"
# the report login's settings compared before and after a 1b run (the role itself, never a session's)
ROLE_KEYS = ("role_exists", "role_can_login", "role_connection_limit", "role_max_parallel_workers_per_gather",
             "role_work_mem", "role_temp_file_limit", "role_statement_timeout")
WRITE_GAP_EDGE_S = 5.0               # the write prober's gaps are looked at from this long before the hold

_DURATION_UNITS = {"us": 1e-6, "ms": 1e-3, "s": 1.0, "min": 60.0, "h": 3600.0, "d": 86400.0}
_SIZE_UNITS = {"b": 1, "kb": 1024, "mb": 1024 ** 2, "gb": 1024 ** 3, "tb": 1024 ** 4}
_SETTING_RE = re.compile(r"^\s*(-?\d+(?:\.\d+)?)\s*([a-zA-Z]*)\s*$")


def duration_s(value: str | None) -> float | None:
    """A PostgreSQL duration setting in seconds (a plain number is milliseconds); None when unset,
    0 or unreadable (0 means "no limit" to PostgreSQL)."""
    m = _SETTING_RE.match(value or "")
    if not m:
        return None
    unit = (m.group(2) or "ms").lower()
    if unit not in _DURATION_UNITS:
        return None
    seconds = float(m.group(1)) * _DURATION_UNITS[unit]
    return seconds if seconds > 0 else None


def size_bytes(value: str | None) -> int | None:
    """A PostgreSQL size setting in bytes (a plain number is kB, as temp_file_limit); None when
    unset, -1 (no limit) or unreadable."""
    m = _SETTING_RE.match(value or "")
    if not m:
        return None
    unit = (m.group(2) or "kb").lower()
    if unit not in _SIZE_UNITS:
        return None
    size = float(m.group(1)) * _SIZE_UNITS[unit]
    return int(size) if size > 0 else None


def hit_ratio(start: dict[str, Any] | None, end: dict[str, Any] | None, group: str = "customers") -> float | None:
    """Pages found in the cache / pages read, between two cumulative snapshots; None without both
    or without any page read. Table counters (evidence): `customers`, `reports`."""
    if not start or not end or not start.get(group) or not end.get(group):
        return None
    hit = end[group]["hit"] - start[group]["hit"]
    read = end[group]["read"] - start[group]["read"]
    return hit / (hit + read) if hit + read > 0 else None


def clerk_hit_ratio(start: dict[str, Any] | None, end: dict[str, Any] | None) -> tuple[float | None, str | None]:
    """The customers' own cache hit ratio between two snapshots, from their TPC-C calls in
    pg_stat_statements: (ratio, None), or (None, why) when the counts cannot be trusted. Never a
    number from counts that may have restarted in between: pg_stat_statements reset, an entry
    discarded to make room (dealloc moved), one of the five calls without an entry, or a count
    that went down."""
    if not start or not end:
        return None, "the counters were not read at both ends of the window"
    a, b = start.get("clerks"), end.get("clerks")
    if not a or not b:
        why = (start if not a else end).get("clerks_unavailable")
        return None, why or "the clerks' statements were not read at both ends of the window"
    if a.get("stats_reset") != b.get("stats_reset"):
        return None, "pg_stat_statements was reset inside the window"
    if a.get("dealloc") != b.get("dealloc"):
        return None, ("pg_stat_statements discarded entries inside the window (pg_stat_statements.max): a clerk "
                      "call's counts may have started again")
    missing = sorted(t for t in contract.TPCC_TYPES
                     if not (a["by_type"].get(t) or {}).get("entries") or not (b["by_type"].get(t) or {}).get("entries"))
    if missing:
        return None, f"pg_stat_statements has no entry for the clerks' {', '.join(missing)} at both ends of the window"
    if any(b["by_type"][t][k] < a["by_type"][t][k] for t in contract.TPCC_TYPES for k in ("calls", "hit", "read")):
        return None, "a clerk call's counts went down inside the window (its entry started again)"
    hit = sum(b["by_type"][t]["hit"] - a["by_type"][t]["hit"] for t in contract.TPCC_TYPES)
    read = sum(b["by_type"][t]["read"] - a["by_type"][t]["read"] for t in contract.TPCC_TYPES)
    if hit + read <= 0:
        return None, "the clerks' calls read no page inside the window"
    return hit / (hit + read), None


def same_setting(read_back: str | None, asked: str | None) -> bool:
    """A setting read back as asked: the same text (case and spaces aside), or the same size."""
    if read_back is None or asked is None:
        return False
    norm = lambda v: v.strip().lower().replace(" ", "")  # noqa: E731
    if norm(read_back) == norm(asked):
        return True
    a, b = size_bytes(read_back), size_bytes(asked)
    return a is not None and a == b


def override_outcome(rec: dict[str, Any]) -> str | None:
    """One session's change of one setting: "overridden" (accepted and read back as asked),
    "enforced" (refused, or accepted but read back otherwise), None (no answer)."""
    if rec.get("answer") == "refused":
        return "enforced"
    if rec.get("answer") == "accepted":
        return "overridden" if same_setting(rec.get("read_back"), rec.get("asked")) else "enforced"
    return None


def overrides_summary(records: list[dict[str, Any]], sessions: int,
                      settings: dict[str, str]) -> tuple[bool | None, str | None, dict[str, Any]]:
    """(overrides_recorded, why not measured, per setting: sessions overridden / enforced, values
    read back, the server's errors). Recorded only when every session answered for every setting;
    a refusal is an answer (a finding), never a failure."""
    per: dict[str, Any] = {}
    for name, asked in settings.items():
        recs = [r for r in records if r.get("setting") == name]
        outcomes = [override_outcome(r) for r in recs]
        per[name] = {"asked": asked,
                     "overridden_sessions": outcomes.count("overridden"),
                     "enforced_sessions": outcomes.count("enforced"),
                     "read_back": sorted({str(r["read_back"]) for r in recs if r.get("read_back") is not None}),
                     "before": sorted({str(r["before"]) for r in recs if r.get("before") is not None}),
                     "refused": sorted({f"{r.get('sqlstate')}: {r.get('error')}" for r in recs
                                        if r.get("answer") == "refused"})}
    missing = [f"session {r.get('session')} {r.get('setting')}: {r.get('error') or 'no answer'}"
               for r in records if override_outcome(r) is None]
    expected = sessions * len(settings)
    answered = sum(1 for r in records if override_outcome(r) is not None)
    if not sessions:
        return None, "no report session was opened", per
    if missing or answered < expected:
        why = (f"{answered} of {expected} session settings answered" + (f"; {'; '.join(missing[:4])}" if missing else ""))
        return None, why, per
    return True, None, per


class AnalyticsSampler:
    """Report sessions and their helpers, every second, into the event stream (analytics/sample);
    a sample that cannot be taken is recorded as unavailable, never as a number."""

    def __init__(self, adapter: Any, stream: Any) -> None:
        self.adapter, self.stream = adapter, stream
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._loop(), name="analytics-sampler")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _loop(self) -> None:
        while True:
            try:
                sample = await self.adapter.analytics_sample()
                self.stream.emit("analytics", "sample", **sample)
                await asyncio.sleep(SAMPLE_INTERVAL_S)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 -- evidence only; retried
                self.stream.emit("analytics", "unavailable", error=f"{type(exc).__name__}: {exc}")
                await asyncio.sleep(SAMPLE_RETRY_S)


def _r(value: float | None) -> float | None:
    return None if value is None else round(value, 5)


def _wait_group(name: str) -> str | None:
    """The group of a wait event ('type:event'), or None for the ones not reported."""
    kind, _, event = name.partition(":")
    if kind == "Lock":
        return "row_locks"
    if kind in ("LWLock", "IO") and "wal" in event.lower():
        return "wal"
    if name == "IO:DataFileRead":
        return "disk_reads"
    return None


def wait_evidence(events: list[Any], start_ns: int, end_ns: int, worst_seconds: int = 0) -> dict[str, Any]:
    """Sessions waiting, on average per 1-second database sample, on row locks, on the write-ahead
    log and on disk reads between start_ns and end_ns; the host's swap in that window; and, when
    worst_seconds is given, the main wait in the customers' slowest seconds (highest 1-second p99).
    A window without samples reports no figure, never a zero it did not measure."""
    db = sorted((e for e in events if e.source == "db_activity" and e.kind == "sample"
                 and start_ns <= e.t_mono_ns <= end_ns), key=lambda e: e.t_mono_ns)
    host = [e for e in events if e.source == "host_stats" and e.kind == "sample"
            and start_ns <= e.t_mono_ns <= end_ns]
    out: dict[str, Any] = {"db_samples": len(db), "host_samples": len(host)}
    if db:
        groups: Counter[str] = Counter()
        for e in db:
            for name, n in (e.data.get("wait_events") or {}).items():
                group = _wait_group(name)
                if group:
                    groups[group] += int(n)
        for group in ("row_locks", "wal", "disk_reads"):
            out[f"{group}_mean"] = round(groups[group] / len(db), 2)
    if host:
        out["swap_in_kb"] = sum(int(e.data.get("si", 0)) for e in host)
        out["swap_out_kb"] = sum(int(e.data.get("so", 0)) for e in host)
    if worst_seconds and db:
        load = [e for e in events if e.source == "workload" and e.kind == "sample"
                and start_ns <= e.t_mono_ns <= end_ns and isinstance(e.data.get("p99_ms"), (int, float))]
        worst = sorted(load, key=lambda e: e.data["p99_ms"], reverse=True)[:worst_seconds]
        waits: Counter[str] = Counter()
        matched = 0
        for w in worst:
            near = min(db, key=lambda e: abs(e.t_mono_ns - w.t_mono_ns))
            if abs(near.t_mono_ns - w.t_mono_ns) <= 1.5e9:      # the database sample of that second
                matched += 1
                for name, n in (near.data.get("wait_events") or {}).items():
                    waits[name] += int(n)
        if matched and waits:
            name, n = waits.most_common(1)[0]
            out["worst"] = {"seconds": matched, "p99_ms_min": round(min(w.data["p99_ms"] for w in worst), 1),
                            "p99_ms_max": round(max(w.data["p99_ms"] for w in worst), 1),
                            "main_wait": name, "main_wait_sessions_mean": round(n / matched, 1)}
    return out


def wait_lines(waits: dict[str, Any] | None) -> list[str]:
    """summary.txt lines for facts["analytics_waits"]."""
    if not waits:
        return []

    def window(w: dict[str, Any] | None) -> str:
        if not w or not w.get("db_samples"):
            return "no sample"
        swap = (f"swap in/out {w['swap_in_kb']}/{w['swap_out_kb']} KB" if w.get("host_samples")
                else "swap not sampled")
        return (f"row locks {w.get('row_locks_mean')}, write-ahead log {w.get('wal_mean')}, disk reads "
                f"{w.get('disk_reads_mean')}; {swap}")

    lines = ["  waits, evidence only (sessions waiting on average; row locks and write-ahead log are the customers' "
             "writes, disk reads include the reports):",
             f"    baseline: {window(waits.get('baseline'))}",
             f"    reports running: {window(waits.get('hold'))}"]
    worst = (waits.get("hold") or {}).get("worst")
    if worst:
        lines.append(f"    the customers' {worst['seconds']} slowest seconds (p99 {worst['p99_ms_min']}-"
                     f"{worst['p99_ms_max']} ms): main wait {worst['main_wait']}, "
                     f"{worst['main_wait_sessions_mean']} sessions on average")
    if waits.get("error"):
        lines.append(f"    not recorded: {waits['error']}")
    return lines


def tenant_lines(facts: dict[str, Any], m: dict[str, Any]) -> list[str]:
    """The "1b findings" block: what the report login was allowed to change and what was refused,
    and the figures recorded without a pass rule. None for a run without session changes."""
    tenant = facts.get("analytics_tenant")
    guard = facts.get("memory_guard")
    if not tenant and not guard:
        return []
    lines = ["  1b findings (recorded, not judged; the rules that must still hold are in the acceptance list):"]
    for name, p in ((tenant or {}).get("overrides") or {}).items():
        state = ("overridden" if p.get("overridden_sessions") and not p.get("enforced_sessions")
                 else "enforced" if p.get("enforced_sessions") and not p.get("overridden_sessions") else "mixed")
        lines.append(f"    {name}: asked {p.get('asked')} (role default {', '.join(p.get('before') or []) or '?'}) -> "
                     f"{state}: {p.get('overridden_sessions')} session(s) overridden, read back "
                     f"{', '.join(p.get('read_back') or []) or '-'}; {p.get('enforced_sessions')} enforced"
                     + (f" ({'; '.join(p['refused'])})" if p.get("refused") else ""))
    for a in (tenant or {}).get("attempts") or []:
        if a.get("answer") == "refused":
            answer = f"refused {a.get('sqlstate')}: {a.get('error')}"
        elif a.get("answer") == "accepted":
            granted = a.get("granted_set")
            answer = (f"ACCEPTED (read back {a.get('read_back')}): "
                      + ("the login was granted SET on it by the operator" if granted
                         else "not a hard cap on this target" if granted is False
                         else "whether the login was granted SET on it is unknown"))
        else:
            answer = f"no answer ({a.get('error')})"
        lines.append(f"    session {a.get('session')} tried {a.get('setting')} = {a.get('asked')}: {answer}")
    server_cap = (facts.get("scenario_observed") or {}).get("max_parallel_workers")
    lines.append(f"    helpers: most per report {m.get('report_helpers_max')}, most at once {m.get('report_helpers_total_max')}"
                 + (f" (the server's own limit for every session together, max_parallel_workers = {server_cap}, set by "
                    "the operator: the tenant cannot raise it)" if server_cap is not None else "")
                 + f";  temporary data written {m.get('temp_gb_written')} GB;  limit test 2 ran {m.get('limit_test_2_ran_s')} s")
    lines.append(f"    customers: TPS ratio {m.get('oltp_tps_ratio')}  p99 ratio {m.get('oltp_p99_ratio')}  worst p90 "
                 f"{m.get('customer_p90_max_s')} s  cache hit {m.get('customer_cache_hit_ratio_vs_baseline')} of baseline;  "
                 f"write prober longest gap {m.get('writer_max_gap_s')} s")
    if guard:
        tripped = guard.get("tripped")
        lines.append(f"    memory: lowest available {guard.get('min_available_mb')} MB, swapped out {guard.get('max_swapped_out_mb')} MB, "
                     f"{guard.get('readings')} readings;  guard: "
                     + (f"ENDED THE REPORTS after {tripped.get('after_armed_s')} s ({tripped.get('reason')})" if tripped
                        else "did not trigger")
                     + f";  reports ran {m.get('reports_ran_s')} s")
    return lines


@register_fault
class AnalyticsLoadFault(HeldFault):
    fault_type = "analytics_load"
    fact_key = "analytics_load"

    def __init__(self, ctx: Any) -> None:
        super().__init__(ctx)
        self._observed: dict[str, Any] | None = None     # first reading (init)
        self._plan: SessionPlan | None = None
        self._cache: dict[str, dict[str, Any]] = {}
        self._baseline_ns: dict[str, int] = {}             # start, end (monotonic ns) for the wait evidence
        self._guard: MemoryGuard | None = None             # run 1b: ends the reports before memory runs out

    # --- init ------------------------------------------------------------------------------------

    async def observe_settings(self) -> dict[str, str]:
        """Read only, at init and again in validate (settings_unchanged). The first reading also
        checks that the test can be valid on this target, and refuses the run otherwise."""
        observed = await self.ctx.adapter.observe_analytics_settings()
        if self._observed is None:
            self._observed = dict(observed)
            self._check_target(observed)
        return {k: ("" if v is None else str(v)) for k, v in observed.items()}

    def _check_target(self, observed: dict[str, Any]) -> None:
        plan = self.ctx.scenario.fault.analytics_load
        assert plan is not None      # the catalog requires it for analytics_load
        login = observed.get("analytics_login")
        if observed.get("role_exists") != "true" or observed.get("role_can_login") != "true":
            raise PhaseAbort(f"invalid run: the analytics login {login!r} does not exist on the target or cannot log in")
        try:
            self._plan = session_plan(plan, as_int(observed.get("cpu_cores")))
        except Exception as exc:  # noqa: BLE001 -- DriverNotAvailable: the plan cannot be made
            raise PhaseAbort(f"invalid run: {exc}") from None
        limit = as_int(observed.get("role_connection_limit"))
        if limit is not None and limit != -1 and limit < self._plan.total:
            raise PhaseAbort(f"invalid run: the analytics login allows {limit} sessions, but this target needs "
                             f"{self._plan.total} report sessions ({self._plan.full_table} full-table for "
                             f"{self._plan.cores} cores + {self._plan.index} index)")
        if not observed.get("pg_stat_statements"):
            raise PhaseAbort("invalid run: pg_stat_statements is not installed in the database, so the customers' "
                             "cache hit ratio over every table they read cannot be measured (the operator adds it to "
                             "shared_preload_libraries, restarts, and runs CREATE EXTENSION pg_stat_statements)")
        track = observed.get("pg_stat_statements_track")
        if track is None:
            raise PhaseAbort("invalid run: the pg_stat_statements library is not loaded (shared_preload_libraries), "
                             "so the customers' cache hit ratio cannot be measured")
        if track not in ("top", "all"):
            raise PhaseAbort(f"invalid run: pg_stat_statements.track is {track!r}, so the customers' calls are not "
                             "counted and their cache hit ratio cannot be measured")
        if plan.limit_tests and "statement_timeout" in plan.session_overrides:
            # run 1b: the sessions set their own time limit, so limit test 2 is not the role's to stop;
            # it runs until the harness ends the fault and its run time is recorded (not judged)
            self.ctx.facts["limit_test_2_note"] = (f"the report sessions set statement_timeout = "
                                                   f"{plan.session_overrides['statement_timeout']!r} themselves")
        if plan.limit_tests:
            if size_bytes(observed.get("role_temp_file_limit")) is None:
                raise PhaseAbort("invalid run: the analytics login has no temp_file_limit, so limit test 1 "
                                 "cannot be judged (the operator sets the role's limits)")
        if plan.limit_tests and "statement_timeout" not in plan.session_overrides:
            hold = float(self.ctx.scenario.fault.duration)          # seconds: the catalog requires it
            timeout = duration_s(observed.get("role_statement_timeout"))
            if timeout is None:
                raise PhaseAbort("invalid run: the analytics login has no statement_timeout, so limit test 2 "
                                 "cannot be judged (the operator sets the role's limits)")
            if timeout > hold - TIMEOUT_MARGIN_S:
                raise PhaseAbort(f"invalid run: the analytics login's statement_timeout ({timeout:g} s) does not end "
                                 f"inside the {hold:g} s hold with {TIMEOUT_MARGIN_S:g} s to spare, so limit test 2 "
                                 "cannot be judged")

    async def check_ready(self) -> None:
        """After the data step: the report login can read what the reports read. A reload drops
        and recreates the TPC-C schema; the load gives back the access others had, and a login
        that still cannot read is refused here rather than failing every report later."""
        access = await self.ctx.adapter.analytics_read_access()
        self.ctx.facts["analytics_read_access"] = access
        missing = ([f"USAGE on schema resilience_tpcc"] if not access.get("schema") else []) + [
            f"SELECT on {name}" for name, ok in (access.get("tables") or {}).items() if not ok]
        if missing or not access.get("tables"):
            raise PhaseAbort(f"invalid run: the analytics login {access.get('login')!r} lacks {', '.join(missing) or 'access'} "
                             "on the TPC-C data, so every report would fail; the operator grants it, e.g. GRANT USAGE ON "
                             "SCHEMA resilience_tpcc / GRANT SELECT ON ALL TABLES IN SCHEMA resilience_tpcc")

    def helpers_per_report(self) -> int | None:
        """Helpers one report may have: the login's max_parallel_workers_per_gather (the server's
        when the login sets none)."""
        o = self._observed or {}
        return as_int(o.get("role_max_parallel_workers_per_gather") or o.get("max_parallel_workers_per_gather"))

    def slo_tps_fraction(self) -> float | None:
        return NORMAL_TPS_FRACTION

    def slo_tps_window_s(self) -> float | None:
        """The document's "60 s straight" is judged as one window of the sustain period, as scenario 3
        does (lock_pileup.py): TPS over the whole window and the p99 of every transaction in it, like
        the baseline p99 it is compared with. Held per second, one checkpoint stall fails the second,
        so even the undisturbed baseline never finds 60 good seconds in a row (21 and 23 November
        runs: 529-553 of 599 seconds compliant) and rto_to_slo_s could never be measured."""
        return rto_decomposer.SLO_SUSTAIN_S      # read at call time: the window IS the sustain period

    def phase_extension_s(self, phase: str) -> float:
        if phase != "recovery":
            return super().phase_extension_s(phase)
        needed = max(RECOVERY_WATCH_S, RECOVERY_LIMIT_S + rto_decomposer.SLO_SUSTAIN_S + RECOVERY_WATCH_SLACK_S)
        return max(0.0, needed - self.ctx.phase_timeout_s("recovery"))

    # --- baseline and the hold -------------------------------------------------------------------

    def samplers(self) -> list[Sampler]:
        out: list[Sampler] = [AnalyticsSampler(self.ctx.adapter, self.ctx.stream)]
        rules = self.ctx.scenario.fault.analytics_load.memory_guard if self.ctx.scenario.fault.analytics_load else None
        if rules is not None:
            self._guard = MemoryGuard(self.ctx.node, self.ctx.stream,
                                      GuardRules(available_min_mb=rules.available_min_mb,
                                                 swap_out_max_mb=rules.swap_out_max_mb,
                                                 interval_s=rules.interval_s, stale_s=rules.stale_s),
                                      on_trigger=self._guard_tripped)
            out.append(self._guard)
        return out

    async def _guard_tripped(self, why: str) -> dict[str, Any]:
        """The memory guard's action: the reports end now; the hold runs on to its planned end with
        the customers alone, and the release at its end finds nothing left to stop."""
        return await self.ctx.adapter.stop_analytics()

    async def _snapshot(self, name: str) -> None:
        try:
            self._cache[name] = await self.ctx.adapter.customer_cache_counters()
        except Exception as exc:  # noqa: BLE001 -- recorded; the ratios are then not measured
            self.ctx.facts.setdefault("analytics_cache_errors", {})[name] = f"{type(exc).__name__}: {exc}"
        self.ctx.facts["analytics_cache"] = self._cache

    async def baseline_started(self) -> None:
        self._baseline_ns["start"] = time.monotonic_ns()
        await self._snapshot("baseline_start")

    async def baseline_ended(self) -> None:
        self._baseline_ns["end"] = time.monotonic_ns()
        await self._snapshot("baseline_end")

    async def on_hold_start(self) -> None:
        await self._snapshot("hold_start")

    async def on_hold_end(self) -> None:
        await self._snapshot("hold_end")

    def planned_rate(self) -> float | None:
        """The rate the customers were planned to run: the running common workload's when attached
        (this run neither sized nor started it), else what the run's own workload resolved at
        prepare (rate_fraction_of_capacity), else the scenario's rate_tps."""
        attached = self.ctx.attached_rate_tps
        if attached:
            return float(attached)
        prepared = (self.ctx.facts.get("workload_prepare") or {}).get("tpcc_data") or {}
        rate = (prepared.get("rate") or {}).get("rate_tps")
        return float(rate) if rate else self.ctx.scenario.workload.rate_tps

    async def before_inject(self) -> None:
        """The validity rules that need the baseline, checked before anything is applied."""
        await super().before_inject()
        ctx = self.ctx
        ratio, why = clerk_hit_ratio(self._cache.get("baseline_start"), self._cache.get("baseline_end"))
        if ratio is None:
            raise PhaseAbort(f"invalid run: the customers' cache hit ratio of the baseline could not be measured: {why}")
        if ratio < BASELINE_HIT_RATIO_MIN:
            raise PhaseAbort(f"invalid run: the customers' baseline cache hit ratio is {ratio:.4f}, below "
                             f"{BASELINE_HIT_RATIO_MIN}: their data is not in the memory cache, so a loss caused by "
                             "the reports could not be seen (fewer active warehouses or a larger cache are needed)")
        rate, baseline = self.planned_rate(), ctx.baseline
        if not rate:
            # never skipped: a baseline that missed the plan could not be told from one that kept it
            raise PhaseAbort("invalid run: the customers' planned rate is not known, so the baseline cannot be "
                             "checked against it")
        if baseline is not None and baseline.tps < RATE_KEPT_FRACTION * rate:
            raise PhaseAbort(f"invalid run: the baseline delivered {baseline.tps:.1f} TPS, below "
                             f"{RATE_KEPT_FRACTION:.0%} of the planned {rate:g} TPS")
        helpers = self.helpers_per_report()
        cores = self._plan.cores if self._plan else None
        if self._plan is not None and helpers is not None and cores:
            processes = self._plan.report_processes(helpers)
            if processes < cores:
                raise PhaseAbort(f"invalid run: the reports can keep only {processes} processes busy "
                                 f"({self._plan.full_table} full-table sessions x (1 + {helpers} helpers) + "
                                 f"{self._plan.index} index), fewer than the target's {cores} cores")
        ctx.facts["analytics_validity"] = {"baseline_hit_ratio": round(ratio, 5), "planned_rate_tps": rate,
                                           "helpers_per_report": helpers,
                                           "plan": self._plan.as_dict() if self._plan else None}
        plan = ctx.scenario.fault.analytics_load
        if plan is not None and plan.memory_guard is not None:
            # armed last, right before the sessions open and change their settings: never run unguarded
            if self._guard is None:
                raise PhaseAbort("invalid run: the memory guard was not started with the run's probes")
            try:
                self._guard.arm()
            except RuntimeError as exc:
                raise PhaseAbort(f"invalid run: the memory guard cannot protect the target: {exc}") from None

    async def hold_and_release(self) -> dict[str, Any]:
        """The hold, then the reports are ended (T1). The memory guard judges only while the reports
        run: disarmed as soon as they are ended, so its lowest reading and any trip belong to them,
        never to the recovery or the checks after (measure() disarms it too, if the hold ended early)."""
        try:
            return await super().hold_and_release()
        finally:
            if self._guard is not None:
                self._guard.disarm()

    async def after_inject(self, detail: dict[str, Any]) -> None:
        if self._guard is not None and self._guard.tripped is not None:
            # the guard tripped while the sessions were opening: its stop found nothing yet to end
            self._guard.tripped["after_start"] = await self.ctx.adapter.stop_analytics()
        d = self.ctx.disclosures
        d.append(f"Reports: {len(detail.get('sessions') or [])} sessions of the login {detail.get('login')!r} "
                 f"(tag {detail.get('application_name')}); the index reports read warehouses "
                 f"{detail.get('extra_warehouses')}, which the customers do not use.")
        d.append("The customers' cache hit ratio counts the pages of their own five TPC-C calls on every table they "
                 "read, order_line and orders included (pg_stat_statements, for the clerks' login; a call's counts "
                 "include every statement inside its function). The reports' pages are not in it: they log in as "
                 "another role. The ratio on the tables the reports never read is kept as evidence only.")
        d.append("The customers log in as the harness role, which has no limits; only the report login is limited.")
        d.append("90% within 5 s is judged on the worst 1-second p90 of each type during the hold, never lower "
                 "than the p90 over the whole hold.")
        plan = self.ctx.scenario.fault.analytics_load
        if plan is not None and plan.session_overrides:
            changes = ", ".join(f"{k} = {v}" for k, v in plan.session_overrides.items())
            d.append(f"Run 1b (misbehaving tenant): each report session changed its own settings for the session "
                     f"({changes}) before its first report; the role itself was not altered.")
        if plan is not None and plan.hard_limit_attempts:
            tries = ", ".join(f"{k} = {v}" for k, v in plan.hard_limit_attempts.items())
            d.append(f"Run 1b: report session {detail.get('attempt_session') or '(see facts)'} also tried {tries}, "
                     "each as a statement on its own, not counted as a report.")
        if plan is not None and plan.memory_guard is not None:
            g = plan.memory_guard
            d.append(f"Memory guard (NEEDS SIGN-OFF): the reports end early if the target's available memory falls "
                     f"below {g.available_min_mb:g} MB, more than {g.swap_out_max_mb:g} MB is swapped out, or no "
                     f"reading arrives for {g.stale_s:g} s (read every {g.interval_s:g} s).")
        d.append(f"Back to normal (rto_to_slo_s): TPS >= {NORMAL_TPS_FRACTION:.0%} of the baseline and p99 <= "
                 f"{rto_decomposer.SLO_P99_MULTIPLIER:g}x for {rto_decomposer.SLO_SUSTAIN_S:g} s straight, counted "
                 "from the moment the reports were stopped.")

    # --- validate --------------------------------------------------------------------------------

    async def measure(self, events: list[Any]) -> dict[str, Any]:
        ctx = self.ctx
        m = await super().measure(events)       # oltp_tps_ratio, oltp_p99_ratio over the hold
        try:
            state = await ctx.adapter.analytics_state()
        except Exception as exc:  # noqa: BLE001 -- recorded; what needs it is not measured
            state = {"started": False, "error": f"{type(exc).__name__}: {exc}"}
        ctx.facts["analytics"] = {k: v for k, v in state.items() if k != "samples"}
        m.update(self._cache_measures())
        m.update(self._p90(events))
        m.update(self._helpers(state))
        m.update(self._limit_tests(state))
        m.update(self._customer_limit_errors())
        m.update(await self._leftovers())
        self._record_waits(events)
        m.update(await self._tenant_measures(state, events))
        return m

    # --- run 1b: the misbehaving tenant ------------------------------------------------------------

    def _declared(self, name: str) -> bool:
        return name in self.ctx.scenario.measure

    async def _tenant_measures(self, state: dict[str, Any], events: list[Any]) -> dict[str, Any]:
        """The 1b measures, each only when the scenario declares it (no ID branches)."""
        ctx = self.ctx
        plan = ctx.scenario.fault.analytics_load
        out: dict[str, Any] = {}
        if self._guard is not None:
            self._guard.disarm()
            guard = self._guard.state()
            ctx.facts["memory_guard"] = guard
            if self._declared("memory_guard_tripped"):
                out["memory_guard_tripped"] = guard["tripped"] is not None
            for name, key in (("available_memory_min_mb", "min_available_mb"), ("swapped_out_max_mb", "max_swapped_out_mb")):
                if self._declared(name):
                    out.update({name: guard[key]} if guard[key] is not None
                               else self._not(name, "the memory guard took no reading while armed"))
        tenant: dict[str, Any] = {}
        if plan is not None and plan.session_overrides and self._declared("overrides_recorded"):
            recorded, why, per = overrides_summary(list(state.get("overrides") or []),
                                                   len(state.get("sessions") or []), dict(plan.session_overrides))
            tenant["overrides"] = per
            out.update({"overrides_recorded": True} if recorded else self._not("overrides_recorded", why or "no answer"))
        if plan is not None and plan.hard_limit_attempts:
            attempts = list(state.get("attempts") or [])
            tenant["attempts"] = attempts
            tenant["attempt_session"] = state.get("attempt_session")
            if self._declared("temp_limit_override_refused"):
                out.update(self._override_refused(attempts, "temp_file_limit", "temp_limit_override_refused"))
        if tenant:
            ctx.facts["analytics_tenant"] = tenant
        if self._declared("role_settings_unchanged"):
            out.update(await self._role_unchanged())
        if self._declared("server_crash_restarts"):
            out.update(self._crash_restarts(events))
        if self._declared("writer_max_gap_s"):
            out.update(self._writer_gap(events))
        samples = state.get("samples") or []
        if self._declared("report_helpers_total_max"):
            out.update({"report_helpers_total_max": max(int(s.get("helpers_total") or 0) for s in samples)} if samples
                       else self._not("report_helpers_total_max", "no activity sample was taken while the reports ran"))
        if self._declared("temp_gb_written"):
            out.update(self._temp_written(samples))
        tests = state.get("limit_tests") or {}
        if self._declared("limit_test_2_ran_s"):
            timed = tests.get("limit_timeout")
            out.update({"limit_test_2_ran_s": float(timed["seconds"])} if timed and timed.get("seconds") is not None
                       else self._not("limit_test_2_ran_s", "limit test 2 never ended (or never started)"))
        if self._declared("reports_ran_s"):
            start, stop = state.get("started_mono_ns"), state.get("stopped_mono_ns")
            out.update({"reports_ran_s": round((int(stop) - int(start)) / 1e9, 1)} if start is not None and stop is not None
                       else self._not("reports_ran_s", "the reports' start or end was not recorded"))
        return out

    def _override_refused(self, attempts: list[dict[str, Any]], setting: str, name: str) -> dict[str, Any]:
        """True when the server refused the change for lack of privilege; False when it was accepted
        (or refused for another reason); not measured without an answer."""
        rec = next((a for a in attempts if a.get("setting") == setting), None)
        if rec is None:
            return self._not(name, f"no report session tried to change {setting} (no session other than limit test 1's)")
        if rec.get("answer") is None:
            return self._not(name, f"no answer to the change of {setting}: {rec.get('error') or 'unknown'}")
        return {name: rec.get("answer") == "refused" and rec.get("sqlstate") == INSUFFICIENT_PRIVILEGE}

    async def _role_unchanged(self) -> dict[str, Any]:
        """The report login's own settings, read again after the reports: equal to init's reading
        (the sessions' changes were for themselves only; the harness never alters the role)."""
        name = "role_settings_unchanged"
        before = self._observed or {}
        try:
            after = await self.ctx.adapter.observe_analytics_settings()
        except Exception as exc:  # noqa: BLE001
            return self._not(name, f"the login's settings could not be read again: {type(exc).__name__}: {exc}")
        if not before:
            return self._not(name, "the login's settings were not read at init")
        changed = {k: {"before": before.get(k), "after": after.get(k)} for k in ROLE_KEYS if before.get(k) != after.get(k)}
        self.ctx.facts["role_settings_after"] = {k: after.get(k) for k in ROLE_KEYS}
        if changed:
            self.ctx.facts["role_settings_changed"] = changed
        return {name: not changed}

    def _crash_restarts(self, events: list[Any]) -> dict[str, Any]:
        """Document check 12: server processes that crashed and made the server end every session
        and restart, from the engine's own log after the fault started (the same lines scenario 3
        counts, control/faults/lock_pileup.py)."""
        name = "server_crash_restarts"
        patterns = [re.compile(p) for p in self.ctx.adapter.crash_restart_log_patterns()]
        t0 = self.ctx.t0_ns
        if not patterns or t0 is None:
            return self._not(name, "this engine names no crash-restart log lines" if not patterns
                             else "the fault's start (T0) was not recorded")
        if not any(e.kind == "log_line" for e in events):
            return self._not(name, "no line of the engine's log reached the harness during the run")
        hits = [e.data.get("line", "") for e in events if e.kind == "log_line" and e.t_mono_ns > t0
                and any(p.search(e.data.get("line", "")) for p in patterns)]
        if hits:
            self.ctx.facts["crash_restart_lines"] = hits[:20]
        return {name: len(hits)}

    def _writer_gap(self, events: list[Any]) -> dict[str, Any]:
        """Document check 13, recorded only in 1b: the longest gap between the write prober's
        successful writes (its own table and connection) during the hold."""
        name = "writer_max_gap_s"
        bounds = self._hold_bounds()
        if bounds is None:
            return self._not(name, "the hold window was not recorded")
        since, until = bounds[0] - int(WRITE_GAP_EDGE_S * 1e9), bounds[1] + int(WRITE_GAP_EDGE_S * 1e9)
        ok = sorted(e.t_mono_ns for e in events
                    if e.kind == "write_probe" and e.data.get("ok") and since <= e.t_mono_ns <= until)
        if len(ok) < 2:
            return self._not(name, "the write prober recorded fewer than two successful writes around the hold")
        return {name: round(max(b - a for a, b in zip(ok, ok[1:])) / 1e9, 3)}

    def _temp_written(self, samples: list[dict[str, Any]]) -> dict[str, Any]:
        """Temporary bytes the database wrote while the reports ran (pg_stat_database, every
        session's; the customers write next to none)."""
        name = "temp_gb_written"
        known = [s for s in samples if s.get("temp_bytes") is not None]
        if len(known) < 2:
            return self._not(name, "fewer than two samples of the database's temporary bytes")
        written = int(known[-1]["temp_bytes"]) - int(known[0]["temp_bytes"])
        self.ctx.facts["temp_files_written"] = int(known[-1].get("temp_files") or 0) - int(known[0].get("temp_files") or 0)
        return {name: round(max(0, written) / 1024 ** 3, 3)}

    def _record_waits(self, events: list[Any]) -> None:
        """facts["analytics_waits"]: what the sessions waited on in the baseline and the hold. Evidence
        only: never judged, never fatal."""
        out: dict[str, Any] = {}
        try:
            start, end = self._baseline_ns.get("start"), self._baseline_ns.get("end")
            if start and end:
                out["baseline"] = wait_evidence(events, start, end)
            hold = self._hold_bounds()
            if hold:
                out["hold"] = wait_evidence(events, *hold, worst_seconds=WORST_SECONDS)
        except Exception as exc:  # noqa: BLE001 -- evidence only
            out["error"] = f"{type(exc).__name__}: {exc}"
        self.ctx.facts["analytics_waits"] = out

    def _not(self, name: str, why: str) -> dict[str, Any]:
        self.ctx.not_measured[name] = why
        return {name: NOT_MEASURED}

    def _cache_measures(self) -> dict[str, Any]:
        c = self._cache
        base, base_why = clerk_hit_ratio(c.get("baseline_start"), c.get("baseline_end"))
        hold, hold_why = clerk_hit_ratio(c.get("hold_start"), c.get("hold_end"))
        self.ctx.facts["analytics_cache_ratios"] = {
            "method": "pg_stat_statements: the clerks' five TPC-C calls, every table",
            "customers_baseline": base, "customers_hold": hold,
            "not_measured": {k: v for k, v in (("baseline", base_why), ("hold", hold_why)) if v},
            # evidence only: table counters, whoever read the pages
            "untouched_tables_baseline": hit_ratio(c.get("baseline_start"), c.get("baseline_end")),
            "untouched_tables_hold": hit_ratio(c.get("hold_start"), c.get("hold_end")),
            "report_tables_hold": hit_ratio(c.get("hold_start"), c.get("hold_end"), group="reports")}
        out: dict[str, Any] = {}
        for name, value, why in (("customer_cache_hit_ratio_baseline", base, base_why),
                                 ("customer_cache_hit_ratio_hold", hold, hold_why)):
            out.update({name: round(value, 5)} if value is not None else self._not(name, why or "not measured"))
        if base and hold is not None:
            out["customer_cache_hit_ratio_vs_baseline"] = round(hold / base, 5)
        else:
            out.update(self._not("customer_cache_hit_ratio_vs_baseline",
                                 "the customers' cache hit ratio was not measured in the baseline and the hold: "
                                 + "; ".join(w for w in (base_why, hold_why) if w)))
        return out

    def _hold_bounds(self) -> tuple[int, int] | None:
        hold = self.ctx.facts.get("hold") or {}
        start, end = hold.get("start_mono_ns"), hold.get("end_mono_ns")
        return (int(start), int(end)) if start and end else None

    def _p90(self, events: list[Any]) -> dict[str, Any]:
        """The worst 1-second p90 of New-Order, Payment and Order-Status during the hold."""
        name = "customer_p90_max_s"
        bounds = self._hold_bounds()
        if bounds is None:
            return self._not(name, "the hold window was not recorded")
        worst: dict[str, float] = {}
        for e in events:
            if e.kind != contract.SAMPLE_KIND or e.source != contract.SAMPLE_SOURCE:
                continue
            if not bounds[0] < e.t_mono_ns <= bounds[1]:
                continue
            for t in P90_TYPES:
                p90 = ((e.data.get(contract.BY_TYPE) or {}).get(t) or {}).get("p90_ms")
                if p90 is not None:
                    worst[t] = max(worst.get(t, 0.0), float(p90))
        missing = [t for t in P90_TYPES if t not in worst]
        if missing:
            return self._not(name, f"no {', '.join(missing)} transaction finished during the hold")
        self.ctx.facts["customer_p90_max_ms"] = {t: round(v, 1) for t, v in worst.items()}
        return {name: round(max(worst.values()) / 1000, 3)}

    def _helpers(self, state: dict[str, Any]) -> dict[str, Any]:
        samples = state.get("samples") or []
        sessions = len(state.get("sessions") or [])
        out: dict[str, Any] = {}
        if not samples or not sessions:
            why = "no activity sample was taken while the reports ran"
            for name in ("report_helpers_max", "helpers_over_limit", "analytics_active_share"):
                out.update(self._not(name, why))
            return out
        out["report_helpers_max"] = max(int(s.get("helpers_max") or 0) for s in samples)
        out["analytics_active_share"] = round(
            sum(1 for s in samples if int(s.get("sessions_open") or 0) >= sessions) / len(samples), 4)
        allowed = self.helpers_per_report()
        if allowed is None:
            out.update(self._not("helpers_over_limit", "the helpers one report may have could not be read"))
        else:
            out["helpers_over_limit"] = sum(1 for s in samples if int(s.get("helpers_max") or 0) > allowed)
        return out

    def _limit_tests(self, state: dict[str, Any]) -> dict[str, Any]:
        plan = self.ctx.scenario.fault.analytics_load
        if plan is None or not plan.limit_tests:
            return {}
        tests = state.get("limit_tests") or {}
        out: dict[str, Any] = {}
        temp = tests.get("limit_temp")
        if temp is None:
            out.update(self._not("temp_limit_enforced", "limit test 1 never ended (or never started)"))
        else:
            out["temp_limit_enforced"] = temp.get("outcome") == "error" and temp.get("sqlstate") == "53400"
        timed = tests.get("limit_timeout")
        timeout = duration_s((self._observed or {}).get("role_statement_timeout"))
        if timed is None or timeout is None:
            why = "limit test 2 never ended (or never started)" if timed is None else "statement_timeout unreadable"
            out.update(self._not("statement_timeout_enforced", why))
            out.update(self._not("statement_timeout_overrun_s", why))
        else:
            overrun = round(float(timed.get("seconds") or 0.0) - timeout, 3)
            out["statement_timeout_overrun_s"] = overrun
            out["statement_timeout_enforced"] = (timed.get("outcome") == "error" and timed.get("sqlstate") == "57014"
                                                 and overrun <= TIMEOUT_TOLERANCE_S)
        return out

    def _customer_limit_errors(self) -> dict[str, Any]:
        """Customer transactions that failed with a limit's error code: from the workload's
        rolled-back journal lines, which carry the code (unknown outcomes have none)."""
        name = "customer_limit_errors"
        paths = journal_paths(self.ctx.run_dir, ABORTED_JOURNAL)
        if not paths:
            return self._not(name, "the workload kept no rolled-back journal")
        hits: dict[str, int] = {}
        for p in paths:
            for r in read_journal(p).records:
                code = r.get("sqlstate")
                if code in CUSTOMER_LIMIT_SQLSTATES:
                    hits[code] = hits.get(code, 0) + 1
        if hits:
            self.ctx.facts["customer_limit_errors"] = hits
        return {name: sum(hits.values())}

    async def _leftovers(self) -> dict[str, Any]:
        ctx = self.ctx
        try:
            ctx.facts["analytics_revert"] = await ctx.adapter.revert_analytics()
        except Exception as exc:  # noqa: BLE001 -- what is left is read back below either way
            ctx.facts["analytics_revert"] = {"error": f"{type(exc).__name__}: {exc}"}
        try:
            left = await ctx.adapter.analytics_leftovers()
        except Exception as exc:  # noqa: BLE001
            why = f"what the reports left could not be read: {type(exc).__name__}: {exc}"
            return {**self._not("analytics_sessions_left", why), **self._not("temp_files_left", why)}
        ctx.facts["analytics_leftovers"] = left
        out: dict[str, Any] = {"analytics_sessions_left": int(left.get("sessions") or 0)}
        if left.get("temp_files") is None:
            out.update(self._not("temp_files_left", f"temporary files unreadable: {left.get('temp_files_error')}"))
        else:
            out["temp_files_left"] = int(left["temp_files"])
        return out

    @classmethod
    def summary_lines(cls, r: dict[str, Any]) -> list[str]:
        facts = r.get("facts") or {}
        state = facts.get("analytics")
        if not state:
            return []
        m = r.get("measured") or {}
        cache = facts.get("analytics_cache_ratios") or {}
        kinds = state.get("by_kind") or {}
        tests = state.get("limit_tests") or {}
        validity = facts.get("analytics_validity") or {}

        def kind_text(name: str) -> str:
            k = kinds.get(name) or {}
            return f"{name} {k.get('completed', 0)}/{k.get('count', 0)}"

        def test_text(name: str) -> str:
            t = tests.get(name) or {}
            return f"{t.get('outcome')} {t.get('sqlstate') or ''} after {t.get('seconds')} s".strip()

        return [
            "",
            f"noisy analytics ({(r.get('scenario') or {}).get('id') or 'NL-N-I01'}):",
            f"  plan: {validity.get('plan')}  helpers per report allowed: {validity.get('helpers_per_report')}  "
            f"planned rate: {validity.get('planned_rate_tps')} TPS",
            f"  reports completed: {', '.join(kind_text(k) for k in ('q1', 'q2', 'q3', 'q4'))}",
            f"  limit test 1 (temp_file_limit): {test_text('limit_temp')}  -> enforced {m.get('temp_limit_enforced')}",
            f"  limit test 2 (statement_timeout): {test_text('limit_timeout')}  -> enforced "
            f"{m.get('statement_timeout_enforced')} (overrun {m.get('statement_timeout_overrun_s')} s)",
            f"  customers: TPS ratio {m.get('oltp_tps_ratio')}  p99 ratio {m.get('oltp_p99_ratio')}  worst p90 "
            f"{m.get('customer_p90_max_s')} s  cache hit {m.get('customer_cache_hit_ratio_baseline')} -> "
            f"{m.get('customer_cache_hit_ratio_hold')} ({m.get('customer_cache_hit_ratio_vs_baseline')} of baseline; "
            f"their calls, every table)",
            f"  cache hit, evidence only: tables the reports never read {_r(cache.get('untouched_tables_baseline'))} -> "
            f"{_r(cache.get('untouched_tables_hold'))}; report tables in the hold {_r(cache.get('report_tables_hold'))}",
            f"  helpers: most per report {m.get('report_helpers_max')}, samples over the limit "
            f"{m.get('helpers_over_limit')}; all sessions open in {m.get('analytics_active_share')} of the samples; "
            f"confirmed at minute {CONFIRM_AT_S / 60:g}: {m.get('fault_confirmed')}",
            f"  customers stopped by a limit: {m.get('customer_limit_errors')}  back to normal: {m.get('rto_to_slo_s')} s  "
            f"left: {m.get('analytics_sessions_left')} sessions, {m.get('temp_files_left')} temporary files",
            *wait_lines(facts.get("analytics_waits")),
            *tenant_lines(facts, m),
        ]
