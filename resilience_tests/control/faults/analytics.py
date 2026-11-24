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

The customers' cache hit ratio counts the pages of their own TPC-C calls on every table they read
(pg_stat_statements, the clerks' login), the report tables included: the reports flood exactly
those, so a ratio over the other tables only would barely move (the 21 November run: 1.00019 of
the baseline while the report tables fell to 0.707).
"""

from __future__ import annotations

import asyncio
import contextlib
import re
from typing import Any

from resilience_tests.analysis import rto_decomposer
from resilience_tests.analysis.predicates import NOT_MEASURED
from resilience_tests.control.faults.base import HeldFault, PhaseAbort, Sampler, register_fault
from resilience_tests.execution.injectors.analytics import CONFIRM_AT_S, SessionPlan, as_int, session_plan
from resilience_tests.execution.workload import contract
from resilience_tests.execution.workload.markers import ABORTED_JOURNAL, journal_paths, read_journal

# Back to normal (document section 9): TPS at least 95% of the baseline and p99 at most 1.5x for 60 s
# straight -- stricter than the Framework §6.2 80% -- within RECOVERY_LIMIT_S of the reports stopping.
NORMAL_TPS_FRACTION = 0.95
RECOVERY_LIMIT_S = 60.0
RECOVERY_WATCH_SLACK_S = 10.0
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


@register_fault
class AnalyticsLoadFault(HeldFault):
    fault_type = "analytics_load"
    fact_key = "analytics_load"

    def __init__(self, ctx: Any) -> None:
        super().__init__(ctx)
        self._observed: dict[str, Any] | None = None     # first reading (init)
        self._plan: SessionPlan | None = None
        self._cache: dict[str, dict[str, Any]] = {}

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
        if plan.limit_tests:
            hold = float(self.ctx.scenario.fault.duration)          # seconds: the catalog requires it
            timeout = duration_s(observed.get("role_statement_timeout"))
            if timeout is None:
                raise PhaseAbort("invalid run: the analytics login has no statement_timeout, so limit test 2 "
                                 "cannot be judged (the operator sets the role's limits)")
            if timeout > hold - TIMEOUT_MARGIN_S:
                raise PhaseAbort(f"invalid run: the analytics login's statement_timeout ({timeout:g} s) does not end "
                                 f"inside the {hold:g} s hold with {TIMEOUT_MARGIN_S:g} s to spare, so limit test 2 "
                                 "cannot be judged")
            if size_bytes(observed.get("role_temp_file_limit")) is None:
                raise PhaseAbort("invalid run: the analytics login has no temp_file_limit, so limit test 1 "
                                 "cannot be judged (the operator sets the role's limits)")

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
        needed = RECOVERY_LIMIT_S + rto_decomposer.SLO_SUSTAIN_S + RECOVERY_WATCH_SLACK_S
        return max(0.0, needed - self.ctx.phase_timeout_s("recovery"))

    # --- baseline and the hold -------------------------------------------------------------------

    def samplers(self) -> list[Sampler]:
        return [AnalyticsSampler(self.ctx.adapter, self.ctx.stream)]

    async def _snapshot(self, name: str) -> None:
        try:
            self._cache[name] = await self.ctx.adapter.customer_cache_counters()
        except Exception as exc:  # noqa: BLE001 -- recorded; the ratios are then not measured
            self.ctx.facts.setdefault("analytics_cache_errors", {})[name] = f"{type(exc).__name__}: {exc}"
        self.ctx.facts["analytics_cache"] = self._cache

    async def baseline_started(self) -> None:
        await self._snapshot("baseline_start")

    async def baseline_ended(self) -> None:
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

    async def after_inject(self, detail: dict[str, Any]) -> None:
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
        return m

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
            "noisy analytics (NL-N-I01):",
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
        ]
