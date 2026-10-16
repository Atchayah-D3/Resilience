"""NL-I-01 / NL-I-02: one byte flipped in one page of a heap or btree index relation."""

from __future__ import annotations

import asyncio
import time
from typing import Any

from resilience_tests.analysis.predicates import NOT_MEASURED
from resilience_tests.control.faults.base import FaultBehaviour, register_fault

# how long validate waits for the server's own report of the failed read to arrive through
# the log tailer before declaring it not seen
CORRUPTION_LOG_WAIT_S = 15.0


@register_fault
class DataCorruption(FaultBehaviour):
    fault_type = "data_corruption"
    fact_key = "data_corruption"
    recovers_unattended = True

    def __init__(self, run: Any) -> None:
        super().__init__(run)
        self._read: dict[str, Any] | None = None

    @property
    def _corrupts_index(self) -> bool:
        return self.run.scenario.fault.relation_kind == "btree_index"

    async def before_integrity_check(self) -> None:
        """The damaged page is read BEFORE the integrity check, so the checksum-failure counter
        the check samples already includes the reads that should have tripped it."""
        run = self.run
        try:
            if self._corrupts_index:
                self._read = await run.adapter.read_index_target()
            else:
                self._read = await run.adapter.read_corruption_target()
        except Exception as exc:  # noqa: BLE001 -- recorded; detection then counts as not measured
            self._read = {"error": f"{type(exc).__name__}: {exc}"}
        run.facts["corruption_read"] = self._read

    async def measure(self, events: list[Any]) -> dict[str, Any]:
        """Was the damaged page detected, on read, at the right place -- and nowhere else? For
        an index (NL-I-02) also: did any read through it return wrong rows, and does REINDEX
        restore it?

        Every value comes from an observation that could have gone the other way: the read the
        harness made, the engine's own checker run on the damaged relation, the engine's own
        failure counter, and the engine's own log. A missing observation is NOT_MEASURED."""
        run = self.run
        read = self._read or {}
        m: dict[str, Any] = {}
        injected = run.facts.get("data_corruption") or {}
        block, path = injected.get("block"), injected.get("relation_path")
        sqlstates = run.adapter.corruption_sqlstates

        attempts = read.get("attempts") or []
        if not attempts:
            for name in ("corruption_detected_on_read", "detection_identifies_block", "detection_repeatable"):
                m[name] = NOT_MEASURED
                run.not_measured[name] = read.get("error", "the damaged relation could not be read at all")
        else:
            first = attempts[0]
            detected = first.get("sqlstate") in sqlstates
            m["corruption_detected_on_read"] = detected
            # rows returned from a relation we know is damaged is the silent-corruption case itself
            run.facts["rows_returned_from_damaged_relation"] = first.get("rows")
            m["detection_identifies_block"] = (detected and first.get("block") == block
                                               and first.get("relation_path") == path)
            m["detection_repeatable"] = all(a.get("sqlstate") in sqlstates for a in attempts)
        if self._corrupts_index:
            m.update(self._measure_index_reads(attempts, sqlstates))

        relation = injected.get("relation")
        try:
            checked = await run.adapter.amcheck_relation(relation, timeout_s=run.profile.phase_timeouts_s["validate"] / 4)
        except Exception as exc:  # noqa: BLE001
            checked = {"detected": None, "note": f"{type(exc).__name__}: {exc}"}
        run.facts["amcheck_on_corrupted_relation"] = checked
        if checked.get("detected") is None:
            m["amcheck_detects_target"] = NOT_MEASURED
            run.not_measured["amcheck_detects_target"] = (
                f"the structural check of {relation} did not say either way: "
                f"{checked.get('note') or str(checked.get('output', ''))[:200]}")
        elif checked["detected"]:
            # it must have found THE damaged page: a finding anywhere else in the relation (or a
            # message that names no page) is not this detection
            named = [(p.get("block"), p.get("relation_path")) for p in checked.get("corrupt_pages") or []]
            checked["names_injected_block"] = (block, path) in named
            m["amcheck_detects_target"] = checked["names_injected_block"]
        else:
            m["amcheck_detects_target"] = False

        failures = run.facts.get("checksum_failures")
        if failures is None:
            m["checksum_failure_reported"] = NOT_MEASURED
            run.not_measured["checksum_failure_reported"] = "the engine's checksum-failure counter could not be read"
        else:
            m["checksum_failure_reported"] = failures >= 1

        # The engine's own report of each failed read. Wait for it: it travels through the log
        # tailer and can trail the read by a moment.
        deadline = time.monotonic() + CORRUPTION_LOG_WAIT_S
        while True:
            assert run.stream is not None and run.t0_ns is not None
            lines = [str(e.data.get("line", "")) for e in run.stream.events()
                     if e.kind == "log_line" and e.t_mono_ns > run.t0_ns]
            locations = run.adapter.corruption_log_locations(lines)
            if (block, path) in locations or time.monotonic() >= deadline:
                break
            await asyncio.sleep(0.5)
        run.facts["corruption_log_locations"] = [{"block": b, "relation_path": p} for b, p in locations]
        if (block, path) not in locations:
            m["corruption_outside_target"] = NOT_MEASURED
            run.not_measured["corruption_outside_target"] = (
                "the server's report of the failed read never reached the harness, so its log cannot show "
                "whether any other relation was damaged (check the node's log_file in the profile)")
        else:
            m["corruption_outside_target"] = sum(1 for loc in locations if loc != (block, path))
        run.disclosures.append(
            "corruption_count includes the checksum failures this scenario caused on purpose; "
            "damage elsewhere is judged by corruption_outside_target and structural_integrity_errors.")
        if self._corrupts_index:
            # after every observation of the damage: the repair destroys the evidence
            m.update(await self._measure_index_rebuild(injected))
        return m

    def _measure_index_reads(self, attempts: list[dict[str, Any]], sqlstates: tuple[str, ...]) -> dict[str, Any]:
        """NL-I-02 "no wrong results returned first": each read through the damaged index either
        failed with the corruption error or returned exactly the rows written. A read the engine
        did not serve from that index, or that failed for another reason, cannot say."""
        m: dict[str, Any] = {}
        if not attempts:
            m["wrong_results_returned"] = NOT_MEASURED
            self.run.not_measured["wrong_results_returned"] = "the table could not be read through its index at all"
            return m
        undecided = [a for a in attempts
                     if a.get("sqlstate") not in sqlstates and not (a.get("error") is None and a.get("used_index") is True)]
        if undecided:
            a = undecided[0]
            m["wrong_results_returned"] = NOT_MEASURED
            self.run.not_measured["wrong_results_returned"] = (
                "a read neither failed with the corruption error nor was served from the index "
                f"(used_index={a.get('used_index')}, plan={a.get('plan')}, error={a.get('sqlstate') or a.get('error')})")
        else:
            m["wrong_results_returned"] = sum(1 for a in attempts
                                              if a.get("error") is None and not a.get("matches_written"))
        return m

    async def _measure_index_rebuild(self, injected: dict[str, Any]) -> dict[str, Any]:
        """NL-I-02 "REINDEX restores": the rebuild succeeded, a read through the rebuilt index
        returns exactly the rows written, and the checker -- table with its index, every row
        cross-checked against the index -- runs clean. All three, from the engine itself."""
        run = self.run
        m: dict[str, Any] = {}
        rebuild: dict[str, Any] = {}
        run.facts["index_rebuild"] = rebuild
        try:
            rebuild["reindex"] = await run.adapter.rebuild_corruption_target()
        except Exception as exc:  # noqa: BLE001 -- could not tell either way
            rebuild["reindex"] = {"ok": None, "error": f"{type(exc).__name__}: {exc}"}
        reindex = rebuild["reindex"]
        if reindex.get("ok") is None:
            m["reindex_restores"] = NOT_MEASURED
            run.not_measured["reindex_restores"] = f"REINDEX could not be run: {reindex.get('error')}"
            return m
        if not reindex["ok"]:
            m["reindex_restores"] = False
            return m
        m["reindex_duration_s"] = reindex.get("duration_s")
        # REINDEX builds new storage; still serving the damaged file means nothing was rebuilt
        before, after_path = injected.get("relation_path"), reindex.get("relation_path_after")
        new_storage = None if not before or not after_path else after_path != before
        rebuild["new_storage"] = new_storage

        try:
            after = (await run.adapter.read_index_target(attempts=1)).get("attempts") or [{}]
        except Exception as exc:  # noqa: BLE001
            after = [{"error": f"{type(exc).__name__}: {exc}", "used_index": None}]
        rebuild["read_after"] = after[0]
        table = injected.get("table") or injected.get("relation")
        try:
            rebuild["amcheck_after"] = await run.adapter.amcheck_relation(
                table, timeout_s=run.profile.phase_timeouts_s["validate"] / 4, heapallindexed=True)
        except Exception as exc:  # noqa: BLE001
            rebuild["amcheck_after"] = {"detected": None, "note": f"{type(exc).__name__}: {exc}"}

        read_ok: bool | None
        a = after[0]
        if a.get("error") is None and a.get("used_index") is True:
            read_ok = bool(a.get("matches_written"))
        elif a.get("error") is not None and a.get("sqlstate") in run.adapter.corruption_sqlstates:
            read_ok = False                       # the rebuilt index is still damaged
        else:
            read_ok = None
        detected_after = rebuild["amcheck_after"].get("detected")
        check_ok = None if detected_after is None else not detected_after
        if read_ok is False or check_ok is False or new_storage is False:
            m["reindex_restores"] = False
        elif read_ok is None or check_ok is None or new_storage is None:
            m["reindex_restores"] = NOT_MEASURED
            if new_storage is None:
                why = (f"the index file after REINDEX is unknown (before: {before}, "
                       f"after: {after_path}), so a rebuild into new storage was not shown")
            elif read_ok is None:
                why = f"the read through the index could not say (read: {a})"
            else:
                why = ("the structural check could not say (check: "
                       f"{rebuild['amcheck_after'].get('note') or rebuild['amcheck_after'].get('output', '')[:200]})")
            run.not_measured["reindex_restores"] = "after REINDEX, " + why
        else:
            m["reindex_restores"] = True
        return m
