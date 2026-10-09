# Data Model: Test Reports

All entities are derived, in memory, from a run directory. Nothing is stored except the
generated files.

## E1. RunEvidence (input)

| Field | Source | Rule |
|---|---|---|
| run_dir | path | must exist |
| results | `results.json` | None if missing -> "no result recorded" |
| events | `events.jsonl` | empty list if missing; malformed lines skipped and counted |

## E2. RunView (what the page shows)

- header: scenario id, name, priority, category, run id, environment (profile, class), target
  (node, role), status, error, total duration
- rules: list of {predicate, outcome (pass/fail/missing/not_measured/not_applicable/error),
  values (each value classified as number/bool/NOT_MEASURED/NOT_APPLICABLE/missing), margin,
  reason}
- measures: name -> classified value, with not-measured reason
- phases: name, outcome, duration, error
- timeline: list of TimelineMarker; series: list of Series; cycles: list of rows from
  `facts.cycles`; during: `facts.during` / `facts.during_verification` when present
- disclosures: list[str], always complete
- evidence_links: files present in the run directory (relative links)
- notes: what could not be shown (e.g. "events.jsonl missing")

## E3. TimelineMarker

{t_s (seconds from run start), kind (run_start | phase | fault | outage_start | first_write |
recovered | slo_recovered | t1 | run_end), cycle (optional), label}

## E4. Series

{name, unit, points [(t_s, value)], downsampled (bool), svg_points (str)}; at most 1,200
points.

## E5. ReportCase (one per pytest scenario case)

| Field | Rule |
|---|---|
| case_id | pytest node id / run key |
| scenario_id, priority, name | from results, or from the catalog for skipped cases |
| status | passed / failed / aborted / error / stopped_before_fault / skipped / no_result |
| run_id, evidence_dir, report_path | when a run happened |
| message | failed rules (failed); reason + phase (aborted/error); skip reason |
| duration_s | sum of phase durations, or pytest duration |

## E6. GateDecision

{failed (bool), p0_non_pass: [cases], other_non_pass: [cases], skipped: [cases]};
failed = any P0 case with status in {failed, aborted, error, no_result}.
