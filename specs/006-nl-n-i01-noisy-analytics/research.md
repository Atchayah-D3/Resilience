# Research: NL-N-I01 Noisy Analytics

Decisions, each with its reason and the alternative not taken. Source plan: the 3 November
document (`docs/Noisy Neighbour/Resilience Scenario 1 Noisy analytics vs priority OLTP.md`).

## R1 One run, internal id NL-N-I01
- Decision: one run with full-table reports, one index report and the two limit tests (document §2).
- Why: the document's plan, modelled on CH-benCHmark (customers and reports together in one run).
- Rejected: three runs 1a/1b/1c (October test plan), superseded by the document.

## R2 Sizes calculated at every run, then checked by measurement
- Decision: warehouses = ceil(8 x shared_buffers / order_line bytes per warehouse); active
  warehouses = max(1, floor(0.5 x shared_buffers / bytes per warehouse)); full-table report
  sessions = ceil(cores / 3), plus 1 index session. Recorded in the results. The measured baseline
  hit ratio (at least 99%) decides whether the customers' data really fits.
- Why: the lab changes; numbers written in a file go stale. The formula proposes, the measurement decides.
- Numbers used: order_line about 29 MB per warehouse (measured locally, 2026-11-15); all TPC-C data
  about 110 MB per warehouse (tpcc/data.py).
- Rejected: fixed numbers per environment file (manual upkeep).

## R3 The customers' rate from the stored maximum
- Decision: `workload.rate_fraction_of_capacity: 0.6` takes 60% of the maximum the capacity test
  stored for the same clerks, warehouses and active warehouses (new table `resilience_tpcc.capacity`).
  Without such a record the run is refused with the command to measure it. A fixed `rate_tps`
  stays possible (exactly one of the two). NEEDS SIGN-OFF (Framework §7.5).
- Why: the maximum depends on the layout; 64 clerks on 4 warehouses compete far more than on 64.
- Rejected: one stored number in `meta.capacity_tps` (it does not say what it was measured with).

## R4 Customers' cache hit ratio on the tables the reports never read (deviation from the document)
- Decision: hit ratio over the TPC-C tables except `order_line` and `orders` (warehouse, district,
  customer, history, new_order, item, stock, with their indexes), from `pg_statio_user_tables`, for
  the baseline window and the hold window. Full-table reports still run as
  `EXPLAIN (ANALYZE, BUFFERS, TIMING OFF)`; their figures are kept as evidence.
- Why: the document subtracts each report's own pages (from EXPLAIN) from the table totals. The
  two limit tests end with an error, and an error returns no page counts, yet they read the whole
  of `order_line`: the subtraction would count their reads as the customers'. The tables the
  reports never read need no subtraction, and the eviction the index report causes shows directly
  as the customers' misses there.
- Rejected: the document's subtraction (wrong during the limit tests).

## R5 p90 per type during the hold: the worst second
- Decision: the largest 1-second p90 of New-Order, Payment and Order-Status during the hold.
- Why: per-second figures are what the samples carry. If at most 10% of each second's
  transactions exceed x, at most 10% of all do, so the true p90 is never above it: no false pass.

## R6 The customers log in as the harness role
- Decision: the harness role (no limits); disclosed. The document names `app_main`.
- Why: only the report role is limited, so the test is the same; no extra operator work.

## R7 Ending the report sessions
- Decision: at the end of the hold each session's running query is cancelled (the client sends the
  server a cancel request) and the connection closed; then every session tagged
  `resilience-analytics` is ended on the server through the cluster's OS user (the harness role may
  not end another role's sessions). The kill switch does the same by tag. A session the backstop
  cannot reach still ends at the role's `statement_timeout`; the revert waits for that and fails
  closed if anything is left.

## R8 Validity rules (invalid, never a pass)
- init: role missing or not able to log in; connection limit below the sessions needed;
  `statement_timeout` not shorter than the hold minus 30 s; no stored maximum for the layout.
- before the fault: baseline hit ratio below 99%; baseline TPS below 95% of the planned rate;
  report processes (sessions with their helpers) fewer than the cores.

## R9 Back to normal
- Decision: 95% of baseline TPS (stricter than the Framework's 80%), p99 at most 1.5x, 60 s
  straight, within 60 s of the reports stopping; the recovery watch is extended to cover a late but
  passing return (as NL-N-I03). Document §9; the 60 s deadline NEEDS SIGN-OFF.

## R10 Unconfigured role limit
- Decision: a limit the limit tests need (temp_file_limit, statement_timeout) that is not set refuses
  the run at init, before the warm-up and baseline; a check whose evidence is missing later is not
  measured, which fails the run.
- Why: never a pass without evidence (constitution III), and refusing at init saves a run of about
  half an hour that could not pass. The operator configured all limits.

## R11 First run on a new target
- The first run loads the data at the calculated size in init, then refuses because no maximum is
  stored for the layout, naming the capacity command. After the capacity test, the next run reuses
  the data (no reload) and runs to a verdict.

## R12 Fixes from the first lab run (16 November 2026, run NL-N-I01-20261116T061502Z-40ef88)
The run was stopped by the driver-limited guard 5 s into the reports (harness process at 1.07 of a
core, 0.55-0.60 for the 15 minutes before). The database log showed the cause: 13,509 errors
"permission denied for schema resilience_tpcc" in 5.5 s. The run had reloaded the TPC-C data at its
calculated size; the reload drops and recreates the schema, and the report login's grants went with
it. Every report failed in about 1 ms and its session sent the next one at once, a tight loop on
the harness side (the database was 65% idle). Fixes:
- A reload gives back the access other roles had to the TPC-C schema and tables (read as GRANT
  statements before the drop, given back after the load; disclosed; anything not given back listed).
- check_ready (a new FaultBehaviour hook, called last in init): the report login must be able to read
  order_line and orders, or the run is refused in seconds instead of after warm-up and baseline.
- A report session that ends with an error other than the role's limits (53400, 57014) stops; a
  session that fails within the first 2 s makes the fault "not landed" with the server's error.
- workload.tpcc.worker_processes: 2: the customers run in worker processes of their own. The same run
  showed the harness's main program pausing for about 1 s at the end of the baseline (reading the
  events file): with the clerks in it, the customers' last baseline second fell to 28 TPS and the
  first second of the fault showed 3,920 missed starts. With the clerks apart, such a pause cannot
  reach their sending or timing.
- EXPLAIN is kept: measured on the lab, the index report takes 5-20 ms and decoding its plan costs
  the harness about 5-10% of a core.
