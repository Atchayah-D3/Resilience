# Run analysis: knee point, p99, errors, DB metrics

```
analysis/
  sampler.py        records DB, host and app metrics during a run (python3 + psql only)
  analyze_run.py    one run -> report.html (+ report.md, steps.csv, summary.json), refreshes the index
  export_metrics.py one run -> metrics/*.csv + events.jsonl (per second; called by analyze_run.py)
  compare_runs.py   two runs -> compare.html (+ compare.md, compare.json), e.g. features ON vs OFF
  build_index.py    results/index.html: every run and comparison with verdict and knee
  report_html.py    the HTML pages (inline CSS + SVG charts, no scripts, one file each)
  mdhtml.py         Markdown -> HTML for the tables
```
No extra Python packages are needed. Normally you only run `../run_test.sh` (below).

## One command per run: `run_test.sh`
```bash
PGHOST=/tmp PGPORT=5432 PGUSER=postgres PGDATABASE=rrb \
APPS=http://localhost:8081 BASE_URL=http://localhost:8080 \
./run_test.sh ramp                      # or smoke | spike;  TOOL=jmeter for JMeter
```
It creates the run id (`<profile>-<UTC time>-<6 hex>`, or `RUN=...`), starts a sampler per
database server (`SAMPLERS="primary=<dsn>;replica=<dsn>"`, default: one sampler using the PG*
variables), runs the load, stops the samplers when the load ends, writes the report and
refreshes `results/index.html`. **Ctrl+C** stops everything cleanly; the report is still
written, marked INTERRUPTED, and steps that did not complete are shown as *not measured*
(never as passed or failed). All settings are listed at the top of `run_test.sh`.

On real infrastructure the sampler should run ON each database server (it measures the CPU
and disks of the machine it runs on): there, start `sampler.py --run <id> --name primary` on
each DB server, run `SAMPLERS=none RUN=<id> ./run_test.sh ramp` on the generator, then copy the
sampler files into `results/<id>/` and re-run `analyze_run.py results/<id>`.

## The report
`report.html` is one self-contained file (inline CSS and SVG, no scripts): it opens from
anywhere, can be mailed or attached. It shows
- a verdict badge (`PASS up to N/s`, `KNEE N/s`, `FAIL`, `NOT MEASURED`, plus `INTERRUPTED` /
  `GENERATOR LIMITED` when they apply), run details, features, and the pass rule;
- charts: target vs achieved sessions/s per step, p99 per endpoint per step against its limit,
  and time series of DB-host CPU, commits/s, WAL MB/s, active DB sessions, app responses/s and
  pool waits, with each step's measured window shaded (green pass, red fail, yellow not measured);
- the tables described below.
`results/index.html` lists every run (newest first) with features, steps, knee and verdict,
and every comparison, with links. Runs never analyzed or stopped early are listed too.

## Everything for a run lives in one folder
Use the same run name (`RUN=...` / `--run ...`) for every tool and the files land together:
```
results/
  R-ON/                              one folder per run
    k6-summary-<generator>.json      per-step load results   (run_k6.sh; one per generator)
    k6-plan-<generator>.json         step windows + start time
    k6-<generator>.log               k6 console output
    jmeter.jtl, jmeter-plan.json, report/   (JMeter runs instead of k6; report/ = JMeter's own dashboard)
    sampler-primary.jsonl            DB/host/app samples      (sampler.py --run R-ON --name primary)
    sampler-replica.jsonl
    feature_status-primary.json      TDE / sdbAudit / sdbImmutable state at the start
    report.html  report.md           the run report           (analyze_run.py)
    steps.csv  summary.json          per-step numbers for charts / input for compare_runs.py
    metrics/                         per-second values (export_metrics.py, run automatically):
      app.csv db-<n>.csv waits-<n>.csv os-<n>.csv   one row per second, opens in Excel
      events.jsonl                 one time-ordered stream: run/step markers + every metric
      README.md                    meaning of every column
  R-OFF/ ...
  compare-R-ON-vs-R-OFF/             comparisons get their own folder
    compare.html  compare.md
```
With several generators or DB servers, copy their files into the same run folder before analyzing.

## 1. During a run, by hand (what run_test.sh automates)
Same run name everywhere (here `R-ON`). **Start the samplers first**, on each database server
(or any machine that reaches it; the host CPU/disk columns are then that machine's):
```bash
# primary: also polls the app servers' /actuator/prometheus
PGHOST=localhost PGPORT=5432 PGUSER=rrb_monitor PGPASSWORD=<secret> PGDATABASE=rrb \
  python3 analysis/sampler.py --run R-ON --name primary --app http://app1:8081 --app http://app2:8081

# replica
PGHOST=localhost PGPORT=5432 PGUSER=rrb_monitor PGPASSWORD=<secret> PGDATABASE=rrb \
  python3 analysis/sampler.py --run R-ON --name replica
```
**Then the load**, in another terminal, and let it finish by itself:
```bash
cd load/k6 && RUN=R-ON BASE_URL=http://<lb> KEYS=../keys.csv ./run_k6.sh ramp
# or JMeter:  cd load/jmeter && RUN=R-ON HOST=<lb> PORT=80 KEYS=... ./run_jmeter.sh ramp
```
**After k6 prints its exit code**, stop each sampler with Ctrl+C in its terminal
(or `pkill -INT -f sampler.py`). Do not Ctrl+C k6: an interrupted step is reported as failed.

## 2. Analyze
```bash
python3 analysis/analyze_run.py results/R-ON          # -> results/R-ON/report.html
python3 analysis/analyze_run.py results/R-OFF
python3 analysis/compare_runs.py results/R-ON results/R-OFF --name-a ON --name-b OFF
                                                      # -> results/compare-R-ON-vs-R-OFF/compare.html
```
Open the `.html` files in any browser (on WSL: `explorer.exe "$(wslpath -w results/index.html)"`).

## What the report contains
- **Verdict** (step pass rule: achieved >= 98 % of target, errors < 0.1 %, p99 login < 500 ms,
  result and cut-off < 300 ms; change with `--p99`, `--max-error`, `--min-achieved`): the knee,
  the first failing step and the saturation signals there. **Run void** when any load generator
  averaged > 70 % CPU in a step or sessions were dropped (proposal 9.2).
- **Acceptance criteria (proposal 11.2)**, evaluated at the target step (`--target-rate`, default
  25,000 sessions/s; `TARGET_RATE=` for run_test.sh). Below the target every criterion is *not at
  target*: nothing is certified, values are shown for information.

  | AC | Checked from |
  |---|---|
  | AC-01 25,000/s for 10 min + 1.2x | step verdicts; needs a >= 9 min measured window and a 30,000/s step |
  | AC-02 DB read p99 <= 50 ms, p99.9 <= 200 ms | app histogram `rrb_db_tx_seconds{op="result"}` |
  | AC-03 DB login transaction p99 <= 100 ms | app histogram `rrb_db_tx_seconds{op="login"}` |
  | AC-04 end-to-end p99 <= 300 / 500 ms | k6 / JMeter |
  | AC-05 errors + late < 0.1 % | request errors + dropped (late) sessions |
  | AC-06 DB CPU <= 70 %, disk latency p99 <= 2 ms | DB-server samplers |
  | AC-07 / AC-08 | not applicable to ramp/spike runs (soak, failover) |
  | AC-09 extension overhead | `compare_runs.py` |
- **Load steps** (client): achieved vs target, p99 / p99.9 per endpoint, max, errors.
- **App-side latency**: DB transaction time p50/p99/p99.9 (login, result; cut-off loads) and
  server time p99 per endpoint, from histograms merged over all app servers (~10-20 % accuracy).
- **Per database server**: commits/s, statements/s (pg_stat_statements), inserts/s, WAL MB/s and
  KB/session, server-log MB/s (sdbAudit), hit %, checkpoints and their average duration, active
  backends, lock waits, top wait events, replica lag; the tables with most dead tuples (dead %,
  autovacuums, last autovacuum); top statements.
- **Per host** (DB servers and load generators): CPU average (1-s max), user/sys/iowait, CPU ms per
  session, run queue, context switches, free memory, swap, IOPS, disk MB/s, busiest disk with
  utilisation and latency (average and p99 of 1-second averages), network.
- **App servers**: pool waits, pool timeouts, DB errors, JVM CPU, GC time and max GC pause, HTTP status counts.
- Charts: target vs achieved, client p99 vs limits, DB-side p99 vs AC-02/AC-03, generator CPU vs
  70 %, disk latency vs 2 ms, DB-host CPU vs 70 %, commits/s, WAL MB/s, active sessions, app
  responses/s, pool waits; step windows shaded.

## Sampling resolution
| What | Interval | Proposal |
|---|---|---|
| OS (each sampler host) | 1 s | 10.3 "OS-level confirmation at 1 s" (also raw vmstat/iostat/sar/pidstat at 1 s via run_test.sh) |
| DB counters, app metrics | 1 s | Prometheus time series (no interval given) |
| Wait events | 10 samples per second (every 100 ms) | 10.3 "every 10 ms" with pg_wait_sampling (not in the local builds) |
| pg_stat_statements, per-table stats | every 5 s | per run |
| Client latency | per step (histograms over the measured window) | 11.1 per run, open-loop |
`sampler.py --interval / --wait-samples / --detail-every` change these.

## Proposal 11.1 metrics not collected yet
- **Pooler** (client wait p99, server connections, queue depth) and **pooler rejections**: need
  sdbPool's statistics interface (ask the sdbPool team).
- **HTTP 429 / 5xx at the load balancer**: need the LB (its access log or exporter).
- **Wait events at 10 ms**: need pg_wait_sampling in the target build.
- Per-I/O disk latency p99 needs block-layer tracing (e.g. biolatency); the report uses the p99 of
  1-second averages. Tools in 10.3 that are run by hand, not by this kit: pgBadger, perf/FlameGraph,
  Java Flight Recorder. Three repetitions with a median (9.2) are run as three runs and compared.

## How the steps are measured
`rrb_session.js` tags every iteration with the step whose constant-rate hold it starts in,
skipping `SETTLE` (default 30 s) after each ramp, so the transition is never counted. The step
is found on k6's own scheduling clock (`exec.scenario.progress`), not the wall clock: on WSL2
the wall clock was measured 1-3.5 % fast, which shifted sessions between steps. The analyzer
also stretches the step windows to the wall-clock duration (k6 logs its end time) so they line
up with the samplers' timestamps.
`run_k6.sh` saves the per-step summary and the step plan with its wall-clock start, which
`analyze_run.py` uses to cut the sampler data into the same windows. JMeter runs do the same
from the `.jtl` and `jmeter-plan.json`.

Disk flushes (fsync/FUA, from /proc/diskstats, kernel 5.5+) are reported separately from read/write latency: WAL commits wait on them. On a standby the report shows WAL received but not yet replayed and queries cancelled by recovery conflicts. `run_k6.sh` also saves k6's own dashboard (`k6-dashboard-<node>.html`, client-side rate/latency/VUs over time; `DASHBOARD=0` disables).

The server-log columns need `logging_collector = on` (the log directory must exist); otherwise they show `-`.

Limits: with several generators, k6 cannot merge percentiles, so a step's p99 is the worst
generator's p99 (conservative; counts are summed). The sampler's own queries add ~8 tiny
commits/s; they are excluded from top statements and wait events.

## Tested locally (2026-11-23, WSL, ShaktiDB 18.6.1.0, primary + streaming replica, 2 lakh rows)
- k6 ramp 20/40/80 sessions/s and JMeter ramp 10/20 sessions/s: per-step session counts
  match the schedule (e.g. 600 / 1,200 / 2,400; JMeter 304 / 597 for 300 / 600 expected).
- Samplers on primary and replica: WAL, waits, replication, app pools, pg_stat_statements.
- ON (immutable `result` + sdbaudit `write, ddl`) vs OFF compare: the feature state is recorded
  per run, and the sdbAudit cost shows as ~0.45 KB server log per session vs 0.
- Local CPU and latency numbers are NOT benchmark results: the app, k6 and both DB nodes
  share one small machine.
