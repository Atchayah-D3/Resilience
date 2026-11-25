# RRB result-release quick win: ShaktiDB benchmark kit

The scenario is candidates checking their results right after release. Each session does
**login → result → cut-off** through a load balancer → Spring Boot → sdbPool → ShaktiDB
(primary for writes, replica for reads).

```
db/          6-table schema, parallel 1.5-crore data load, partitions, smoke check, app role
app/         Spring Boot 3.4 / Java 21 service: 3 endpoints, primary/replica routing, Prometheus metrics
load/        Zipf key generator, k6 script (recommended), JMeter plan (alternative)
features/    sdbTDE / sdbAudit / sdbImmutable on-off procedure and scripts      -> features/README.md
monitoring/  Prometheus, Grafana dashboard, exporters, pg_stat_statements setup -> monitoring/README.md
analysis/    run sampler, knee-point report (HTML), ON-vs-OFF comparison, index -> analysis/README.md
run_test.sh  one command per run: run folder, samplers, load, report, index
```

## 1. Database (DBA, on the primary)

```bash
cd db
# 1.5 crore candidates, CEN 202501, 16 parallel sessions, 5 lakh rows per transaction
PGHOST=<primary> PGPORT=5432 PGUSER=<owner> PGDATABASE=rrb ./load_data.sh 15000000 202501 16 500000
psql -v app_password="'<secret>'" -f 04_roles.sql          # creates role rrb_app for the API
```

- Connect directly to the primary for the load, not through sdbPool. The replica receives the data through replication.
- `load_data.sh` creates the schema, loads `candidate`, `application` and `result` in parallel chunks, builds keys and indexes, inserts 105 cut-off rows, runs `VACUUM ANALYZE`, creates 48 h of `user_session`/`audit_event` partitions, and finally runs `smoke_check.sql`. That check prints counts, the result-status mix, and the plans of the three hot queries:
  - login: Index Only Scan on `candidate_regno_uk`
  - result: Index Scan on one result partition's PK
- To rebuild, set `RESET=1`. **This drops schema `rrb`.**
- **Before every run:** `SELECT rrb.ensure_partitions(now(), 48);`
- **After every run:** `SELECT * FROM rrb.check_partitions;` should be 0/0. Otherwise rows went to the DEFAULT partition.
- Restore from the snapshot between runs, as the proposal requires. `user_session` and `audit_event` grow by about 50K rows/s at peak.

| Table | Rows | Partitioning | Hot path |
|---|---|---|---|
| candidate | N | none | login: covering unique index `(registration_no) INCLUDE (candidate_id, dob)` |
| application | N | LIST (cen_id) | not read on the hot path. It's present for realistic size and cache pressure |
| result | N | LIST (cen_id) | PK `(cen_id, candidate_id)`, read from the replica |
| exam_cutoff | 105 | none | read from the replica, cached 60 s in each app server |
| user_session | +1 per login | RANGE hourly | INSERT on the primary |
| audit_event | +1 per login | RANGE daily | INSERT on the primary (application audit; sdbAudit is separate) |

The data is deterministic: `rrb.registration_no(id)` gives `RRB0000000042`, and `rrb.dob(id)` gives a fixed DOB.
The key generator uses the same formulas, so no export is needed.

## 2. API (each app server)

```bash
cd app && mvn -B -DskipTests package           # builds target/rrb-api.jar
DB_PRIMARY_HOST=<sdbpool-rw> DB_PRIMARY_PORT=6432 \
DB_REPLICA_ENABLED=true DB_REPLICA_HOST=<sdbpool-ro> DB_REPLICA_PORT=6432 \
DB_NAME=rrb DB_USER=rrb_app DB_PASSWORD=<secret> \
RRB_TOKEN_SECRET=<same 32+ byte secret on ALL app servers> \
java -Xms4g -Xmx4g -XX:+UseZGC -jar target/rrb-api.jar
```

| Endpoint | DB work |
|---|---|
| `POST /api/v1/auth/login` `{"registrationNo","dob","cenId"?}` | One transaction on the primary: index-only read, then INSERT session and INSERT audit. A wrong DOB returns 401 and writes an audit FAIL row. |
| `GET /api/v1/results/{cenId}` with `Authorization: Bearer <token>` | One PK read on the replica |
| `GET /api/v1/cutoffs/{cenId}?rrbCode=&category=` | Served from memory. At most one replica read per CEN per server per 60 s. |

Metrics are on port 8081 at `/actuator/prometheus` (`rrb_db_tx_seconds{op}` is the DB-side transaction time histogram used for AC-02/AC-03):
- `http_server_requests_seconds` (histogram per endpoint)
- `hikaricp_connections_*{pool=primary|replica}` (pending threads = pool wait)
- `rrb_login_total{outcome}`
- `rrb_db_errors_total{kind}`
- `cache_*{cache=cutoff}`

When overloaded, the service fails fast with HTTP 503 after a 2 s pool wait (`DB_CONNECTION_TIMEOUT_MS`).

| Setting (env) | Default | Use |
|---|---|---|
| `DB_PREPARE_THRESHOLD` | `0` | Safe with transaction-mode pooling. Set to `5` only if sdbPool supports prepared statements in transaction mode. **Confirm this with the sdbPool team.** |
| `DB_PRIMARY_POOL_SIZE` / `DB_REPLICA_POOL_SIZE` | 30 / 30 | Per app server. Eight servers give about 240 client connections into each sdbPool. |
| `RRB_SESSION_VALIDATE_IN_DB` | `false` | `true` adds a `user_session` PK read on the **primary** to every result call (the session-table variant) |
| `RRB_CUTOFF_CACHE_TTL` | `60s` | `0s` sends every cut-off call to the DB |
| `DB_REPLICA_ENABLED` | `false` | `false` sends reads to the primary pool (single node) |

## 3. Load

```bash
cd load
python3 -m pip install --user numpy                                           # once; the generator needs numpy
python3 gen_zipf_csv.py --n 15000000 --rows 3000000 --s 0.7 --out keys.csv   # takes about 5 s and 1 GB RAM
```
Copy `keys.csv` to the same path on every generator node.

**Zipf exponent.** The proposal says s ≈ 1.1. Over 1.5 crore keys, that sends **11.5% of all logins to a single
candidate** and 87% to the top 1%. The hot set then fits in cache and the DB looks better than it would in production.
The default here is 0.7: the hottest key gets 0.2%, the top 1% gets 25%, and 70% of rows are distinct.
Agree the value with your senior. `--s 1.1` reproduces the proposal, and `--s 0` is uniform (the worst case for cache).
`--bad-pct 1` adds 1% wrong-DOB rows to exercise failed-login audit writes.

### k6 (recommended)
Rates are **sessions/s**, and each session makes 3 HTTP calls. For measured runs use `./run_k6.sh <profile>`
(same env vars), which also saves the per-step summary and step plan for `analysis/analyze_run.py`.
```bash
cd load/k6
k6 run -e PROFILE=smoke -e BASE_URL=http://<lb> -e KEYS=../keys.csv rrb_session.js
k6 run -e PROFILE=ramp  -e BASE_URL=http://<lb> -e KEYS=../keys.csv rrb_session.js   # 5k..35k/s, 10 min per step
k6 run -e PROFILE=spike -e BASE_URL=http://<lb> -e KEYS=../keys.csv rrb_session.js   # idle -> 25k/s in 30 s, hold 10 min
```
For 4 generator nodes, run the same command on each with:
```
node1: --execution-segment "0:1/4"     --execution-segment-sequence "0,1/4,2/4,3/4,1"
node2: --execution-segment "1/4:2/4"   --execution-segment-sequence "0,1/4,2/4,3/4,1"
node3: --execution-segment "2/4:3/4"   --execution-segment-sequence "0,1/4,2/4,3/4,1"
node4: --execution-segment "3/4:1"     --execution-segment-sequence "0,1/4,2/4,3/4,1"
```
Start all four within the same second, for example with `pssh` or `ansible -f 4`. k6 splits both the rate and the key list between the nodes.

Thresholds are p99 login < 500 ms, result and cut-off < 300 ms, and errors < 0.1%. **`dropped_iterations` > 0 makes the run void**,
because it means the generator could not start sessions on time. In that case raise `PRE_VUS` or add nodes.
`NEW_CONN=1` opens a new TCP connection per session, which is closer to real browsers.

### JMeter (alternative)
This uses the built-in Open Model Thread Group (JMeter 5.5+), which is open-loop like k6.
```bash
cd load/jmeter
HOST=<lb> PORT=80 KEYS=/data/keys.csv ./run_jmeter.sh smoke
HOST=<lb> PORT=80 KEYS=/data/keys.csv REMOTE=gen1,gen2,gen3,gen4 HEAP="-Xms16g -Xmx16g" ./run_jmeter.sh spike
```
With `REMOTE`, the script divides the total rate by the number of servers. It writes `results/<run>/` with the `.jtl`, an HTML dashboard and `jmeter-plan.json` (step windows for `analysis/analyze_run.py`).
JMeter starts one thread per arrival, so each session uses a new connection. At 25K sessions/s, which is about 6K new threads/s per node,
it needs much more CPU than k6. Use k6 for the spike if generator CPU goes above 70%.

## 4. Monitoring, feature-cost runs and analysis

**One command per run** (creates `results/<run>/`, samples the DB, runs the load, writes
`report.html`, refreshes `results/index.html`; Ctrl+C still produces a report):
```bash
PGHOST=<db> PGPORT=5432 PGUSER=rrb_monitor PGDATABASE=rrb APPS=http://app1:8081,http://app2:8081 \
BASE_URL=http://<lb> ./run_test.sh ramp          # smoke | ramp | spike;  TOOL=jmeter for JMeter
```
- `monitoring/README.md`: pg_stat_statements on both nodes, exporters, Prometheus + Grafana dashboard.
- `features/README.md`: the ON run (TDE + sdbAudit + immutable `result`) vs the OFF run, dataset preparation,
  per-run checklist. sdbImmutable and TDE are set when the dataset is built, so ON and OFF need separate datasets.
- `analysis/README.md`: `run_test.sh` in detail, the report, running samplers on the DB servers,
  `analyze_run.py` (knee, p99, errors, DB metrics) and `compare_runs.py` (ON vs OFF cost).
  Each run keeps everything in `results/<run>/` (load results, samples, feature state,
  `report.html` and `report.md`); comparisons go to `results/compare-<A>-vs-<B>/`.
  Per-second values for every metric are in `results/<run>/metrics/` (CSV files + `events.jsonl`, columns explained in `metrics/README.md`).
- Run `db/04_roles.sql` and `monitoring/db_setup.sql` before starting the app servers: passwd_change fails
  `ALTER ROLE ... PASSWORD` for a role with open sessions in a database without its audit table.

## Verified locally (ShaktiDB 18.6.1.0, 2 lakh rows, WSL)
- The data load, indexes, partitions and smoke-check plans work as described above.
- All endpoints work, including the 400, 401 and 403 cases. Session and audit rows land in the correct hourly and daily partitions.
- Routing: with the primary role's SELECT on `result`/`exam_cutoff` revoked, result and cut-off still succeeded, which proves reads go through the replica pool. `validate-in-db` rejects a token once its session row is deleted.
- The Zipf keys match the DB: 50,000 of 50,000 registration numbers found, and DOB matches on every row except the deliberately bad ones.
- k6 smoke, spike and ramp ran with 0 errors and 0 dropped iterations. A 2-segment run split the arrivals 2,450 / 2,449.
- JMeter smoke and spike ran with 0 errors, and the arrival counts match the schedule.
- **Not tested:** sdbPool, a real replica, and anything at production scale. Those need the target infrastructure.

### Local quick-win feature runs (WSL)
`local/quickwin_local.sh off|on|compare` runs R-OFF / R-ON on an isolated test cluster
(`/var/lib/postgresql/releasetesting/qw`: primary :5440, replica :5441, pg_stat_statements,
logging_collector and track_io_timing on; app instance :8090/:8091; datasets `rrb_off` = heap and
`rrb_on` = tde_heap + immutable `rrb.result`, 50,000 candidates, keys `load/keys-50k.csv`).
It follows the per-run checklist in `features/README.md` section 4.
