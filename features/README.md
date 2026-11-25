# Feature-cost runs: sdbTDE, sdbAudit, sdbImmutable

Goal (proposal 1.1): quantify what sdbTDE, sdbAudit and sdbImmutable cost at the RRB
release load. Method: run the same ramp twice, once with all three **on** (the production
setup and the headline run) and once with all three **off**, and compare the knee point,
p99 latency and DB cost per session. Everything else stays identical.

```
features/
  feature_status.sql   prints the feature state as JSON (attach to every run; the sampler records it too)
  set_audit.sh         sdbAudit on | off | status, live (ALTER SYSTEM + reload, no restart)
  apply_immutable.sql  makes rrb.result immutable (irreversible)
```
`db/load_data.sh` takes `TABLE_AM=<encrypted access method>` for the TDE dataset.

## 1. What can be switched, and how

| Feature | How it is switched | Reversible? | Consequence |
|---|---|---|---|
| **sdbAudit** | `set_audit.sh on/off` (`sdbaudit.log`) | yes, live | Toggle between runs on the same dataset |
| **sdbImmutable** | `apply_immutable.sql` after the load | **no** (DROP TABLE / TRUNCATE / DROP SCHEMA are refused afterwards; only DROP DATABASE works) | Needs its own dataset |
| **sdbTDE** | tables created with the encrypted access method (`TABLE_AM=`) | only by rewriting every table | Needs its own dataset |

So the quick win needs **two datasets**, each loaded once and snapshotted:

| Dataset | Load | Then |
|---|---|---|
| **ON** | TDE extension + key set up, `TABLE_AM=<tde am> ./load_data.sh …` | `apply_immutable.sql`, snapshot |
| **OFF** | plain `./load_data.sh …` | snapshot (never mark it) |

Both datasets: same N, same CEN, same server, same postgresql.conf, same app and pool settings.

## 2. Run matrix

**Quick win (agreed scope): 2 runs**

| Run | Dataset | sdbAudit | Answers |
|---|---|---|---|
| R-ON | ON | on | headline numbers (proposal: "all headline runs use TDE on") |
| R-OFF | OFF | off | combined cost of the three features = R-ON vs R-OFF |

**Optional, proposal FT-01 (one feature off at a time)**: adds two datasets
(TDE without immutable, immutable without TDE) and three runs. Ask before planning it;
each run is ~30-75 min plus restore and warm-up.

| Run | Dataset | sdbAudit | Cost isolated |
|---|---|---|---|
| R-AUD | ON | off | sdbAudit (R-ON vs R-AUD) |
| R-TDE | immutable only | on | sdbTDE (R-ON vs R-TDE) |
| R-IMM | TDE only | on | sdbImmutable (R-ON vs R-IMM) |

Run the same profile for every run: the step ramp (finds the knee) and, if time allows,
the spike. Repeat a run if its two halves disagree by more than a few percent.

## 3. Preparing the datasets

### 3.1 sdbTDE (ON dataset only)
TDE is not in the local test build, so these steps follow **pg_tde 2.x** and must be
confirmed with the ShaktiDB team (extension name, access method name, key provider,
function names, WAL encryption):

```bash
# postgresql.conf on the primary AND the replica (not ALTER SYSTEM: it does not split lists)
shared_preload_libraries = 'pg_tde, sdbaudit, sdb_immutable, pg_stat_statements, ...'
# restart both nodes
```
```sql
-- in database rrb on the primary
CREATE EXTENSION pg_tde;
SELECT pg_tde_add_database_key_provider_file('local-keyring', '/var/lib/shaktidb/keyring/rrb.per');
SELECT pg_tde_create_key_using_database_key_provider('rrb-key', 'local-keyring');
SELECT pg_tde_set_key_using_database_key_provider('rrb-key', 'local-keyring');
-- WAL encryption only if production uses it (ask):
-- ALTER SYSTEM SET pg_tde.wal_encrypt = on;   + restart
```
Use the key provider production uses (file, Vault or KMIP). With a file provider the
keyring file must also exist on the replica at the same path.

```bash
cd db
TABLE_AM=tde_heap ./load_data.sh 15000000 202501 16 500000
```
`TABLE_AM` creates every table encrypted and sets it as the database default, so the
hourly/daily partitions created later by `rrb.ensure_partitions()` are encrypted too.
The loader refuses to start if the access method does not exist.

### 3.2 sdbImmutable (ON dataset only)
```bash
psql -X -d rrb -f ../features/apply_immutable.sql
```
Marks `rrb.result` (parent and all partitions). Takes milliseconds: no table rewrite.
The proposal also lists answer-sheet *response* tables as immutable; the quick win has
no response table, so only `result` is marked. **Irreversible**: to reload this dataset,
drop and re-create database `rrb` (the loader's `RESET=1` refuses, on purpose).

### 3.3 sdbAudit (both datasets, per run)
The production audit policy decides what "on" means. **Agree it before the runs**
(today `sdbaudit.log = none`, so "on" would audit nothing).
```bash
AUDIT_LOG='write, ddl' ./set_audit.sh on      # replace with the agreed policy
./set_audit.sh off
```
If the policy includes `read`, set it on the replica too (ALTER SYSTEM is per node).
Keep `logging_collector = on` in both runs: the audit disk cost is measured as server-log
growth, which needs a log directory.
Audit lines go to the server log: on R-ON the log directory grows with the write rate,
and the sampler records log MB/s so the disk cost is visible.

"Off" = `sdbaudit.log = none` (library still loaded). If the agreed meaning is "not
loaded", remove `sdbaudit` from `shared_preload_libraries` in postgresql.conf and
restart both nodes for R-OFF instead.

### 3.4 Snapshot
Snapshot each dataset right after preparation (VM/disk snapshot, or `pg_basebackup`),
including the replica. Every run starts from its snapshot: `user_session` and
`audit_event` grow by ~50K rows/s at peak, so a reused database is a different database.

## 4. Per-run checklist

1. Restore the dataset snapshot (primary and replica). Start both nodes.
2. Set sdbAudit for this run (`set_audit.sh on|off`).
3. `psql -d rrb -c "SELECT rrb.ensure_partitions(now(), 48);"`
4. Check the feature state (the sampler also saves it as `results/<run>/feature_status-<server>.json`):
   `psql -X -At -d rrb -f features/feature_status.sql`, and check access method, immutable tables, `sdbaudit_log`, preload list.
5. `SELECT pg_stat_statements_reset();` on the primary and the replica.
6. Restart the app servers (clean pools and JIT), run a 2-minute smoke as warm-up.
7. `RUN=<run> ./run_test.sh ramp` (or samplers on the DB servers + the load by hand, see `analysis/README.md`).
8. After the run: stop samplers, `SELECT * FROM rrb.check_partitions;` must be 0/0. With
   several generators/DB servers, copy their files into `results/<run>/`.
9. `report.html` is written automatically (or `python3 analysis/analyze_run.py results/<run>`).

Compare: `python3 analysis/compare_runs.py results/R-ON results/R-OFF` -> `results/compare-R-ON-vs-R-OFF/compare.html`.

## 5. Must stay identical between runs
Hardware and OS settings, postgresql.conf (except the features under test),
`shared_preload_libraries` (except a removed sdbaudit, if agreed), pg_stat_statements on
in both, replication mode (async/sync), sdbPool config, app version and pool sizes,
key file (`--s`, `--rows`, seed), load profile and generator count, and the app's own
`audit_event` insert, which is part of the login scenario and is **not** sdbAudit.

## 6. Reading the result
`compare_runs.py` reports, per ramp step and for the knee:
- knee point (sessions/s) ON vs OFF: the capacity cost;
- p99 per endpoint at the same offered rate: the latency cost;
- DB CPU ms per session and WAL bytes per session: the resource cost, which still shows
  when both runs pass every step;
- server-log MB/s: the sdbAudit disk cost.
