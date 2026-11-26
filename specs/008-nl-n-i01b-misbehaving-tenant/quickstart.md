# NL-N-I01b: pilot and first run

## 1. Pilot (spec FR-012b): once per target, before the first 1b run

Purpose: see how much memory Q2, Q3 and limit test 1 really take with the 1b settings, and confirm
that limit test 1 still ends with 53400 when work_mem is 1 GB.

Do it **when no common workload and no scenario is running** (the reports would disturb the
workload's own baseline). Run **one query at a time**.

**Terminal 1: on the target VM, watch memory** (stop the query in terminal 2 with Ctrl-C if
MemAvailable goes below about 2 GB, i.e. 2097152 kB):

```bash
while true; do grep -E '^(MemAvailable|SwapFree):' /proc/meminfo | tr -s ' ' | tr '\n' ' '; grep '^pswpout ' /proc/vmstat; sleep 1; done
```

**Terminal 2: on the harness VM, as the report login** (password from `~/.pgpass`):

```bash
/usr/lib/postgresql/17.11.1.0/bin/psql -h 10.11.21.14 -p 5434 -U analytics_role -d resilience
```

```sql
-- the 1b settings, for this session only
SET max_parallel_workers_per_gather = 8;
SET work_mem = '1GB';
SET statement_timeout = 0;
SHOW max_parallel_workers_per_gather; SHOW work_mem; SHOW statement_timeout;

-- expected: ERROR: permission denied to set parameter "temp_file_limit" (42501)
SET temp_file_limit = -1;

-- half the rows (for Q3) and the rows minus 10 (for limit test 1)
SELECT reltuples::bigint / 2 AS half, reltuples::bigint - 10 AS minus_10
  FROM pg_class WHERE oid = 'resilience_tpcc.order_line'::regclass;

-- Q2 (the join): note "Workers Launched" and the Hash "Memory Usage"
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF)
SELECT o.o_ol_cnt,
       sum(CASE WHEN o.o_carrier_id IN (1, 2) THEN 1 ELSE 0 END)     AS high_line_count,
       sum(CASE WHEN o.o_carrier_id NOT IN (1, 2) THEN 1 ELSE 0 END) AS low_line_count
  FROM resilience_tpcc.orders o
  JOIN resilience_tpcc.order_line ol
    ON ol.ol_w_id = o.o_w_id AND ol.ol_d_id = o.o_d_id AND ol.ol_o_id = o.o_id
 WHERE o.o_entry_d <= ol.ol_delivery_d AND ol.ol_delivery_d < '2100-01-01 00:00:00'
 GROUP BY o.o_ol_cnt ORDER BY o.o_ol_cnt;

-- Q3 (the big sort): put the "half" number in place of <half>; note "Sort Method" and memory
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF)
SELECT ol_w_id, ol_d_id, ol_o_id, ol_amount FROM resilience_tpcc.order_line
 ORDER BY ol_amount DESC, ol_o_id OFFSET <half> LIMIT 100;

-- limit test 1 (one process): put the "minus_10" number in place of <minus_10>
-- expected: ERROR: temporary file size exceeds temp_file_limit (53400)
BEGIN;
SET LOCAL max_parallel_workers_per_gather = 0;
SELECT *, repeat('x', 50) AS pad FROM resilience_tpcc.order_line
 ORDER BY ol_amount DESC, ol_o_id, ol_number OFFSET <minus_10> LIMIT 10;
ROLLBACK;
```

**Send back:** the lowest MemAvailable and the pswpout change in terminal 1 for each query, the
Q2 and Q3 EXPLAIN output, and the error of `SET temp_file_limit` and of limit test 1.

If Q2 or Q3 alone takes the target below about 4 GB available, three sessions at once will trip
the guard: then the lead decides a lower work_mem for 1b (it differs from the document).

## 2. The first 1b run

After the pilot, on the harness VM in `~/resilience` (as for 1a, attached to the common workload):

```bash
.venv/bin/pytest resilience_tests/test_scenarios.py --env e2-u22-3 --common-workload workloads/oltp-common.yaml \
    --scenario NL-N-I01a --scenario NL-N-I01b -q -rA --report-dir reports
```

Expected: NL-N-I01a fails as before (p99 ratio); NL-N-I01b passes on the rules that must hold, with
a "1b findings" block in its summary.txt.
