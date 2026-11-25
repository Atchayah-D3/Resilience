-- Post-load sanity: counts, distributions and the plans of the 3 hot queries.
--   psql -v cen=202501 -f smoke_check.sql
\set ON_ERROR_STOP on
SET search_path = rrb;

\echo '== counts (must all equal N, cutoffs = 105)'
SELECT (SELECT count(*) FROM candidate)                       AS candidates,
       (SELECT count(*) FROM application WHERE cen_id = :cen)  AS applications,
       (SELECT count(*) FROM result      WHERE cen_id = :cen)  AS results,
       (SELECT count(*) FROM exam_cutoff WHERE cen_id = :cen)  AS cutoffs;

\echo '== result status mix'
SELECT status, count(*), round(100.0 * count(*) / sum(count(*)) OVER (), 1) AS pct
FROM result WHERE cen_id = :cen GROUP BY status ORDER BY status;

\echo '== plan: login lookup (expect Index Only Scan on candidate_regno_uk)'
EXPLAIN (COSTS OFF)
SELECT candidate_id, dob FROM candidate WHERE registration_no = 'RRB0000000001';

\echo '== plan: result read (expect Index Scan on result_c<cen>_pkey, one partition)'
EXPLAIN (COSTS OFF)
SELECT roll_no, rrb_code, category, raw_score, normalized_score, status, published_at
FROM result WHERE cen_id = :cen AND candidate_id = 1;

\echo '== plan: cut-off read'
EXPLAIN (COSTS OFF)
SELECT rrb_code, category, cutoff_score, vacancies FROM exam_cutoff WHERE cen_id = :cen;

\echo '== partitions in the next 48 h and default-partition leakage (expect 0 in both)'
SELECT count(*) AS session_partitions FROM pg_inherits WHERE inhparent = 'rrb.user_session'::regclass;
SELECT * FROM check_partitions;
