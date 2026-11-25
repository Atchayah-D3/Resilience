-- Run after all load_chunk.sql chunks: keys, indexes, cut-offs, stats,
-- visibility map (for index-only scans) and the first session/audit partitions.
--   psql -v cen=202501 -f 03_finalize.sql
\set ON_ERROR_STOP on
SET search_path = rrb;
SET maintenance_work_mem = '4GB';
SET max_parallel_maintenance_workers = 8;

\echo '== primary keys / unique indexes'
ALTER TABLE candidate   ADD CONSTRAINT candidate_pk   PRIMARY KEY (candidate_id);
-- Login path: covering index so authentication is an index-only scan.
CREATE UNIQUE INDEX candidate_regno_uk ON candidate (registration_no) INCLUDE (candidate_id, dob);

ALTER TABLE application ADD CONSTRAINT application_pk PRIMARY KEY (cen_id, application_id);
CREATE UNIQUE INDEX application_cand_uk ON application (cen_id, candidate_id);

-- Result path: primary-key read by (cen_id, candidate_id).
ALTER TABLE result      ADD CONSTRAINT result_pk      PRIMARY KEY (cen_id, candidate_id);

\echo '== exam_cutoff'
INSERT INTO exam_cutoff (cen_id, rrb_code, category, cutoff_score, vacancies, published_at)
SELECT :cen, r::smallint, c, cutoff_for(r::smallint, c), 200 + (r * 37) % 900,
       timestamptz '2026-01-15 10:00+05:30'
FROM generate_series(1, 21) r
CROSS JOIN unnest(ARRAY['UR','OBC','SC','ST','EWS']) c
ON CONFLICT (cen_id, rrb_code, category) DO NOTHING;

\echo '== session/audit partitions for the next 48 h'
SELECT rrb.ensure_partitions(now(), 48) AS partitions_created;

\echo '== VACUUM ANALYZE (sets visibility map for index-only scans)'
VACUUM (ANALYZE) candidate;
VACUUM (ANALYZE) application;
VACUUM (ANALYZE) result;
VACUUM (ANALYZE) exam_cutoff;
ANALYZE user_session;
ANALYZE audit_event;

\echo '== row counts and sizes'
SELECT relname AS table, n_live_tup AS rows_est,
       pg_size_pretty(pg_total_relation_size(relid)) AS size_incl_indexes
FROM pg_stat_user_tables
WHERE schemaname = 'rrb' AND relname IN ('candidate','application_c' || :cen,'result_c' || :cen,'exam_cutoff')
ORDER BY relname;
