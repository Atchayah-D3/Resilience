-- Prints the security-feature state of the database you are connected to (run it in rrb).
-- Attach the output to every run: the feature-cost comparison is only valid if the two
-- runs differ in exactly the features under test. The analysis sampler records the same
-- JSON automatically at the start of each run.
--
--   psql -X -At -f feature_status.sql            # one JSON document
--
-- tde_tables / plain_tables count the rrb tables per access method: on a TDE dataset every
-- rrb table should use the encrypted access method (tde_heap or what sdbTDE calls it).
SELECT jsonb_pretty(jsonb_build_object(
  'server_version',            current_setting('server_version'),
  'shared_preload_libraries',  current_setting('shared_preload_libraries'),
  'sdbaudit_loaded',           current_setting('shared_preload_libraries') ~* '\msdbaudit\M',
  'sdbaudit_log',              current_setting('sdbaudit.log', true),
  'sdbaudit_log_parameter',    current_setting('sdbaudit.log_parameter', true),
  'sdbaudit_log_relation',     current_setting('sdbaudit.log_relation', true),
  'sdbaudit_role',             current_setting('sdbaudit.role', true),
  'extensions',                (SELECT jsonb_object_agg(extname, extversion) FROM pg_extension),
  'tde_extension',             (SELECT string_agg(extname, ',') FROM pg_extension WHERE extname ~* 'tde'),
  'table_access_methods',      (SELECT jsonb_object_agg(am, n) FROM (
                                  SELECT coalesce(a.amname, '(none)') AS am, count(*) AS n
                                  FROM pg_class c
                                  JOIN pg_namespace s ON s.oid = c.relnamespace AND s.nspname = 'rrb'
                                  LEFT JOIN pg_am a ON a.oid = c.relam
                                  WHERE c.relkind IN ('r', 'p')
                                  GROUP BY 1) x),
  -- sdb_immutable.mark_table() adds the system columns imtcrby/imtcrdt to every table it
  -- protects (parent and partitions). Checking the columns works even where the extension
  -- was never installed, which is exactly the "off" dataset.
  'immutable_tables',          (SELECT coalesce(jsonb_agg(t ORDER BY t), '[]'::jsonb) FROM (
                                  SELECT c.oid::regclass::text AS t
                                  FROM pg_class c
                                  JOIN pg_namespace s ON s.oid = c.relnamespace AND s.nspname = 'rrb'
                                  WHERE c.relkind = 'p' OR (c.relkind = 'r' AND NOT c.relispartition)
                                  GROUP BY c.oid
                                  HAVING EXISTS (SELECT 1 FROM pg_attribute a
                                                 WHERE a.attrelid = c.oid AND a.attname = 'imtcrdt'
                                                   AND NOT a.attisdropped)) x),
  'pg_stat_statements',        EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_stat_statements'),
  'synchronous_standby_names', current_setting('synchronous_standby_names'),
  'synchronous_commit',        current_setting('synchronous_commit')
));
