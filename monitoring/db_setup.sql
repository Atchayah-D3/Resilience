-- Monitoring prerequisites on the PRIMARY (the replica gets the extension and role by replication).
--
-- 1. postgresql.conf on BOTH nodes, then restart (edit the file: ALTER SYSTEM does not split
--    comma-separated lists on ShaktiDB). Keep the rest of the preload list as it is:
--      shared_preload_libraries = '<existing list>, pg_stat_statements'
--      pg_stat_statements.max   = 5000
--      pg_stat_statements.track = top
--      track_io_timing          = on        -- block read/write times in pg_stat_statements and pg_stat_io
--    pg_wait_sampling (proposal 4.3) also goes in the preload list if this ShaktiDB build ships it.
--    Use the SAME settings for every run (feature on/off runs included).
--
-- 2. Then, in database rrb:   psql -d rrb -v monitor_password="'<secret>'" -f db_setup.sql
--    Run it BEFORE the app servers start: passwd_change fails password changes for roles with
--    open sessions in a database without its audit table.
\set ON_ERROR_STOP on

CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

SELECT format('CREATE ROLE rrb_monitor LOGIN PASSWORD %L', :monitor_password)
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rrb_monitor') \gexec
GRANT pg_monitor TO rrb_monitor;          -- pg_stat_*, pg_ls_logdir, other sessions' queries

-- Before every run: SELECT pg_stat_statements_reset();
SELECT current_setting('shared_preload_libraries') AS preload,
       current_setting('track_io_timing') AS track_io_timing,
       (SELECT extversion FROM pg_extension WHERE extname = 'pg_stat_statements') AS pg_stat_statements;
