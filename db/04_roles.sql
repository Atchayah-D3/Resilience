-- Least-privilege role for the API (used through sdbPool). Run as the schema owner.
--   psql -v app_password="'secret'" -f 04_roles.sql
-- Inserts go through the partitioned parents, so no grants are needed on partitions,
-- and new hourly/daily partitions created by ensure_partitions() need no extra grants.
\set ON_ERROR_STOP on

DO $$BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rrb_app') THEN
    CREATE ROLE rrb_app LOGIN;
  END IF;
END$$;
ALTER ROLE rrb_app PASSWORD :app_password;

GRANT USAGE  ON SCHEMA rrb TO rrb_app;
GRANT SELECT ON rrb.candidate, rrb.application, rrb.result, rrb.exam_cutoff, rrb.user_session TO rrb_app;
GRANT INSERT ON rrb.user_session, rrb.audit_event TO rrb_app;
