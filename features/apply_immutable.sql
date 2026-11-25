-- Makes rrb.result immutable with sdbImmutable ("tamper-proof results", proposal 1.2).
-- Run ONCE on the PRIMARY of the "features on" dataset, AFTER load_data.sh has finished
-- (data loaded, indexes built, VACUUM ANALYZE done). The replica receives it via replication.
--
--   psql -X -v ON_ERROR_STOP=1 -d rrb -f apply_immutable.sql
--
-- What it does (verified on ShaktiDB 18.6.1.0):
--   * Marks the partitioned parent; every existing AND future partition becomes immutable.
--     (Marking a single partition is refused.)
--   * Adds two system columns (imtcrby, imtcrdt). They use constant defaults, so the table
--     is NOT rewritten: marking takes milliseconds even on 1.5 crore rows.
--   * After marking, UPDATE, DELETE, TRUNCATE, DROP TABLE and DROP SCHEMA rrb all fail.
--     INSERT and SELECT work. Result reads keep the same PK index scan.
--
-- THIS CANNOT BE UNDONE. To rebuild an immutable dataset you must DROP DATABASE rrb
-- (load_data.sh RESET=1 drops schema rrb, which is refused) or restore a snapshot.
-- Keep the "features off" dataset in a separate database or snapshot that is never marked.
--
-- no_drop_days / no_delete_days = 0 means "never allowed" (checked: DELETE is refused with
-- "DELETE permanently disabled due to retention settings").

\set ON_ERROR_STOP on

CREATE EXTENSION IF NOT EXISTS sdb_immutable;

SELECT sdb_immutable.mark_table('rrb.result'::regclass, 0, 0);

-- Verify: parent and every partition must report true.
SELECT c.oid::regclass AS table_name, sdb_immutable.is_immutable(c.oid::regclass) AS immutable
FROM pg_class c
WHERE c.oid = 'rrb.result'::regclass
   OR c.oid IN (SELECT inhrelid FROM pg_inherits WHERE inhparent = 'rrb.result'::regclass)
ORDER BY 1;
