-- Partition maintenance for the two write-hot tables.
-- Call rrb.ensure_partitions() before every run so the test window never
-- writes into the DEFAULT partitions:
--     SELECT rrb.ensure_partitions(now(), 48);   -- next 48 hours
\set ON_ERROR_STOP on

CREATE OR REPLACE FUNCTION rrb.ensure_partitions(
        p_from  timestamptz DEFAULT now(),
        p_hours int         DEFAULT 48)
RETURNS int LANGUAGE plpgsql AS $$
DECLARE
    h        timestamptz;
    d        date;
    created  int := 0;
    name     text;
BEGIN
    -- hourly user_session partitions (named in UTC)
    FOR i IN 0 .. p_hours - 1 LOOP
        h := date_bin('1 hour', p_from, timestamptz '2000-01-01 00:00+00') + make_interval(hours => i);
        name := 'user_session_' || to_char(h AT TIME ZONE 'UTC', 'YYYYMMDD"h"HH24');
        IF to_regclass('rrb.' || name) IS NULL THEN
            EXECUTE format('CREATE TABLE rrb.%I PARTITION OF rrb.user_session FOR VALUES FROM (%L) TO (%L)',
                           name, h, h + interval '1 hour');
            created := created + 1;
        END IF;
    END LOOP;

    -- daily audit_event partitions (UTC days)
    FOR d IN SELECT generate_series((p_from AT TIME ZONE 'UTC')::date,
                                    ((p_from + make_interval(hours => p_hours)) AT TIME ZONE 'UTC')::date,
                                    interval '1 day')::date LOOP
        name := 'audit_event_' || to_char(d, 'YYYYMMDD');
        IF to_regclass('rrb.' || name) IS NULL THEN
            EXECUTE format('CREATE TABLE rrb.%I PARTITION OF rrb.audit_event FOR VALUES FROM (%L) TO (%L)',
                           name, d::timestamp AT TIME ZONE 'UTC', (d + 1)::timestamp AT TIME ZONE 'UTC');
            created := created + 1;
        END IF;
    END LOOP;
    RETURN created;
END $$;

-- Drop user_session partitions older than p_keep (sessions are 30 min tokens).
CREATE OR REPLACE FUNCTION rrb.drop_old_session_partitions(p_keep interval DEFAULT '24 hours')
RETURNS int LANGUAGE plpgsql AS $$
DECLARE r record; dropped int := 0;
BEGIN
    FOR r IN
        SELECT c.relname
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        WHERE i.inhparent = 'rrb.user_session'::regclass
          AND c.relname ~ '^user_session_\d{8}h\d{2}$'
          AND to_timestamp(substr(c.relname, 14), 'YYYYMMDD"h"HH24')::timestamp AT TIME ZONE 'UTC'
              < now() - p_keep
    LOOP
        EXECUTE format('DROP TABLE rrb.%I', r.relname);
        dropped := dropped + 1;
    END LOOP;
    RETURN dropped;
END $$;

-- Post-run check: rows in DEFAULT partitions mean ensure_partitions() was not run
-- for the test window (results are still valid, but partition pruning was lost).
CREATE OR REPLACE VIEW rrb.check_partitions AS
SELECT 'user_session_default' AS partition, count(*) AS rows FROM rrb.user_session_default
UNION ALL
SELECT 'audit_event_default', count(*) FROM rrb.audit_event_default;
