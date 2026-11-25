-- RRB quick-win benchmark schema: 6 tables
--   candidate, application, result, exam_cutoff, user_session, audit_event
-- Run once on the primary as the schema owner. Primary keys and secondary
-- indexes are added AFTER the bulk load (03_finalize.sql) to keep the load fast.
\set ON_ERROR_STOP on

CREATE SCHEMA IF NOT EXISTS rrb;
SET search_path = rrb;

-------------------------------------------------------------------------------
-- Deterministic data rules. The Zipf CSV generator (load/gen_zipf_csv.py)
-- re-implements registration_no() and dob() so the load keys match the DB
-- without exporting 1.5 crore rows. Change them here => change them there.
-------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION rrb.registration_no(id bigint) RETURNS text
  LANGUAGE sql IMMUTABLE PARALLEL SAFE
  RETURN 'RRB' || lpad(id::text, 10, '0');

CREATE OR REPLACE FUNCTION rrb.dob(id bigint) RETURNS date
  LANGUAGE sql IMMUTABLE PARALLEL SAFE
  RETURN date '1990-01-01' + ((id * 7919) % 5844)::int;      -- 1990-01-01 .. 2005-12-31

CREATE OR REPLACE FUNCTION rrb.category(id bigint) RETURNS text
  LANGUAGE sql IMMUTABLE PARALLEL SAFE
  RETURN CASE WHEN (id * 31) % 1000 < 400 THEN 'UR'
              WHEN (id * 31) % 1000 < 670 THEN 'OBC'
              WHEN (id * 31) % 1000 < 820 THEN 'SC'
              WHEN (id * 31) % 1000 < 895 THEN 'ST'
              ELSE 'EWS' END;

CREATE OR REPLACE FUNCTION rrb.rrb_code(id bigint) RETURNS smallint
  LANGUAGE sql IMMUTABLE PARALLEL SAFE
  RETURN (1 + (id * 17) % 21)::smallint;                    -- 21 regional RRBs

-- Cut-off per (RRB, category); result status is derived from the same rule so
-- the result table is consistent with exam_cutoff without a post-load UPDATE.
CREATE OR REPLACE FUNCTION rrb.cutoff_for(p_rrb smallint, p_cat text) RETURNS numeric
  LANGUAGE sql IMMUTABLE PARALLEL SAFE
  RETURN (CASE p_cat WHEN 'UR' THEN 70 WHEN 'EWS' THEN 65 WHEN 'OBC' THEN 62
                     WHEN 'SC' THEN 55 ELSE 50 END) + p_rrb * 0.5;

-------------------------------------------------------------------------------
-- 1. candidate (one-time registration). Login = registration_no + dob.
-------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS candidate (
    candidate_id     bigint       NOT NULL,
    registration_no  varchar(16)  NOT NULL,
    dob              date         NOT NULL,
    full_name        varchar(80)  NOT NULL,
    gender           char(1)      NOT NULL,
    category         varchar(4)   NOT NULL,
    state_code       char(2)      NOT NULL,
    mobile           char(10)     NOT NULL,
    email            varchar(80)  NOT NULL,
    created_at       timestamptz  NOT NULL
);

-------------------------------------------------------------------------------
-- 2. application: one per candidate per notification (CEN). LIST by cen_id.
-------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS application (
    application_id   bigint       NOT NULL,
    cen_id           int          NOT NULL,
    candidate_id     bigint       NOT NULL,
    rrb_code         smallint     NOT NULL,
    post_code        varchar(8)   NOT NULL,
    status           varchar(12)  NOT NULL,
    submitted_at     timestamptz  NOT NULL
) PARTITION BY LIST (cen_id);

-------------------------------------------------------------------------------
-- 3. result: one per candidate per CEN, immutable after publication.
-------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS result (
    cen_id           int          NOT NULL,
    candidate_id     bigint       NOT NULL,
    application_id   bigint       NOT NULL,
    roll_no          varchar(20)  NOT NULL,
    rrb_code         smallint     NOT NULL,
    category         varchar(4)   NOT NULL,
    raw_score        numeric(6,2),
    normalized_score numeric(7,3),
    status           varchar(14)  NOT NULL,       -- QUALIFIED | NOT_QUALIFIED | ABSENT
    published_at     timestamptz  NOT NULL
) PARTITION BY LIST (cen_id);

-------------------------------------------------------------------------------
-- 4. exam_cutoff: small (21 RRBs x 5 categories per CEN), cached in the app.
-------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS exam_cutoff (
    cen_id           int          NOT NULL,
    rrb_code         smallint     NOT NULL,
    category         varchar(4)   NOT NULL,
    cutoff_score     numeric(7,3) NOT NULL,
    vacancies        int          NOT NULL,
    published_at     timestamptz  NOT NULL,
    PRIMARY KEY (cen_id, rrb_code, category)
);

-------------------------------------------------------------------------------
-- 5. user_session: written on every login. RANGE by created_at, hourly;
--    expiry = DROP PARTITION (no delete bloat). PK leads with created_at so
--    inserts are append-mostly and token lookups prune to one partition.
-------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS user_session (
    session_id       uuid         NOT NULL,
    candidate_id     bigint       NOT NULL,
    cen_id           int          NOT NULL,
    created_at       timestamptz  NOT NULL,
    expires_at       timestamptz  NOT NULL,
    client_ip        inet,
    user_agent       varchar(256),
    PRIMARY KEY (created_at, session_id)
) PARTITION BY RANGE (created_at);

-------------------------------------------------------------------------------
-- 6. audit_event: application-level audit row per login. RANGE daily,
--    append-only, archived by DETACH. (sdbAudit, if enabled, is separate.)
-------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit_event (
    audit_id         bigint GENERATED ALWAYS AS IDENTITY (CACHE 1000),
    event_time       timestamptz  NOT NULL,
    event_type       varchar(16)  NOT NULL,       -- LOGIN
    candidate_id     bigint,
    session_id       uuid,
    client_ip        inet,
    outcome          varchar(8)   NOT NULL,       -- SUCCESS | FAIL
    detail           jsonb
) PARTITION BY RANGE (event_time);

CREATE INDEX IF NOT EXISTS audit_event_candidate_idx ON audit_event (candidate_id, event_time);

-- Safety nets so a missed ensure_partitions() run degrades instead of failing logins.
-- check_partitions() in 02 reports any rows that land here.
CREATE TABLE IF NOT EXISTS user_session_default PARTITION OF user_session DEFAULT;
CREATE TABLE IF NOT EXISTS audit_event_default  PARTITION OF audit_event  DEFAULT;
