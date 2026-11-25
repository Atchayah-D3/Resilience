-- Loads candidate, application and result rows for ids [:lo, :hi].
-- Called by load_data.sh once per chunk, several chunks in parallel:
--   psql -v lo=1 -v hi=500000 -v cen=202501 -f load_chunk.sql
\set ON_ERROR_STOP on
SET synchronous_commit = off;
SET search_path = rrb;

BEGIN;

INSERT INTO candidate (candidate_id, registration_no, dob, full_name, gender,
                       category, state_code, mobile, email, created_at)
SELECT g,
       registration_no(g),
       dob(g),
       (ARRAY['Aarav','Vivaan','Aditya','Arjun','Sai','Rohan','Karthik','Manoj','Suresh','Rahul',
              'Priya','Ananya','Divya','Kavya','Lakshmi','Meena','Pooja','Sneha','Deepa','Nisha'])[1 + (g * 7) % 20]
         || ' ' ||
       (ARRAY['Kumar','Sharma','Singh','Yadav','Reddy','Naidu','Patel','Das','Nair','Iyer',
              'Gupta','Verma','Mishra','Pandey','Rao','Pillai','Mehta','Joshi','Ghosh','Murugan'])[1 + (g * 11) % 20],
       CASE WHEN (g * 13) % 100 < 62 THEN 'M' ELSE 'F' END,
       category(g),
       (ARRAY['AP','AR','AS','BR','CG','GA','GJ','HR','HP','JH','KA','KL','MP','MH','MN','ML','MZ','NL',
              'OD','PB','RJ','SK','TN','TS','TR','UP','UK','WB','AN','CH','DN','DL','JK','LA','LD','PY'])[1 + g % 36],
       (6000000000 + (g * 104729) % 3999999999)::text,
       'cand' || g || '@example.in',
       timestamptz '2025-01-01 00:00+05:30' + ((g % 5000000) * interval '1 second')
FROM generate_series(:lo::bigint, :hi::bigint) AS g;

INSERT INTO application (application_id, cen_id, candidate_id, rrb_code, post_code, status, submitted_at)
SELECT g, :cen, g, rrb_code(g), 'P' || (1 + g % 12), 'ACCEPTED',
       timestamptz '2025-03-01 00:00+05:30' + ((g % 2500000) * interval '1 second')
FROM generate_series(:lo::bigint, :hi::bigint) AS g;

-- ~15% absent; scores 0..100; status consistent with cutoff_for().
INSERT INTO result (cen_id, candidate_id, application_id, roll_no, rrb_code, category,
                    raw_score, normalized_score, status, published_at)
SELECT :cen, g, g, :cen || lpad(g::text, 10, '0'), rrb_code(g), category(g),
       s.raw, s.norm,
       CASE WHEN s.raw IS NULL                               THEN 'ABSENT'
            WHEN s.norm >= cutoff_for(rrb_code(g), category(g)) THEN 'QUALIFIED'
            ELSE 'NOT_QUALIFIED' END,
       timestamptz '2026-01-15 10:00+05:30'
FROM generate_series(:lo::bigint, :hi::bigint) AS g
CROSS JOIN LATERAL (
    SELECT CASE WHEN (g * 37) % 100 < 15 THEN NULL ELSE ((g * 7907) % 10001) / 100.0 END::numeric(6,2) AS raw
) r
CROSS JOIN LATERAL (
    SELECT r.raw, round(r.raw * (0.95 + (g % 11) / 100.0), 3)::numeric(7,3) AS norm
) s;

COMMIT;
