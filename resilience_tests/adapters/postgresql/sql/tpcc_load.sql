-- TPC-C-like workload: the loader, run inside the database (TPC-C 5.11, Clause 4.3.3.1).
-- BenchBase's loader is the reference for the population rules.
--
-- load_items() once, then load_warehouse(w, c) once per warehouse, each in its own transaction
-- (warehouses can load in parallel). Random strings are hex text built from md5(): the
-- lengths follow the Clause; the character set is simpler than "random alphanumeric", which
-- changes no rule and no measurement.

CREATE FUNCTION resilience_tpcc.rnd_int(lo int, hi int) RETURNS int
LANGUAGE sql VOLATILE
AS $$ SELECT lo + floor(random() * (hi - lo + 1))::int $$;

-- random text of a random length in [lo, hi] (at most 512 characters)
CREATE FUNCTION resilience_tpcc.rnd_str(lo int, hi int) RETURNS text
LANGUAGE sql VOLATILE
AS $$ SELECT left(repeat(md5(random()::text), 16), resilience_tpcc.rnd_int(lo, hi)) $$;

-- I_DATA / S_DATA: [lo, hi] characters; 10% contain "ORIGINAL" at a random place (Clause 4.3.3.1)
CREATE FUNCTION resilience_tpcc.rnd_data(lo int, hi int) RETURNS text
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    s text := resilience_tpcc.rnd_str(lo, hi);
BEGIN
    IF random() < 0.1 THEN
        s := overlay(s PLACING 'ORIGINAL' FROM resilience_tpcc.rnd_int(1, length(s) - 7) FOR 8);
    END IF;
    RETURN s;
END
$$;

-- C_LAST from a number 0..999: three syllables (Clause 4.3.2.3)
CREATE FUNCTION resilience_tpcc.last_name(n int) RETURNS text
LANGUAGE sql IMMUTABLE
AS $$
    SELECT a[n / 100 + 1] || a[(n / 10) % 10 + 1] || a[n % 10 + 1]
    FROM (SELECT ARRAY['BAR','OUGHT','ABLE','PRI','PRES','ESE','ANTI','CALLY','ATION','EING'] AS a) syllables
$$;

-- NURand(A, x, y) with run constant C (Clause 2.1.6)
CREATE FUNCTION resilience_tpcc.nurand(a int, x int, y int, c int) RETURNS int
LANGUAGE sql VOLATILE
AS $$ SELECT (((resilience_tpcc.rnd_int(0, a) | resilience_tpcc.rnd_int(x, y)) + c) % (y - x + 1)) + x $$;

-- zip code: 4 random digits followed by "11111" (Clause 4.3.2.7)
CREATE FUNCTION resilience_tpcc.rnd_zip() RETURNS text
LANGUAGE sql VOLATILE
AS $$ SELECT lpad(resilience_tpcc.rnd_int(0, 9999)::text, 4, '0') || '11111' $$;

CREATE FUNCTION resilience_tpcc.rnd_state() RETURNS text
LANGUAGE sql VOLATILE
AS $$ SELECT chr(65 + resilience_tpcc.rnd_int(0, 25)) || chr(65 + resilience_tpcc.rnd_int(0, 25)) $$;

-- ITEM: 100,000 rows (Clause 4.3.3.1)
CREATE FUNCTION resilience_tpcc.load_items(p_items int) RETURNS void
LANGUAGE sql VOLATILE
AS $$
    INSERT INTO resilience_tpcc.item (i_id, i_im_id, i_name, i_price, i_data)
    SELECT i, resilience_tpcc.rnd_int(1, 10000), resilience_tpcc.rnd_str(14, 24),
           round((1 + random() * 99)::numeric, 2), resilience_tpcc.rnd_data(26, 50)
    FROM generate_series(1, p_items) AS i;
$$;

-- One warehouse with its 10 districts, 30,000 customers and history rows, 30,000 orders (the
-- last 900 per district undelivered, with a new_order row each), their order lines, and
-- 100,000 stock rows (Clause 4.3.3.1).
CREATE FUNCTION resilience_tpcc.load_warehouse(p_w int, p_items int, p_c_last int) RETURNS void
LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    t timestamp := clock_timestamp();
BEGIN
    INSERT INTO resilience_tpcc.warehouse
    VALUES (p_w, resilience_tpcc.rnd_str(6, 10), resilience_tpcc.rnd_str(10, 20), resilience_tpcc.rnd_str(10, 20),
            resilience_tpcc.rnd_str(10, 20), resilience_tpcc.rnd_state(), resilience_tpcc.rnd_zip(),
            round((random() * 0.2)::numeric, 4), 300000.00);

    INSERT INTO resilience_tpcc.district
    SELECT d, p_w, resilience_tpcc.rnd_str(6, 10), resilience_tpcc.rnd_str(10, 20), resilience_tpcc.rnd_str(10, 20),
           resilience_tpcc.rnd_str(10, 20), resilience_tpcc.rnd_state(), resilience_tpcc.rnd_zip(),
           round((random() * 0.2)::numeric, 4), 30000.00, 3001
    FROM generate_series(1, 10) AS d;

    -- C_LAST: the first 1,000 customers of a district get the names of 0..999 in order; the
    -- others NURand(255, 0, 999) with the load constant (Clause 4.3.3.1)
    INSERT INTO resilience_tpcc.customer
    SELECT c, d, p_w, resilience_tpcc.rnd_str(8, 16), 'OE',
           CASE WHEN c <= 1000 THEN resilience_tpcc.last_name(c - 1)
                ELSE resilience_tpcc.last_name(resilience_tpcc.nurand(255, 0, 999, p_c_last)) END,
           resilience_tpcc.rnd_str(10, 20), resilience_tpcc.rnd_str(10, 20), resilience_tpcc.rnd_str(10, 20),
           resilience_tpcc.rnd_state(), resilience_tpcc.rnd_zip(),
           lpad((floor(random() * 1e16))::bigint::text, 16, '0'), t,
           CASE WHEN random() < 0.1 THEN 'BC' ELSE 'GC' END, 50000.00,
           round((random() * 0.5)::numeric, 4), -10.00, 10.00, 1, 0, resilience_tpcc.rnd_str(300, 500)
    FROM generate_series(1, 10) AS d, generate_series(1, 3000) AS c;

    INSERT INTO resilience_tpcc.history
    SELECT c, d, p_w, d, p_w, t, 10.00, resilience_tpcc.rnd_str(12, 24)
    FROM generate_series(1, 10) AS d, generate_series(1, 3000) AS c;

    -- ORDERS: customer ids are a random permutation of 1..3000 per district; orders below 2101
    -- are delivered (carrier set, lines with delivery date and amount 0), the rest are new.
    -- One statement, so the lines and the new_order rows are made from exactly these orders.
    WITH o AS (
        INSERT INTO resilience_tpcc.orders
        SELECT o_id, d, p_w, c_id, t,
               CASE WHEN o_id < 2101 THEN resilience_tpcc.rnd_int(1, 10) END,
               resilience_tpcc.rnd_int(5, 15), 1
        FROM (SELECT d, c AS c_id, row_number() OVER (PARTITION BY d ORDER BY random())::int AS o_id
              FROM generate_series(1, 10) AS d, generate_series(1, 3000) AS c) AS permuted
        RETURNING o_id, o_d_id, o_w_id, o_entry_d, o_ol_cnt
    ), n AS (
        INSERT INTO resilience_tpcc.new_order
        SELECT o_id, o_d_id, o_w_id FROM o WHERE o_id >= 2101
    )
    INSERT INTO resilience_tpcc.order_line
    SELECT o.o_id, o.o_d_id, o.o_w_id, line, resilience_tpcc.rnd_int(1, p_items), p_w,
           CASE WHEN o.o_id < 2101 THEN o.o_entry_d END, 5,
           CASE WHEN o.o_id < 2101 THEN 0.00 ELSE round((0.01 + random() * 9999.98)::numeric, 2) END,
           resilience_tpcc.rnd_str(24, 24)
    FROM o, generate_series(1, o.o_ol_cnt) AS line;

    INSERT INTO resilience_tpcc.stock
    SELECT i, p_w, resilience_tpcc.rnd_int(10, 100),
           resilience_tpcc.rnd_str(24, 24), resilience_tpcc.rnd_str(24, 24), resilience_tpcc.rnd_str(24, 24),
           resilience_tpcc.rnd_str(24, 24), resilience_tpcc.rnd_str(24, 24), resilience_tpcc.rnd_str(24, 24),
           resilience_tpcc.rnd_str(24, 24), resilience_tpcc.rnd_str(24, 24), resilience_tpcc.rnd_str(24, 24),
           resilience_tpcc.rnd_str(24, 24), 0, 0, 0, resilience_tpcc.rnd_data(26, 50)
    FROM generate_series(1, p_items) AS i;
END
$$;
