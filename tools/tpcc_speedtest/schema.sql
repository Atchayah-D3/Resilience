-- TPC-C speed test (build step 0, docs/tpcc-build-plan.md).
--
-- A cut-down TPC-C data set and ONE server-side function that does the work of a TPC-C
-- New-Order (Clause 2.4.2) plus the harness's marker row. The speed test calls it once per
-- transaction to measure how many transactions one Python process can drive.
--
-- Everything lives in schema resilience_speedtest, owned by the harness role, and is dropped
-- by the teardown command: the target is left as found. No server or role setting is changed.

CREATE SCHEMA resilience_speedtest;

CREATE TABLE resilience_speedtest.meta (
    warehouses int NOT NULL,
    items      int NOT NULL,
    loaded_at  timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE resilience_speedtest.warehouse (
    w_id  int PRIMARY KEY,
    w_tax numeric(4,4) NOT NULL,
    w_ytd numeric(12,2) NOT NULL
);

CREATE TABLE resilience_speedtest.district (
    d_w_id      int NOT NULL,
    d_id        int NOT NULL,
    d_tax       numeric(4,4) NOT NULL,
    d_next_o_id int NOT NULL,
    PRIMARY KEY (d_w_id, d_id)
);

CREATE TABLE resilience_speedtest.customer (
    c_w_id     int NOT NULL,
    c_d_id     int NOT NULL,
    c_id       int NOT NULL,
    c_discount numeric(4,4) NOT NULL,
    c_last     varchar(16) NOT NULL,
    c_credit   char(2) NOT NULL,
    PRIMARY KEY (c_w_id, c_d_id, c_id)
);

CREATE TABLE resilience_speedtest.item (
    i_id    int PRIMARY KEY,
    i_price numeric(5,2) NOT NULL,
    i_name  varchar(24) NOT NULL,
    i_data  varchar(50) NOT NULL
);

CREATE TABLE resilience_speedtest.stock (
    s_w_id       int NOT NULL,
    s_i_id       int NOT NULL,
    s_quantity   int NOT NULL,
    s_ytd        int NOT NULL DEFAULT 0,
    s_order_cnt  int NOT NULL DEFAULT 0,
    s_remote_cnt int NOT NULL DEFAULT 0,
    s_dist_info  char(24) NOT NULL,
    s_data       varchar(50) NOT NULL,
    PRIMARY KEY (s_w_id, s_i_id)
);

CREATE TABLE resilience_speedtest.orders (
    o_w_id      int NOT NULL,
    o_d_id      int NOT NULL,
    o_id        int NOT NULL,
    o_c_id      int NOT NULL,
    o_entry_d   timestamptz NOT NULL,
    o_ol_cnt    int NOT NULL,
    o_all_local int NOT NULL,
    PRIMARY KEY (o_w_id, o_d_id, o_id)
);

CREATE TABLE resilience_speedtest.new_order (
    no_w_id int NOT NULL,
    no_d_id int NOT NULL,
    no_o_id int NOT NULL,
    PRIMARY KEY (no_w_id, no_d_id, no_o_id)
);

CREATE TABLE resilience_speedtest.order_line (
    ol_w_id        int NOT NULL,
    ol_d_id        int NOT NULL,
    ol_o_id        int NOT NULL,
    ol_number      int NOT NULL,
    ol_i_id        int NOT NULL,
    ol_supply_w_id int NOT NULL,
    ol_quantity    int NOT NULL,
    ol_amount      numeric(6,2) NOT NULL,
    ol_dist_info   char(24) NOT NULL,
    PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number)
);

-- The marker row of every write transaction, as resilience.markers in the harness.
CREATE TABLE resilience_speedtest.markers (
    uuid uuid PRIMARY KEY,
    seq  bigint NOT NULL,
    ts   timestamptz NOT NULL DEFAULT clock_timestamp()
);

-- Loads the data inside the database. Not a full TPC-C population (Clause 4.3): only the
-- tables and columns New-Order touches, which is all the speed test needs.
CREATE FUNCTION resilience_speedtest.load(p_warehouses int, p_items int) RETURNS void
LANGUAGE plpgsql
SET search_path = resilience_speedtest, pg_catalog
AS $$
BEGIN
    INSERT INTO item
    SELECT i, round((1 + random() * 99)::numeric, 2), 'item-' || i, repeat('x', 30)
    FROM generate_series(1, p_items) AS i;

    INSERT INTO warehouse
    SELECT w, round((random() * 0.2)::numeric, 4), 300000
    FROM generate_series(1, p_warehouses) AS w;

    -- 3,000 orders per district already exist in a loaded TPC-C database (Clause 4.3.3.1)
    INSERT INTO district
    SELECT w, d, round((random() * 0.2)::numeric, 4), 3001
    FROM generate_series(1, p_warehouses) AS w, generate_series(1, 10) AS d;

    INSERT INTO customer
    SELECT w, d, c, round((random() * 0.5)::numeric, 4), 'NAME' || (c % 1000),
           CASE WHEN random() < 0.1 THEN 'BC' ELSE 'GC' END
    FROM generate_series(1, p_warehouses) AS w, generate_series(1, 10) AS d,
         generate_series(1, 3000) AS c;

    INSERT INTO stock (s_w_id, s_i_id, s_quantity, s_dist_info, s_data)
    SELECT w, i, 10 + (random() * 90)::int, repeat('d', 24), repeat('s', 30)
    FROM generate_series(1, p_warehouses) AS w, generate_series(1, p_items) AS i;

    INSERT INTO meta (warehouses, items) VALUES (p_warehouses, p_items);
END
$$;

-- One New-Order (Clause 2.4.2) plus the marker row, in one call. Called on its own (not
-- inside BEGIN ... COMMIT), so PostgreSQL commits it before it reports the call finished.
-- The caller sends the lines sorted by (supply warehouse, item), so two New-Orders always
-- lock stock rows in the same order and cannot deadlock (Clause 3.5.2, comment 2).
-- An unknown item raises SQLSTATE TP001: the 1% business rollback of Clause 2.4.1.4; the
-- whole call, marker row included, is rolled back.
-- Names are schema-qualified rather than set with a SET clause: a SET clause would add a
-- setting save and restore to every call, which the real functions will not have.
CREATE FUNCTION resilience_speedtest.new_order(
    p_uuid uuid, p_seq bigint, p_w int, p_d int, p_c int,
    p_items int[], p_supply_w int[], p_qty int[]
) RETURNS int
LANGUAGE plpgsql
AS $$
DECLARE
    v_cnt       int := array_length(p_items, 1);
    v_all_local int := 1;
    v_w_tax     numeric;
    v_d_tax     numeric;
    v_o_id      int;
    v_discount  numeric;
    v_price     numeric;
    v_dist      char(24);
    i           int;
BEGIN
    INSERT INTO resilience_speedtest.markers (uuid, seq) VALUES (p_uuid, p_seq);

    SELECT w_tax INTO v_w_tax FROM resilience_speedtest.warehouse WHERE w_id = p_w;

    UPDATE resilience_speedtest.district
       SET d_next_o_id = d_next_o_id + 1
     WHERE d_w_id = p_w AND d_id = p_d
    RETURNING d_tax, d_next_o_id - 1 INTO v_d_tax, v_o_id;

    SELECT c_discount INTO v_discount
      FROM resilience_speedtest.customer
     WHERE c_w_id = p_w AND c_d_id = p_d AND c_id = p_c;

    FOR i IN 1 .. v_cnt LOOP
        IF p_supply_w[i] <> p_w THEN
            v_all_local := 0;
        END IF;
    END LOOP;

    INSERT INTO resilience_speedtest.orders
    VALUES (p_w, p_d, v_o_id, p_c, clock_timestamp(), v_cnt, v_all_local);

    INSERT INTO resilience_speedtest.new_order VALUES (p_w, p_d, v_o_id);

    FOR i IN 1 .. v_cnt LOOP
        SELECT i_price INTO v_price FROM resilience_speedtest.item WHERE i_id = p_items[i];
        IF NOT FOUND THEN
            RAISE EXCEPTION 'item % not found (business rollback)', p_items[i]
                USING ERRCODE = 'TP001';
        END IF;

        UPDATE resilience_speedtest.stock
           SET s_quantity = CASE WHEN s_quantity - p_qty[i] >= 10
                                 THEN s_quantity - p_qty[i]
                                 ELSE s_quantity - p_qty[i] + 91 END,
               s_ytd = s_ytd + p_qty[i],
               s_order_cnt = s_order_cnt + 1,
               s_remote_cnt = s_remote_cnt + CASE WHEN p_supply_w[i] <> p_w THEN 1 ELSE 0 END
         WHERE s_w_id = p_supply_w[i] AND s_i_id = p_items[i]
        RETURNING s_dist_info INTO v_dist;

        INSERT INTO resilience_speedtest.order_line
        VALUES (p_w, p_d, v_o_id, i, p_items[i], p_supply_w[i], p_qty[i],
                p_qty[i] * v_price * (1 + v_w_tax + v_d_tax) * (1 - v_discount), v_dist);
    END LOOP;

    RETURN v_o_id;
END
$$;
