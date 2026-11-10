-- TPC-C-like workload: the five transactions as server-side functions (TPC-C 5.11, Clauses
-- 2.4.2, 2.5.2, 2.6.2, 2.7.4, 2.8.2). BenchBase's SQL is the reference for the statements.
--
-- The harness sends ONE call per transaction, on its own (not inside BEGIN ... COMMIT), so
-- PostgreSQL commits it before it reports the call finished: "the call returned" means
-- "committed", and a call cut off by a fault has an unknown outcome (proposal 6.2).
--
-- The three write transactions insert the harness's marker row FIRST, in the same transaction
-- as their TPC-C work: the marker and the TPC-C rows are saved together, or not at all
-- (docs/tpcc-workload-proposal.md 7.5). The two read-only transactions write nothing.
--
-- CREATE OR REPLACE: installed by the loader and again before every run, so a newer harness
-- brings its functions without reloading the data.

-- New-Order (Clause 2.4.2). Returns the new order number. The caller sends the lines sorted by
-- (supply warehouse, item), so stock rows are always locked in the same order (no deadlock
-- between New-Orders). An item number that does not exist raises SQLSTATE TP001: the expected
-- 1% business rollback (Clause 2.4.1.4); the whole call, marker included, is rolled back.
CREATE OR REPLACE FUNCTION resilience_tpcc.new_order(
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
    INSERT INTO resilience.markers (uuid, seq, ts) VALUES (p_uuid, p_seq, clock_timestamp());

    SELECT w_tax INTO v_w_tax FROM resilience_tpcc.warehouse WHERE w_id = p_w;

    UPDATE resilience_tpcc.district SET d_next_o_id = d_next_o_id + 1
     WHERE d_w_id = p_w AND d_id = p_d
    RETURNING d_tax, d_next_o_id - 1 INTO v_d_tax, v_o_id;

    SELECT c_discount INTO v_discount FROM resilience_tpcc.customer
     WHERE c_w_id = p_w AND c_d_id = p_d AND c_id = p_c;

    FOR i IN 1 .. v_cnt LOOP
        IF p_supply_w[i] <> p_w THEN v_all_local := 0; END IF;
    END LOOP;

    INSERT INTO resilience_tpcc.orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local)
    VALUES (v_o_id, p_d, p_w, p_c, clock_timestamp(), NULL, v_cnt, v_all_local);
    INSERT INTO resilience_tpcc.new_order (no_o_id, no_d_id, no_w_id) VALUES (v_o_id, p_d, p_w);

    FOR i IN 1 .. v_cnt LOOP
        SELECT i_price INTO v_price FROM resilience_tpcc.item WHERE i_id = p_items[i];
        IF NOT FOUND THEN
            RAISE EXCEPTION 'item % not found (TPC-C business rollback)', p_items[i] USING ERRCODE = 'TP001';
        END IF;

        UPDATE resilience_tpcc.stock
           SET s_quantity = CASE WHEN s_quantity - p_qty[i] >= 10 THEN s_quantity - p_qty[i]
                                 ELSE s_quantity - p_qty[i] + 91 END,
               s_ytd = s_ytd + p_qty[i],
               s_order_cnt = s_order_cnt + 1,
               s_remote_cnt = s_remote_cnt + CASE WHEN p_supply_w[i] <> p_w THEN 1 ELSE 0 END
         WHERE s_w_id = p_supply_w[i] AND s_i_id = p_items[i]
        RETURNING CASE p_d WHEN 1 THEN s_dist_01 WHEN 2 THEN s_dist_02 WHEN 3 THEN s_dist_03
                           WHEN 4 THEN s_dist_04 WHEN 5 THEN s_dist_05 WHEN 6 THEN s_dist_06
                           WHEN 7 THEN s_dist_07 WHEN 8 THEN s_dist_08 WHEN 9 THEN s_dist_09
                           ELSE s_dist_10 END
        INTO v_dist;

        INSERT INTO resilience_tpcc.order_line
            (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info)
        VALUES (v_o_id, p_d, p_w, i, p_items[i], p_supply_w[i], NULL, p_qty[i], p_qty[i] * v_price, v_dist);
    END LOOP;
    -- The order total, (sum of OL_AMOUNT) * (1 - C_DISCOUNT) * (1 + W_TAX + D_TAX), is only
    -- shown to the clerk (Clause 2.4.2.2); nothing stores it, so it is not computed here.
    RETURN v_o_id;
END
$$;

-- Find a customer by last name: of all customers with that name in the district, sorted by
-- first name, the one in the middle, position n/2 rounded up (Clause 2.5.2.2, 2.6.2.2).
CREATE OR REPLACE FUNCTION resilience_tpcc.customer_by_last_name(p_w int, p_d int, p_last text) RETURNS int
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    ids int[];
BEGIN
    SELECT array_agg(c_id ORDER BY c_first) INTO ids
      FROM resilience_tpcc.customer WHERE c_w_id = p_w AND c_d_id = p_d AND c_last = p_last;
    IF ids IS NULL THEN
        RAISE EXCEPTION 'no customer named % in warehouse % district %', p_last, p_w, p_d USING ERRCODE = 'TP002';
    END IF;
    RETURN ids[(array_length(ids, 1) + 1) / 2];
END
$$;

-- Payment (Clause 2.5.2). Returns the customer number that paid.
CREATE OR REPLACE FUNCTION resilience_tpcc.payment(
    p_uuid uuid, p_seq bigint, p_w int, p_d int, p_c_w int, p_c_d int,
    p_c_id int, p_c_last text, p_amount_cents int
) RETURNS int
LANGUAGE plpgsql
AS $$
DECLARE
    v_amount numeric := p_amount_cents / 100.0;
    v_w_name varchar(10);
    v_d_name varchar(10);
    v_c      int := p_c_id;
BEGIN
    INSERT INTO resilience.markers (uuid, seq, ts) VALUES (p_uuid, p_seq, clock_timestamp());

    UPDATE resilience_tpcc.warehouse SET w_ytd = w_ytd + v_amount WHERE w_id = p_w RETURNING w_name INTO v_w_name;
    UPDATE resilience_tpcc.district SET d_ytd = d_ytd + v_amount
     WHERE d_w_id = p_w AND d_id = p_d RETURNING d_name INTO v_d_name;

    IF v_c IS NULL THEN
        v_c := resilience_tpcc.customer_by_last_name(p_c_w, p_c_d, p_c_last);
    END IF;

    -- a bad-credit customer's C_DATA starts with this payment's details (Clause 2.5.2.2)
    UPDATE resilience_tpcc.customer
       SET c_balance = c_balance - v_amount,
           c_ytd_payment = c_ytd_payment + v_amount,
           c_payment_cnt = c_payment_cnt + 1,
           c_data = CASE WHEN c_credit = 'BC'
                         THEN left(format('%s %s %s %s %s %s | ', v_c, p_c_d, p_c_w, p_d, p_w, v_amount) || c_data, 500)
                         ELSE c_data END
     WHERE c_w_id = p_c_w AND c_d_id = p_c_d AND c_id = v_c;

    INSERT INTO resilience_tpcc.history (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data)
    VALUES (v_c, p_c_d, p_c_w, p_d, p_w, clock_timestamp(), v_amount, v_w_name || '    ' || v_d_name);
    RETURN v_c;
END
$$;

-- Order-Status (Clause 2.6.2), read-only. Returns the number of lines of the customer's
-- latest order (and reads them, as the clerk's screen shows them).
CREATE OR REPLACE FUNCTION resilience_tpcc.order_status(p_w int, p_d int, p_c_id int, p_c_last text) RETURNS int
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    v_c     int := p_c_id;
    v_o_id  int;
    v_lines int;
BEGIN
    IF v_c IS NULL THEN
        v_c := resilience_tpcc.customer_by_last_name(p_w, p_d, p_c_last);
    END IF;
    PERFORM c_balance, c_first, c_middle, c_last FROM resilience_tpcc.customer
      WHERE c_w_id = p_w AND c_d_id = p_d AND c_id = v_c;
    SELECT o_id INTO v_o_id FROM resilience_tpcc.orders
     WHERE o_w_id = p_w AND o_d_id = p_d AND o_c_id = v_c ORDER BY o_id DESC LIMIT 1;
    SELECT count(*) INTO v_lines FROM (
        SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM resilience_tpcc.order_line
         WHERE ol_w_id = p_w AND ol_d_id = p_d AND ol_o_id = v_o_id) AS lines;
    RETURN v_lines;
END
$$;

-- Delivery (Clause 2.7.4). For each of the 10 districts: the oldest undelivered order is
-- delivered (new_order row removed, carrier set, lines dated, the customer's balance raised by
-- the order's amount). Returns how many districts had an order to deliver; a district with
-- none is skipped (Clause 2.7.4.2). FOR UPDATE (not SKIP LOCKED): two Deliveries on the same
-- warehouse take orders strictly oldest-first, so no gap can appear among the new orders.
CREATE OR REPLACE FUNCTION resilience_tpcc.delivery(p_uuid uuid, p_seq bigint, p_w int, p_carrier int) RETURNS int
LANGUAGE plpgsql
AS $$
DECLARE
    d         int;
    v_o_id    int;
    v_c       int;
    v_total   numeric;
    delivered int := 0;
BEGIN
    INSERT INTO resilience.markers (uuid, seq, ts) VALUES (p_uuid, p_seq, clock_timestamp());

    FOR d IN 1 .. 10 LOOP
        SELECT no_o_id INTO v_o_id FROM resilience_tpcc.new_order
         WHERE no_w_id = p_w AND no_d_id = d ORDER BY no_o_id LIMIT 1 FOR UPDATE;
        CONTINUE WHEN NOT FOUND;

        DELETE FROM resilience_tpcc.new_order WHERE no_w_id = p_w AND no_d_id = d AND no_o_id = v_o_id;
        UPDATE resilience_tpcc.orders SET o_carrier_id = p_carrier
         WHERE o_w_id = p_w AND o_d_id = d AND o_id = v_o_id RETURNING o_c_id INTO v_c;
        WITH dated AS (
            UPDATE resilience_tpcc.order_line SET ol_delivery_d = clock_timestamp()
             WHERE ol_w_id = p_w AND ol_d_id = d AND ol_o_id = v_o_id RETURNING ol_amount)
        SELECT coalesce(sum(ol_amount), 0) INTO v_total FROM dated;
        UPDATE resilience_tpcc.customer SET c_balance = c_balance + v_total, c_delivery_cnt = c_delivery_cnt + 1
         WHERE c_w_id = p_w AND c_d_id = d AND c_id = v_c;
        delivered := delivered + 1;
    END LOOP;
    RETURN delivered;
END
$$;

-- Stock-Level (Clause 2.8.2), read-only. Returns how many distinct items of the district's
-- last 20 orders have a stock quantity below the threshold.
CREATE OR REPLACE FUNCTION resilience_tpcc.stock_level(p_w int, p_d int, p_threshold int) RETURNS int
LANGUAGE plpgsql STABLE
AS $$
DECLARE
    v_next int;
    v_low  int;
BEGIN
    SELECT d_next_o_id INTO v_next FROM resilience_tpcc.district WHERE d_w_id = p_w AND d_id = p_d;
    SELECT count(DISTINCT s.s_i_id) INTO v_low
      FROM resilience_tpcc.order_line ol
      JOIN resilience_tpcc.stock s ON s.s_w_id = p_w AND s.s_i_id = ol.ol_i_id
     WHERE ol.ol_w_id = p_w AND ol.ol_d_id = p_d AND ol.ol_o_id >= v_next - 20 AND ol.ol_o_id < v_next
       AND s.s_quantity < p_threshold;
    RETURN v_low;
END
$$;
