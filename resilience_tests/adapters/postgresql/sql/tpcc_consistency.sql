-- TPC-C-like workload: the consistency conditions (TPC-C 5.11, Clause 3.3.2.1-3.3.2.12).
--
-- consistency(rule, w[], d[]) returns how many warehouses / districts / orders / order lines /
-- customers break the rule: 0 = the rule holds.
--   w[], d[]  a sample of districts (pairs w[i], d[i]); NULL = every district.
--             Rules 1-3 always check everything (they read only small tables or indexes).
--             The others look only at the sampled districts (rule 8: their warehouses), so a
--             check before a run stays fast on a large data set.
-- Rule 11 holds only right after loading (Clause 3.3.2.11): it is not used after a run.

CREATE FUNCTION resilience_tpcc.consistency(p_rule int, p_w int[] DEFAULT NULL, p_d int[] DEFAULT NULL)
RETURNS bigint
LANGUAGE plpgsql VOLATILE   -- it fills a temporary table with the sample
AS $$
DECLARE
    bad bigint;
BEGIN
    CREATE TEMP TABLE IF NOT EXISTS tpcc_sel (w int, d int) ON COMMIT DROP;
    TRUNCATE tpcc_sel;
    IF p_w IS NULL THEN
        INSERT INTO tpcc_sel SELECT d_w_id, d_id FROM resilience_tpcc.district;
    ELSE
        INSERT INTO tpcc_sel SELECT * FROM unnest(p_w, p_d);
    END IF;

    CASE p_rule
    WHEN 1 THEN   -- W_YTD = sum(D_YTD)
        SELECT count(*) INTO bad
          FROM resilience_tpcc.warehouse w
          JOIN (SELECT d_w_id, sum(d_ytd) AS s FROM resilience_tpcc.district GROUP BY d_w_id) x ON x.d_w_id = w.w_id
         WHERE w.w_ytd <> x.s;

    WHEN 2 THEN   -- D_NEXT_O_ID - 1 = max(O_ID) = max(NO_O_ID)
        SELECT count(*) INTO bad
          FROM resilience_tpcc.district dd
          CROSS JOIN LATERAL (SELECT max(o_id) AS mo FROM resilience_tpcc.orders
                               WHERE o_w_id = dd.d_w_id AND o_d_id = dd.d_id) o
          CROSS JOIN LATERAL (SELECT max(no_o_id) AS mn FROM resilience_tpcc.new_order
                               WHERE no_w_id = dd.d_w_id AND no_d_id = dd.d_id) n
         WHERE (dd.d_next_o_id - 1) IS DISTINCT FROM o.mo
            OR (n.mn IS NOT NULL AND n.mn <> dd.d_next_o_id - 1);

    WHEN 3 THEN   -- max(NO_O_ID) - min(NO_O_ID) + 1 = number of new_order rows
        SELECT count(*) INTO bad
          FROM (SELECT max(no_o_id) - min(no_o_id) + 1 AS span, count(*) AS n
                  FROM resilience_tpcc.new_order GROUP BY no_w_id, no_d_id) x
         WHERE x.span <> x.n;

    WHEN 4 THEN   -- sum(O_OL_CNT) = number of order_line rows, per district
        SELECT count(*) INTO bad
          FROM tpcc_sel s
          CROSS JOIN LATERAL (SELECT coalesce(sum(o_ol_cnt), 0) AS v FROM resilience_tpcc.orders
                               WHERE o_w_id = s.w AND o_d_id = s.d) o
          CROSS JOIN LATERAL (SELECT count(*) AS v FROM resilience_tpcc.order_line
                               WHERE ol_w_id = s.w AND ol_d_id = s.d) l
         WHERE o.v <> l.v;

    WHEN 5 THEN   -- O_CARRIER_ID is NULL exactly when a new_order row exists
        SELECT count(*) INTO bad
          FROM tpcc_sel s
          JOIN resilience_tpcc.orders o ON o.o_w_id = s.w AND o.o_d_id = s.d
          LEFT JOIN resilience_tpcc.new_order n
                 ON (n.no_w_id, n.no_d_id, n.no_o_id) = (o.o_w_id, o.o_d_id, o.o_id)
         WHERE (o.o_carrier_id IS NULL) <> (n.no_o_id IS NOT NULL);

    WHEN 6 THEN   -- O_OL_CNT = number of order_line rows, per order
        SELECT count(*) INTO bad
          FROM tpcc_sel s
          JOIN resilience_tpcc.orders o ON o.o_w_id = s.w AND o.o_d_id = s.d
          LEFT JOIN (SELECT ol_w_id, ol_d_id, ol_o_id, count(*) AS n
                       FROM resilience_tpcc.order_line l JOIN tpcc_sel s2 ON l.ol_w_id = s2.w AND l.ol_d_id = s2.d
                      GROUP BY 1, 2, 3) l
                 ON (l.ol_w_id, l.ol_d_id, l.ol_o_id) = (o.o_w_id, o.o_d_id, o.o_id)
         WHERE o.o_ol_cnt <> coalesce(l.n, 0);

    WHEN 7 THEN   -- OL_DELIVERY_D is NULL exactly when the order's O_CARRIER_ID is NULL
        SELECT count(*) INTO bad
          FROM tpcc_sel s
          JOIN resilience_tpcc.order_line l ON l.ol_w_id = s.w AND l.ol_d_id = s.d
          JOIN resilience_tpcc.orders o ON (o.o_w_id, o.o_d_id, o.o_id) = (l.ol_w_id, l.ol_d_id, l.ol_o_id)
         WHERE (l.ol_delivery_d IS NULL) <> (o.o_carrier_id IS NULL);

    WHEN 8 THEN   -- W_YTD = sum(H_AMOUNT), per warehouse
        SELECT count(*) INTO bad
          FROM resilience_tpcc.warehouse w
          LEFT JOIN (SELECT h_w_id, sum(h_amount) AS s FROM resilience_tpcc.history GROUP BY h_w_id) h ON h.h_w_id = w.w_id
         WHERE w.w_id IN (SELECT DISTINCT tpcc_sel.w FROM tpcc_sel)
           AND w.w_ytd <> coalesce(h.s, 0);

    WHEN 9 THEN   -- D_YTD = sum(H_AMOUNT), per district
        SELECT count(*) INTO bad
          FROM tpcc_sel s
          JOIN resilience_tpcc.district dd ON (dd.d_w_id, dd.d_id) = (s.w, s.d)
          LEFT JOIN (SELECT h_w_id, h_d_id, sum(h_amount) AS v FROM resilience_tpcc.history GROUP BY 1, 2) h
                 ON (h.h_w_id, h.h_d_id) = (s.w, s.d)
         WHERE dd.d_ytd <> coalesce(h.v, 0);

    WHEN 10 THEN  -- C_BALANCE = sum(delivered OL_AMOUNT) - sum(H_AMOUNT), per customer
        SELECT count(*) INTO bad
          FROM tpcc_sel s
          JOIN resilience_tpcc.customer c ON c.c_w_id = s.w AND c.c_d_id = s.d
          LEFT JOIN (SELECT o.o_w_id, o.o_d_id, o.o_c_id, sum(l.ol_amount) AS v
                       FROM tpcc_sel s2
                       JOIN resilience_tpcc.orders o ON o.o_w_id = s2.w AND o.o_d_id = s2.d
                       JOIN resilience_tpcc.order_line l ON (l.ol_w_id, l.ol_d_id, l.ol_o_id) = (o.o_w_id, o.o_d_id, o.o_id)
                      WHERE l.ol_delivery_d IS NOT NULL
                      GROUP BY 1, 2, 3) ol ON (ol.o_w_id, ol.o_d_id, ol.o_c_id) = (c.c_w_id, c.c_d_id, c.c_id)
          LEFT JOIN (SELECT h_c_w_id, h_c_d_id, h_c_id, sum(h_amount) AS v
                       FROM resilience_tpcc.history h JOIN tpcc_sel s3 ON h.h_c_w_id = s3.w AND h.h_c_d_id = s3.d
                      GROUP BY 1, 2, 3) h ON (h.h_c_w_id, h.h_c_d_id, h.h_c_id) = (c.c_w_id, c.c_d_id, c.c_id)
         WHERE c.c_balance <> coalesce(ol.v, 0) - coalesce(h.v, 0);

    WHEN 11 THEN  -- number of orders - number of new_order rows = 2100, per district (after load only)
        SELECT count(*) INTO bad
          FROM tpcc_sel s
          CROSS JOIN LATERAL (SELECT count(*) AS v FROM resilience_tpcc.orders WHERE o_w_id = s.w AND o_d_id = s.d) o
          CROSS JOIN LATERAL (SELECT count(*) AS v FROM resilience_tpcc.new_order WHERE no_w_id = s.w AND no_d_id = s.d) n
         WHERE o.v - n.v <> 2100;

    WHEN 12 THEN  -- C_BALANCE + C_YTD_PAYMENT = sum(delivered OL_AMOUNT), per customer
        SELECT count(*) INTO bad
          FROM tpcc_sel s
          JOIN resilience_tpcc.customer c ON c.c_w_id = s.w AND c.c_d_id = s.d
          LEFT JOIN (SELECT o.o_w_id, o.o_d_id, o.o_c_id, sum(l.ol_amount) AS v
                       FROM tpcc_sel s2
                       JOIN resilience_tpcc.orders o ON o.o_w_id = s2.w AND o.o_d_id = s2.d
                       JOIN resilience_tpcc.order_line l ON (l.ol_w_id, l.ol_d_id, l.ol_o_id) = (o.o_w_id, o.o_d_id, o.o_id)
                      WHERE l.ol_delivery_d IS NOT NULL
                      GROUP BY 1, 2, 3) ol ON (ol.o_w_id, ol.o_d_id, ol.o_c_id) = (c.c_w_id, c.c_d_id, c.c_id)
         WHERE c.c_balance + c.c_ytd_payment <> coalesce(ol.v, 0);

    ELSE
        RAISE EXCEPTION 'unknown TPC-C consistency rule %', p_rule;
    END CASE;
    RETURN bad;
END
$$;
