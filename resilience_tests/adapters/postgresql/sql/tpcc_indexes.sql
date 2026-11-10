-- TPC-C-like workload: primary keys and the two extra indexes, added after loading.
-- HISTORY has no primary key (Clause 1.3.1).

ALTER TABLE resilience_tpcc.warehouse  ADD PRIMARY KEY (w_id);
ALTER TABLE resilience_tpcc.district   ADD PRIMARY KEY (d_w_id, d_id);
ALTER TABLE resilience_tpcc.customer   ADD PRIMARY KEY (c_w_id, c_d_id, c_id);
ALTER TABLE resilience_tpcc.orders     ADD PRIMARY KEY (o_w_id, o_d_id, o_id);
ALTER TABLE resilience_tpcc.new_order  ADD PRIMARY KEY (no_w_id, no_d_id, no_o_id);
ALTER TABLE resilience_tpcc.order_line ADD PRIMARY KEY (ol_w_id, ol_d_id, ol_o_id, ol_number);
ALTER TABLE resilience_tpcc.item       ADD PRIMARY KEY (i_id);
ALTER TABLE resilience_tpcc.stock      ADD PRIMARY KEY (s_w_id, s_i_id);

-- Payment and Order-Status find 60% of customers by last name (Clause 2.5.1.2, 2.6.1.2)
CREATE INDEX customer_last_name ON resilience_tpcc.customer (c_w_id, c_d_id, c_last, c_first);
-- Order-Status reads a customer's latest order (Clause 2.6.2.2)
CREATE INDEX orders_customer ON resilience_tpcc.orders (o_w_id, o_d_id, o_c_id, o_id);
