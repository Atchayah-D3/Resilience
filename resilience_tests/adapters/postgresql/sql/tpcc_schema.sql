-- TPC-C-like workload: the 9 tables (TPC-C 5.11, Clause 1.3) and the harness's meta table.
-- BenchBase's PostgreSQL DDL is the reference for names and types.
--
-- All objects live in schema resilience_tpcc, owned by the harness role
-- (docs/tpcc-contract.md §5); nothing outside it is touched. Primary keys and the two extra
-- indexes are added AFTER loading (tpcc_indexes.sql), which loads much faster.

CREATE SCHEMA resilience_tpcc;

CREATE TABLE resilience_tpcc.warehouse (
    w_id       int           NOT NULL,
    w_name     varchar(10)   NOT NULL,
    w_street_1 varchar(20)   NOT NULL,
    w_street_2 varchar(20)   NOT NULL,
    w_city     varchar(20)   NOT NULL,
    w_state    char(2)       NOT NULL,
    w_zip      char(9)       NOT NULL,
    w_tax      numeric(4,4)  NOT NULL,
    w_ytd      numeric(12,2) NOT NULL
);

CREATE TABLE resilience_tpcc.district (
    d_id        int           NOT NULL,
    d_w_id      int           NOT NULL,
    d_name      varchar(10)   NOT NULL,
    d_street_1  varchar(20)   NOT NULL,
    d_street_2  varchar(20)   NOT NULL,
    d_city      varchar(20)   NOT NULL,
    d_state     char(2)       NOT NULL,
    d_zip       char(9)       NOT NULL,
    d_tax       numeric(4,4)  NOT NULL,
    d_ytd       numeric(12,2) NOT NULL,
    d_next_o_id int           NOT NULL
);

CREATE TABLE resilience_tpcc.customer (
    c_id           int           NOT NULL,
    c_d_id         int           NOT NULL,
    c_w_id         int           NOT NULL,
    c_first        varchar(16)   NOT NULL,
    c_middle       char(2)       NOT NULL,
    c_last         varchar(16)   NOT NULL,
    c_street_1     varchar(20)   NOT NULL,
    c_street_2     varchar(20)   NOT NULL,
    c_city         varchar(20)   NOT NULL,
    c_state        char(2)       NOT NULL,
    c_zip          char(9)       NOT NULL,
    c_phone        char(16)      NOT NULL,
    c_since        timestamp     NOT NULL,
    c_credit       char(2)       NOT NULL,
    c_credit_lim   numeric(12,2) NOT NULL,
    c_discount     numeric(4,4)  NOT NULL,
    c_balance      numeric(12,2) NOT NULL,
    c_ytd_payment  numeric(12,2) NOT NULL,
    c_payment_cnt  int           NOT NULL,
    c_delivery_cnt int           NOT NULL,
    c_data         varchar(500)  NOT NULL
);

-- TPC-C gives HISTORY no primary key (Clause 1.3.1).
CREATE TABLE resilience_tpcc.history (
    h_c_id   int          NOT NULL,
    h_c_d_id int          NOT NULL,
    h_c_w_id int          NOT NULL,
    h_d_id   int          NOT NULL,
    h_w_id   int          NOT NULL,
    h_date   timestamp    NOT NULL,
    h_amount numeric(6,2) NOT NULL,
    h_data   varchar(24)  NOT NULL
);

CREATE TABLE resilience_tpcc.orders (
    o_id         int       NOT NULL,
    o_d_id       int       NOT NULL,
    o_w_id       int       NOT NULL,
    o_c_id       int       NOT NULL,
    o_entry_d    timestamp NOT NULL,
    o_carrier_id int,                   -- NULL until the order is delivered
    o_ol_cnt     int       NOT NULL,
    o_all_local  int       NOT NULL
);

CREATE TABLE resilience_tpcc.new_order (
    no_o_id int NOT NULL,
    no_d_id int NOT NULL,
    no_w_id int NOT NULL
);

CREATE TABLE resilience_tpcc.order_line (
    ol_o_id        int          NOT NULL,
    ol_d_id        int          NOT NULL,
    ol_w_id        int          NOT NULL,
    ol_number      int          NOT NULL,
    ol_i_id        int          NOT NULL,
    ol_supply_w_id int          NOT NULL,
    ol_delivery_d  timestamp,           -- NULL until the order is delivered
    ol_quantity    int          NOT NULL,
    ol_amount      numeric(6,2) NOT NULL,
    ol_dist_info   char(24)     NOT NULL
);

CREATE TABLE resilience_tpcc.item (
    i_id    int          NOT NULL,
    i_im_id int          NOT NULL,
    i_name  varchar(24)  NOT NULL,
    i_price numeric(5,2) NOT NULL,
    i_data  varchar(50)  NOT NULL
);

CREATE TABLE resilience_tpcc.stock (
    s_i_id       int         NOT NULL,
    s_w_id       int         NOT NULL,
    s_quantity   int         NOT NULL,
    s_dist_01    char(24)    NOT NULL,
    s_dist_02    char(24)    NOT NULL,
    s_dist_03    char(24)    NOT NULL,
    s_dist_04    char(24)    NOT NULL,
    s_dist_05    char(24)    NOT NULL,
    s_dist_06    char(24)    NOT NULL,
    s_dist_07    char(24)    NOT NULL,
    s_dist_08    char(24)    NOT NULL,
    s_dist_09    char(24)    NOT NULL,
    s_dist_10    char(24)    NOT NULL,
    s_ytd        int         NOT NULL,
    s_order_cnt  int         NOT NULL,
    s_remote_cnt int         NOT NULL,
    s_data       varchar(50) NOT NULL
);

-- The harness's own record of the load. Written LAST, after every row, index and check: a load
-- that stopped part-way leaves no meta row, and init then refuses the data.
CREATE TABLE resilience_tpcc.meta (
    warehouses     int         NOT NULL,
    items          int         NOT NULL,
    loader_version int         NOT NULL,
    c_last_load    int         NOT NULL,  -- NURand constant C used for C_LAST at load (Clause 2.1.6)
    loaded_orders  bigint      NOT NULL,  -- orders right after the load: the base of the growth check
    loaded_at      timestamptz NOT NULL DEFAULT clock_timestamp(),
    load_seconds   numeric     NOT NULL,
    capacity_tps   numeric,               -- the target's measured maximum (build step 8)
    load_seed      bigint                 -- the loader's seed: the same seed gives the same data (loader version 2)
);
