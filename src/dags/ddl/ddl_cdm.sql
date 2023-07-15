-- DROP TABLE IF EXISTS cdm.dm_courier_ledger;

CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
    id serial PRIMARY KEY,
    courier_id varchar NOT NULL,
    courier_name varchar NOT NULL,
    settlement_year int NOT NULL CHECK ( settlement_year between 2022 and 2099 ),
    settlement_month int NOT NULL CHECK ( settlement_month between 1 and 12 ),
    orders_count int NOT NULL CHECK ( orders_count >= 0 ) DEFAULT 0,
    orders_total_sum numeric(14,2) NOT NULL CHECK ( orders_total_sum >= 0 ) DEFAULT 0.0,
    rate_avg numeric(5,2) NOT NULL CHECK ( rate_avg >= 0 ) DEFAULT 0.0,
    order_processing_fee numeric(14,2) NOT NULL CHECK ( order_processing_fee >= 0 ) DEFAULT 0.0,
    courier_order_sum numeric(14,2) NOT NULL CHECK ( courier_order_sum >= 0 ) DEFAULT 0.0,
    courier_tips_sum numeric(14,2) NOT NULL CHECK ( courier_tips_sum >= 0 ) DEFAULT 0.0,
    courier_reward_sum numeric(14,2) NOT NULL CHECK ( courier_reward_sum >= 0 ) DEFAULT 0.0,
    CONSTRAINT dm_courier_ledger_unique UNIQUE (courier_id, settlement_year, settlement_month)
);
