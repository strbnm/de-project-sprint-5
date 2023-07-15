-- DROP TABLE IF EXISTS stg.deliverysystem_courier;

CREATE TABLE IF NOT EXISTS stg.deliverysystem_courier(
    id serial PRIMARY KEY,
    _id varchar NOT NULL GENERATED ALWAYS AS (("json" ->> '_id')::text) STORED,
    "name" varchar NOT NULL GENERATED ALWAYS AS (("json" ->> 'name')::text) STORED,
    "json" jsonb NOT NULL,
    created_at timestamp NOT NULL
);
ALTER TABLE stg.deliverysystem_courier ADD CONSTRAINT deliverysystem_courier_un UNIQUE ("_id");

-- DROP TABLE IF EXISTS stg.deliverysystem_delivery;

CREATE TABLE IF NOT EXISTS stg.deliverysystem_delivery(
    id serial PRIMARY KEY,
    order_id varchar NOT NULL GENERATED ALWAYS AS (("json" ->> 'order_id')::text) STORED,
    order_ts varchar NOT NULL GENERATED ALWAYS AS (("json" ->> 'order_ts')::text) STORED,
    delivery_id varchar NOT NULL GENERATED ALWAYS AS (("json" ->> 'delivery_id')::text) STORED,
    rate varchar NOT NULL GENERATED ALWAYS AS (("json" ->> 'rate')::int2) STORED,
    sum varchar NOT NULL GENERATED ALWAYS AS (("json" ->> 'sum')::numeric(14,2)) STORED,
    tip_sum varchar NOT NULL GENERATED ALWAYS AS (("json" ->> 'tip_sum')::numeric(14,2)) STORED,
    "json" jsonb NOT NULL ,
    created_at timestamp NOT NULL
);
ALTER TABLE stg.deliverysystem_delivery ADD CONSTRAINT deliverysystem_delivery_un UNIQUE ("delivery_id");