-- DROP TABLE IF EXISTS dds.dm_couriers;

CREATE TABLE IF NOT EXISTS dds.dm_couriers(
    id serial PRIMARY KEY,
    courier_id varchar NOT NULL,
    courier_name varchar NOT NULL
);
CREATE UNIQUE INDEX  dm_couriers_unique ON dds.dm_couriers (courier_id);

ALTER TABLE dds.dm_orders ADD COLUMN courier_id int REFERENCES dds.dm_couriers(id);

-- dds.dm_deliveries definition

-- Drop table

-- DROP TABLE dds.dm_deliveries;

CREATE TABLE dds.dm_deliveries (
	id serial,
	order_id int4 NOT NULL,
	delivery_id varchar NOT NULL,
	rate int4 NOT NULL,
	delivery_sum numeric(14, 2) NOT NULL DEFAULT 0.0,
	tip_sum numeric(14, 2) NOT NULL DEFAULT 0.0,
	CONSTRAINT dm_delivery_delivery_sum_check CHECK ((delivery_sum >= (0)::numeric)),
	CONSTRAINT dm_delivery_pkey PRIMARY KEY (id),
	CONSTRAINT dm_delivery_rate_check CHECK (((rate >= 1) AND (rate <= 5))),
	CONSTRAINT dm_delivery_tip_sum_check CHECK ((tip_sum >= (0)::numeric))
);
ALTER TABLE dds.dm_deliveries ADD CONSTRAINT dm_delivery_order_id_fkey FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id);
ALTER TABLE dds.dm_deliveries ADD CONSTRAINT dm_deliveries_un UNIQUE (delivery_id);