CREATE TABLE cdm.dm_courier_ledger (
    id serial,
    courier_id integer NOT NULL,
    courier_name varchar(255) NOT NULL,
    settlement_year integer NOT NULL,
    settlement_month integer NOT NULL,
    orders_count integer NOT NULL DEFAULT 0,
    orders_total_sum numeric(14, 2) NOT NULL DEFAULT 0,
    rate_avg numeric(2, 1) NOT NULL DEFAULT 0,
    order_processing_fee numeric(14, 2) NOT NULL DEFAULT 0,
    courier_order_sum numeric(14, 2) NOT NULL DEFAULT 0,
    courier_tips_sum numeric (14, 2) NOT NULL DEFAULT 0,
    courier_reward_sum numeric(14, 2) NOT NULL DEFAULT 0,
    PRIMARY KEY (courier_id, settlement_year, settlement_mounth)  
);

CREATE TABLE dds.dm_couriers (
    id serial,
    courier_id varchar(255) NOT NULL,
    courier_name varchar(255) NOT NULL,
    PRIMARY KEY (id),
    CONSTRAINT dm_couriers_courier_id_unique UNIQUE(courier_id)
);

CREATE TABLE dds.dm_restaurants (
    id serial,
    restaurant_id varchar(255) NOT NULL,
    restaurant_name varchar(255) NOT NULL,
    PRIMARY KEY(id),
    CONSTRAINT dm_restaurants_restaurant_id_unique UNIQUE(restaurant_id)
)

CREATE TABLE dds.dm_orders (
    id serial,
    order_id varchar(255) NOT NULL,
    order_ts timestamp NOT NULL,
    "sum" numeric(14, 2) NOT NULL DEFAULT 0,
    PRIMARY KEY(id),
    CONSTRAINT dm_orders_order_id_unique UNIQUE(order_id)
)

CREATE TABLE dds.dm_deliveries (
    id serial,
    delivery_id varchar(255) NOT NULL,
    delivery_ts timestamp NOT NULL,
    adress text NOT NULL,
    rate integer NOT NULL DEFAULT 1,
    tip_sum numeric (14, 2) NOT NULL DEFAULT 0,
    PRIMARY KEY(id),
    CONSTRAINT dm_deliveries_delivery_id_unique UNIQUE(delivery_id)
);

CREATE TABLE dds.fct_delivery_reports (
    id serial,
    order_id integer,
    order_ts timestamp NOT NULL,
    delivery_id integer NOT NULL,
    courier_id integer NOT NULL,
    address text NOT NULL,
    delivery_ts timestamp NOT NULL,
    rate integer NOT NULL DEFAULT 1,
    "sum" numeric(14, 2) NOT NULL DEFAULT 0,
    tip_sum numeric (14, 2) NOT NULL DEFAULT 0,
    PRIMARY KEY(id),
    CONSTRAINT fct_delivery_reports_delivery_id_unique UNIQUE (delivery_id),
    CONSTRAINT fct_delivery_reports_order_id_fkey FOREIGN KEY (order_id) REFERENCES dds.dm_orders(id),
    CONSTRAINT fct_delivery_reports_delivery_id_fkey FOREIGN KEY (delivery_id) REFERENCES dds.dm_deliveries(id),
    CONSTRAINT fct_delivery_reports_courier_id_fkey FOREIGN KEY (courier_id) REFERENCES dds.dm_couriers(id)
);

CREATE TABLE stg.couriers (
    id serial,
    object_id varchar(255) NOT NULL,
    object_value varchar(255) NOT NULL,
    update_ts timestamp NOT NULL,
    PRIMARY KEY(id),
    CONSTRAINT couriers_object_id_unique UNIQUE(object_id)
);

CREATE TABLE stg.restaurants (
    id serial,
    object_id varchar(255) NOT NULL,
    object_value varchar(255) NOT NULL,
    update_ts timestamp NOT NULL,
    PRIMARY KEY(id),
    CONSTRAINT restaurants_object_id_unique UNIQUE(object_id)
);

CREATE TABLE stg.deliveries (
    id serial,
    object_id varchar(255) NOT NULL,
    object_value varchar(255) NOT NULL,
    update_ts timestamp NOT NULL,
    PRIMARY KEY(id),
    CONSTRAINT deliveries_object_id_unique UNIQUE(object_id)
);

CREATE TABLE cdm.srv_wf_settings (
	id serial,
	workflow_key varchar NOT NULL,
	workflow_settings json NOT NULL,
	CONSTRAINT srv_wf_settings_pkey PRIMARY KEY (id),
	CONSTRAINT srv_wf_settings_workflow_key_key UNIQUE (workflow_key)
);

CREATE TABLE dds.srv_wf_settings (
	id serial,
	workflow_key varchar NOT NULL,
	workflow_settings json NOT NULL,
	CONSTRAINT srv_wf_settings_pkey PRIMARY KEY (id),
	CONSTRAINT srv_wf_settings_workflow_key_key UNIQUE (workflow_key)
);

CREATE TABLE stg.srv_wf_settings (
	id serial,
	workflow_key varchar NOT NULL,
	workflow_settings json NOT NULL,
	CONSTRAINT srv_wf_settings_pkey PRIMARY KEY (id),
	CONSTRAINT srv_wf_settings_workflow_key_key UNIQUE (workflow_key)
);

