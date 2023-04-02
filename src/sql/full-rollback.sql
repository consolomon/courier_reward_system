TRUNCATE TABLE stg.deliveries;
ALTER SEQUENCE stg.deliveries_id_seq RESTART 1;

TRUNCATE TABLE stg.couriers;
ALTER SEQUENCE stg.couriers_id_seq RESTART 1;

TRUNCATE TABLE stg.couriers;
ALTER SEQUENCE stg.restaurants_id_seq RESTART 1;

TRUNCATE TABLE stg.srv_wf_settings;
ALTER SEQUENCE stg.srv_wf_settings_id_seq RESTART 1;

TRUNCATE TABLE dds.dm_restaurants;
ALTER SEQUENCE dds.dm_restaurants_id_seq RESTART 1;

TRUNCATE TABLE dds.dm_couriers CASCADE;
ALTER SEQUENCE dds.dm_couriers_id_seq RESTART 1;

TRUNCATE TABLE dds.dm_orders CASCADE ;
ALTER SEQUENCE dds.dm_orders_id_seq RESTART 1;

TRUNCATE TABLE dds.dm_deliveries CASCADE ;
ALTER SEQUENCE dds.dm_deliveries_id_seq RESTART 1;

TRUNCATE TABLE dds.fct_delivery_reports CASCADE ;
ALTER SEQUENCE dds.fct_delivery_reports_id_seq RESTART 1;

TRUNCATE TABLE dds.srv_wf_settings;
ALTER SEQUENCE dds.srv_wf_settings_id_seq RESTART 1;