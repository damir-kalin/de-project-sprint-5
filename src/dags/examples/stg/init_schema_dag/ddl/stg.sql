CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE IF NOT EXISTS stg.srv_wf_settings (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    workflow_key varchar NOT NULL UNIQUE,
    workflow_settings JSON NOT NULL
);


CREATE TABLE IF NOT EXISTS stg.bonussystem_ranks (
    id INTEGER NOT NULL PRIMARY KEY,
    name VARCHAR NOT NULL,
    bonus_percent NUMERIC(19, 5) DEFAULT 0 NOT NULL CHECK (bonus_percent >= 0),
    min_payment_threshold NUMERIC(19, 5) DEFAULT 0 NOT NULL CHECK (min_payment_threshold >= 0)
);

CREATE TABLE if not exists stg.bonussystem_events (
	id int4 NOT NULL,
	event_ts timestamp NOT NULL,
	event_type varchar NOT NULL,
	event_value text NOT NULL,
	CONSTRAINT bonussystem_events_pkey PRIMARY KEY (id)
);
CREATE INDEX if not exists idx_bonussystem_events__event_ts ON stg.bonussystem_events USING btree (event_ts);

CREATE TABLE if not exists stg.bonussystem_users (
	id int4 NOT NULL,
	order_user_id text NOT NULL,
	CONSTRAINT bonussystem_users_pkey PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS stg.ordersystem_restaurants (
    id int NOT NULL PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    object_id varchar NOT NULL UNIQUE,
    object_value text NOT NULL,
    update_ts timestamp NOT NULL
);

create table if not exists stg.ordersystem_orders (
	id serial not null,
	object_id varchar(100) not null,
	object_value text not null,
	update_ts timestamp not null,
	constraint ordersystem_orders_pkey primary key (id),
	constraint ordersystem_orders_object_id_uindex unique (object_id)
);

create table if not exists stg.ordersystem_users (
	id serial not null,
	object_id varchar(100) not null,
	object_value text not null,
	update_ts timestamp not null,
	constraint ordersystem_users_pkey primary key (id),
	constraint ordersystem_users_object_id_uindex unique (object_id)
);

create table if not exists stg.apisystem_couriers(
	id serial not null,
	object_id varchar not null,
	object_value text not null,
	update_ts timestamp not null,
	constraint apisystem_couriers_pkey primary key(id),
	constraint apisystem_couriers_object_id unique(object_id)
);

create table if not exists stg.apisystem_deliveries(
	id serial not null,
	object_id varchar not null,
	object_value text not null,
	update_ts timestamp not null,
	constraint apisystem_deliveries_pk primary key(id),
	constraint apisystem_deliveries_object_id_uq unique(object_id)
);
