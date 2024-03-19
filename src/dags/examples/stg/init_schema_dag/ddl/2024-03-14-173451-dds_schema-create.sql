CREATE SCHEMA IF NOT EXISTS stg;

create table if not exists dds.srv_wf_settings(
	id serial not null constraint srv_wf_settings_pkey primary key,
	workflow_key varchar not null,
	workflow_settings json not null,
	constraint srv_wf_settings_workflow_key_key unique (workflow_key)
);

create table if not exists dds.dm_users(
	id serial constraint dm_users_pkey primary key,
	user_id varchar not null,
	user_name varchar not null,
	user_login varchar not null,
	constraint dm_users_id_uniq unique(user_id)
);

create table if not exists dds.dm_restaurants(
	id serial constraint dm_restaurants_pkey primary key,
	restaurant_id varchar not null,
	restaurant_name varchar not null,
	active_from timestamp not null,
	active_to timestamp not null,
	constraint dm_restaurants_id_uniq unique(restaurant_id)
);

create table if not exists dds.dm_products(
	id serial constraint dm_products_pkey primary key,
	restaurant_id int not null,
	product_id varchar not null,
	product_name varchar not null,
	product_price numeric(14, 2) default 0 not null
		constraint dm_products_price check (product_price>=0 and product_price<=999000000000.99),
	active_from timestamp not null,
	active_to timestamp not null,
	constraint dm_products_restaurant_id_fkey foreign key (restaurant_id) references dds.dm_restaurants(id),
	constraint dm_products_product_id_uq unique(product_id)
);

create table if not exists dds.dm_timestamps(
	id serial constraint dm_timestamps_pkey primary key,
	ts timestamp not null,
	"year" smallint not null constraint dm_timestamps_year_check check ("year" >= 2022 and "year" < 2500),
	"month" smallint not null constraint dm_timestamps_month_check check ("month" >= 1 and "month"<=12),
	"day" smallint not null constraint dm_timestamps_day_check check ("day" >= 1 and "day" <= 31),
	"time" time not null,
	"date" date not null,
	constraint dm_timestamps_ts_uniq unique(ts)
);

create table if not exists dds.dm_orders(
	id serial constraint dm_orders_pkey primary key,
	user_id int not null,
	restaurant_id int not null,
	timestamp_id int not null,
	order_key varchar not null,
	order_status varchar not null,
	constraint dm_orders_order_key_uq unique(order_key),
	constraint orders_user_id_fk foreign key (user_id) references dds.dm_users(id),
	constraint orders_restaurant_id_fk foreign key (restaurant_id) references dds.dm_restaurants(id),
	constraint orders_timestamp_id_fk foreign key (timestamp_id) references dds.dm_timestamps(id)
);

create table if not exists dds.fct_product_sales(
	id serial constraint fct_product_sales_pkey primary key,
	product_id int not null,
	order_id int not null,
	"count" int default 0 not null constraint fct_product_sales_count_check check ("count">=0),
	price numeric(14,2) default 0 not null constraint fct_product_sales_price_check check (price>=0),
	total_sum numeric(14,2) default 0 not null constraint fct_product_sales_total_sum_check check (total_sum>=0),
	bonus_payment numeric(14,2) default 0 not null constraint fct_product_sales_bonus_payment_check check (bonus_payment>=0),
	bonus_grant numeric(14,2) default 0 not null constraint fct_product_sales_bonus_grant_check check (bonus_grant>=0),
	constraint fct_product_sales_product_id_order_id_uq unique(product_id, order_id),
	constraint product_sales_product_id_fk foreign key (product_id) references dds.dm_products(id),
	constraint product_sales_order_id_fk foreign key (order_id) references dds.dm_orders(id)
);

create table if not exists dds.dm_couriers(
	id serial not null,
	courier_id varchar not null,
	courier_name varchar not null,
	constraint dm_couriers_pk primary key(id),
	constraint dm_couriers_courier_id_uq unique(courier_id)
);

create table if not exists dds.dm_deliveries(
	id serial not null,
	delivery_id varchar  not null,
	address varchar not null,
	delivery_ts timestamp not null,
	constraint dm_deliveries_pk primary key(id),
	constraint dm_deliveries_delivery_id unique(delivery_id)
);

create table if not exists dds.fct_delivery_of_orders(
	id serial not null,
	order_id int not null,
	delivery_id int not null,
	courier_id int not null,
	rate int not null,
	tip_sum numeric(14, 2) default 0 not null,
	constraint fct_delivery_of_orders_pk primary key(id),
	constraint fct_delivery_of_orders_uq unique(order_id, delivery_id, courier_id),
	constraint fct_delivery_of_orders_order_id_fk foreign key(order_id) references dds.dm_orders(id),
	constraint fct_delivery_of_orders_delivery_id_fk foreign key(delivery_id) references dds.dm_deliveries(id),
	constraint fct_delivery_of_orders_courier_id_fk foreign key(courier_id) references dds.dm_couriers(id),
	constraint fct_delivery_of_orders_rate_check check(rate>=1 and rate<=5),
	constraint fct_delivery_of_orders_tip_sum_check check(tip_sum>=0)
);