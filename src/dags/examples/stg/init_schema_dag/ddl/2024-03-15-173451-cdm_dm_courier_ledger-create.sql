CREATE SCHEMA IF NOT EXISTS stg;

create table if not exists cdm.srv_wf_settings(
	id serial not null constraint srv_wf_settings_pkey primary key,
	workflow_key varchar not null,
	workflow_settings json not null,
	constraint srv_wf_settings_workflow_key_key unique (workflow_key)
);

create table if not exists cdm.dm_settlement_report
(
	id serial,
	restaurant_id varchar(50) not null,
	restaurant_name varchar(100) not null,
	settlement_date date not null,
	orders_count int default 0 not null,
	orders_total_sum numeric(14,2) default 0 not null,
	orders_bonus_payment_sum numeric(14,2) default 0 not null,
	orders_bonus_granted_sum numeric(14,2) default 0 not null,
	order_processing_fee numeric(14,2) default 0 not null,
	restaurant_reward_sum numeric(14,2) default 0 not null,
	constraint pk_dm_settlement_report primary key (id),
	constraint dm_settlement_report_settlement_date_check check (settlement_date >= '2022-01-01' AND settlement_date < '2500-01-01'),
	constraint dm_settlement_report_orders_count_check check (orders_count>= 0),
	constraint dm_settlement_report_orders_total_sum_check check (orders_total_sum>= 0),
	constraint dm_settlement_report_orders_bonus_payment_sum_check check (orders_bonus_payment_sum>= 0),
	constraint dm_settlement_report_orders_bonus_granted_sum_check check (orders_bonus_granted_sum>= 0),
	constraint dm_settlement_report_order_processing_fee_check check (order_processing_fee>= 0),
	constraint dm_settlement_report_restaurant_reward_sum_check check (restaurant_reward_sum>= 0),
	constraint restaurant_id_settlement_date_uniq unique(restaurant_id, settlement_date)
);

create table if not exists cdm.dm_courier_ledger(
	id serial not null,
	courier_id varchar not null,
	courier_name varchar not null,
	settlement_year int not null,
	settlement_month int not null,
	orders_count int default 0 not null,
	orders_total_sum numeric(14, 2) default 0 not null,
	rate_avg numeric(14, 2) default 0 not null,
	order_processing_fee numeric(14, 2) default 0 not null,
	courier_order_sum numeric(14, 2) default 0 not null,
	courier_tips_sum numeric(14, 2) default 0 not null,
	courier_reward_sum numeric(14, 2) default 0 not null,
	constraint dm_courier_ledger_pk primary key(id),
	constraint dm_courier_ledger_settlement_year_check check(settlement_year >= 2022 and settlement_year <= 2099),
	constraint dm_courier_ledger_settlement_month_check check(settlement_month >= 1 and settlement_month <= 12),
	constraint dm_courier_ledger_orders_count check(orders_count >= 0),
	constraint dm_courier_ledger_orders_total_sum check(orders_total_sum >= 0),
	constraint dm_courier_ledger_rate_avg check(rate_avg >= 0),
	constraint dm_courier_ledger_order_processing_fee check(order_processing_fee >= 0),
	constraint dm_courier_ledger_courier_order_sum check(courier_order_sum >= 0),
	constraint dm_courier_ledger_courier_tips_sum check(courier_tips_sum >= 0),
	constraint dm_courier_ledger_courier_reward_sum check(courier_reward_sum >= 0),
	constraint dm_courier_ledger_uq unique(courier_id, settlement_year, settlement_month)
);