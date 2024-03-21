with tmp_prc as (
	select 
	fps.order_id,
	fdoo.delivery_id,
	fdoo.courier_id,
	sum(fps.price * fps.count) as order_sum,
	fdoo.rate, 
	case when fdoo.rate < 4 and sum(fps.price * fps.count)*0.05 < 100 then 100
	    when fdoo.rate < 4 and sum(fps.price * fps.count)*0.05 >= 100 then sum(fps.price * fps.count)*0.05
	    when fdoo.rate between 4 and 4.4 and sum(fps.price * fps.count)*0.07 < 150 then 150 
	    when fdoo.rate between 4 and 4.4 and sum(fps.price * fps.count)*0.07 >= 150 then sum(fps.price * fps.count)*0.07
	    when fdoo.rate between 4.5 and 4.8 and sum(fps.price * fps.count)*0.08 < 175 then 175
	    when fdoo.rate between 4.5 and 4.8 and sum(fps.price * fps.count)*0.08 >= 175 then sum(fps.price * fps.count)*0.08
	    when fdoo.rate >= 4.9 and sum(fps.price * fps.count)*0.1 < 200 then  200
	    when fdoo.rate >= 4.9 and sum(fps.price * fps.count)*0.1 >= 200 then  sum(fps.price * fps.count)*0.1 
	    end as courier_order_sum,
	fdoo.tip_sum
	from dds.fct_product_sales fps
	inner join dds.fct_delivery_of_orders fdoo on fdoo.order_id = fps.order_id 
	group by 
	fps.order_id,
	fdoo.rate,
	fdoo.delivery_id,
	fdoo.courier_id,
	fdoo.tip_sum
)
insert into cdm.dm_courier_ledger(
    courier_id,
    courier_name,
    settlement_year,
    settlement_month,
    orders_count,
    orders_total_sum,
    rate_avg,
    order_processing_fee,
    courier_order_sum,
    courier_tips_sum,
    courier_reward_sum)
select 
    dc.courier_id,
    dc.courier_name,
    dt.year as settlement_year, 
    dt.month as settlement_month,
    count(distinct do2.id) as orders_count,
    sum(order_sum) as orders_total_sum,
    avg(tp.rate) as rate_avg,
    sum(tp.order_sum)*0.25 as order_processing_fee,
    sum(tp.courier_order_sum) as courier_order_sum,
    sum(tp.tip_sum) as courier_tips_sum,
    (sum(tp.courier_order_sum) + sum(tp.tip_sum)) * 0.95 as courier_reward_sum
from tmp_prc as tp 
inner join dds.dm_couriers dc on tp.courier_id=dc.id 
inner join dds.dm_orders do2 on tp.order_id = do2.id 
inner join dds.dm_timestamps dt on do2.timestamp_id =dt.id 
where do2.order_status = 'CLOSED' 
    and dt.year = date_part('year','{{ds}}'::date - interval '1 month') 
    and dt.month = date_part('month','{{ds}}'::date - interval '1 month')
    group by dc.courier_id,
        dc.courier_name,
        dt.year,
        dt.month
order by dc.courier_id asc
on conflict on constraint dm_courier_ledger_uq
do update set 
    orders_count = EXCLUDED.orders_count,
    orders_total_sum = EXCLUDED.orders_total_sum,
    rate_avg = EXCLUDED.rate_avg,
    order_processing_fee = EXCLUDED.order_processing_fee,
    courier_order_sum = EXCLUDED.courier_order_sum,
    courier_tips_sum = EXCLUDED.courier_tips_sum,
    courier_reward_sum = EXCLUDED.courier_reward_sum;