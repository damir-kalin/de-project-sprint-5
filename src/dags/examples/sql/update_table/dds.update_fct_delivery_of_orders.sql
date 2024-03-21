insert into dds.fct_delivery_of_orders(order_id, delivery_id, courier_id, rate, tip_sum)
select 
    do2.id as order_id,
    dd.id as delivery_id,
    dc.id as courier_id,
    (ad.object_value::json ->> 'rate')::int as rate,
    (ad.object_value::json ->> 'tip_sum')::numeric(14,2) as tip_sum
from stg.apisystem_deliveries ad 
    inner join dds.dm_orders do2 on (ad.object_value::json ->> 'order_id')=do2.order_key 
    inner join dds.dm_deliveries dd on (ad.object_value::json ->> 'delivery_id')=dd.delivery_id 
    inner join dds.dm_couriers dc on (ad.object_value::json ->> 'courier_id')=dc.courier_id
on conflict (order_id, delivery_id, courier_id) do update
set
	rate = EXCLUDED.rate,
    tip_sum = EXCLUDED.tip_sum;
   
select * from dds.fct_delivery_of_orders fdoo;