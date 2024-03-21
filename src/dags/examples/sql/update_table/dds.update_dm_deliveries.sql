insert into dds.dm_deliveries(delivery_id, address, delivery_ts)
select 
    object_value::json ->> 'delivery_id' as delivery_id,
    object_value::json ->> 'address' as address,
    (object_value::json ->> 'delivery_ts')::timestamp as delivery_ts
from stg.apisystem_deliveries ad 
on conflict (delivery_id) do update
set
    address = EXCLUDED.address,
    delivery_ts = EXCLUDED.delivery_ts;