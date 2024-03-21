insert into dds.dm_orders(order_key, order_status, restaurant_id, timestamp_id, user_id )
select 
    oo.object_id as order_key,
    (oo.object_value::json ->> 'final_status') as order_status,
    dr.id as restaurant_id,
    dt.id as timestamp_id,
    du.id as user_id
from stg.ordersystem_orders oo 
    inner join dds.dm_restaurants dr on dr.restaurant_id = (oo.object_value::json->'restaurant'->>'id')
    inner join dds.dm_timestamps dt on dt.ts = date_trunc('seconds', oo.update_ts)
    inner join dds.dm_users du on du.user_id = (oo.object_value::json->'user'->>'id')
on conflict (order_key) 
do update set 
    order_status = EXCLUDED.order_status,
    restaurant_id = EXCLUDED.restaurant_id,
    timestamp_id = EXCLUDED.timestamp_id,
    user_id = EXCLUDED.user_id;