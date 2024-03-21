insert into dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
select 
    object_value::json ->>'_id' as restaurant_id,
    object_value ::json ->> 'name' as restaurant_name,
    update_ts as active_from,
    '2099-12-31 00:00:00'::timestamp as active_to
from stg.ordersystem_restaurants
on conflict (restaurant_id) do update
set
restaurant_name = EXCLUDED.restaurant_name,
active_from = EXCLUDED.active_from,
active_to = EXCLUDED.active_to;