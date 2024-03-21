insert into dds.dm_products(product_id, product_name, product_price, active_from, active_to, restaurant_id)
select
    p._id as product_id,
    p.name as product_name,
    p.price as product_price,
    or1.update_ts as active_from,
    '2099-12-31'::timestamp as active_to,
    dr.id as restaurant_id
from stg.ordersystem_restaurants or1
    cross join lateral json_to_recordset(object_value::json->'menu') as p(_id varchar, name varchar, price int)
    left join dds.dm_restaurants dr on or1.object_id = dr.restaurant_id
on conflict (product_id) do update set active_to = EXCLUDED.active_from;