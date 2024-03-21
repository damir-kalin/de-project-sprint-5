insert into dds.fct_product_sales(product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
select 
    dp.id as product_id,
    do2.id as order_id,
    p.quantity as count,
    p.price::numeric(19, 5) as price,
    p.quantity * p.price::numeric(19, 5) as total_sum,
    coalesce(aa.bonus_payment, 0)::numeric(19, 5) as bonus_payment,
    coalesce(aa.bonus_grant, 0)::numeric(19, 5) as bonus_grant
from stg.ordersystem_orders oo 
    cross join lateral json_to_recordset(oo.object_value::json -> 'order_items') as p(id varchar
        ,name varchar
        ,price int
        ,quantity int)
    inner join dds.dm_orders do2 on do2.order_key = oo.object_id 
    inner join dds.dm_products dp on dp.product_id = p.id
    inner join (select 
                (be.event_value::json ->> 'order_id') as order_id,
                p.product_id,
                p.bonus_payment,
                p.bonus_grant
            from stg.bonussystem_events be 
                cross join lateral json_to_recordset(be.event_value::json->'product_payments') as p(product_id varchar, 
                    bonus_payment float,
                    bonus_grant float)
            where event_type ='bonus_transaction') as aa on do2.order_key = aa.order_id and dp.product_id=aa.product_id
where do2.order_status = 'CLOSED'
on conflict on constraint fct_product_sales_product_id_order_id_uq do update
set
    count = EXCLUDED.count,
    price = EXCLUDED.price,
    total_sum = EXCLUDED.total_sum,
    bonus_payment = EXCLUDED.bonus_payment,
    bonus_grant = EXCLUDED.bonus_grant;