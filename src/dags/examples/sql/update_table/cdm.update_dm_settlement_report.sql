insert into cdm.dm_settlement_report(
    restaurant_id, 
    restaurant_name, 
    settlement_date, 
    orders_count, 
    orders_total_sum, 
    orders_bonus_payment_sum, 
    orders_bonus_granted_sum, 
    order_processing_fee,
    restaurant_reward_sum)
select 
    dr.restaurant_id,
    dr.restaurant_name,
    dt.date as settlement_date,
    count(distinct do2.id) as orders_count,
    sum(fps.total_sum) as orders_total_sum,
    sum(fps.bonus_payment) as orders_bonus_payment_sum,
    sum(fps.bonus_grant) as orders_bonus_granted_sum,
    sum(fps.total_sum)*0.25 as order_processing_fee,
    sum(fps.total_sum)*0.75 - sum(fps.bonus_payment) as restaurant_reward_sum
from dds.dm_orders do2 
    inner join dds.dm_restaurants dr on do2.restaurant_id = dr.id 
    inner join dds.dm_timestamps dt on do2.timestamp_id =dt.id 
    inner join dds.fct_product_sales fps on fps.order_id = do2.id
where do2.order_status = 'CLOSED' 
group by 
    dr.restaurant_id,
    dr.restaurant_name,
    dt."date"
on conflict on constraint restaurant_id_settlement_date_uniq
do update set 
    restaurant_name = EXCLUDED.restaurant_name,
    orders_count = EXCLUDED.orders_count,
    orders_total_sum = EXCLUDED.orders_total_sum,
    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
    order_processing_fee = EXCLUDED.order_processing_fee,
    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;