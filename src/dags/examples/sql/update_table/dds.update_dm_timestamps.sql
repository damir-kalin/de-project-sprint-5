with tmp_dates as (
    select
    id,
    (object_value::json->>'date')::timestamp as ts
    from stg.ordersystem_orders oo 
)
insert into dds.dm_timestamps(ts, year, month, day, date, time)
select
    ts,
    date_part('year', ts) as year,
    date_part('month', ts) as month,
    date_part('day', ts) as day,
    ts::date as date,
    ts::time as time
from tmp_dates as td
on conflict (ts) do nothing;