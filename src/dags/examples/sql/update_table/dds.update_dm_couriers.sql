insert into dds.dm_couriers(courier_id, courier_name)
select
    object_id as courier_id,
    (object_value::json->>'name') as name
from stg.apisystem_couriers ac
on conflict (courier_id) do update
set
    courier_id = EXCLUDED.courier_id,
    courier_name = EXCLUDED.courier_name;