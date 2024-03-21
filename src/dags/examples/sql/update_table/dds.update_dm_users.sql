INSERT INTO dds.dm_users(user_id, user_name, user_login)
SELECT
    (object_value::json->>'_id') as id,
    (object_value::json->>'name') as user_name,
    (object_value::json->>'login') as user_login
FROM stg.ordersystem_users             
ON CONFLICT (user_id) DO UPDATE
SET
    user_name = EXCLUDED.user_name,
    user_login = EXCLUDED.user_login;
