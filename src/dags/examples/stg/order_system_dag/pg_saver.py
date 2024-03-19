from datetime import datetime
from typing import Any

from lib.dict_util import json2str
from psycopg import Connection


class PgSaver:

    def save_object(self, table_name: str, conn: Connection, id: str, update_ts: datetime, val: Any):
        str_val = json2str(val)
        with conn.cursor() as cur:
            if table_name=='ordersystem_restaurants':
                cur.execute(
                    """
                        INSERT INTO stg.ordersystem_restaurants(object_id, object_value, update_ts)
                        VALUES (%(id)s, %(val)s, %(update_ts)s)
                        ON CONFLICT (object_id) DO UPDATE
                        SET
                            object_value = EXCLUDED.object_value,
                            update_ts = EXCLUDED.update_ts;
                    """,
                    {
                        "id": id,
                        "val": str_val,
                        "update_ts": update_ts
                    }
                )
            elif table_name=='ordersystem_users':
                cur.execute(
                    """
                        INSERT INTO stg.ordersystem_users(object_id, object_value, update_ts)
                        VALUES (%(id)s, %(val)s, %(update_ts)s)
                        ON CONFLICT (object_id) DO UPDATE
                        SET
                            object_value = EXCLUDED.object_value,
                            update_ts = EXCLUDED.update_ts;
                    """,
                    {
                        "id": id,
                        "val": str_val,
                        "update_ts": update_ts
                    }
                )
            elif table_name=='ordersystem_orders':
                cur.execute(
                    """
                        INSERT INTO stg.ordersystem_orders(object_id, object_value, update_ts)
                        VALUES (%(id)s, %(val)s, %(update_ts)s)
                        ON CONFLICT (object_id) DO UPDATE
                        SET
                            object_value = EXCLUDED.object_value,
                            update_ts = EXCLUDED.update_ts;
                    """,
                    {
                        "id": id,
                        "val": str_val,
                        "update_ts": update_ts
                    }
                )
