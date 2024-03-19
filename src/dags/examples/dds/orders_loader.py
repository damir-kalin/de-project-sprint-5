from logging import Logger
from typing import List

from examples.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from datetime import datetime, date, time


class OrderObj(BaseModel):
    id: int
    order_key: str
    order_status: str
    restaurant_id: int
    timestamp_id: int
    user_id: int

class OrdersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_orders(self, order_threshold: int, limit: int) -> List[OrderObj]:
        with self._db.client().cursor(row_factory=class_row(OrderObj)) as cur:
            cur.execute(
                """select 
                        oo.id as id, 
                        oo.object_id as order_key,
                        (oo.object_value::json -> 'final_status') as order_status,
                        dr.id as restaurant_id,
                        dt.id as timestamp_id,
                        du.id as user_id
                    from stg.ordersystem_orders oo 
                        inner join dds.dm_restaurants dr on dr.restaurant_id = (oo.object_value::json->'restaurant'->>'id')
                        inner join dds.dm_timestamps dt on dt.ts = date_trunc('seconds', oo.update_ts)
                        inner join dds.dm_users du on du.user_id = (oo.object_value::json->'user'->>'id')
                    WHERE oo.id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY oo.id asc --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    limit %(limit)s
                    ;
                """, {
                    "threshold": order_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class OrderDestRepository:

    def insert_order(self, conn: Connection, order: OrderObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(order_key, order_status, restaurant_id, timestamp_id, user_id )
                    VALUES (%(order_key)s, %(order_status)s, %(restaurant_id)s, %(timestamp_id)s, %(user_id)s)
                    ON CONFLICT (order_key) 
                    DO UPDATE SET 
                        order_status = EXCLUDED.order_status,
                        restaurant_id = EXCLUDED.restaurant_id,
                        timestamp_id = EXCLUDED.timestamp_id,
                        user_id = EXCLUDED.user_id;
                """,
                {
                    "order_key": order.order_key,
                    "order_status": order.order_status,
                    "restaurant_id": order.restaurant_id,
                    "timestamp_id": order.timestamp_id,
                    "user_id": order.user_id
                },
            )

class OrderLoader:
    WF_KEY = "example_orders_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 200

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = OrdersOriginRepository(pg_dest)
        self.stg = OrderDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_orders(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_orders(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} orders to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for order in load_queue:
                #self.log.info(f"---------------{user["object_value"]}")
                self.stg.insert_order(conn, order)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")


