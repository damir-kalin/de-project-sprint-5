from logging import Logger
from typing import List

from examples.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class DeliveryOfOrdersObj(BaseModel):
    order_id: int
    delivery_id: int
    courier_id: int
    rate:int
    tip_sum: float

class DeliveryOfOrdersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_delivery_of_orders(self, delivery_threshold: int, limit: int) -> List[DeliveryOfOrdersObj]:
        with self._db.client().cursor(row_factory=class_row(DeliveryOfOrdersObj)) as cur:
            cur.execute(
                """
                    select 
                        do2.id as order_id,
                        dd.id as delivery_id,
                        dc.id as courier_id,
                        (ad.object_value::json ->> 'rate')::int as rate,
                        (ad.object_value::json ->> 'tip_sum')::numeric(14,2) as tip_sum
                    from stg.apisystem_deliveries ad 
                        inner join dds.dm_orders do2 on (ad.object_value::json ->> 'order_id')=do2.order_key 
                        inner join dds.dm_deliveries dd on (ad.object_value::json ->> 'delivery_id')=dd.delivery_id 
                        inner join dds.dm_couriers dc on (ad.object_value::json ->> 'courier_id')=dc.courier_id 
                    WHERE do2.id > %(delivery_threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY do2.id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    limit %(limit)s
                    ;
                """, {
                    "delivery_threshold": delivery_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DeliveryOfOrdersDestRepository:
    def insert_delivery_of_orders(self, conn: Connection, delivery: DeliveryOfOrdersObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_delivery_of_orders(order_id, delivery_id, courier_id, rate, tip_sum)
                    VALUES (%(order_id)s, %(delivery_id)s, %(courier_id)s, %(rate)s, %(tip_sum)s)
                    ON CONFLICT (order_id, delivery_id, courier_id) DO UPDATE
                    SET
                        rate = EXCLUDED.rate,
                        tip_sum = EXCLUDED.tip_sum;
                """,
                {
                    "order_id": delivery.order_id,
                    "delivery_id": delivery.delivery_id,
                    "courier_id": delivery.courier_id,
                    "rate": delivery.rate,
                    "tip_sum": delivery.tip_sum
                },
            )

class DeliveryOfOrdersLoader:
    WF_KEY = "example_deliveriy_of_orders_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 200

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DeliveryOfOrdersOriginRepository(pg_dest)
        self.stg = DeliveryOfOrdersDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_delivery_of_orders(self):
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
            self.log.info(last_loaded)
            load_queue = self.origin.list_delivery_of_orders(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for delivery in load_queue:
                #self.log.info(f"---------------{user["object_value"]}")
                self.stg.insert_delivery_of_orders(conn, delivery)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.order_id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
