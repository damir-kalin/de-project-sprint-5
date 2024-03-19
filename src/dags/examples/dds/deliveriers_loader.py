from logging import Logger
from typing import List

from examples.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class DeliveryObj(BaseModel):
    id: int
    delivery_id: str
    address: str
    delivery_ts: datetime

class DeliveriersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_deliveriers(self, delivery_threshold: int, limit: int) -> List[DeliveryObj]:
        with self._db.client().cursor(row_factory=class_row(DeliveryObj)) as cur:
            cur.execute(
                """
                    select 
                        id,
                        object_value::json ->> 'delivery_id' as delivery_id,
                        object_value::json ->> 'address' as address,
                        (object_value::json ->> 'delivery_ts')::timestamp as delivery_ts
                    from stg.apisystem_deliveries ad 
                    WHERE id > %(delivery_threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    limit %(limit)s
                    ;
                """, {
                    "delivery_threshold": delivery_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class DeliveryDestRepository:
    def insert_delivery(self, conn: Connection, delivery: DeliveryObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_deliveries(delivery_id, address, delivery_ts)
                    VALUES (%(delivery_id)s, %(address)s, %(delivery_ts)s)
                    ON CONFLICT (delivery_id) DO UPDATE
                    SET
                        address = EXCLUDED.address,
                        delivery_ts = EXCLUDED.delivery_ts;
                """,
                {
                    "delivery_id": delivery.delivery_id,
                    "address": delivery.address,
                    "delivery_ts": delivery.delivery_ts
                },
            )

class DeliveryLoader:
    WF_KEY = "example_deliveries_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 200

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = DeliveriersOriginRepository(pg_dest)
        self.stg = DeliveryDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_deliveriers(self):
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
            load_queue = self.origin.list_deliveriers(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for delivery in load_queue:
                #self.log.info(f"---------------{user["object_value"]}")
                self.stg.insert_delivery(conn, delivery)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
