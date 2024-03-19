from logging import Logger
from typing import List

from examples.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from datetime import datetime, date, time


class ProductObj(BaseModel):
    id: int
    product_id: str
    product_name: str
    product_price: float
    active_from: datetime
    active_to: datetime
    restaurant_id: int

class ProductsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_products(self, product_threshold: int, limit: int) -> List[ProductObj]:
        with self._db.client().cursor(row_factory=class_row(ProductObj)) as cur:
            cur.execute(
                """select
                        or1.id as id,
                        p._id as product_id,
                        p.name as product_name,
                        p.price as product_price,
                        or1.update_ts as active_from,
                        '2099-12-31'::timestamp as active_to,
                        dr.id as restaurant_id
                    from stg.ordersystem_restaurants or1
                        cross join lateral json_to_recordset(object_value::json->'menu') as p(_id varchar, name varchar, price int)
                        left join dds.dm_restaurants dr on or1.object_id = dr.restaurant_id
                    WHERE or1.id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY or1.id asc --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    limit %(limit)s
                    ;
                """, {
                    "threshold": product_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class ProductDestRepository:

    def insert_product(self, conn: Connection, product: ProductObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_products(product_id, product_name, product_price, active_from, active_to, restaurant_id)
                    VALUES (%(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s, %(restaurant_id)s)
                    ON CONFLICT (product_id) DO UPDATE SET active_to = EXCLUDED.active_from;
                """,
                {
                    "product_id": product.product_id,
                    "product_name": product.product_name,
                    "product_price": product.product_price,
                    "active_from": product.active_from,
                    "active_to": product.active_to, 
                    "restaurant_id": product.restaurant_id
                },
            )

class ProductLoader:
    WF_KEY = "example_products_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 200

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = ProductsOriginRepository(pg_dest)
        self.stg = ProductDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_products(self):
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
            load_queue = self.origin.list_products(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} products to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for product in load_queue:
                #self.log.info(f"---------------{user["object_value"]}")
                self.stg.insert_product(conn, product)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")


