from logging import Logger
from typing import List

from examples.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from datetime import datetime, date, time


class ProductSaleObj(BaseModel):
    id: int
    product_id: int
    order_id: int
    count: int
    price: float
    total_sum: float
    bonus_payment: float
    bonus_grant: float

class ProductSalesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_product_sales(self, product_sale_threshold: int, limit: int) -> List[ProductSaleObj]:
        with self._db.client().cursor(row_factory=class_row(ProductSaleObj)) as cur:
            cur.execute(
                """
                    select 
                        oo.id as id,
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
                    and oo.id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY oo.id asc --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    limit %(limit)s
                    ;
                """, {
                    "threshold": product_sale_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class ProductSaleDestRepository:

    def insert_product_sale(self, conn: Connection, product_sale: ProductSaleObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_product_sales(product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                    VALUES (%(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s)
                    ON CONFLICT ON CONSTRAINT fct_product_sales_product_id_order_id_uq 
                    DO UPDATE SET 
                        count = EXCLUDED.count,
                        price = EXCLUDED.price,
                        total_sum = EXCLUDED.total_sum,
                        bonus_payment = EXCLUDED.bonus_payment,
                        bonus_grant = EXCLUDED.bonus_grant;
                """,
                {
                    "product_id": product_sale.product_id,
                    "order_id": product_sale.order_id,
                    "count": product_sale.count,
                    "price": product_sale.price,
                    "total_sum": product_sale.total_sum,
                    "bonus_payment": product_sale.bonus_payment,
                    "bonus_grant": product_sale.bonus_grant
                },
            )

class ProductSaleLoader:
    WF_KEY = "example_product_sales_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 200

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = ProductSalesOriginRepository(pg_dest)
        self.stg = ProductSaleDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_product_sales(self):
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
            load_queue = self.origin.list_product_sales(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} product sales to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for product_sale in load_queue:
                #self.log.info(f"---------------{user["object_value"]}")
                self.stg.insert_product_sale(conn, product_sale)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")


