from logging import Logger
from typing import List

from examples.cdm import EtlSetting, CdmEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from datetime import datetime, date, time


class Obj(BaseModel):
    restaurant_id: str
    restaurant_name: str
    settlement_date: date
    orders_count: float
    orders_total_sum: float
    orders_bonus_payment_sum: float
    orders_bonus_granted_sum: float
    order_processing_fee: float
    restaurant_reward_sum: float

class OriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_obj(self, threshold: datetime) -> List[Obj]:
        with self._db.client().cursor(row_factory=class_row(Obj)) as cur:
            cur.execute(
                """select 
                        dr.restaurant_id,
                        dr.restaurant_name,
                        dt."date" as settlement_date,
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
                    where do2.order_status = 'CLOSED' and dt."date" > %(threshold)s 
                    group by 
                        dr.restaurant_id,
                        dr.restaurant_name,
                        dt."date"
                    ORDER BY dt."date" asc --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    ;
                """, {
                    "threshold": threshold
                }
            )
            objs = cur.fetchall()
        return objs


class DestRepository:

    def insert_obj(self, conn: Connection, obj: Obj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_settlement_report(
                        restaurant_id, 
                        restaurant_name, 
                        settlement_date, 
                        orders_count, 
                        orders_total_sum, 
                        orders_bonus_payment_sum, 
                        orders_bonus_granted_sum, 
                        order_processing_fee,
                        restaurant_reward_sum)
                    VALUES (%(restaurant_id)s, 
                        %(restaurant_name)s, 
                        %(settlement_date)s, 
                        %(orders_count)s, 
                        %(orders_total_sum)s,
                        %(orders_bonus_payment_sum)s,
                        %(orders_bonus_granted_sum)s,
                        %(order_processing_fee)s,
                        %(restaurant_reward_sum)s)
                    on conflict on constraint restaurant_id_settlement_date_uniq
                    do update set 
                        restaurant_name = EXCLUDED.restaurant_name,
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                        orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
                """,
                {
                    "restaurant_id": obj.restaurant_id,
                    "restaurant_name": obj.restaurant_name,
                    "settlement_date": obj.settlement_date,
                    "orders_count": obj.orders_count,
                    "orders_total_sum": obj.orders_total_sum,
                    "orders_bonus_payment_sum": obj.orders_bonus_payment_sum,
                    "orders_bonus_granted_sum": obj.orders_bonus_granted_sum,
                    "order_processing_fee": obj.order_processing_fee,
                    "restaurant_reward_sum": obj.restaurant_reward_sum
                },
            )

class Loader:
    WF_KEY = "example_dds_to_cdm_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = OriginRepository(pg_dest)
        self.stg = DestRepository()
        self.settings_repository = CdmEtlSettingsRepository()
        self.log = log

    def load_obj(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        # JSON ничего не знает про даты. Поэтому записываем строку, которую будем кастить при использовании.
                        # А в БД мы сохраним именно JSON.
                        self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                    }
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"starting to load from last checkpoint: {last_loaded_ts}")

            # Вычитываем очередную пачку объектов.
            load_queue = self.origin.list_obj(last_loaded_ts)

            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for obj in load_queue:
                self.stg.insert_obj(conn, obj)


            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max([x.settlement_date.isoformat() for x in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")


