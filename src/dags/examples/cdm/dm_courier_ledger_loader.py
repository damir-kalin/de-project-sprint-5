from logging import Logger
from typing import List

from examples.cdm import EtlSetting, CdmEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from datetime import datetime, date, time, timedelta



class CLObj(BaseModel):
    courier_id: str
    courier_name: str
    settlement_year: int
    settlement_month: int
    orders_count: int
    orders_total_sum: float
    rate_avg: float
    order_processing_fee: float
    courier_order_sum: float
    courier_tips_sum: float
    courier_reward_sum: float

class CLOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_obj(self) -> List[CLObj]:
        dt = datetime.now() - timedelta(days=30)
        with self._db.client().cursor(row_factory=class_row(CLObj)) as cur:
            cur.execute(
                """with tmp_prc as (
                        select 
                        fps.order_id,
                        fdoo.delivery_id,
                        fdoo.courier_id,
                        sum(fps.price * fps.count) as order_sum,
                        fdoo.rate, 
                        case when fdoo.rate < 4 and sum(fps.price * fps.count)*0.05 < 100 then 100
                            when fdoo.rate < 4 and sum(fps.price * fps.count)*0.05 >= 100 then sum(fps.price * fps.count)*0.05
                            when fdoo.rate between 4 and 4.4 and sum(fps.price * fps.count)*0.07 < 150 then 150 
                            when fdoo.rate between 4 and 4.4 and sum(fps.price * fps.count)*0.07 >= 150 then sum(fps.price * fps.count)*0.07
                            when fdoo.rate between 4.5 and 4.8 and sum(fps.price * fps.count)*0.08 < 175 then 175
                            when fdoo.rate between 4.5 and 4.8 and sum(fps.price * fps.count)*0.08 >= 175 then sum(fps.price * fps.count)*0.08
                            when fdoo.rate >= 4.9 and sum(fps.price * fps.count)*0.1 < 200 then  200
                            when fdoo.rate >= 4.9 and sum(fps.price * fps.count)*0.1 >= 200 then  sum(fps.price * fps.count)*0.1 
                            end as courier_order_sum,
                        fdoo.tip_sum
                        from dds.fct_product_sales fps
                        inner join dds.fct_delivery_of_orders fdoo on fdoo.order_id = fps.order_id 
                        group by 
                        fps.order_id,
                        fdoo.rate,
                        fdoo.delivery_id,
                        fdoo.courier_id,
                        fdoo.tip_sum
                        )
                        select 
                            dc.courier_id,
                            dc.courier_name,
                            dt.year as settlement_year, 
                            dt.month as settlement_month,
                            count(distinct do2.id) as orders_count,
                            sum(order_sum) as orders_total_sum,
                            avg(tp.rate) as rate_avg,
                            sum(tp.order_sum)*0.25 as order_processing_fee,
                            sum(tp.courier_order_sum) as courier_order_sum,
                            sum(tp.tip_sum) as courier_tips_sum,
                            (sum(tp.courier_order_sum) + sum(tp.tip_sum)) * 0.95 as courier_reward_sum
                        from tmp_prc as tp 
                        inner join dds.dm_couriers dc on tp.courier_id=dc.id 
                        inner join dds.dm_orders do2 on tp.order_id = do2.id 
                        inner join dds.dm_timestamps dt on do2.timestamp_id =dt.id 
                        where do2.order_status = 'CLOSED' and dt.year = %(last_year)s and dt.month = %(last_month)s
                        group by dc.courier_id,
                            dc.courier_name,
                            dt.year,
                            dt.month
                    ORDER BY dc.courier_id asc --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    ;
                """, {
                    "last_year": dt.year,
                    "last_month": dt.month
                }
            )
            objs = cur.fetchall()
        return objs


class CLDestRepository:

    def insert_obj(self, conn: Connection, obj: CLObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO cdm.dm_courier_ledger(
                        courier_id,
                        courier_name,
                        settlement_year,
                        settlement_month,
                        orders_count,
                        orders_total_sum,
                        rate_avg,
                        order_processing_fee,
                        courier_order_sum,
                        courier_tips_sum,
                        courier_reward_sum)
                    VALUES (%(courier_id)s, 
                        %(courier_name)s, 
                        %(settlement_year)s, 
                        %(settlement_month)s, 
                        %(orders_count)s,
                        %(orders_total_sum)s,
                        %(rate_avg)s,
                        %(order_processing_fee)s,
                        %(courier_order_sum)s,
                        %(courier_tips_sum)s,
                        %(courier_reward_sum)s)
                    on conflict on constraint dm_courier_ledger_uq
                    do update set 
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        rate_avg = EXCLUDED.rate_avg,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        courier_order_sum = EXCLUDED.courier_order_sum,
                        courier_tips_sum = EXCLUDED.courier_tips_sum,
                        courier_reward_sum = EXCLUDED.courier_reward_sum;
                """,
                {
                    "courier_id": obj.courier_id,
                    "courier_name": obj.courier_name,
                    "settlement_year": obj.settlement_year,
                    "settlement_month": obj.settlement_month,
                    "orders_count": obj.orders_count,
                    "orders_total_sum": obj.orders_total_sum,
                    "rate_avg": obj.rate_avg,
                    "order_processing_fee": obj.order_processing_fee,
                    "courier_order_sum": obj.courier_order_sum,
                    "courier_tips_sum": obj.courier_tips_sum,
                    "courier_reward_sum": obj.courier_reward_sum
                },
            )

class CLLoader:

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = CLOriginRepository(pg_dest)
        self.stg = CLDestRepository()
        self.settings_repository = CdmEtlSettingsRepository()
        self.log = log

    def load_obj(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            load_queue = self.origin.list_obj()

            # Сохраняем объекты в базу dwh.
            for obj in load_queue:
                self.stg.insert_obj(conn, obj)





