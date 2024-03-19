from logging import Logger
from typing import List

from examples.dds import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from datetime import datetime, date, time


class TSObj(BaseModel):
    id: int
    ts: datetime
    year: int
    month: int
    day: int
    date: date
    time: time


class TSOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_ts(self, user_threshold: int, limit: int) -> List[TSObj]:
        with self._db.client().cursor(row_factory=class_row(TSObj)) as cur:
            cur.execute(
                """
                    with tmp_dates as (
                        select
                        id,
                        (object_value::json->>'date')::timestamp as ts
                        from stg.ordersystem_orders oo 
                    )
                    select
                        id,
                        ts,
                        date_part('year', ts) as year,
                        date_part('month', ts) as month,
                        date_part('day', ts) as day,
                        ts::date as date,
                        ts::time as time
                    from tmp_dates
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id asc --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    limit %(limit)s
                    ;
                """, {
                    "threshold": user_threshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class TSDestRepository:

    def insert_ts(self, conn: Connection, ts: TSObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, time, date)
                    VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(time)s, %(date)s)
                    ON CONFLICT (ts) DO NOTHING;
                """,
                {
                    "ts": ts.ts,
                    "year": ts.year,
                    "month": ts.month,
                    "day": ts.day,
                    "time": ts.time,
                    "date": ts.date
                },
            )

class TSLoader:
    WF_KEY = "example_ts_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 500

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = TSOriginRepository(pg_dest)
        self.stg = TSDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_ts(self):
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
            load_queue = self.origin.list_ts(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} ts to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for ts in sorted(load_queue, key=lambda x: x.ts, reverse=True):
                #self.log.info(f"---------------{user["object_value"]}")
                self.stg.insert_ts(conn, ts)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
