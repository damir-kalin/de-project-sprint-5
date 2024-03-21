import requests
import json
from airflow.hooks.http_hook import HttpHook
from logging import Logger
from datetime import datetime
from lib.dict_util import str2json, json2str
from psycopg import Connection
from lib import PgConnect
from examples.stg import EtlSetting, StgEtlSettingsRepository

class CourierOriginRepository:
    def get_data(self,  api_url: str, headers: dict,params={}) -> list:
        url = f"{api_url}/couriers"
        response = requests.get(url, headers=headers, params=params)
        return str2json(response.content)
        

class CourierDestRepository:
    def insert_courier(self, conn: Connection, courier: dict) -> None:
        with conn.cursor() as cur:
            cur.execute(
                    """
                        INSERT INTO stg.apisystem_couriers(object_id, object_value, update_ts)
                        VALUES (%(object_id)s, %(object_value)s, %(update_ts)s)
                        ON CONFLICT (object_id) DO UPDATE
                        SET
                            object_value = EXCLUDED.object_value,
                            update_ts = EXCLUDED.update_ts;
                    """,
                    {
                        "object_id": courier['_id'],
                        "object_value": json2str(courier),
                        "update_ts": datetime.now()
                    },
                )

class CourierLoader:
    LAST_LOADED_OFFSET_KEY = "last_loaded_offset"
    BATCH_LIMIT = 50
    WF_KEY = f"example_apisystem_couriers_origin_to_stg_workflow"

    def __init__(self, date:str,  connection_name: str, nickname: str, cohort: str, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.dest = CourierDestRepository()
        self.origin = CourierOriginRepository()
        self.log = log
        hhtp_conn = HttpHook.get_connection(connection_name)
        api_key = hhtp_conn.extra_dejson.get('api_key')
        self.api_url = hhtp_conn.host
        self.headers = {
                'X-Nickname': nickname,
                'X-Cohort': cohort,
                'X-API-KEY': api_key
            }
        self.log = log
        self.settings_repository = StgEtlSettingsRepository()
        self.date = date + ' 00:00:00'

    def load(self):
        with self.pg_dest.connection() as conn:
            offset = 0
            while True:
                params = {'sort_field': 'date', 'limit':self.BATCH_LIMIT, 'offset':offset}                
                
                couriers = self.origin.get_data(self.api_url, self.headers, params)
                self.log.info(f'Get data from api (count_rows - {len(couriers)}).')
                
                if couriers != None and len(couriers)>0:
                    self.log.info(couriers)
                    i = 0
                    for courier in couriers:
                        self.dest.insert_courier(conn, courier)
                        i += 1
                    self.log.info(f'Processed {i} rows.')
                    offset += len(couriers)
                else:
                    break