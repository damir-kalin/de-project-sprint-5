import requests
import json
from airflow.hooks.http_hook import HttpHook
from logging import Logger
from datetime import datetime, timedelta
from lib.dict_util import str2json, json2str
from psycopg import Connection
from lib import PgConnect
from examples.stg import EtlSetting, StgEtlSettingsRepository
from airflow.models.dagrun import DagRun


class DeliveriesOriginRepository:
    def get_data(self,  api_url: str, headers: dict, params={}) -> list:
        url = f"{api_url}/deliveries"
        response = requests.get(url, headers=headers, params=params)
        return str2json(response.content)

class DeliveryDestRepository:
    def insert_data(self, conn: Connection, delivery: dict) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO stg.apisystem_deliveries(object_id, object_value, update_ts)
                    VALUES (%(object_id)s, %(object_value)s, %(update_ts)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts;
                """,
                {
                    "object_id": delivery['order_id'],
                    "object_value": json2str(delivery),
                    "update_ts": delivery['order_ts']
                },
            )

class DeliveryLoader:
    LAST_LOADED_DT_KEY = "last_loaded_dt"
    BATCH_LIMIT = 50
    WF_KEY = f"example_apisystem_deliveries_origin_to_stg_workflow"

    def __init__(self, date:str, connection_name: str, nickname: str, cohort: str, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.dest = DeliveryDestRepository()
        self.origin = DeliveriesOriginRepository()
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
            print(self.date)
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        self.LAST_LOADED_DT_KEY: self.date
                    }
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_DT_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"starting to load from last checkpoint: {last_loaded_ts}")

            while True:
                params = {'sort_field': 'date', 'limit':self.BATCH_LIMIT, 'offset':offset, 'from': last_loaded_ts, 'to': last_loaded_ts + timedelta(days=1)}
                self.log.info(f"Parameters for retrieving data from the API - {params}")
                deliveries = self.origin.get_data(self.api_url, self.headers, params)
                self.log.info(f'Get data from api (count_rows - {len(deliveries)}).')
                
                if deliveries != None and len(deliveries) > 0:
                    self.log.info(deliveries)
                    i = 0
                    for delivery in deliveries:
                        self.dest.insert_data(conn, delivery)
                        i += 1
                    self.log.info(f'Processed {i} rows.')
                    offset += len(deliveries)
                else:
                    break
            
            wf_setting.workflow_settings[self.LAST_LOADED_DT_KEY] = str(last_loaded_ts + timedelta(days=1))
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            