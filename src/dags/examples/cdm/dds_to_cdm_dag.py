import logging

import pendulum
from airflow.decorators import dag, task
from examples.cdm.dm_settlement_report_loader import Loader
from examples.cdm.dm_courier_ledger_loader import CLLoader

from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def dds_to_cdm_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")



    # Объявляем таск, который загружает данные.
    @task(task_id="dm_setlement_report_loader")
    def load_dm_setlement_report():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = Loader(dwh_pg_connect, log)
        rest_loader.load_obj()  # Вызываем функцию, которая перельет данные.

    @task(task_id='dm_courier_ledger_loader')
    def load_dm_courier_ledger():
        rest_loader = CLLoader(dwh_pg_connect, log)
        rest_loader.load_obj()

    # Инициализируем объявленные таски.
    dm_setlement_report_dict = load_dm_setlement_report()
    dm_courier_ledger_dict = load_dm_courier_ledger()
    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    [dm_setlement_report_dict, dm_courier_ledger_dict]


dds_to_cdm = dds_to_cdm_dag()
