
from examples.stg.api_system_dag.couriers_loader import CourierLoader
from examples.stg.api_system_dag.deliveries_loader import DeliveryLoader
import logging
import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder

log = logging.getLogger(__name__)

api_con_id = 'PG_API_CONNECTION'
NICKNAME = "damir-kalinin7"
COHORT = "22"

# read_data = Reader(api_con_id, NICKNAME, COHORT, log)

# read_data.get_data('restaurants')

@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def api_system_to_stg():
    

    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id = 'get_couriers')
    def get_couriers():
        read_data = CourierLoader(api_con_id, NICKNAME, COHORT, dwh_pg_connect, log)
        read_data.load_couriers()

    @task(task_id = 'get_deliveries')
    def get_deliveries():
        read_data = DeliveryLoader(api_con_id, NICKNAME, COHORT, dwh_pg_connect, log)
        read_data.load_delivery()

    get_couriers_dict = get_couriers()

    get_deliveries_dict = get_deliveries()

    [get_couriers_dict, get_deliveries_dict]

api = api_system_to_stg()