import logging

import pendulum
from airflow.decorators import dag, task
from examples.dds.users_loader import UserLoader
from examples.dds.restaurants_loader import RestaurantLoader
from examples.dds.ts_loader import TSLoader
from examples.dds.products_loader import ProductLoader
from examples.dds.orders_loader import OrderLoader
from examples.dds.product_sales_loader import ProductSaleLoader
from examples.dds.couriers_loader import CourierLoader
from examples.dds.deliveriers_loader import DeliveryLoader
from examples.dds.delivery_of_orders_loader import DeliveryOfOrdersLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def stg_to_dds_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")



    # Объявляем таск, который загружает данные.
    @task(task_id="users_load")
    def load_users():
        # создаем экземпляр класса, в котором реализована логика.
        rest_loader = UserLoader(dwh_pg_connect, log)
        rest_loader.load_users()  # Вызываем функцию, которая перельет данные.
    
    @task(task_id="restaurants_load")
    def load_restaurants():
        rest_loader = RestaurantLoader(dwh_pg_connect, log)
        rest_loader.load_restaurants()

    @task(task_id="ts_load")
    def load_ts():
        rest_loader = TSLoader(dwh_pg_connect, log)
        rest_loader.load_ts()

    @task(task_id="products_load")
    def load_products():
        rest_loader = ProductLoader(dwh_pg_connect, log)
        rest_loader.load_products()

    @task(task_id="orders_load")
    def load_orders():
        rest_loader = OrderLoader(dwh_pg_connect, log)
        rest_loader.load_orders()

    @task(task_id="product_sales_load")
    def load_product_sales():
        rest_loader = ProductSaleLoader(dwh_pg_connect, log)
        rest_loader.load_product_sales()

    @task(task_id="couriers_load")
    def load_couriers():
        rest_loader = CourierLoader(dwh_pg_connect, log)
        rest_loader.load_couriers()
    
    @task(task_id="deliveriers_load")
    def load_deliveriers():
        rest_loader = DeliveryLoader(dwh_pg_connect, log)
        rest_loader.load_deliveriers()

    @task(task_id='delivery_of_orders_load')
    def load_delivery_of_orders():
        rest_loader = DeliveryOfOrdersLoader(dwh_pg_connect, log)
        rest_loader.load_delivery_of_orders()
    
    # Инициализируем объявленные таски.
    users_dict = load_users()
    restaurants_dict = load_restaurants()
    ts_dict = load_ts()
    products_dict = load_products()
    orders_dict = load_orders()
    couriers_dict = load_couriers()
    product_sales_dict = load_product_sales()
    deliveriers_dict = load_deliveriers()
    delivery_of_orders_dict = load_delivery_of_orders()

    # Далее задаем последовательность выполнения тасков.
    # Т.к. таск один, просто обозначим его здесь.
    [users_dict, restaurants_dict, ts_dict, couriers_dict]
    restaurants_dict >> products_dict
    [users_dict, restaurants_dict, ts_dict] >> orders_dict
    [products_dict, orders_dict] >> product_sales_dict  # type: ignore
    ts_dict >> deliveriers_dict
    [couriers_dict, deliveriers_dict, orders_dict] >> delivery_of_orders_dict

stg_to_dds = stg_to_dds_dag()
