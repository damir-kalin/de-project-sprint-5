from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.dummy import DummyOperator
from airflow.models.variable import Variable
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator

from examples.stg.order_system_dag.pg_saver import PgSaver
from examples.stg.order_system_dag.loader import Loader
from examples.stg.order_system_dag.reader import Reader

from examples.stg.bonus_system_ranks_dag.ranks_loader import RankLoader
from examples.stg.bonus_system_ranks_dag.users_loader import UserLoader
from examples.stg.bonus_system_ranks_dag.events_loader import EventLoader

from examples.stg.api_system_dag.couriers_loader import CourierLoader
from examples.stg.api_system_dag.deliveries_loader import DeliveryLoader


from lib import ConnectionBuilder, MongoConnect

import logging
from datetime import datetime, timedelta

business_dt = '{{ ds }}'

log = logging.getLogger(__name__)

api_conn_id = "PG_API_CONNECTION"
dwh_pg_conn_id = "PG_WAREHOUSE_CONNECTION"
origin_pg_conn_id = "PG_ORIGIN_BONUS_SYSTEM_CONNECTION"
dwh_pg_connect = ConnectionBuilder.pg_conn(dwh_pg_conn_id)
origin_pg_connect = ConnectionBuilder.pg_conn(origin_pg_conn_id)


cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
db_user = Variable.get("MONGO_DB_USER")
db_pw = Variable.get("MONGO_DB_PASSWORD")
rs = Variable.get("MONGO_DB_REPLICA_SET")
db = Variable.get("MONGO_DB_DATABASE_NAME")
host = Variable.get("MONGO_DB_HOST")


NICKNAME = "damir-kalinin7"
COHORT = "22"

def ordersystem_load(table_name:str, collection_name:str):
    pg_saver = PgSaver()
    mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
    collection_reader = Reader(collection_name, mongo_connect)
    loader = Loader(table_name, collection_name, collection_reader, dwh_pg_connect, pg_saver, log)
    loader.run_copy()

def bonussystem_load(name:str, dt):
    switch = {'ranks': RankLoader, 'users': UserLoader, 'events':EventLoader}
    rest_loader = switch[name](dt, origin_pg_connect, dwh_pg_connect, log)
    rest_loader.load()

def apisystem_load(name:str, dt):
    switch = {'couriers': CourierLoader, 'deliveries': DeliveryLoader}
    read_data = switch[name](dt, api_conn_id, NICKNAME, COHORT, dwh_pg_connect, log)
    read_data.load()

def is_tenth_day(date):
    dt = datetime.strptime(date, '%Y-%m-%d').day
    logging.info(f'Day number - {dt}')
    return dt == 10

args = {
    "owner": "damir_sibgatov",
    'email': ['sibgatov.damir@mail.ru'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2
}

dag = DAG(
        'project_sprint_5',
        default_args=args,
        description='Provide default dag for project_sprint5',
        catchup=True,
        start_date=datetime.today() - timedelta(days=8),
        end_date=datetime.today(),
        schedule_interval='@daily',
        max_active_runs = 1
) 

truncate_table_stg = PostgresOperator(
        task_id='truncate_table_stg',
        postgres_conn_id=dwh_pg_conn_id,
        sql='sql/truncate_table/stg.sql',
        dag=dag
        )

ordersystem_load_list = []
for name in ['restaurants', 'users', 'orders']:
    ordersystem_load_list.append(PythonOperator(
    task_id=f'ordersystem_load_{name}',
    python_callable=ordersystem_load,
    op_kwargs={
        'table_name': f'ordersystem_{name}', 
        'collection_name': name
    },
    dag=dag)
    )
    
bonussystem_load_list = []
for name in ['ranks', 'users', 'events']:
    bonussystem_load_list.append(PythonOperator(
    task_id=f'bonussystem_load_{name}',
    python_callable=bonussystem_load,
    op_kwargs={
        'name': name,
        'dt': business_dt
    },
    dag=dag)
    )

apisystem_load_list = []
for name in ['couriers', 'deliveries']:
    apisystem_load_list.append(PythonOperator(
    task_id=f'apisystem_load_{name}',
    python_callable=apisystem_load,
    op_kwargs={
        'name': name,
        'dt': business_dt
    },
    dag=dag)
    )

dummy_connect_list_to_list_1 =  DummyOperator(
    task_id = 'dummy_connect_1'
)

dummy_connect_list_to_list_2 =  DummyOperator(
    task_id = 'dummy_connect_2'
)

dummy_connect_list_to_list_3 =  DummyOperator(
    task_id = 'dummy_connect_3'
)

update_dm_users = PostgresOperator(
        task_id='update_dm_users',
        postgres_conn_id=dwh_pg_conn_id,
        sql='sql/update_table/dds.update_dm_users.sql',
        dag=dag
        )

update_dm_timestamps = PostgresOperator(
        task_id='update_dm_timestamps',
        postgres_conn_id=dwh_pg_conn_id,
        sql='sql/update_table/dds.update_dm_timestamps.sql',
        dag=dag
        )

update_dm_restaurants = PostgresOperator(
        task_id='update_dm_restaurants',
        postgres_conn_id=dwh_pg_conn_id,
        sql='sql/update_table/dds.update_dm_restaurants.sql',
        dag=dag
        )

update_dm_couriers = PostgresOperator(
        task_id='update_dm_couriers',
        postgres_conn_id=dwh_pg_conn_id,
        sql='sql/update_table/dds.update_dm_couriers.sql',
        dag=dag
        )

update_dm_deliveries = PostgresOperator(
        task_id='update_dm_deliveries',
        postgres_conn_id=dwh_pg_conn_id,
        sql='sql/update_table/dds.update_dm_deliveries.sql',
        dag=dag
        )

update_dm_products = PostgresOperator(
        task_id='update_dm_products',
        postgres_conn_id=dwh_pg_conn_id,
        sql='sql/update_table/dds.update_dm_products.sql',
        dag=dag
        )

update_dm_orders = PostgresOperator(
        task_id='update_dm_orders',
        postgres_conn_id=dwh_pg_conn_id,
        sql='sql/update_table/dds.update_dm_orders.sql',
        dag=dag
        )

update_fct_delivery_of_orders = PostgresOperator(
        task_id='update_fct_delivery_of_orders',
        postgres_conn_id=dwh_pg_conn_id,
        sql='sql/update_table/dds.update_fct_delivery_of_orders.sql',
        dag=dag
        )

update_fct_product_sales = PostgresOperator(
        task_id='update_fct_product_sales',
        postgres_conn_id=dwh_pg_conn_id,
        sql='sql/update_table/dds.update_fct_product_sales.sql',
        dag=dag
        )

update_dm_settlement_report = PostgresOperator(
        task_id='update_dm_settlement_report',
        postgres_conn_id=dwh_pg_conn_id,
        sql='sql/update_table/cdm.update_dm_settlement_report.sql',
        dag=dag
        )

tenth_day = ShortCircuitOperator(
    task_id='is_tenth_day',
    python_callable=is_tenth_day,
    op_kwargs = {"date": business_dt}
)

update_dm_courier_ledger = PostgresOperator(
        task_id='update_dm_courier_ledger',
        postgres_conn_id=dwh_pg_conn_id,
        sql='sql/update_table/cdm.update_dm_courier_ledger.sql',
        parameters={"date": {business_dt}},
        dag=dag
        )

truncate_table_stg >> ordersystem_load_list 
truncate_table_stg >> bonussystem_load_list
truncate_table_stg >> apisystem_load_list
bonussystem_load_list >> dummy_connect_list_to_list_3
ordersystem_load_list >> dummy_connect_list_to_list_1 >> [update_dm_users, update_dm_timestamps, update_dm_restaurants]
apisystem_load_list >> dummy_connect_list_to_list_2 >> [update_dm_couriers, update_dm_deliveries]
[dummy_connect_list_to_list_1, update_dm_restaurants] >> update_dm_products
[dummy_connect_list_to_list_1, update_dm_restaurants, update_dm_timestamps, update_dm_users] >> update_dm_orders
[dummy_connect_list_to_list_2, update_dm_orders, update_dm_couriers, update_dm_deliveries] >> update_fct_delivery_of_orders
[dummy_connect_list_to_list_1, update_dm_orders, update_dm_products, dummy_connect_list_to_list_3] >> update_fct_product_sales
[update_dm_orders, update_dm_restaurants, update_dm_timestamps, update_fct_product_sales] >> update_dm_settlement_report
[update_fct_product_sales, update_fct_delivery_of_orders, update_dm_couriers, update_dm_orders, update_dm_timestamps] >> tenth_day
tenth_day >> update_dm_courier_ledger

