import logging

import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

from dds.couriers_loader import CouriersLoader
from dds.deliveries_loader import DeliveriesLoader
from dds.fct_product_sales_loader import ProductSalesLoader
from dds.orders_loader import OrdersLoader
from dds.products_loader import ProductsLoader
from dds.restaurants_loader import RestaurantsLoader
from dds.timestamps_loader import TimestampsLoader
from dds.users_loader import UserLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    dag_id="dds_load_dag",
    schedule_interval=None,  # Запускается по триггеру из основного дага загрузки DWH
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=["sprint5", "stg", "dds"],
    is_paused_upon_creation=True,
)
def dds_load_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="dm_users_load")
    def load_dm_users():
        loader = UserLoader(dwh_pg_connect, dwh_pg_connect, log)
        loader.load_users()

    @task(task_id="dm_restaurants_load")
    def load_dm_restaurants():
        loader = RestaurantsLoader(dwh_pg_connect, dwh_pg_connect, log)
        loader.load_restaurants()

    @task(task_id="dm_timestamps_load")
    def load_dm_timestamps():
        loader = TimestampsLoader(dwh_pg_connect, dwh_pg_connect, log)
        loader.load_timestamps()

    @task(task_id="dm_products_load")
    def load_dm_products():
        loader = ProductsLoader(dwh_pg_connect, dwh_pg_connect, log)
        loader.load_products()

    @task(task_id="dm_orders_load")
    def load_dm_orders():
        loader = OrdersLoader(dwh_pg_connect, dwh_pg_connect, log)
        loader.load_orders()

    @task(task_id="fct_product_sales_load")
    def load_fct_product_sales():
        loader = ProductSalesLoader(dwh_pg_connect, dwh_pg_connect, log)
        loader.load_product_sales()

    @task(task_id="dm_couriers_load")
    def load_dm_couriers():
        loader = CouriersLoader(dwh_pg_connect, dwh_pg_connect, log)
        loader.load_couriers()

    @task(task_id="dm_deliveries_load")
    def load_dm_deliveries():
        loader = DeliveriesLoader(dwh_pg_connect, dwh_pg_connect, log)
        loader.load_deliveries()

    users_dds = load_dm_users()
    restaurants_dds = load_dm_restaurants()
    timestamps_dds = load_dm_timestamps()
    products_dds = load_dm_products()
    orders_dds = load_dm_orders()
    product_sales_dds = load_fct_product_sales()
    couriers_dds = load_dm_couriers()
    deliveries_dds = load_dm_deliveries()

    start = EmptyOperator(task_id="start_load")
    stop = EmptyOperator(task_id="stop_load")

    (
        start
        >> [users_dds, restaurants_dds, timestamps_dds, couriers_dds]
        >> products_dds
        >> orders_dds
        >> deliveries_dds
        >> product_sales_dds
        >> stop
    )


dds_load = dds_load_dag()
