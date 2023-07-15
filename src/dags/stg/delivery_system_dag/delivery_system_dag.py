import logging

import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.http.hooks.http import HttpHook

from lib import ConnectionBuilder
from stg.delivery_system_dag.courier_loader import CouriersLoader
from stg.delivery_system_dag.delivery_loader import DeliveriesLoader

log = logging.getLogger(__name__)


@dag(
    dag_id="stg_delivery_system_load_dag",
    schedule_interval=None,  # Запускается по триггеру из основного дага загрузки DWH
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=["sprint5", "stg", "origin", "delivery"],
    is_paused_upon_creation=True,
)
def stg_delivery_system_load_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    http_conn_id = HttpHook.get_connection("http_conn_id")

    @task(task_id="stg_couriers_load")
    def load_couriers():
        loader = CouriersLoader(http_conn_id, dwh_pg_connect, log)
        loader.load_couriers()

    @task(task_id="stg_deliveries_load")
    def load_deliveries():
        loader = DeliveriesLoader(http_conn_id, dwh_pg_connect, log)
        loader.load_deliveries()

    couriers_stg = load_couriers()
    deliveries_stg = load_deliveries()
    start = EmptyOperator(task_id="start_load_from_delivery_api")
    end = EmptyOperator(task_id="end_load_from_delivery_api")

    start >> [couriers_stg, deliveries_stg] >> end


delivery_load = stg_delivery_system_load_dag()
