import logging

import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

from stg.bonus_system_dag.outbox_loader import EventLoader
from stg.bonus_system_dag.ranks_loader import RankLoader
from stg.bonus_system_dag.users_loader import UserLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    dag_id="stg_bonus_system_load_dag",
    schedule_interval=None,  # Запускается по триггеру из основного дага загрузки DWH
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=["sprint5", "stg", "origin"],
    is_paused_upon_creation=True,
)
def stg_bonus_system_load_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    @task(task_id="ranks_load")
    def load_ranks():
        rest_loader = RankLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_ranks()

    @task(task_id="users_load")
    def load_users():
        rest_loader = UserLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_users()

    @task(task_id="events_load")
    def load_events():
        rest_loader = EventLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_events()

    users_dict = load_users()
    ranks_dict = load_ranks()
    events_dict = load_events()
    start = EmptyOperator(task_id="start_load")
    end = EmptyOperator(task_id="end_load")

    start >> [ranks_dict, users_dict, events_dict] >> end


stg_bonus_system_ranks_dag = stg_bonus_system_load_dag()
