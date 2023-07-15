import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.operators.empty import EmptyOperator

from stg.order_system_dag.order_system_loader import ObjLoader
from stg.order_system_dag.order_system_reader import ObjReader
from lib import ConnectionBuilder, MongoConnect

log = logging.getLogger(__name__)


@dag(
    dag_id="stg_order_system_load_dag",
    schedule_interval=None,  # Запускается по триггеру из основного дага загрузки DWH
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=["sprint5", "stg", "origin"],
    is_paused_upon_creation=True,
)
def stg_order_system_load_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task()
    def load_restaurants():
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        collection_reader = ObjReader(mongo_connect)
        loader = ObjLoader(collection_reader, dwh_pg_connect, log, "restaurants")
        loader.run_copy()

    @task()
    def load_users():
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        collection_reader = ObjReader(mongo_connect)
        loader = ObjLoader(collection_reader, dwh_pg_connect, log, "users")
        loader.run_copy()

    @task()
    def load_orders():
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
        collection_reader = ObjReader(mongo_connect)
        loader = ObjLoader(collection_reader, dwh_pg_connect, log, "orders")
        loader.run_copy()

    restaurant_loader = load_restaurants()
    user_loader = load_users()
    order_loader = load_orders()
    start = EmptyOperator(task_id="start_load_from_mongo_db")
    end = EmptyOperator(task_id="end_load_from_mongo_db")

    start >> [restaurant_loader, user_loader, order_loader] >> end


order_stg_dag = stg_order_system_load_dag()  # noqa
