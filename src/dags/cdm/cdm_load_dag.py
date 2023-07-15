import logging

import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

from cdm.dm_courier_ledger_loader import CourierLedgerLoader
from cdm.dm_settlement_report_loader import SattlementReportsLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    dag_id="cdm_load_dag",
    schedule_interval=None,  # Запускается по триггеру из основного дага загрузки DWH
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=["sprint5", "dds", "cdm"],
    is_paused_upon_creation=True,
)
def cdm_load_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="dm_settlement_report_load")
    def load_dm_settlement_report():
        loader = SattlementReportsLoader(dwh_pg_connect, dwh_pg_connect, log)
        loader.load_sattlement_reports()

    @task(task_id="dm_courier_ledger_load")
    def load_dm_courier_ledger():
        loader = CourierLedgerLoader(dwh_pg_connect, dwh_pg_connect, log)
        loader.load_courier_ledger()

    dm_settlement_report_load = load_dm_settlement_report()
    dm_courier_ledger_load = load_dm_courier_ledger()

    start = EmptyOperator(task_id="start_load_cdm")
    end = EmptyOperator(task_id="end_load_cdm")

    start >> [dm_settlement_report_load, dm_courier_ledger_load] >> end


cdm_load = cdm_load_dag()
