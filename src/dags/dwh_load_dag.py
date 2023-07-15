import logging

import pendulum
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

log = logging.getLogger(__name__)


@dag(
    dag_id="dwh_load_dag",
    schedule_interval="0/15 * * * *",
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=["sprint5", "stg", "dds", "cdm"],
    is_paused_upon_creation=True,
)
def dwh_load_dag():
    start = EmptyOperator(task_id="start_load_dwh")
    stg_bonus_system_load = TriggerDagRunOperator(
        task_id="trigger_load_stg_bonus_system",
        trigger_dag_id="stg_bonus_system_load_dag",
        execution_date="{{ ts }}",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
    )
    stg_order_system_load = TriggerDagRunOperator(
        task_id="trigger_load_stg_order_system",
        trigger_dag_id="stg_order_system_load_dag",
        execution_date="{{ ts }}",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
    )
    dds_load = TriggerDagRunOperator(
        task_id="trigger_load_dds",
        trigger_dag_id="dds_load_dag",
        execution_date="{{ ts }}",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
    )
    cdm_load = TriggerDagRunOperator(
        task_id="trigger_load_cdm",
        trigger_dag_id="cdm_load_dag",
        execution_date="{{ ts }}",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=60,
    )
    end = EmptyOperator(task_id="end_load_dwh")

    (
        start
        >> [stg_bonus_system_load, stg_order_system_load]
        >> dds_load
        >> cdm_load
        >> end
    )


dwh_load = dwh_load_dag()
