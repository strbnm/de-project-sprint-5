import json
from datetime import date
from decimal import Decimal
from logging import Logger
from typing import List

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from cdm.cdm_settings_repository import CdmEtlSettingsRepository
from dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting
from lib import PgConnect
from lib.dict_util import json2str


class SattlementReportObj(BaseModel):
    restaurant_id: int
    restaurant_name: str
    settlement_date: date
    orders_count: int
    orders_total_sum: Decimal
    orders_bonus_payment_sum: Decimal
    orders_bonus_granted_sum: Decimal
    order_processing_fee: Decimal
    restaurant_reward_sum: Decimal


class LoadThreshold(BaseModel):
    restaurant_id: int
    settlement_date: date


class SattlementReportSourceRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_sattlement_reports(
        self, load_threshold: LoadThreshold, batch_size: int
    ) -> List[SattlementReportObj]:
        with self._db.client().cursor(
            row_factory=class_row(SattlementReportObj)
        ) as cur:
            cur.execute(
                """
                WITH tmp_table as (SELECT
                        dmr.id as restaurant_id,
                        dmr.restaurant_name,
                        dmts.date as settlement_date,
                        count(distinct fps.order_id) AS orders_count,
                        sum(fps.total_sum) AS orders_total_sum,
                        sum(fps.bonus_payment) AS orders_bonus_payment_sum,
                        sum(fps.bonus_grant) AS orders_bonus_granted_sum
                    FROM dds.fct_product_sales fps
                    LEFT JOIN dds.dm_orders dmo ON fps.order_id = dmo.id
                    LEFT JOIN dds.dm_timestamps dmts ON dmo.timestamp_id = dmts.id
                    LEFT JOIN dds.dm_restaurants dmr ON dmo.restaurant_id = dmr.id
                    WHERE dmo.order_status = 'CLOSED'
                        AND dmts.date > %(settlement_date)s
                        AND dmr.id > %(restaurant_id)s
                    GROUP BY dmr.id, dmr.restaurant_name, dmts.date)
                SELECT *,
                       orders_total_sum * 0.25 as order_processing_fee,
                       orders_total_sum - (orders_total_sum * 0.25) - orders_bonus_payment_sum as restaurant_reward_sum
                FROM tmp_table
                """,
                {
                    "restaurant_id": load_threshold.restaurant_id,
                    "settlement_date": load_threshold.settlement_date,
                },
            )
            while True:
                rows = cur.fetchmany(size=batch_size)
                if not rows:
                    break
                yield from rows


class SattlementReportDestRepository:
    def insert_sattlement_reports(
        self, conn: Connection, sattlement_reports: List[SattlementReportObj]
    ) -> None:
        with conn.cursor() as cur:
            cur.execute("BEGIN;")
            cur.executemany(
                """
                    INSERT INTO cdm.dm_settlement_report(
                        restaurant_id,
                        restaurant_name,
                        settlement_date,
                        orders_count, 
                        orders_total_sum,
                        orders_bonus_payment_sum,
                        orders_bonus_granted_sum,
                        order_processing_fee,
                        restaurant_reward_sum
                    )
                    VALUES (%(restaurant_id)s,
                            %(restaurant_name)s,
                            %(settlement_date)s,
                             %(orders_count)s,
                             %(orders_total_sum)s,
                             %(orders_bonus_payment_sum)s,
                             %(orders_bonus_granted_sum)s,
                             %(order_processing_fee)s,
                             %(restaurant_reward_sum)s
                             )
                    ON CONFLICT (restaurant_id, settlement_date) DO UPDATE
                    SET restaurant_name = excluded.restaurant_name,
                        orders_count = excluded.orders_count,
                        orders_total_sum = excluded.orders_total_sum,
                        orders_bonus_payment_sum = excluded.orders_bonus_payment_sum,
                        orders_bonus_granted_sum = excluded.orders_bonus_granted_sum,
                        order_processing_fee = excluded.order_processing_fee,
                        restaurant_reward_sum = excluded.restaurant_reward_sum;
                """,
                [sattlement_report.dict() for sattlement_report in sattlement_reports],
            )
            cur.execute("COMMIT;")


class SattlementReportsLoader:
    WF_KEY = "sattlement_report_dds_to_cdm_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_ids"
    BATCH_SIZE = 500

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = SattlementReportSourceRepository(pg_origin)
        self.dds = SattlementReportDestRepository()
        self.settings_repository = CdmEtlSettingsRepository()
        self.log = log

    def load_sattlement_reports(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        self.LAST_LOADED_ID_KEY: LoadThreshold(
                            restaurant_id=-1, settlement_date=date(2022, 1, 1)
                        ).json()
                    },
                )

            last_loaded_json = json.loads(
                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            )
            last_loaded = LoadThreshold.parse_obj(last_loaded_json)
            self.log.info(f"starting to load from last checkpoint: {last_loaded}")

            data = self.stg.list_sattlement_reports(
                last_loaded, batch_size=self.BATCH_SIZE
            )
            rows = []
            count, total = 0, 0
            for row in data:
                rows.append(row)
                count += 1
                if count >= self.BATCH_SIZE:
                    self.dds.insert_sattlement_reports(conn, sattlement_reports=rows)
                    total += len(rows)
                    self.log.info(
                        f"{len(rows)} records are written to the dm_settlement_report table"
                    )
                    wf_setting.workflow_settings[
                        self.LAST_LOADED_ID_KEY
                    ] = LoadThreshold(
                        restaurant_id=max([t.restaurant_id for t in rows]),
                        settlement_date=max([t.settlement_date for t in rows]),
                    ).json()
                    wf_setting_json = json2str(wf_setting.workflow_settings)
                    self.settings_repository.save_setting(
                        conn, wf_setting.workflow_key, wf_setting_json
                    )
                    rows = []
                    count = 0
            if rows:
                self.dds.insert_sattlement_reports(conn, sattlement_reports=rows)
                self.log.info(
                    f"{len(rows)} records are written to the dm_settlement_report table"
                )

                total += len(rows)
                self.log.info(
                    f"There are total {total} records written to table dm_settlement_report"
                )

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = LoadThreshold(
                    restaurant_id=max([t.restaurant_id for t in rows]),
                    settlement_date=max([t.settlement_date for t in rows]),
                ).json()
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(
                    conn, wf_setting.workflow_key, wf_setting_json
                )

                self.log.info(
                    f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}"
                )
