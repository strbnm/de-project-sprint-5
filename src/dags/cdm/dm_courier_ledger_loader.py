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


class CourierLedgerObj(BaseModel):
    courier_id: str
    courier_name: str
    settlement_year: int
    settlement_month: int
    orders_count: int
    orders_total_sum: Decimal
    rate_avg: Decimal
    order_processing_fee: Decimal
    courier_order_sum: Decimal
    courier_tips_sum: Decimal
    courier_reward_sum: Decimal
    id: int


class LoadThreshold(BaseModel):
    courier_id: int
    settlement_year: int
    settlement_month: int


class CourierLedgerSourceRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_courier_ledger(
        self, load_threshold: LoadThreshold, batch_size: int
    ) -> List[CourierLedgerObj]:
        with self._db.client().cursor(row_factory=class_row(CourierLedgerObj)) as cur:
            cur.execute(
                """
                WITH 
                delivery_month AS (
                    SELECT
                        dc.id,
                        dc.courier_id,
                        dc.courier_name,
                        dt."year" settlement_year,
                        dt."month" settlement_month,
                        count(do2.id) orders_count,
                        sum(dd.delivery_sum) orders_total_sum,
                        avg(dd.rate)::numeric(14,2) rate_avg,
                        sum(dd.tip_sum) courier_tips_sum
                    FROM dds.dm_orders do2 
                    JOIN dds.dm_deliveries dd ON dd.order_id = do2.id
                    JOIN dds.dm_couriers dc ON dc.id = do2.courier_id
                    JOIN dds.dm_timestamps dt ON do2.timestamp_id = dt.id
                    WHERE dc.id > %(courier_id)s 
                    AND dt."year" >= %(settlement_year)s
                    AND dt."month" >= %(settlement_month)s
                    GROUP BY dc.id,dc.courier_id,dc.courier_name,dt."year",dt."month"
                    ORDER BY dc.id, dt."year", dt."month"),
                courier_month_total AS (
                    SELECT 
                        courier_id,
                        courier_name,
                        settlement_year,
                        settlement_month,
                        orders_count,
                        orders_total_sum,
                        rate_avg,
                        orders_total_sum * 0.25 AS order_processing_fee,
                        CASE 
                            WHEN rate_avg < 4 THEN GREATEST(orders_total_sum * 0.05, 100.0)
                            WHEN rate_avg >= 4 AND rate_avg < 4.5 THEN GREATEST(orders_total_sum * 0.07, 150.0)
                            WHEN rate_avg >= 4.5 AND rate_avg < 4.9 THEN GREATEST(orders_total_sum * 0.08, 175.0)
                            WHEN rate_avg >= 4.9 THEN GREATEST(orders_total_sum * 0.1, 200.0)
                        END AS courier_order_sum,
                        courier_tips_sum,
                        id
                    FROM delivery_month)
                SELECT *,
                       courier_order_sum + courier_tips_sum * 0.95 AS courier_reward_sum
                FROM courier_month_total;
                """,
                {
                    "courier_id": load_threshold.courier_id,
                    "settlement_year": load_threshold.settlement_year,
                    "settlement_month": load_threshold.settlement_month,
                },
            )
            while True:
                rows = cur.fetchmany(size=batch_size)
                if not rows:
                    break
                yield from rows


class CourierLedgerDestRepository:
    def insert_courier_ledger(
        self, conn: Connection, courier_ledger: List[CourierLedgerObj]
    ) -> None:
        with conn.cursor() as cur:
            cur.execute("BEGIN;")
            cur.executemany(
                """
                    INSERT INTO cdm.dm_courier_ledger(
                       courier_id,
                       courier_name,
                       settlement_year,
                       settlement_month,
                       orders_count,
                       orders_total_sum,
                       rate_avg,
                       order_processing_fee,
                       courier_order_sum,
                       courier_tips_sum,
                       courier_reward_sum
                    )
                    VALUES (%(courier_id)s,
                            %(courier_name)s,
                            %(settlement_year)s,
                            %(settlement_month)s,
                            %(orders_count)s,
                            %(orders_total_sum)s,
                            %(rate_avg)s,
                            %(order_processing_fee)s,
                            %(courier_order_sum)s,
                            %(courier_tips_sum)s,
                            %(courier_reward_sum)s
                            )
                    ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE
                    SET courier_name = excluded.courier_name,
                        orders_count = excluded.orders_count,
                        orders_total_sum = excluded.orders_total_sum,
                        rate_avg = excluded.rate_avg,
                        order_processing_fee = excluded.order_processing_fee,
                        courier_order_sum = excluded.courier_order_sum,
                        courier_tips_sum = excluded.courier_tips_sum,
                        courier_reward_sum = excluded.courier_reward_sum;
                """,
                [
                    courier_record.dict(exclude={"id"})
                    for courier_record in courier_ledger
                ],
            )
            cur.execute("COMMIT;")


class CourierLedgerLoader:
    WF_KEY = "courier_ladger_dds_to_cdm_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_ids"
    BATCH_SIZE = 50

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = CourierLedgerSourceRepository(pg_origin)
        self.dds = CourierLedgerDestRepository()
        self.settings_repository = CdmEtlSettingsRepository()
        self.log = log

    def load_courier_ledger(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        self.LAST_LOADED_ID_KEY: LoadThreshold(
                            courier_id=-1, settlement_year=2022, settlement_month=1
                        ).json()
                    },
                )

            last_loaded_json = json.loads(
                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            )
            last_loaded = LoadThreshold.parse_obj(last_loaded_json)
            self.log.info(f"starting to load from last checkpoint: {last_loaded}")

            data = self.stg.list_courier_ledger(last_loaded, batch_size=self.BATCH_SIZE)
            rows = []
            count, total = 0, 0
            for row in data:
                rows.append(row)
                count += 1
                if count >= self.BATCH_SIZE:
                    self.dds.insert_courier_ledger(conn, courier_ledger=rows)
                    total += len(rows)
                    self.log.info(
                        f"{len(rows)} records are written to the dm_courier_ledger table"
                    )
                    wf_setting.workflow_settings[
                        self.LAST_LOADED_ID_KEY
                    ] = LoadThreshold(
                        courier_id=max([t.id for t in rows]),
                        settlement_year=max([t.settlement_year for t in rows]),
                        settlement_month=max([t.settlement_month for t in rows]),
                    ).json()
                    wf_setting_json = json2str(wf_setting.workflow_settings)
                    self.settings_repository.save_setting(
                        conn, wf_setting.workflow_key, wf_setting_json
                    )
                    rows = []
                    count = 0
            if rows:
                self.dds.insert_courier_ledger(conn, courier_ledger=rows)
                self.log.info(
                    f"{len(rows)} records are written to the dm_courier_ledger table"
                )

                total += len(rows)
                self.log.info(
                    f"There are total {total} records written to table dm_courier_ledger"
                )

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = LoadThreshold(
                    courier_id=max([t.id for t in rows]),
                    settlement_year=max([t.settlement_year for t in rows]),
                    settlement_month=max([t.settlement_month for t in rows]),
                ).json()
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(
                    conn, wf_setting.workflow_key, wf_setting_json
                )

                self.log.info(
                    f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}"
                )
