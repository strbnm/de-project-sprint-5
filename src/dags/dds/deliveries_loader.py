from datetime import datetime
from decimal import Decimal
from logging import Logger
from typing import List

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting
from lib import PgConnect
from lib.dict_util import json2str


class DeliveryObj(BaseModel):
    order_id: int
    delivery_id: str
    rate: int
    delivery_sum: Decimal
    tip_sum: Decimal
    created_at: datetime


class DeliverySourceRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_deliveries(
        self, load_threshold: datetime, batch_size: int
    ) -> List[DeliveryObj]:
        with self._db.client().cursor(row_factory=class_row(DeliveryObj)) as cur:
            cur.execute(
                """
                    SELECT  do2.id AS order_id,
                            dsd.delivery_id,
                            dsd.rate,
                            dsd.sum AS delivery_sum,
                            dsd.tip_sum,
                            dsd.created_at
                    FROM stg.deliverysystem_delivery dsd
                    JOIN dds.dm_orders do2 ON dsd.order_id = do2.order_key
                    WHERE created_at > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY created_at; --Обязательна сортировка по created_at, т.к. created_at используем в качестве курсора. 
                """,
                {"threshold": load_threshold},
            )
            while True:
                rows = cur.fetchmany(size=batch_size)
                if not rows:
                    break
                yield from rows


class DeliveriesDestRepository:
    def insert_deliveries(
        self, conn: Connection, deliveries: List[DeliveryObj]
    ) -> None:
        with conn.cursor() as cur:
            cur.executemany(
                """
                    INSERT INTO dds.dm_deliveries(order_id, delivery_id, rate, delivery_sum, tip_sum)
                    VALUES (%(order_id)s, %(delivery_id)s, %(rate)s, %(delivery_sum)s, %(tip_sum)s)
                    ON CONFLICT (delivery_id) DO NOTHING;
                """,
                [delivery.dict(exclude={"created_at"}) for delivery in deliveries],
            )


class DeliveriesLoader:
    WF_KEY = "deliveries_stg_to_dds_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_SIZE = 50

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = DeliverySourceRepository(pg_origin)
        self.dds = DeliveriesDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_deliveries(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                    },
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"starting to load from last checkpoint: {last_loaded_ts}")

            data = self.stg.list_deliveries(last_loaded_ts, batch_size=self.BATCH_SIZE)
            rows = []
            count, total = 0, 0
            for row in data:
                rows.append(row)
                count += 1
                if count >= self.BATCH_SIZE:
                    self.dds.insert_deliveries(conn, deliveries=rows)
                    total += len(rows)
                    self.log.info(
                        f"{len(rows)} records are written to the dm_deliveries table"
                    )
                    wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max(
                        [t.created_at for t in rows]
                    )
                    wf_setting_json = json2str(wf_setting.workflow_settings)
                    self.settings_repository.save_setting(
                        conn, wf_setting.workflow_key, wf_setting_json
                    )
                    rows = []
                    count = 0
            if rows:
                self.dds.insert_deliveries(conn, deliveries=rows)
                self.log.info(
                    f"{len(rows)} records are written to the dm_deliveries table"
                )

                total += len(rows)
                self.log.info(
                    f'There are total {total} records written to table dm_deliveries"'
                )

                wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max(
                    [t.created_at for t in rows]
                )
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(
                    conn, wf_setting.workflow_key, wf_setting_json
                )

                self.log.info(
                    f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}"
                )
