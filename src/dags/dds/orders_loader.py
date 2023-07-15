from logging import Logger
from typing import List, Optional

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting
from lib import PgConnect
from lib.dict_util import json2str


class OrderObj(BaseModel):
    user_id: int
    restaurant_id: int
    timestamp_id: int
    order_key: str
    order_status: str
    cursor_id: int
    courier_id: Optional[int] = None


class OrdersSourceRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_orders(self, load_threshold: str, batch_size: int) -> List[OrderObj]:
        with self._db.client().cursor(row_factory=class_row(OrderObj)) as cur:
            cur.execute(
                """
                WITH courier_info AS (
                    SELECT
                        dc.id AS courier_id,
                        dsd.order_id
                    FROM dds.dm_couriers dc
                    JOIN stg.deliverysystem_delivery dsd ON dc.courier_id = dsd.json->>'courier_id'
                )
                    SELECT  du.id AS user_id, 
                            dr.id AS restaurant_id,
                            dt.id AS timestamp_id,
                            soo.object_id AS order_key,
                            soo.object_value::json ->> 'final_status' AS order_status,
                            soo.id AS cursor_id,
                            ci.courier_id
                    FROM stg.ordersystem_orders soo
                    LEFT JOIN dds.dm_restaurants dr ON soo.object_value::json #>>'{restaurant,id}' = dr.restaurant_id
                    LEFT JOIN dds.dm_users du ON soo.object_value::json #>>'{user,id}' = du.user_id
                    LEFT JOIN dds.dm_timestamps dt ON CAST(soo.object_value::json->>'date' AS timestamp) = dt.ts
                    LEFT JOIN courier_info ci ON ci.order_id = soo.object_id
                    WHERE soo.id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY soo.id; --Обязательна сортировка по object_id, т.к. object_id используем в качестве курсора. 
                """,
                {"threshold": load_threshold},
            )
            while True:
                rows = cur.fetchmany(size=batch_size)
                if not rows:
                    break
                yield from rows


class OrdersDestRepository:
    def insert_orders(self, conn: Connection, orders: List[OrderObj]) -> None:
        with conn.cursor() as cur:
            cur.execute("BEGIN;")
            cur.executemany(
                """
                    INSERT INTO dds.dm_orders(user_id, restaurant_id, timestamp_id, order_key, order_status, courier_id)
                    VALUES (%(user_id)s, %(restaurant_id)s, %(timestamp_id)s,  %(order_key)s, %(order_status)s, %(courier_id)s)
                    ON CONFLICT (order_key) DO NOTHING;
                """,
                [order.dict(exclude={"cursor_id"}) for order in orders],
            )
            cur.execute("COMMIT;")


class OrdersLoader:
    WF_KEY = "orders_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_SIZE = 500

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = OrdersSourceRepository(pg_origin)
        self.dds = OrdersDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_orders(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_ID_KEY: -1},
                )

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            self.log.info(f"starting to load from last checkpoint: {last_loaded}")

            data = self.stg.list_orders(last_loaded, batch_size=self.BATCH_SIZE)
            rows = []
            count, total = 0, 0
            for row in data:
                rows.append(row)
                count += 1
                if count >= self.BATCH_SIZE:
                    self.dds.insert_orders(conn, orders=rows)
                    total += len(rows)
                    self.log.info(
                        f"{len(rows)} records are written to the dm_orders table"
                    )
                    wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(
                        [t.cursor_id for t in rows]
                    )
                    wf_setting_json = json2str(wf_setting.workflow_settings)
                    self.settings_repository.save_setting(
                        conn, wf_setting.workflow_key, wf_setting_json
                    )
                    rows = []
                    count = 0
            if rows:
                self.dds.insert_orders(conn, orders=rows)
                self.log.info(f"{len(rows)} records are written to the dm_orders table")

                total += len(rows)
                self.log.info(
                    f'There are total {total} records written to table dm_orders"'
                )

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(
                    [t.cursor_id for t in rows]
                )
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(
                    conn, wf_setting.workflow_key, wf_setting_json
                )

                self.log.info(
                    f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}"
                )
