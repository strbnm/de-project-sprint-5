from datetime import datetime
from logging import Logger
from typing import List

from dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class CourierObj(BaseModel):
    courier_id: str
    courier_name: str
    created_at: datetime


class CourierSourceRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_couriers(
        self, load_threshold: datetime, batch_size: int
    ) -> List[CourierObj]:
        with self._db.client().cursor(row_factory=class_row(CourierObj)) as cur:
            cur.execute(
                """
                    SELECT  _id AS courier_id, 
                            "name" AS courier_name,
                            created_at
                    FROM stg.deliverysystem_courier
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


class CourierDestRepository:
    def insert_couriers(self, conn: Connection, couriers: List[CourierObj]) -> None:
        with conn.cursor() as cur:
            cur.executemany(
                """
                    INSERT INTO dds.dm_couriers(courier_id, courier_name)
                    VALUES (%(courier_id)s, %(courier_name)s)
                    ON CONFLICT (courier_id) DO NOTHING;
                """,
                [courier.dict(exclude={"created_at"}) for courier in couriers],
            )


class CouriersLoader:
    WF_KEY = "couriers_stg_to_dds_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_SIZE = 50

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = CourierSourceRepository(pg_origin)
        self.dds = CourierDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_couriers(self):
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

            data = self.stg.list_couriers(last_loaded_ts, batch_size=self.BATCH_SIZE)
            rows = []
            count, total = 0, 0
            for row in data:
                rows.append(row)
                count += 1
                if count >= self.BATCH_SIZE:
                    self.dds.insert_couriers(conn, couriers=rows)
                    total += len(rows)
                    self.log.info(
                        f"{len(rows)} records are written to the dm_couriers table"
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
                self.dds.insert_couriers(conn, couriers=rows)
                self.log.info(
                    f"{len(rows)} records are written to the dm_couriers table"
                )

                total += len(rows)
                self.log.info(
                    f'There are total {total} records written to table dm_couriers"'
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
