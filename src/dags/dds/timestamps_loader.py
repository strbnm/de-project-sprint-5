from datetime import datetime, date, time
from logging import Logger
from typing import List

from dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class TimestampObj(BaseModel):
    ts: datetime
    year: int
    month: int
    day: int
    date: date
    time: time
    update_ts: datetime


class TimestampsSourceRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_timestamps(
        self, load_threshold: datetime, batch_size: int
    ) -> List[TimestampObj]:
        with self._db.client().cursor(row_factory=class_row(TimestampObj)) as cur:
            cur.execute(
                """
                    SELECT  (object_value::json ->> 'date')::timestamp AS ts, 
                            EXTRACT(YEAR FROM (object_value::json ->> 'date')::timestamp) AS year,
                            EXTRACT(MONTH FROM (object_value::json ->> 'date')::timestamp) AS month,
                            EXTRACT(DAY FROM (object_value::json ->> 'date')::timestamp) AS day,
                            (object_value::json ->> 'date')::date AS "date",
                            (object_value::json ->> 'date')::time AS "time",
                            update_ts
                    FROM stg.ordersystem_orders
                    WHERE update_ts > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    AND (object_value::json ->> 'final_status') IN ('CLOSED', 'CANCELLED')
                    ORDER BY update_ts; --Обязательна сортировка по update_ts, т.к. update_ts используем в качестве курсора. 
                """,
                {"threshold": load_threshold},
            )
            while True:
                rows = cur.fetchmany(size=batch_size)
                if not rows:
                    break
                yield from rows


class TimestampsDestRepository:
    def insert_timestamps(
        self, conn: Connection, timestamps: List[TimestampObj]
    ) -> None:
        with conn.cursor() as cur:
            cur.executemany(
                """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, time, date)
                    VALUES (%(ts)s, %(year)s, %(month)s,  %(day)s, %(time)s, %(date)s)
                    ON CONFLICT (ts) DO NOTHING;
                """,
                [timestamps.dict(exclude={"update_ts"}) for timestamps in timestamps],
            )


class TimestampsLoader:
    WF_KEY = "timestamps_stg_to_dds_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_SIZE = 500

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = TimestampsSourceRepository(pg_origin)
        self.dds = TimestampsDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_timestamps(self):
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

            data = self.stg.list_timestamps(last_loaded_ts, batch_size=self.BATCH_SIZE)
            rows = []
            count, total = 0, 0
            for row in data:
                rows.append(row)
                count += 1
                if count >= self.BATCH_SIZE:
                    self.dds.insert_timestamps(conn, timestamps=rows)
                    total += len(rows)
                    self.log.info(
                        f"{len(rows)} records are written to the dm_timestamps table"
                    )
                    wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max(
                        [t.update_ts for t in rows]
                    )
                    wf_setting_json = json2str(wf_setting.workflow_settings)
                    self.settings_repository.save_setting(
                        conn, wf_setting.workflow_key, wf_setting_json
                    )
                    rows = []
                    count = 0
            if rows:
                self.dds.insert_timestamps(conn, timestamps=rows)
                self.log.info(
                    f"{len(rows)} records are written to the dm_timestamps table"
                )

                total += len(rows)
                self.log.info(
                    f'There are total {total} records written to table dm_timestamps"'
                )

                wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max(
                    [t.update_ts for t in rows]
                )
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(
                    conn, wf_setting.workflow_key, wf_setting_json
                )

                self.log.info(
                    f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}"
                )
