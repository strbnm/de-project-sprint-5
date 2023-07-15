from datetime import datetime
from logging import Logger
from typing import List

from stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class OutboxObj(BaseModel):
    id: int
    event_ts: datetime
    event_type: str
    event_value: str


class EventsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_events(self, outbox_threshold: int, batch_size: int) -> List[OutboxObj]:
        with self._db.client().cursor(row_factory=class_row(OutboxObj)) as cur:
            cur.execute(
                """
                    SELECT id, event_ts, event_type, event_value
                    FROM outbox
                    WHERE id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY id; --Обязательна сортировка по id, т.к. id используем в качестве курсора. 
                """,
                {"threshold": outbox_threshold},
            )
            while True:
                rows = cur.fetchmany(size=batch_size)
                if not rows:
                    break
                yield from rows


class EventsDestRepository:
    def insert_events(self, conn: Connection, events: List[OutboxObj]) -> None:
        with conn.cursor() as cur:
            cur.executemany(
                """
                    INSERT INTO stg.bonussystem_events(id, event_ts, event_type, event_value)
                    VALUES (%(id)s, %(event_ts)s, %(event_type)s, %(event_value)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        event_ts = EXCLUDED.event_ts,
                        event_type = EXCLUDED.event_type,
                        event_value = EXCLUDED.event_value;
                """,
                [event.dict() for event in events],
            )


class EventLoader:
    WF_KEY = "example_events_origin_to_stg_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_SIZE = 500

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = EventsOriginRepository(pg_origin)
        self.stg = EventsDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_events(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_ID_KEY: -1},
                )

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            data = self.origin.list_events(last_loaded, batch_size=self.BATCH_SIZE)
            rows = []
            count, total = 0, 0
            for row in data:
                rows.append(row)
                count += 1
                if count >= self.BATCH_SIZE:
                    self.stg.insert_events(conn, events=rows)
                    total += len(rows)
                    self.log.info(
                        f"{len(rows)} records are written to the events table"
                    )
                    wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(
                        [t.id for t in rows]
                    )
                    wf_setting_json = json2str(wf_setting.workflow_settings)
                    self.settings_repository.save_setting(
                        conn, wf_setting.workflow_key, wf_setting_json
                    )
                    rows = []
                    count = 0
            if rows:
                self.stg.insert_events(conn, events=rows)
                self.log.info(f"{len(rows)} records are written to the events table")

                total += len(rows)
                self.log.info(
                    f'There are total {total} records written to table events"'
                )

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max(
                    [t.id for t in rows]
                )
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(
                    conn, wf_setting.workflow_key, wf_setting_json
                )

                self.log.info(
                    f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}"
                )
