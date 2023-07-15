from datetime import datetime
from logging import Logger
from typing import List

from dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class RestaurantObj(BaseModel):
    restaurant_id: str
    restaurant_name: str
    active_from: datetime
    active_to: datetime = datetime(2099, 12, 31)


class RestaurantsSourceRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_restaurants(
        self, load_threshold: datetime, batch_size: int
    ) -> List[RestaurantObj]:
        with self._db.client().cursor(row_factory=class_row(RestaurantObj)) as cur:
            cur.execute(
                """
                    SELECT  object_id AS restaurant_id, 
                            object_value::json ->> 'name' AS restaurant_name,
                            update_ts AS active_from
                    FROM stg.ordersystem_restaurants
                    WHERE update_ts > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY update_ts; --Обязательна сортировка по update_ts, т.к. update_ts используем в качестве курсора. 
                """,
                {"threshold": load_threshold},
            )
            while True:
                rows = cur.fetchmany(size=batch_size)
                if not rows:
                    break
                yield from rows


class RestaurantsDestRepository:
    def insert_restaurants(
        self, conn: Connection, restaurants: List[RestaurantObj]
    ) -> None:
        with conn.cursor() as cur:
            cur.executemany(
                """
                    INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(active_from)s,  %(active_to)s)
                    ON CONFLICT (restaurant_id, restaurant_name) DO NOTHING;
                """,
                [restaurant.dict() for restaurant in restaurants],
            )
            cur.execute(
                """
                    WITH tmp AS (
                        SELECT *,
                               lead(active_from, 1, '2099-12-31'::timestamp) OVER(PARTITION BY restaurant_id ORDER BY active_from) AS active_to_
                        FROM dds.dm_restaurants
                    )
                    UPDATE dds.dm_restaurants r
                    SET active_to = t.active_to_
                    FROM tmp t
                    WHERE r.active_to <> t.active_to_
                """
            )


class RestaurantsLoader:
    WF_KEY = "restaurants_stg_to_dds_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_SIZE = 10

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = RestaurantsSourceRepository(pg_origin)
        self.dds = RestaurantsDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_restaurants(self):
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

            data = self.stg.list_restaurants(last_loaded_ts, batch_size=self.BATCH_SIZE)
            rows = []
            count, total = 0, 0
            for row in data:
                rows.append(row)
                count += 1
                if count >= self.BATCH_SIZE:
                    self.dds.insert_restaurants(conn, restaurants=rows)
                    total += len(rows)
                    self.log.info(
                        f"{len(rows)} records are written to the dm_restaurants table"
                    )
                    wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max(
                        [t.active_from for t in rows]
                    )
                    wf_setting_json = json2str(wf_setting.workflow_settings)
                    self.settings_repository.save_setting(
                        conn, wf_setting.workflow_key, wf_setting_json
                    )
                    rows = []
                    count = 0
            if rows:
                self.dds.insert_restaurants(conn, restaurants=rows)
                self.log.info(
                    f"{len(rows)} records are written to the dm_restaurants table"
                )

                total += len(rows)
                self.log.info(
                    f'There are total {total} records written to table dm_restaurants"'
                )

                wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max(
                    [t.active_from for t in rows]
                )
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(
                    conn, wf_setting.workflow_key, wf_setting_json
                )

                self.log.info(
                    f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}"
                )
