from datetime import datetime
from logging import Logger
from typing import Any

from psycopg import Connection

from stg import EtlSetting, StgEtlSettingsRepository
from stg.order_system_dag.queries import QUERY
from stg.order_system_dag.order_system_reader import ObjReader
from lib import PgConnect
from lib.dict_util import json2str


class ObjLoader:
    _LOG_THRESHOLD = 2
    _SESSION_LIMIT = 10000

    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(
        self, collection_loader: ObjReader, pg_dest: PgConnect, logger: Logger, obj: str
    ) -> None:
        self.collection_loader = collection_loader
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger
        self.wf_key = f"example_ordersystem_{obj}_origin_to_stg_workflow"
        self.obj = obj

    def run_copy(self) -> int:
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.wf_key)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.wf_key,
                    workflow_settings={
                        self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat()
                    },
                )

            last_loaded_ts_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_ts_str)
            self.log.info(f"starting to load from last checkpoint: {last_loaded_ts}")

            load_queue = self.collection_loader.get_obj(
                last_loaded_ts, self._SESSION_LIMIT, obj=self.obj
            )
            self.log.info(
                f"Found {len(load_queue)} documents to sync from restaurants collection."
            )
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            i = 0
            for d in load_queue:
                self.save_object(conn, str(d["_id"]), d["update_ts"], d, obj=self.obj)

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(
                        f"processed {i} documents of {len(load_queue)} while syncing restaurants."
                    )

            wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = max(
                [t["update_ts"] for t in load_queue]
            )
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(
                conn, wf_setting.workflow_key, wf_setting_json
            )

            self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")

            return len(load_queue)

    def save_object(
        self, conn: Connection, id: str, update_ts: datetime, val: Any, obj: str
    ):
        str_val = json2str(val)
        with conn.cursor() as cur:
            cur.execute(
                QUERY.format(obj=obj),
                {"id": id, "val": str_val, "update_ts": update_ts},
            )
