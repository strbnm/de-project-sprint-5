from datetime import datetime
from decimal import Decimal
from logging import Logger
from typing import List

import requests
from airflow.models import Connection
from pydantic import BaseModel

from lib import PgConnect
from lib.dict_util import json2str
from stg import StgEtlSettingsRepository, EtlSetting


class DeliveryObj(BaseModel):
    order_id: str
    order_ts: datetime
    delivery_id: str
    courier_id: str
    address: str
    delivery_ts: datetime
    rate: int
    sum: Decimal
    tip_sum: Decimal


class DeliveryAPISource:
    def __init__(self, http_conn: Connection, log: Logger) -> None:
        self.conn = http_conn
        self.headers = {
            "X-Nickname": "strbnm",
            "X-Cohort": "14",
            "X-API-KEY": self.conn.extra_dejson.get("api_key"),
        }
        self.target_url = f"{self.conn.host}/deliveries"
        self.log = log
        self.session = None

    def get_deliveries(self, offset: int, limit: int):
        payload = {
            "sort_field": "date",
            "sort_direction": "acs",
            "limit": limit,
            "offset": offset,
        }
        try:
            self.log.info("%s %s", self.headers, self.target_url)
            response = self.session.get(url=self.target_url, params=payload)
            self.log.info(response.url)
            if response.status_code == 200:
                self.log.info("Get response: %s", response.json())
                return [DeliveryObj(**item) for item in response.json()]
            else:
                self.log.error(
                    "Error during get couriers with params %s. "
                    "Status code: %s. Message: %s",
                    payload,
                    response.status_code,
                    response.text,
                )
        except requests.RequestException as err:
            self.log.error(
                "Error during get couriers with params %s: %s",
                payload,
                err,
                exc_info=True,
            )

    def __enter__(self):
        self.session = requests.Session()
        self.session.headers = self.headers
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()


class DeliveryDestRepository:
    def insert_deliveries(
        self, conn: Connection, deliveries: List[DeliveryObj]
    ) -> None:
        with conn.cursor() as cur:
            cur.execute("BEGIN;")
            cur.executemany(
                """
                    INSERT INTO stg.deliverysystem_delivery("json", created_at)
                    VALUES (%(json)s, %(created_at)s)
                    ON CONFLICT (delivery_id) DO NOTHING;
                """,
                [
                    {"json": delivery.json(), "created_at": datetime.utcnow()}
                    for delivery in deliveries
                ],
            )
            cur.execute("COMMIT;")


class DeliveriesLoader:
    WF_KEY = "delivery_api_to_stg_workflow"
    LAST_LOADED_OFFSET = "last_loaded_offset"
    BATCH_SIZE = 50

    def __init__(self, http_conn: Connection, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.conn = http_conn
        self.dds = DeliveryDestRepository()
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_deliveries(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_OFFSET: 0},
                )
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_OFFSET]
            self.log.info("Starting to load from last checkpoint: %s", last_loaded)

            with DeliveryAPISource(http_conn=self.conn, log=self.log) as api:
                offset = last_loaded
                while data := api.get_deliveries(offset=offset, limit=self.BATCH_SIZE):
                    self.dds.insert_deliveries(conn, deliveries=data)
                    offset += self.BATCH_SIZE
                    self.log.info(
                        "%s records are written to the "
                        "stg.deliverysystem_delivery table",
                        len(data),
                    )
                    wf_setting.workflow_settings[self.LAST_LOADED_OFFSET] = offset
                    wf_setting_json = json2str(wf_setting.workflow_settings)
                    self.settings_repository.save_setting(
                        conn, wf_setting.workflow_key, wf_setting_json
                    )
                self.log.info(
                    "Load finished on %s",
                    wf_setting.workflow_settings[self.LAST_LOADED_OFFSET],
                )
