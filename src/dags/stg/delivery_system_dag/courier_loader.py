from datetime import datetime
from logging import Logger
from typing import List

import requests
from airflow.models import Connection
from pydantic import BaseModel, Field

from lib import PgConnect


class CourierObj(BaseModel):
    courier_id: str = Field(..., alias="_id")
    name: str


class CourierAPISource:
    def __init__(self, http_conn: Connection, log: Logger) -> None:
        self.conn = http_conn
        self.headers = {
            "X-Nickname": "strbnm",
            "X-Cohort": "14",
            "X-API-KEY": self.conn.extra_dejson.get("api_key"),
        }
        self.target_url = f"{self.conn.host}/couriers"
        self.log = log
        self.session = None

    def get_couriers(self, offset: int, limit: int):
        payload = {
            "sort_field": "id",
            "sort_direction": "acs",
            "limit": limit,
            "offset": offset,
        }
        try:
            self.log.info("%s %s", self.headers, self.target_url)
            response = self.session.get(url=self.target_url, params=payload)
            self.log.info(response.url)
            if response.status_code == 200:
                return [CourierObj(**item) for item in response.json()]
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


class CourierDestRepository:
    def insert_couriers(self, conn: Connection, couriers: List[CourierObj]) -> None:
        with conn.cursor() as cur:
            cur.execute("BEGIN;")
            cur.executemany(
                """
                    INSERT INTO stg.deliverysystem_courier("json", created_at)
                    VALUES (%(json)s, %(created_at)s)
                    ON CONFLICT (_id) DO NOTHING;
                """,
                [
                    {
                        "json": courier.json(by_alias=True),
                        "created_at": datetime.utcnow(),
                    }
                    for courier in couriers
                ],
            )
            cur.execute("COMMIT;")


class CouriersLoader:
    BATCH_SIZE = 50

    def __init__(self, http_conn: Connection, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.conn = http_conn
        self.dds = CourierDestRepository()
        self.log = log

    def load_couriers(self):
        with self.pg_dest.connection() as conn:
            with CourierAPISource(http_conn=self.conn, log=self.log) as api:
                offset = 0
                while data := api.get_couriers(offset=offset, limit=self.BATCH_SIZE):
                    self.dds.insert_couriers(conn, couriers=data)
                    offset += self.BATCH_SIZE
                self.log.info("Load finished")
