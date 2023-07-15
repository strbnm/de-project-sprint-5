import json
from decimal import Decimal
from logging import Logger
from typing import List

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting
from lib import PgConnect
from lib.dict_util import json2str


class ProductSaleObj(BaseModel):
    product_id: int
    order_id: int
    count: int
    price: Decimal
    total_sum: Decimal
    bonus_payment: Decimal
    bonus_grant: Decimal


class LoadThreshold(BaseModel):
    product_id: int
    order_id: int


class ProductSaleSourceRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_product_sales(
        self, load_threshold: LoadThreshold, batch_size: int
    ) -> List[ProductSaleObj]:
        with self._db.client().cursor(row_factory=class_row(ProductSaleObj)) as cur:
            cur.execute(
                """
                    SELECT
                        dp.id AS product_id,
                        do2.id AS order_id,
                        t.count::int AS "count",
                        dp.product_price AS price,
                        t.count::int * dp.product_price AS total_sum,
                        t.bonus_payment,
                        t.bonus_grant
                    FROM dds.dm_orders do2 
                    LEFT JOIN dds.dm_products dp USING(restaurant_id)
                    INNER JOIN (SELECT
                        event_value::json->>'order_id' order_id,
                        items->>'product_id' product_id,
                        items->>'quantity' "count",
                        CAST(items->>'bonus_payment' AS numeric(19,5)) bonus_payment,
                        CAST(items->>'bonus_grant' AS numeric(19,5)) bonus_grant
                    FROM stg.bonussystem_events be,
                    json_array_elements(event_value::json->'product_payments') items) AS t ON t.order_id = do2.order_key AND t.product_id = dp.product_id 
                    WHERE dp.id > %(product_id)s  AND do2.id > %(order_id)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY order_id, product_id; --Обязательна сортировка по object_id, т.к. object_id используем в качестве курсора. 
                """,
                {
                    "product_id": load_threshold.product_id,
                    "order_id": load_threshold.order_id,
                },
            )
            while True:
                rows = cur.fetchmany(size=batch_size)
                if not rows:
                    break
                yield from rows


class ProductSaleDestRepository:
    def insert_product_sales(
        self, conn: Connection, product_sales: List[ProductSaleObj]
    ) -> None:
        with conn.cursor() as cur:
            cur.execute("BEGIN;")
            cur.executemany(
                """
                    INSERT INTO dds.fct_product_sales(product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                    VALUES (%(product_id)s, %(order_id)s, %(count)s,  %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s)
                    ON CONFLICT (product_id, order_id) DO UPDATE
                    SET count = excluded.count,
                        price = excluded.price,
                        total_sum = excluded.total_sum,
                        bonus_payment = excluded.bonus_payment,
                        bonus_grant = excluded.bonus_grant;
                """,
                [product_sale.dict() for product_sale in product_sales],
            )
            cur.execute("COMMIT;")


class ProductSalesLoader:
    WF_KEY = "product_sales_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_ids"
    BATCH_SIZE = 500

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = ProductSaleSourceRepository(pg_origin)
        self.dds = ProductSaleDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_product_sales(self):
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        self.LAST_LOADED_ID_KEY: LoadThreshold(
                            product_id=-1, order_id=-1
                        ).json()
                    },
                )

            last_loaded_json = json.loads(
                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            )
            last_loaded = LoadThreshold.parse_obj(last_loaded_json)
            self.log.info(f"starting to load from last checkpoint: {last_loaded}")

            data = self.stg.list_product_sales(last_loaded, batch_size=self.BATCH_SIZE)
            rows = []
            count, total = 0, 0
            for row in data:
                rows.append(row)
                count += 1
                if count >= self.BATCH_SIZE:
                    self.dds.insert_product_sales(conn, product_sales=rows)
                    total += len(rows)
                    self.log.info(
                        f"{len(rows)} records are written to the fct_product_sales table"
                    )
                    wf_setting.workflow_settings[
                        self.LAST_LOADED_ID_KEY
                    ] = LoadThreshold(
                        product_id=max([t.product_id for t in rows]),
                        order_id=max([t.order_id for t in rows]),
                    ).json()
                    wf_setting_json = json2str(wf_setting.workflow_settings)
                    self.settings_repository.save_setting(
                        conn, wf_setting.workflow_key, wf_setting_json
                    )
                    rows = []
                    count = 0
            if rows:
                self.dds.insert_product_sales(conn, product_sales=rows)
                self.log.info(
                    f"{len(rows)} records are written to the fct_product_sales table"
                )

                total += len(rows)
                self.log.info(
                    f"There are total {total} records written to table fct_product_sales"
                )

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = LoadThreshold(
                    product_id=max([t.product_id for t in rows]),
                    order_id=max([t.order_id for t in rows]),
                ).json()
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(
                    conn, wf_setting.workflow_key, wf_setting_json
                )

                self.log.info(
                    f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}"
                )
