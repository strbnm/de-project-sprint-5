import json
from datetime import datetime
from decimal import Decimal
from logging import Logger
from typing import List

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel, Field, validator

from dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting
from lib import PgConnect
from lib.dict_util import json2str


class RawProductObj(BaseModel):
    product_id: str = Field(alias="_id")
    category: str
    name: str
    price: Decimal


class RestaurantObj(BaseModel):
    restaurant_id: int
    restaurant_menu: list
    active_from: datetime
    active_to: datetime = datetime(2099, 12, 31)

    @validator("restaurant_menu", pre=True)
    def str_to_list(cls, v):
        if isinstance(v, str):
            result = []
            for item in json.loads(v):
                result.append(RawProductObj(**item))
            return result


class ProductObj(BaseModel):
    restaurant_id: str
    product_id: str
    product_name: str
    product_price: Decimal
    active_from: datetime
    active_to: datetime


class RestaurantsSourceRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_restaurants(
        self, load_threshold: datetime, batch_size: int
    ) -> List[RestaurantObj]:
        with self._db.client().cursor(row_factory=class_row(RestaurantObj)) as cur:
            cur.execute(
                """
                    SELECT  dr.id AS restaurant_id, 
                            sor.object_value::json ->> 'menu' AS restaurant_menu,
                            sor.update_ts AS active_from
                    FROM stg.ordersystem_restaurants sor
                    JOIN dds.dm_restaurants dr ON sor.object_id = dr.restaurant_id
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


class ProductsDestRepository:
    def insert_products(
        self, conn: Connection, restaurants: List[RestaurantObj]
    ) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                BEGIN;
                CREATE TEMP TABLE tmp_product (
                    restaurant_id int4, 
                    product_id varchar, 
                    product_name varchar, 
                    product_price numeric(14,2), 
                    active_from timestamp, 
                    active_to timestamp)
                ON COMMIT DROP;
                """
            )
            cur.executemany(
                """
                    INSERT INTO tmp_product(restaurant_id, product_id, product_name, product_price, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(product_id)s, %(product_name)s,  %(product_price)s, %(active_from)s, %(active_to)s);
                """,
                [
                    ProductObj(
                        restaurant_id=restaurant.restaurant_id,
                        product_id=product.product_id,
                        product_name=product.name,
                        product_price=product.price,
                        active_from=restaurant.active_from,
                        active_to=restaurant.active_to,
                    ).dict()
                    for restaurant in restaurants
                    for product in restaurant.restaurant_menu
                ],
            )
            cur.execute(
                """
                WITH 
                delete_products AS (
                    SELECT dp.id, dp.restaurant_id
                    FROM tmp_product  t
                    RIGHT JOIN dds.dm_products dp ON t.restaurant_id = dp.restaurant_id and t.product_id = dp.product_id
                    WHERE t.product_id IS NULL
                ),
                active_from AS (
                    SELECT active_from AS active_from_new, restaurant_id
                    FROM tmp_product
                    GROUP BY active_from, restaurant_id
                )
                UPDATE dds.dm_products dp
                SET active_to = af.active_from_new
                FROM delete_products d
                JOIN active_from af USING(restaurant_id)
                WHERE dp.id = d.id;
                
                UPDATE dds.dm_products dp
                SET active_to = t.active_from
                FROM tmp_product t
                WHERE 1 = 1
                    and t.restaurant_id = dp.restaurant_id
                    and t.product_id = dp.product_id
                    and (t.product_name != dp.product_name
                        or t.product_price != dp.product_price)
                    and dp.active_to = '2099-12-31';
                    
                INSERT INTO dds.dm_products (restaurant_id, product_id, product_name, product_price, active_from, active_to)
                SELECT 
                    t.restaurant_id,
                    t.product_id,
                    t.product_name,
                    t.product_price,
                    t.active_from,
                    t.active_to
                FROM tmp_product t
                LEFT JOIN dds.dm_products dp ON t.restaurant_id = dp.restaurant_id and t.product_id = dp.product_id
                WHERE 1=1
                    AND ((t.product_name != dp.product_name
                        OR t.product_price != dp.product_price)
                    AND dp.active_to != '2099-12-31')
                    OR dp.product_id IS NULL;
                COMMIT;
                """
            )


class ProductsLoader:
    WF_KEY = "products_stg_to_dds_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"
    BATCH_SIZE = 100

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.stg = RestaurantsSourceRepository(pg_origin)
        self.dds = ProductsDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_products(self):
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
                    self.dds.insert_products(conn, restaurants=rows)
                    total += len(rows)
                    self.log.info(
                        f"{len(rows)} records are written to the dm_products table"
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
                self.dds.insert_products(conn, restaurants=rows)
                self.log.info(
                    f"{len(rows)} records are written to the dm_products table"
                )

                total += len(rows)
                self.log.info(
                    f'There are total {total} records written to table dm_products"'
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
