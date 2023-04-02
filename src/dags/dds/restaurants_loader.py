from logging import Logger
from typing import List, Dict
from datetime import datetime

from dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class StgObj(BaseModel):
    id: int
    object_id: str
    object_value: str


class RestaurantObj:
    def __init__(self, id: int, restaurant_dict: Dict) -> None:
        self.id = id
        self.restaurant_id = restaurant_dict['_id']
        self.restaurant_name = restaurant_dict['name']



class RestaurantsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_restaurants(self, restaurant_treshold: int, limit: int) -> List[StgObj]:
        with self._db.client().cursor(row_factory=class_row(StgObj)) as cur:
            cur.execute(
                """
                    SELECT sr.id, sr.object_id, sr.object_value
                    FROM stg.restaurants AS sr
                    WHERE sr.id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY sr.id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": restaurant_treshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class RestaurantsDestRepository:

    def insert_restaurant(self, conn: Connection, restaurant: RestaurantObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_restaurants(
                        restaurant_id,
                        restaurant_name
                    )
                    VALUES (
                        %(restaurant_id)s,
                        %(restaurant_name)s
                    )
                    ON CONFLICT (restaurant_id) DO UPDATE
                    SET
                        restaurant_id = EXCLUDED.restaurant_id,
                        restaurant_name = EXCLUDED.restaurant_name
                """,
                {
                    "restaurant_id": restaurant.restaurant_id,
                    "restaurant_name": restaurant.restaurant_name
                },
            )


class RestaurantLoader:
    WF_KEY = "example_restaurants_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100  # Пользователей мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_conn: PgConnect, log: Logger) -> None:
        self.pg_conn = pg_conn
        self.stg = RestaurantsOriginRepository(pg_conn)
        self.dds = RestaurantsDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_restaurants(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_conn.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: 0})

            # Вычитываем очередную пачку объектов.
            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.stg.list_restaurants(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} restaurants to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for stg_obj in load_queue:

                # Преобразуем из StgObj в RestaurantObj
                restaurant_dict = str2json(stg_obj.object_value)
                restaurant = RestaurantObj(stg_obj.id, restaurant_dict)
               
                self.dds.insert_restaurant(conn, restaurant)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
