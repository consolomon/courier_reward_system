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


class CourierObj:
    def __init__(self, courier_dict: Dict) -> None:
        self.courier_id = courier_dict['_id']
        self.courier_name = courier_dict['name'] 


class CouriersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_couriers(self, courier_treshold: int, limit: int) -> List[StgObj]:
        with self._db.client().cursor(row_factory=class_row(StgObj)) as cur:
            cur.execute(
                """
                    SELECT sc.id, sc.object_id, sc.object_value
                    FROM stg.couriers AS sc
                    WHERE sc.id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY sc.id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": courier_treshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class CouriersDestRepository:

    def insert_courier(self, conn: Connection, courier: CourierObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_couriers(courier_id, courier_name)
                    VALUES (%(courier_id)s, %(courier_name)s)
                    ON CONFLICT (courier_id) DO UPDATE
                    SET
                        courier_id = EXCLUDED.courier_id,
                        courier_name = EXCLUDED.courier_name
                """,
                {
                    "courier_id": courier.courier_id,
                    "courier_name": courier.courier_name,
                },
            )


class CourierLoader:
    WF_KEY = "couriers_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100  # Пользователей мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_conn: PgConnect, log: Logger) -> None:
        self.pg_conn = pg_conn
        self.stg = CouriersOriginRepository(pg_conn)
        self.dds = CouriersDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_couriers(self):
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
            load_queue = self.stg.list_couriers(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} couriers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for stg_obj in load_queue:

                # Преобразуем из StgObj в CourierObj
                courier_dict = str2json(stg_obj.object_value)
                courier = CourierObj(courier_dict)
               
                self.dds.insert_courier(conn, courier)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
