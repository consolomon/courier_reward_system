from logging import Logger
from typing import List, Dict

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


class OrderObj:
    def __init__(self, order_dict: Dict) -> None:
        self.order_id = order_dict['order_id']
        self.order_ts = order_dict['order_ts']
        self.sum = order_dict['sum']



class OrdersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_orders(self, order_treshold: int, limit: int) -> List[StgObj]:
        with self._db.client().cursor(row_factory=class_row(StgObj)) as cur:
            cur.execute(
                """
                    SELECT 
                        sd.id,
                        sd.object_value::JSON->>'order_id' AS object_id,
                        sd.object_value
                    FROM stg.deliveries AS sd
                    WHERE sd.id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY sd.id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": order_treshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class OrdersDestRepository:

    def insert_order(self, conn: Connection, order: OrderObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(
                        order_id,
                        order_ts,
                        sum
                    )
                    VALUES (
                        %(order_id)s,
                        %(order_ts)s,
                        %(sum)s
                    )
                    ON CONFLICT (order_id) DO UPDATE
                    SET
                        order_id = EXCLUDED.order_id,
                        order_ts = EXCLUDED.order_ts,
                        sum = EXCLUDED.sum;
                """,
                {
                    "order_id": order.order_id,
                    "order_ts": order.order_ts,
                    "sum": order.sum
                },
            )


class OrderLoader:
    WF_KEY = "orders_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100  # Пользователей мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_conn: PgConnect, log: Logger) -> None:
        self.pg_conn = pg_conn
        self.stg = OrdersOriginRepository(pg_conn)
        self.dds = OrdersDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_orders(self):
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
            load_queue = self.stg.list_orders(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} orders to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for stg_obj in load_queue:

                # Преобразуем из StgObj в OrderObj
                order_dict = str2json(stg_obj.object_value)
                order = OrderObj(order_dict)
                self.dds.insert_order(conn, order)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
