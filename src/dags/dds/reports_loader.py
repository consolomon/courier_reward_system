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
    order_id: int
    delivery_id: int
    courier_id: int
    object_value: str


class ReportObj:
    def __init__(self, delivery_fks: Dict, delivery_dict: Dict) -> None:
        self.order_id = delivery_fks['order_id']
        self.order_ts = delivery_dict['order_ts']
        self.delivery_id = delivery_fks['delivery_id']
        self.courier_id = delivery_fks['courier_id']
        self.address = delivery_dict['address']
        self.delivery_ts = delivery_dict['delivery_ts']
        self.rate = delivery_dict['rate']
        self.sum = delivery_dict['sum']
        self.tip_sum = delivery_dict['tip_sum']


class ReportsOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_reports(self, reports_treshold: int, limit: int) -> List[StgObj]:
        with self._db.client().cursor(row_factory=class_row(StgObj)) as cur:
            cur.execute(
                """
                    SELECT 
                        sd.id,
                        "do".id AS order_id,
                        dd.id AS delivery_id,
                        dc.id AS courier_id,
                        sd.object_value
                    FROM stg.deliveries AS sd
                    LEFT JOIN dds.dm_orders AS "do" ON
                        sd.object_value::JSON->>'order_id' = "do".order_id
                    LEFT JOIN dds.dm_deliveries AS dd ON
                        sd.object_id = dd.delivery_id    
                    LEFT JOIN dds.dm_couriers AS dc ON
                        sd.object_value::JSON->>'courier_id' = dc.courier_id
                    WHERE sd.id > %(threshold)s --Пропускаем те объекты, которые уже загрузили.
                    ORDER BY sd.id ASC --Обязательна сортировка по id, т.к. id используем в качестве курсора.
                    LIMIT %(limit)s; --Обрабатываем только одну пачку объектов.
                """, {
                    "threshold": reports_treshold,
                    "limit": limit
                }
            )
            objs = cur.fetchall()
        return objs


class ReportsDestRepository:

    def insert_reports(self, conn: Connection, report: ReportObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_delivery_reports (
                        order_id,
                        order_ts,
                        delivery_id,
                        courier_id,
                        address,
                        delivery_ts,
                        rate,
                        sum,
                        tip_sum
                    )
                    VALUES (
                        %(order_id)s,
                        %(order_ts)s,
                        %(delivery_id)s,
                        %(courier_id)s,
                        %(address)s,
                        %(delivery_ts)s,
                        %(rate)s,
                        %(sum)s,
                        %(tip_sum)s
                    )
                    ON CONFLICT (delivery_id) DO UPDATE
                    SET
                        order_id = EXCLUDED.order_id,
                        order_ts = EXCLUDED.order_ts,
                        delivery_id = EXCLUDED.delivery_id,
                        courier_id = EXCLUDED.courier_id,
                        address = EXCLUDED.address,
                        delivery_ts = EXCLUDED.delivery_ts,
                        rate = EXCLUDED.rate,
                        sum = EXCLUDED.sum,
                        tip_sum = EXCLUDED.tip_sum;
                """,
                {
                    "order_id": report.order_id,
                    "order_ts": report.order_ts,
                    "delivery_id": report.delivery_id,
                    "courier_id": report.courier_id,
                    "address": report.address,
                    "delivery_ts": report.delivery_ts,
                    "rate": report.rate,
                    "sum": report.sum,
                    "tip_sum": report.tip_sum,
                },
            )


class ReportsLoader:
    WF_KEY = "reports_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 100  # Пользователей мало, но мы хотим продемонстрировать инкрементальную загрузку рангов.

    def __init__(self, pg_conn: PgConnect, log: Logger) -> None:
        self.pg_conn = pg_conn
        self.stg = ReportsOriginRepository(pg_conn)
        self.dds = ReportsDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_reports(self):
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
            load_queue = self.stg.list_reports(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} reports to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            # Сохраняем объекты в базу dwh.
            for stg_obj in load_queue:

                # Преобразуем из StgObj в ReportObj
                report_dict = str2json(stg_obj.object_value)
                report_fks = {
                    "order_id": stg_obj.order_id,
                    "courier_id": stg_obj.courier_id,
                    "delivery_id": stg_obj.delivery_id
                }
                report = ReportObj(report_fks, report_dict)
                self.dds.insert_reports(conn, report)

            # Сохраняем прогресс.
            # Мы пользуемся тем же connection, поэтому настройка сохранится вместе с объектами,
            # либо откатятся все изменения целиком.
            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)  # Преобразуем к строке, чтобы положить в БД.
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")