from datetime import datetime, timedelta
from logging import Logger
from pathlib import Path

from cdm.cdm_settings_repository import EtlSetting, CdmEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection


class ReportLoader:

    WF_KEY = "cdm_courier_ledger_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_courier_id"
    BATCH_LIMIT = 100
    
    def __init__(self, pg_dest: PgConnect, logger: Logger, path_to_script: str) -> None:
        self.pg_dest = pg_dest
        self.settings_repository = CdmEtlSettingsRepository()
        self.log = logger
        self.path_to_script = path_to_script


    def load_reports(self) -> int:
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={self.LAST_LOADED_ID_KEY: 0}
                )
            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            id_range = self.get_couriers_count(conn) - last_loaded_id
            if id_range > 0:
                self.log.info(f"Found {id_range} couriers to sync.")
                self.insert_reports(conn, last_loaded_id, self.BATCH_LIMIT, self.path_to_script)
            else:    
                self.log.info("Quitting.")
                return 0
            
            processed_range = min(self.BATCH_LIMIT, id_range)
            self.log.info(f"Processed {processed_range} days while syncing.")

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] += processed_range
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.set_setting(conn, wf_setting.workflow_key, wf_setting_json)

        return processed_range


    def insert_reports(self, conn: Connection, load_treshold: int, batch_limit: int, path_to_script: str) -> None:
        script = Path(path_to_script).read_text()
        with conn.cursor() as cur:
            cur.execute(
                script,
                {
                    "load_treshold": load_treshold,
                    "batch_limit": batch_limit
                }
            )


    def get_couriers_count(self, conn: Connection) -> int:
        count = 0
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT 
                        COUNT(DISTINCT courier_id)
                    FROM dds.fct_delivery_reports
                    WHERE
                        DATE_PART('year', order_ts) = DATE_PART('year', CURRENT_DATE) AND
                        DATE_PART('month', order_ts) = DATE_PART('month', CURRENT_DATE) - 1; 
                """
            )
            count = cur.fetchone()[0]
        return count              
