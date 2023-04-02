from datetime import datetime
from logging import Logger

from stg import EtlSetting, StgEtlSettingsRepository
from stg.pg_saver import PgSaver
from stg.collection_reader import CollectionReader
from lib import PgConnect
from lib.dict_util import json2str


class CollectionLoader:
    _LOG_THRESHOLD = 50
    _BATCH_LIMIT = 50
    
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, collection_reader: CollectionReader, pg_dest: PgConnect, pg_saver: PgSaver, logger: Logger) -> None:
        self.collection_reader = collection_reader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
        self.log = logger
        self.collection = collection_reader.collection
        self.wf_key = f"{collection_reader.collection}_origin_to_stg_workflow"

    def load_collection(self) -> int:
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.pg_dest.connection() as conn:

            # Прочитываем состояние загрузки
            # Если настройки еще нет, заводим ее.
            wf_setting = self.settings_repository.get_setting(conn, self.wf_key)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.wf_key,
                    workflow_settings={self.LAST_LOADED_ID_KEY: 0}
                )

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

            load_queue = self.collection_reader.get_collection(last_loaded_id, self._BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} documents to sync from {self.collection} collection.")
            if not load_queue:
                self.log.info("Quitting.")
                return 0

            i = 0
            id_key = ("delivery_id" if self.collection == "deliveries" else "_id")
            for d in load_queue:
                self.pg_saver.save_object(conn, self.collection, str(d[id_key]), datetime.today(), d)

                i += 1
                if i % self._LOG_THRESHOLD == 0:
                    self.log.info(f"processed {i} documents of {len(load_queue)} while syncing {self.collection}.")

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] += len(load_queue)
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            return len(load_queue)
