from datetime import datetime
from typing import Dict, List

from lib.api_connect import APIConnection


class CollectionReader:

    def __init__(self, api_conn: APIConnection, collection: str) -> None:
        self.api_conn = api_conn
        self.collection = collection

    def get_collection(self, load_threshold: int, limit: int) -> List[Dict]:
        # Формируем фильтр: по последнему загруженному id
        parameters = {
            "sort_direction": "asc",
            "sort_field": "id",
            "limit": limit,
            "offset": load_threshold
        } 

        # Вычитываем данные из API с применением фильтра и сортировки.
        data = self.api_conn.get_request(endpoint=self.collection, parameters=parameters)
        return data
