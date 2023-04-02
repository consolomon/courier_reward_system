from datetime import datetime
from typing import Any

from lib.dict_util import json2str
from psycopg import Connection


class PgSaver:

    def save_object(self, conn: Connection, collection: str, id: str, update_ts: datetime, val: Any):
        str_val = json2str(val)
        with conn.cursor() as cur:
            cur.execute(
                f"""
                    INSERT INTO stg.{collection}(object_id, object_value, update_ts)
                    VALUES (%(object_id)s, %(object_value)s, %(update_ts)s)
                    ON CONFLICT (object_id) DO UPDATE
                    SET
                        object_value = EXCLUDED.object_value,
                        update_ts = EXCLUDED.update_ts;
                """,
                {
                    "collection": collection,
                    "object_id": id,
                    "object_value": str_val,
                    "update_ts": update_ts
                }
            )
