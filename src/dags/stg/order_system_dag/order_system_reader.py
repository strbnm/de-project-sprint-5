from datetime import datetime
from typing import Dict, List

from lib import MongoConnect


class ObjReader:
    def __init__(self, mc: MongoConnect) -> None:
        self.dbs = mc.client()

    def get_obj(self, load_threshold: datetime, limit, obj: str) -> List[Dict]:
        filter = {"update_ts": {"$gt": load_threshold}}

        sort = [("update_ts", 1)]

        docs = list(
            self.dbs.get_collection(obj).find(filter=filter, sort=sort, limit=limit)
        )
        return docs
