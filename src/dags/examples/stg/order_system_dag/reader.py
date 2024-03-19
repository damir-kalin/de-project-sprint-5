from datetime import datetime
from typing import Dict, List

from lib import MongoConnect


class Reader:
    def __init__(self, collection_name: str, mc: MongoConnect) -> None:
        self.dbs = mc.client()
        self.collection_name = collection_name


    def get_data(self, load_threshold: datetime, limit) -> List[Dict]:
        # Формируем фильтр: больше чем дата последней загрузки
        filter = {'update_ts': {'$gt': load_threshold}}

        # Формируем сортировку по update_ts. Сортировка обязательна при инкрементальной загрузке.
        sort = [('update_ts', 1)]

        # Вычитываем документы из MongoDB с применением фильтра и сортировки.
        docs = list(self.dbs.get_collection(self.collection_name).find(filter=filter, sort=sort, limit=limit))
        return docs
