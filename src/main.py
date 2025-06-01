import json
import requests
from logger   import Logger
from predict  import Predictor
from pyspark.sql import DataFrame


class InferenceJob:
    def __init__(self):
        self.log          = Logger().get_logger(__name__)
        self.pred         = Predictor()
        self.datamart_url = "http://datamart:8080/api"

        self.limit  = 100000
        self.offset = 0

    def run(self):
        batch = 0
        while True:
            df_slice = self.fetch_slice(self.offset, self.limit)
            if df_slice is None:
                break        
            batch += 1
            self.log.info(f"Принято {batch} порций")
            
            preds_df = self.pred.predict(df_slice)
            self.send_to_datamart(preds_df)
            self.offset += self.limit
            self.log.info(f"Отправлено {batch} порций")

        self.pred.stop()
        self.log.info("Работа завершена")

    def fetch_slice(self, offset: int, limit: int) -> DataFrame | None:
        url = f"{self.datamart_url}/processed-data?offset={offset}&limit={limit}"
        r   = requests.get(url, timeout=(10, 600))
        r.raise_for_status()

        data = r.json()
        if not data:
            return None

        rdd = self.pred.spark.sparkContext.parallelize(map(json.dumps, data))
        return self.pred.spark.read.json(rdd)

    def send_to_datamart(self, df: DataFrame):
        payload = [
            {"_id": row["_id"], "cluster": row["cluster"]}
            for row in df.select("_id", "cluster").toLocalIterator()
        ]
        r = requests.post(f"{self.datamart_url}/predictions", json=payload, timeout=(10, 600))
        r.raise_for_status()


if __name__ == "__main__":
    InferenceJob().run()