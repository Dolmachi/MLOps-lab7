import json
import requests
from logger   import Logger
from predict  import Predictor
from pyspark.sql import DataFrame

class InferenceJob:
    def __init__(self):
        self.log = Logger().get_logger(__name__)
        self.pred = Predictor()
        self.datamart_url = "http://datamart:8080/api"

        self.num_partitions = 30
        self.current_partition = 0

    def run(self):
        batch = 0
        while self.current_partition < self.num_partitions:
            df_slice = self.fetch_slice(self.current_partition)
            if df_slice is None:
                break        
            batch += 1
            self.log.info(f"Принято {batch} порций")
            
            preds_df = self.pred.predict(df_slice)
            self.send_to_datamart(preds_df)
            self.current_partition += 1
            self.log.info(f"Отправлено {batch} порций")
        
        self.pred.stop()
        self.log.info("Работа завершена")

    def fetch_slice(self, partition_index: int) -> DataFrame | None:
        url = f"{self.datamart_url}/processed-data?partitionIndex={partition_index}"
        self.log.info(f"Fetching slice: partitionIndex={partition_index}")
        try:
            r = requests.get(url, timeout=(30, 1200))
            r.raise_for_status()
            data = r.json()
            self.log.info(f"Fetch slice: partitionIndex={partition_index}, rows={len(data)}")
            if not data:
                return None

            rdd = self.pred.spark.sparkContext.parallelize(map(json.dumps, data))
            return self.pred.spark.read.json(rdd)
        except requests.exceptions.RequestException as e:
            self.log.error(f"Failed to fetch slice {partition_index}: {e}")
            return None

    def send_to_datamart(self, df: DataFrame):
        payload = [
            {"_id": row["_id"], "cluster": row["cluster"]}
            for row in df.select("_id", "cluster").toLocalIterator()
        ]
        r = requests.post(f"{self.datamart_url}/predictions", json=payload, timeout=(30, 1200))
        r.raise_for_status()

if __name__ == "__main__":
    InferenceJob().run()