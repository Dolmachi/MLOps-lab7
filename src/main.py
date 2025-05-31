import json
import requests
from logger import Logger
from predict import Predictor
from pyspark.sql import DataFrame

class InferenceJob:
    def __init__(self):
        self.log = Logger().get_logger(__name__)
        self.pred = Predictor()
        self.datamart_url = "http://datamart:8080/api"

    def run(self):
        # Получаем предобработанные данные от витрины
        df = self.get_from_datamart()
        self.log.info(f"Источник: {df.count():,} документов")

        # Делаем предсказание
        df_pred = self.pred.predict(df)
        
        # Отправляем предсказания витрине
        self.send_to_datamart(df_pred)
        self.log.info("Предсказания отправлены витрине!")

        self.pred.stop()

    def get_from_datamart(self) -> DataFrame:
        all_parts = []
        offset, limit = 0, 10_000

        while True:
            url = f"{self.datamart_url}/processed-data?offset={offset}&limit={limit}"
            r   = requests.get(url, stream=True)
            r.raise_for_status()

            # сервер шлёт полноценный JSON-массив
            part = r.json()
            if not part:
                break

            all_parts.extend(part)
            offset += limit

        # превращаем список json-строк в Spark-DF
        rdd = self.pred.spark.sparkContext.parallelize(map(json.dumps, all_parts))
        return self.pred.spark.read.json(rdd)


    def send_to_datamart(self, df: DataFrame):
        """Отправляем предсказания витрине через API"""
        predictions = df.select("_id", "cluster").collect()
        payload = [{"_id": row["_id"], "cluster": row["cluster"]} for row in predictions]
        response = requests.post(f"{self.datamart_url}/predictions", json=payload)
        response.raise_for_status()

if __name__ == "__main__":
    InferenceJob().run()