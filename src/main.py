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
        """Получаем предобработанные данные через API витрины"""
        response = requests.get(f"{self.datamart_url}/processed-data")
        response.raise_for_status()
        data = response.json()
        return self.pred.spark.read.json(self.pred.spark.sparkContext.parallelize([json.dumps(data)]))

    def send_to_datamart(self, df: DataFrame):
        """Отправляем предсказания витрине через API"""
        predictions = df.select("_id", "cluster").collect()
        payload = [{"_id": row["_id"], "cluster": row["cluster"]} for row in predictions]
        response = requests.post(f"{self.datamart_url}/predictions", json=payload)
        response.raise_for_status()

if __name__ == "__main__":
    InferenceJob().run()