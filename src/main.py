from logger import Logger
from predict import Predictor
from pyspark.sql import DataFrame


class InferenceJob:
    def __init__(self):
        self.log = Logger().get_logger(__name__)
        self.pred = Predictor()

    def run(self):
        # Считываем данные
        df = self.read_from_mongo()
        self.log.info(f"Источник: {df.count():,} документов")
        
        # Предсказываем и записываем в другую коллекцию
        df_pred = self.pred.predict(df)
        self.write_to_mongo(df_pred.select("_id", "cluster"))
        self.log.info("Предсказания сохранены!")

        self.pred.stop()

    def read_from_mongo(self):
        """Загружаем коллекцию `products` через Spark-коннектор"""
        return (
            self.pred.spark.read
            .format("mongodb")
            .option("database",   "products_database")
            .option("collection", "products")
            .load()
        )

    def write_to_mongo(self, df: DataFrame):
        """Пишем в новую коллекцию, чтобы не трогать оригинальные документы"""
        (
            df.write
            .format("mongodb")
            .mode("append")
            .option("database",   "products_database")
            .option("collection", "products_clusters")
            .save()
        )

if __name__ == "__main__":
    InferenceJob().run()