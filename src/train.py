import configparser
from pathlib import Path
from logger import Logger
from pyspark import SparkConf
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StandardScaler


root_dir = Path(__file__).parent.parent
CONFIG_PATH = str(root_dir / 'config.ini')
DATA_PATH   = str(root_dir / 'data' / 'processed_products.csv')
MODEL_PATH  = str(root_dir / 'model')


class Trainer:
    def __init__(self):
        self.logger = Logger().get_logger(__name__)
        
        # Конфигурация Spark
        config = configparser.ConfigParser()
        config.optionxform = str
        config.read(CONFIG_PATH)
        spark_conf = SparkConf().setAll(config['SPARK'].items())
        
        # Создаем сессию
        self.spark = SparkSession.builder \
            .appName("KMeans") \
            .master("local[*]") \
            .config(conf=spark_conf) \
            .getOrCreate()
        
    def train_pipeline(self, k=5):
        """Обучает алгоритм кластеризации kmeans в виде пайплайна"""
        
        # Считываем данные
        df = self.spark.read.option("header", True) \
               .option("sep", "\t") \
               .option("inferSchema", True) \
               .csv(DATA_PATH)
               
        # Составляем этапы Pipeline
        assembler = VectorAssembler(
            inputCols=df.columns,
            outputCol="features"
        )
        scaler = StandardScaler(
            inputCol="features",
            outputCol="scaled_features",
            withMean=True,
            withStd=True
        )
        kmeans = KMeans(
            k=k,
            seed=42,
            featuresCol="scaled_features",
            predictionCol="cluster"
        )
        
        pipeline = Pipeline(stages=[assembler, scaler, kmeans])
        
        # Обучаем PipelineModel
        pipeline_model = pipeline.fit(df)
        
        # Сохраняем весь PipelineModel
        pipeline_model.write().overwrite().save(MODEL_PATH)
        self.logger.info(f"Модель успешно сохранена!")
        
    def stop(self):
        self.spark.stop()
        self.logger.info("SparkSession остановлен")
        
        
if __name__ == "__main__":
    trainer = Trainer()
    trainer.train_pipeline(k=5)
    trainer.stop()