import configparser
from pathlib import Path
from logger import Logger
import pyspark
from pyspark import SparkConf
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession


root_dir = Path(__file__).parent.parent
CONFIG_PATH = str(root_dir / 'config.ini')
DATA_PATH = str(root_dir / 'data' / 'processed_products.csv')
MODEL_PATH  = str(root_dir / 'model')


class Predictor:
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
            
        # Загрузка пайплайна
        self.pipeline = PipelineModel.load(MODEL_PATH)
        self.logger.info("Модель успешно загружена")
        
    def predict(self, df: pyspark.sql.DataFrame):
        """Выдает DataFrame с предсказанными метками"""
        result_df = self.pipeline.transform(df)
        cols = df.columns + ["cluster"] # Выдаем результат без промежуточных колонок обработки
        return result_df.select(*cols)
    
    def stop(self):
        self.spark.stop()
        self.logger.info("SparkSession остановлен")
        
        
if __name__ == "__main__":
    pred = Predictor()
    
    # Пример чтения новых данных
    df_new = pred.spark.read.option("header", True) \
                       .option("sep", "\t") \
                       .option("inferSchema", True) \
                       .csv(DATA_PATH)
    
    # Делаем предсказание
    labels = pred.predict(df_new).select('cluster')
    labels.show(10, truncate=False)
    pred.stop()