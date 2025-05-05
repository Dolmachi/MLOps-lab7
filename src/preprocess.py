import os
import shutil
import configparser
from pathlib import Path
from logger import Logger
from functools import reduce
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, when


root_dir = Path(__file__).parent.parent
CONFIG_PATH = str(root_dir / 'config.ini')
DATA_PATH = str(root_dir / 'data' / 'products.csv')
OUTPUT_PATH = str(root_dir / 'data' / 'processed_products.csv')


class DataMaker:
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
        
    def prepare_data(self):
        """Фильтрует и обрабатывает основные данные"""
        
        # Считываем данные
        df = self.spark.read.option("header", True) \
               .option("sep", "\t") \
               .option("inferSchema", True) \
               .csv(DATA_PATH)
               
        # Оставляем только колонки с веществами
        nutrient_columns = df.columns[88:]
        df_nutrients = df.select(nutrient_columns)
        
        # Удаляем полностью пустые записи
        df_cleaned = df_nutrients.filter(
            ~reduce(lambda a, b: a & b, [col(c).isNull() for c in df_nutrients.columns])
        )
        
        # Одиночные пустые ячейки заменяем на 0
        df_filled = df_cleaned.fillna(0.0)
        
        # Заменяем выбросы на медиану
        lower_bound = 0.0
        upper_bound = 1000.0
        median_exprs = [expr(f"percentile_approx(`{c}`, 0.5)").alias(c) for c in df_filled.columns]
        medians = df_filled.agg(*median_exprs).collect()[0].asDict()
        df_cleansed = df_filled
        for c in df_filled.columns:
            if c in medians and medians[c] is not None:
                median = medians[c]
                df_cleansed = df_cleansed.withColumn(
                    c,
                    when((col(c) < lower_bound) | (col(c) > upper_bound), median).otherwise(col(c))
                )
                
        # Сохраняем обработанные данные
        temp_output_path = OUTPUT_PATH[:-4]
        df_cleansed.coalesce(1).write \
            .mode("overwrite") \
            .option("header", True) \
            .option("sep", "\t") \
            .csv(temp_output_path)
        for file in os.listdir(temp_output_path):
            if file.startswith("part-00000"):
                shutil.move(os.path.join(temp_output_path, file), OUTPUT_PATH)
                break
        shutil.rmtree(temp_output_path)
        self.logger.info("Данные успешно обработаны и сохранены!")
        
    def stop(self):
        self.spark.stop()
        self.logger.info("SparkSession остановлен")


if __name__ == "__main__":
    data_maker = DataMaker()
    data_maker.prepare_data()
    data_maker.stop()