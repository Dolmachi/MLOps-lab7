import sys
import subprocess
import configparser
from pathlib import Path
from pyspark.sql import SparkSession
from logger import Logger


root_dir = Path(__file__).parent.parent
CONFIG_PATH = str(root_dir / 'config.ini')
CSV_PATH = str(root_dir / 'data' / 'products.csv')
DUMP_PATH = str(root_dir / 'data' / 'mongo-dump')


class Dumper:
    def __init__(self):
        self.logger = Logger().get_logger(__name__)
        self.config = configparser.ConfigParser()
        self.config.read(CONFIG_PATH)
        self.db_config = self.config['DATABASE']
        
        self.port = self.db_config.getint('port')
        self.user = self.db_config.get('user')
        self.password = self.db_config.get('password')
        self.dbname = self.db_config.get('name')
        
        self.uri = f"mongodb://{self.user}:{self.password}@localhost:{self.port}/"
        
        self.spark = (SparkSession.builder
                      .appName("Csv_to_Mongo")
                      .master("local[*]")
                      .config(
                          "spark.jars.packages",
                          "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0"
                      )
                      .config("spark.mongodb.write.connection.uri", self.uri)
                      .getOrCreate())
        
        
        
    def create_mongo_dump(self):
        # Считываем данные
        df = (self.spark.read
              .option("sep", "\t")
              .option("header", "true")
              .option("inferSchema", "true")
              .option("mode", "PERMISSIVE")
              .csv(str(CSV_PATH)))
        self.logger.info(f"Прочитано {df.count():,} строк")

        # Записываем в коллекцию "products_raw"
        (df.write
         .format("mongodb")
         .mode("overwrite")
         .option("database", self.dbname)
         .option("collection", "products_raw")
         .save())
        self.logger.info('Данные успешно записаны в Mongo')

        # Создаем дамп базы данных
        try:
            subprocess.run([
                "mongodump",
                "--host", "localhost",
                "--port", str(self.port),
                "--username", self.user,
                "--password", self.password,
                "--authenticationDatabase", "admin",
                "--db", self.dbname,
                "--out", DUMP_PATH
            ], check=True)
            self.logger.info(f"Дамп успешно создан!")
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Ошибка при создании дампа: {e}", exc_info=True)
            sys.exit(1)
    

if __name__ == "__main__":
    dumper = Dumper()
    dumper.create_mongo_dump()