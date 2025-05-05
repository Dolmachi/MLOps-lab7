import pandas as pd
from pymongo import MongoClient
import subprocess
from pathlib import Path
import configparser
from logger import Logger
import sys


root_dir = Path(__file__).parent.parent
CONFIG_PATH = str(root_dir / 'config.ini')
CSV_PATH = str(root_dir / 'data' / 'processed_products.csv')
DUMP_PATH = str(root_dir / 'data' / 'mongo-dump')


class Dumper:
    def __init__(self):
        self.logger = Logger().get_logger(__name__)
        self.config = configparser.ConfigParser()
        self.config.read(CONFIG_PATH)
        self.db_config = self.config['DATABASE']
        
    def create_mongo_dump(self):
        # Параметры подключения
        port = self.db_config.getint('port')
        user = self.db_config.get('user')
        password = self.db_config.get('password')
        dbname = self.db_config.get('name')

        try:
            # Подключаемся к MongoDB
            client = MongoClient(f"mongodb://{user}:{password}@localhost:{port}/")
            client.admin.command('ping')  # Проверка подключения
            self.logger.info("Успешно подключились к MongoDB")
            db = client[dbname]
            collection = db["products"]

            # Удаляем старую коллекцию
            collection.drop()
            self.logger.info("Старая коллекция 'products' удалена")

            # Чтение CSV порциями и загрузка в базу
            chunk_size = 10000  # Размер пакета
            total_records = 0
            for chunk in pd.read_csv(CSV_PATH, sep='\t', chunksize=chunk_size, dtype=float):
                records = chunk.to_dict('records')
                collection.insert_many(records)
                total_records += len(records)
                self.ogger.info(f"Загружено {total_records} записей")
            self.logger.info(f"Всего загружено {total_records} записей в коллекцию 'products' базы '{dbname}'")
        except Exception as e:
            self.logger.error(f"Ошибка при загрузке данных в MongoDB: {e}", exc_info=True)
            sys.exit(1)
        finally:
            client.close()

        # Создаем дамп базы данных
        try:
            subprocess.run([
                "mongodump",
                "--host", "localhost",
                "--port", str(port),
                "--username", user,
                "--password", password,
                "--authenticationDatabase", "admin",
                "--db", dbname,
                "--out", DUMP_PATH
            ], check=True)
            self.logger.info(f"Дамп успешно создан!")
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Ошибка при создании дампа: {e}", exc_info=True)
            sys.exit(1)
    

if __name__ == "__main__":
    dumper = Dumper()
    dumper.create_mongo_dump()