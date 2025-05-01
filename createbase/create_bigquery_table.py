import json
import os
from google.oauth2 import service_account
from google.cloud import bigquery
import logging

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Конфигурация BigQuery
project_id = 'wise-trainer-250014'
dataset_id = 'wisetrainer250014_test'
table_id = 'realtime_data_tst'
table_id_glass = 'realtime_data_glass_tst'

# Получаем путь к директории скрипта
script_directory = os.path.dirname(os.path.abspath(__file__))

# Путь к файлу с учетными данными
credentials_path = os.path.join(script_directory, 'cred', 'wise-trainer-250014-917afbf4c8fe.json')

# Путь к файлу со схемой
schema_path = os.path.join(script_directory, 'btcusdt1min_raw.dat')
schema_path_glass = os.path.join(script_directory, 'btcusdt1min_glass.dat')

def load_schema_from_json(schema_path: str) -> list:
    """Чтение схемы из JSON-файла и преобразование в формат BigQuery."""
    try:
        with open(schema_path, 'r', encoding='utf-8') as f:
            schema_data = json.load(f)
        
        def parse_field(field: dict) -> bigquery.SchemaField:
            """Рекурсивное преобразование поля в SchemaField."""
            name = field['name']
            field_type = field['type']
            mode = field.get('mode', 'NULLABLE')
            description = field.get('description', '')
            
            if field_type == 'RECORD':
                sub_fields = [parse_field(sub_field) for sub_field in field.get('fields', [])]
                return bigquery.SchemaField(name, field_type, mode=mode, description=description, fields=sub_fields)
            return bigquery.SchemaField(name, field_type, mode=mode, description=description)
        
        schema = [parse_field(item) for item in schema_data]
        logger.info(f"Схема успешно загружена из {schema_path}")
        return schema
    
    except FileNotFoundError:
        logger.error(f"Файл схемы не найден: {schema_path}")
        raise
    except json.JSONDecodeError:
        logger.error(f"Ошибка декодирования JSON в файле: {schema_path}")
        raise
    except Exception as e:
        logger.error(f"Ошибка при загрузке схемы: {e}")
        raise

def ensure_dataset(client, dataset_ref):
    """Проверка и создание датасета, если он не существует."""
    try:
        client.get_dataset(dataset_ref)
        logger.info(f"Датасет {dataset_ref} уже существует.")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset = client.create_dataset(dataset)
        logger.info(f"Датасет {dataset_ref} создан.")

def create_bigquery_table():
    """Создание таблицы в BigQuery с использованием схемы из JSON."""
    try:
        # Создаем учетные данные
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        logger.info(f"Учетные данные загружены из {credentials_path}")

        # Создаем клиент BigQuery
        client = bigquery.Client(credentials=credentials, project=project_id)

        # Получаем ссылку на набор данных
        dataset_ref = client.dataset(dataset_id)

        # Проверяем и создаем датасет, если нужно
        ensure_dataset(client, dataset_ref)

        # Получаем ссылку на таблицу
        table_ref = dataset_ref.table(table_id)

        # Загружаем схему
        schema = load_schema_from_json(schema_path)
        schema_glass = load_schema_from_json(schema_path_glass)

        # Создаем объект таблицы
        table = bigquery.Table(table_ref, schema=schema)
        table_glass = bigquery.Table(table_ref, schema=schema_glass)


        # Создаем новую таблицу
        table = client.create_table(table)
        table_glass = client.create_table(table_glass)
        logger.info(f"Новая таблица {table.full_table_id} успешно создана.")
        logger.info(f"Новая таблица {table_glass.full_table_id} успешно создана.")

    except FileNotFoundError as e:
        logger.error(f"Файл не найден: {e}")
        raise
    except Exception as e:
        logger.error(f"Ошибка при создании таблицы: {e}")
        raise

if __name__ == "__main__":
    create_bigquery_table()
