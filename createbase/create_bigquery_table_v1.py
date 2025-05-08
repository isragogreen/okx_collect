import json
import os
import argparse
from google.oauth2 import service_account
from google.cloud import bigquery
import logging
from google.api_core.exceptions import Forbidden

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Конфигурация BigQuery
project_id = 'wise-trainer-250014'
dataset_id = 'wisetrainer250014_test'
table_id = 'realtime_data_tst'

# Путь к директории скрипта
script_directory = os.path.dirname(os.path.abspath(__file__))

# Путь к файлу учетных данных
credentials_path = os.path.join(script_directory, 'cred', 'wise-trainer-250014-917afbf4c8fe.json')

# Значение по умолчанию для схемы
DEFAULT_SCHEMA = 'btcusdt1min_raw.dat'

def load_schema_from_json(schema_file: str) -> list:
    """Чтение схемы из файла и преобразование в формат BigQuery."""
    schema_path = os.path.join(script_directory, schema_file)
    try:
        with open(schema_path, 'r', encoding='utf-8') as f:
            schema_data = json.load(f)
        
        def parse_field(field: dict) -> bigquery.SchemaField:
            name = field['name']
            field_type = field['type']
            mode = field.get('mode', 'NULLABLE')
            description = field.get('description', '')
            
            if field_type == 'RECORD':
                sub_fields = [parse_field(sub_field) for sub_field in field.get('fields', [])]
                return bigquery.SchemaField(name, field_type, mode=mode, description=description, fields=sub_fields)
            return bigquery.SchemaField(name, field_type, mode=mode, description=description)
        
        schema = [parse_field(item) for item in schema_data]
        logger.info(f"Схема загружена из {schema_path}")
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
        logger.info(f"Датасет {dataset_ref} существует")
    except Forbidden as e:
        if "SERVICE_DISABLED" in str(e):
            logger.error(
                "BigQuery API не активирован для проекта. "
                "Активируйте API по ссылке: "
                "https://console.developers.google.com/apis/api/bigquery.googleapis.com/overview?project=" + project_id +
                " и подождите несколько минут."
            )
            raise
        raise
    except Exception:
        try:
            dataset = bigquery.Dataset(dataset_ref)
            client.create_dataset(dataset)
            logger.info(f"Датасет {dataset_ref} создан")
        except Forbidden as e:
            if "SERVICE_DISABLED" in str(e):
                logger.error(
                    "BigQuery API не активирован для проекта. "
                    "Активируйте API по ссылке: "
                    "https://console.developers.google.com/apis/api/bigquery.googleapis.com/overview?project=" + project_id +
                    " и подождите несколько минут."
                )
                raise
            raise

def get_dat_files() -> list:
    """Получение списка .dat файлов в текущей папке."""
    try:
        return [f for f in os.listdir(script_directory) if f.endswith('.dat')]
    except Exception as e:
        logger.error(f"Ошибка при чтении файлов в текущей папке: {e}")
        return []

def prompt_user_for_params(default_table: str) -> tuple:
    """Запрос у пользователя имени таблицы и файла схемы."""
    dat_files = get_dat_files()
    if not dat_files:
        logger.error("В текущей папке нет файлов с расширением .dat")
        raise FileNotFoundError("Нет доступных .dat файлов")
    
    print("Доступные .dat файлы:")
    for i, file in enumerate(dat_files, 1):
        print(f"{i}. {file}")
    
    table_id = input(f"Введите имя таблицы (по умолчанию {default_table}): ").strip() or default_table
    schema_choice = input("Введите номер файла схемы или имя файла: ").strip()
    
    if schema_choice.isdigit():
        idx = int(schema_choice) - 1
        if 0 <= idx < len(dat_files):
            schema_file = dat_files[idx]
        else:
            logger.error("Неверный номер файла")
            raise ValueError("Неверный выбор файла схемы")
    else:
        schema_file = schema_choice if schema_choice.endswith('.dat') else schema_choice + '.dat'
        if schema_file not in dat_files:
            logger.error(f"Файл {schema_file} не найден в текущей папке")
            raise FileNotFoundError(f"Файл {schema_file} не существует")
    
    return table_id, schema_file

def create_bigquery_table(table_id: str, schema_file: str):
    """Создание таблицы в BigQuery с использованием схемы из файла."""
    try:
        # Проверка существования файла учетных данных
        if not os.path.exists(credentials_path):
            logger.error(f"Файл учетных данных не найден: {credentials_path}")
            raise FileNotFoundError(f"Файл учетных данных не найден: {credentials_path}")

        # Загрузка учетных данных
        credentials = service_account.Credentials.from_service_account_file(credentials_path)
        logger.info(f"Учетные данные загружены из {credentials_path}")

        # Создание клиента BigQuery
        client = bigquery.Client(credentials=credentials, project=project_id)

        # Получение ссылки на датасет
        dataset_ref = client.dataset(dataset_id)
        ensure_dataset(client, dataset_ref)

        # Получение ссылки на таблицу
        table_ref = dataset_ref.table(table_id)

        # Загрузка схемы
        schema = load_schema_from_json(schema_file)

        # Создание таблицы
        table = bigquery.Table(table_ref, schema=schema)
        created_table = client.create_table(table)  # Используем возвращаемый объект
        logger.info(f"Таблица {created_table.full_table_id} создана")

    except Exception as e:
        logger.error(f"Ошибка при создании таблицы: {e}")
        raise

def main():
    """Обработка аргументов командной строки и создание таблицы."""
    parser = argparse.ArgumentParser(description="Создание таблицы в BigQuery")
    parser.add_argument('--table', type=str, default=table_id,
                        help=f"Имя таблицы (по умолчанию {table_id})")
    parser.add_argument('--schema', type=str, default=DEFAULT_SCHEMA,
                        help=f"Имя файла схемы (по умолчанию {DEFAULT_SCHEMA})")
    
    args = parser.parse_args()

    selected_table_id = args.table.strip()
    schema_file = args.schema.strip()

    # Если параметры пустые или используются значения по умолчанию, проверяем файл схемы
    if not selected_table_id or not schema_file or (selected_table_id == table_id and schema_file == DEFAULT_SCHEMA):
        schema_path = os.path.join(script_directory, DEFAULT_SCHEMA)
        if not os.path.exists(schema_path):
            logger.info("Файл схемы по умолчанию не найден, запрашиваем параметры у пользователя")
            selected_table_id, schema_file = prompt_user_for_params(table_id)
        else:
            selected_table_id = table_id
            schema_file = DEFAULT_SCHEMA
    else:
        # Проверяем, что указанный файл схемы существует
        schema_path = os.path.join(script_directory, schema_file)
        if not schema_file.endswith('.dat'):
            schema_path += '.dat'
        if not os.path.exists(schema_path):
            logger.error(f"Указанный файл схемы {schema_file} не найден")
            selected_table_id, schema_file = prompt_user_for_params(table_id)

    logger.info(f"Создание таблицы {selected_table_id} с использованием схемы {schema_file}")
    create_bigquery_table(selected_table_id, schema_file)

if __name__ == "__main__":
    main()
