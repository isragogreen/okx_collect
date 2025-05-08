import aiohttp
import asyncio
import json
import os
from datetime import datetime
from google.oauth2 import service_account
from google.cloud import bigquery
import logging
import tempfile
import pandas as pd
import glob
import time
import random

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class OKXDataCollector:
    def __init__(self, buffer_size=10, debug=False):
        self.symbol = "BTC-USDT"  # Торговая пара
        self.api_base_url = "https://www.okx.com"
        self.buffer_size = buffer_size
        self.debug = debug  # Режим отладки
        self.buffer = []  # Буфер для накопления данных
        self.requests = [
            {
                'endpoint': '/api/v5/market/books',
                'params': {'instId': self.symbol, 'sz': 25},
                'name': 'order_book'
            },
            {
                'endpoint': '/api/v5/market/candles',
                'params': {'instId': self.symbol, 'bar': '1m', 'limit': 1},
                'name': 'candlestick'
            },
            {
                'endpoint': '/api/v5/market/ticker',
                'params': {'instId': self.symbol},
                'name': 'ticker'
            }
        ]
        # Конфигурация BigQuery
        self.project_id = 'wise-trainer-250014'
        self.dataset_id = 'wisetrainer250014_test'
        self.table_id = 'realtime_data'
        self.script_directory = os.path.dirname(os.path.abspath(__file__))
        self.credentials_path = os.path.join(self.script_directory, 'cred', 'wise-trainer-250014-917afbf4c8fe.json')
        
        # Загрузка credentials и настройка BigQuery
        try:
            self.credentials = service_account.Credentials.from_service_account_file(self.credentials_path)
            self.client = bigquery.Client(credentials=self.credentials, project=self.project_id)
            self.table_ref = self.client.dataset(self.dataset_id).table(self.table_id)
            self.job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                autodetect=False
            )
            # Проверка доступа к таблице
            self.client.get_table(self.table_ref)
            logger.info(f"Успешно инициализирован клиент BigQuery для таблицы {self.table_ref}")
        except Exception as e:
            logger.error(f"Ошибка инициализации BigQuery: {e}")
            raise RuntimeError(f"Не удалось инициализировать BigQuery: {e}")

        # Чтение сохраненных данных с диска при инициализации
        self.load_buffer_from_disk()

    def load_buffer_from_disk(self):
        """Чтение сохраненных данных из JSON-файлов в буфер."""
        try:
            buffer_files = glob.glob(os.path.join(self.script_directory, "okx_buffer_*.json"))
            if not buffer_files:
                logger.info("Не найдено сохраненных буферов на диске")
                return

            for file_path in buffer_files:
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                        if isinstance(data, list):
                            self.buffer.extend(data)
                        else:
                            self.buffer.append(data)
                    logger.info(f"Загружен буфер из файла {file_path}")
                    # Удаление файла после успешной загрузки
                    os.remove(file_path)
                    logger.info(f"Файл {file_path} удален после загрузки")
                except Exception as e:
                    logger.error(f"Ошибка при чтении файла {file_path}: {e}")
                    continue

            logger.info(f"Всего загружено {len(self.buffer)} записей из сохраненных файлов")
            # Попытка немедленной записи загруженных данных в BigQuery
            if self.buffer:
                self.attempt_save_buffer()
        except Exception as e:
            logger.error(f"Ошибка при загрузке буфера с диска: {e}")

    def attempt_save_buffer(self):
        """Попытка сохранить буфер в BigQuery с несколькими попытками, затем на диск при неудаче."""
        max_bq_attempts = 3  # Максимальное количество попыток записи в BigQuery
        min_retry_delay = 10  # Минимальная задержка между попытками (секунды)
        max_retry_delay = 15  # Максимальная задержка между попытками (секунды)

        if not self.buffer:
            logger.info("Буфер пуст, сохранение не требуется")
            return

        logger.info(f"Попытка сохранить {len(self.buffer)} записей из буфера в BigQuery...")
        for attempt in range(max_bq_attempts):
            try:
                job = self.client.load_table_from_json(self.buffer, self.table_ref, job_config=self.job_config)
                job.result()
                logger.info(f"Успешно загружено {len(self.buffer)} записей в BigQuery")
                self.buffer.clear()
                return
            except Exception as e:
                logger.error(f"Попытка {attempt + 1}/{max_bq_attempts} записи в BigQuery не удалась: {e}")
                if attempt < max_bq_attempts - 1:
                    retry_delay = random.uniform(min_retry_delay, max_retry_delay)
                    logger.info(f"Ожидание {retry_delay:.2f} секунд перед следующей попыткой...")
                    time.sleep(retry_delay)
                else:
                    logger.error("Не удалось сохранить буфер в BigQuery. Сохранение на диск...")
                    self.save_to_file(self.buffer)
                    self.buffer.clear()
                    logger.info("Буфер сохранен в файл")

    async def fetch_data(self, session, request):
        """Асинхронный запрос данных с биржи."""
        url = f"{self.api_base_url}{request['endpoint']}"
        try:
            async with session.get(url, params=request['params']) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('code') == '0':
                        return {
                            'name': request['name'],
                            'data': data['data'],
                            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        }
                    else:
                        logger.error(f"Ошибка API для {request['name']}: {data.get('msg', 'Unknown error')}")
                        return None
                else:
                    logger.error(f"Ошибка HTTP для {request['name']}: {response.status}")
                    return None
        except Exception as e:
            logger.error(f"Исключение при запросе {request['name']}: {e}")
            return None

    def process_data(self, order_book, candlestick, ticker):
        """Преобразование сырых данных в формат таблицы realtime_data."""
        try:
            # Проверка наличия всех данных
            if not (order_book and candlestick and ticker):
                logger.error("Неполные данные для обработки")
                return None

            # Извлечение timestamp
            ts = int(ticker[0]['ts'])
            if self.debug:
                logger.debug(f"Обработка ts: {ticker[0]['ts']} -> {ts}")

            # Время устройства
            now = datetime.now()
            date_str = now.strftime('%Y-%m-%d')
            time_str = now.strftime('%H:%M:%S')

            # Обработка bids и asks с использованием pandas
            bids_df = pd.DataFrame(order_book[0]['bids'][:25], columns=['price', 'quantity', '_', 'num_orders'])
            bids = bids_df[['price', 'quantity', 'num_orders']].astype({
                'price': 'float',
                'quantity': 'float',
                'num_orders': 'int'
            }).to_dict('records')

            asks_df = pd.DataFrame(order_book[0]['asks'][:25], columns=['price', 'quantity', '_', 'num_orders'])
            asks = asks_df[['price', 'quantity', 'num_orders']].astype({
                'price': 'float',
                'quantity': 'float',
                'num_orders': 'int'
            }).to_dict('records')

            # Формирование структуры данных
            data = {
                "ts": ts,
                "date": date_str,
                "time": time_str,
                "data": {
                    "bids": bids,
                    "asks": asks,
                    "open": float(candlestick[0][1]),
                    "high": float(candlestick[0][2]),
                    "low": float(candlestick[0][3]),
                    "close": float(candlestick[0][4]),
                    "volume": float(candlestick[0][5]),
                    "volCcy": float(candlestick[0][6]),
                    "volCcyQuote": float(candlestick[0][7]),
                    "confirm": str(candlestick[0][8]),
                    "instType": str(ticker[0]['instType']),
                    "instId": str(ticker[0]['instId']),
                    "last": float(ticker[0]['last']),
                    "lastSz": float(ticker[0]['lastSz']),
                    "askPx": float(ticker[0]['askPx']),
                    "askSz": float(ticker[0]['askSz']),
                    "bidPx": float(ticker[0]['bidPx']),
                    "bidSz": float(ticker[0]['bidSz']),
                    "open24h": float(ticker[0]['open24h']),
                    "high24h": float(ticker[0]['high24h']),
                    "low24h": float(ticker[0]['low24h']),
                    "volCcy24h": float(ticker[0]['volCcy24h']),
                    "vol24h": float(ticker[0]['vol24h']),
                    "sodUtc0": float(ticker[0]['sodUtc0']),
                    "sodUtc8": float(ticker[0]['sodUtc8'])
                }
            }
            return data
        except Exception as e:
            logger.error(f"Ошибка при обработке данных: {e}")
            return None

    def save_to_file(self, data):
        """Сохранение данных в JSON-файл."""
        filename = f"okx_buffer_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        try:
            with open(os.path.join(self.script_directory, filename), 'w') as f:
                json.dump(data, f, indent=2)
            logger.info(f"Данные сохранены в {filename}")
        except Exception as e:
            logger.error(f"Ошибка при сохранении в {filename}: {e}")

    def insert_into_bigquery(self):
        """Загрузка буфера в BigQuery через batch-загрузку из памяти."""
        try:
            # Загрузка данных напрямую из памяти
            job = self.client.load_table_from_json(self.buffer, self.table_ref, job_config=self.job_config)
            job.result()

            logger.info(f"Успешно загружено {len(self.buffer)} записей в {self.table_ref}")
            self.buffer.clear()
        except Exception as e:
            logger.error(f"Ошибка при загрузке в BigQuery: {e}")
            self.attempt_save_buffer()

    async def collect_data(self, interval=60):
        """Основной цикл сбора данных."""
        async with aiohttp.ClientSession() as session:
            while True:
                start_time = datetime.now()
                logger.info(f"Начало цикла сбора данных: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

                # Выполнение запросов
                tasks = [self.fetch_data(session, req) for req in self.requests]
                responses = await asyncio.gather(*tasks, return_exceptions=True)

                # Обработка ответов
                data_dict = {}
                for response in responses:
                    if isinstance(response, Exception):
                        logger.error(f"Ошибка в запросе: {response}")
                        continue
                    if response:
                        data_dict[response['name']] = response['data']
                    else:
                        logger.warning(f"Не удалось получить данные для {response['name']}")

                # Преобразование данных
                if all(key in data_dict for key in ['order_book', 'candlestick', 'ticker']):
                    processed_data = self.process_data(
                        data_dict['order_book'],
                        data_dict['candlestick'],
                        data_dict['ticker']
                    )
                    if processed_data:
                        self.buffer.append(processed_data)
                        logger.info(f"Добавлено в буфер: {len(self.buffer)}/{self.buffer_size} записей")
                    else:
                        logger.warning("Не удалось обработать данные")
                else:
                    logger.warning("Неполные данные, пропуск обработки")

                # Сохранение буфера в BigQuery при достижении размера
                if len(self.buffer) >= self.buffer_size:
                    logger.info(f"Буфер полон, загрузка в BigQuery...")
                    self.insert_into_bigquery()

                # Ожидание до следующего цикла
                elapsed = (datetime.now() - start_time).total_seconds()
                sleep_time = max(0, interval - elapsed)
                await asyncio.sleep(sleep_time)

async def run_collector(collector):
    """Запуск сборщика данных с переданным коллектором."""
    try:
        await collector.collect_data(interval=60)
    except Exception as e:
        logger.error(f"Критическая ошибка в сборщике: {e}")
        collector.attempt_save_buffer()
        raise

async def main_with_retry():
    """Основной цикл с повторными попытками при сбоях и обработкой Ctrl+C."""
    retry_delay = 10  # Задержка перед повторной попыткой (в секундах)

    try:
        collector = OKXDataCollector(buffer_size=10, debug=True)
        while True:
            try:
                await run_collector(collector)
            except Exception as e:
                logger.error(f"Программа упала с ошибкой: {e}. Перезапуск через {retry_delay} секунд...")
                time.sleep(retry_delay)
                continue
    except KeyboardInterrupt:
        logger.info("Получен сигнал прерывания (Ctrl+C). Завершение работы...")
        collector.attempt_save_buffer()
        logger.info("Программа завершена.")
        exit(0)

if __name__ == "__main__":
    asyncio.run(main_with_retry())
