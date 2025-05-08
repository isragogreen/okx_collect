import aiohttp
import asyncio
import numpy as np
import pandas as pd
from scipy.stats import entropy
from sklearn.cluster import KMeans
from google.cloud import bigquery
from google.oauth2 import service_account
from numba import jit
import logging
import time
from datetime import datetime
import os
import json

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Numba-оптимизированная функция для поиска минимального окна
@jit(nopython=True)
def find_range(weights, fraction=0.75):
    """
    Поиск минимального окна в массиве weights, где сумма >= fraction * sum(weights).
    
    Parameters:
    - weights: массив неотрицательных значений (например, частоты ордеров или объемы).
    - fraction: целевая доля суммы (например, 0.75).
    
    Returns:
    - np.array([left, right]): индексы начала и конца диапазона.
    """
    if len(weights) == 0:
        return np.array([0, 0], dtype=np.int64)
    
    total = np.sum(weights)
    if total == 0:
        return np.array([0, 0], dtype=np.int64)
    
    target = fraction * total
    min_length = np.inf
    best_range = np.array([0, 0], dtype=np.int64)
    current_sum = 0.0
    left = 0
    
    for right in range(len(weights)):
        current_sum += weights[right]
        while current_sum >= target and left <= right:
            current_length = right - left + 1
            if current_length < min_length:
                min_length = current_length
                best_range = np.array([left, right], dtype=np.int64)
            current_sum -= weights[left]
            left += 1
    
    return best_range

# Numba-оптимизированная функция для построения гистограммы частот ордеров
@jit(nopython=True)
def build_order_histogram(counts):
    """
    Строит гистограмму частот для массива counts (целочисленные значения).
    """
    if len(counts) == 0:
        return np.zeros(0, dtype=np.float64)
    
    # Убедимся, что counts - это массив целых чисел
    counts = counts.astype(np.int64)
    max_orders = np.max(counts)
    if max_orders < 0:
        return np.zeros(0, dtype=np.float64)
    
    # Создаем гистограмму с размером max_orders + 1
    hist = np.zeros(max_orders + 1, dtype=np.float64)
    for count in counts:
        hist[count] += 1.0
    
    return hist

# Функция для применения K-means
def apply_kmeans(data, max_clusters=3):
    if len(data) == 0:
        return []
    X = data[['price', 'volume']].values
    n_clusters = min(max_clusters, len(X))
    kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
    kmeans.fit(X)
    labels = kmeans.labels_
    centroids = []
    for i in range(kmeans.n_clusters):
        cluster_mask = labels == i
        cluster_prices = data['price'].values[cluster_mask]
        cluster_volumes = data['volume'].values[cluster_mask]
        cluster_counts = data['count'].values[cluster_mask]
        centroid_price = kmeans.cluster_centers_[i][0]
        centroid_volume = np.sum(cluster_volumes)
        centroid_count = np.sum(cluster_counts)
        centroids.append({
            'price': centroid_price,
            'vol': centroid_volume,
            'cnt': float(centroid_count)
        })
    return centroids

class OKXPerformanceTest:
    def __init__(self, debug=False, fraction=0.75):
        self.symbol = "BTC-USDT"
        self.api_base_url = "https://www.okx.com"
        self.debug = debug
        self.fraction = fraction
        self.project_id = 'wise-trainer-250014'
        self.dataset_id = 'wisetrainer250014_test'
        self.table_id = 'glass'
        self.script_directory = os.path.dirname(os.path.abspath(__file__))
        self.credentials_path = os.path.join(self.script_directory, 'cred', 'wise-trainer-250014-917afbf4c8fe.json')
        
        # Инициализация BigQuery
        try:
            self.credentials = service_account.Credentials.from_service_account_file(self.credentials_path)
            self.client = bigquery.Client(credentials=self.credentials, project=self.project_id)
            self.table_ref = self.client.dataset(self.dataset_id).table(self.table_id)
            self.job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                autodetect=False
            )
            self.client.get_table(self.table_ref)
            logger.info(f"BigQuery client initialized for table {self.table_ref}")
        except Exception as e:
            logger.error(f"Ошибка инициализации BigQuery: {e}")
            raise RuntimeError(f"Не удалось инициализировать BigQuery: {e}")

    async def fetch_order_book(self, session):
        """Асинхронный запрос данных по стакану на 5000 уровней."""
        start_time = time.time()
        url = f"{self.api_base_url}/api/v5/market/books-full"
        params = {'instId': self.symbol, 'sz': 5000}
        try:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('code') == '0':
                        elapsed = time.time() - start_time
                        logger.info(f"Запрос API завершен за {elapsed:.4f} секунд")
                        return data['data'][0]
                    else:
                        logger.error(f"Ошибка API: {data.get('msg', 'Неизвестная ошибка')}")
                        return None
                else:
                    logger.error(f"Ошибка HTTP: {response.status}")
                    return None
        except Exception as e:
            logger.error(f"Ошибка запроса: {e}")
            return None
        finally:
            elapsed = time.time() - start_time
            logger.info(f"Общее время запроса API: {elapsed:.4f} секунд")

    def calculate_statistics(self, order_book):
        """
        Вычисление всех статистик с замером времени.
        """
        if not order_book:
            logger.error("Пустой стакан ордеров")
            return None
        
        start_total = time.time()
        stats = {}
        start_prepare = time.time()
        
#        logger.info(f"Ключи стакана: {order_book.keys()}")
#        logger.info(f"Пример asks (первые 5): {order_book['asks'][:5] if order_book['asks'] else 'Пусто'}")
#       logger.info(f"Пример bids (первые 5): {order_book['bids'][:5] if order_book['bids'] else 'Пусто'}")
        
        try:
            # Проверка на пустые списки
            if not order_book['asks'] or not order_book['bids']:
                logger.error("Пустые asks или bids в стакане")
                return None
            
            # Преобразуем данные в списки списков
            asks_data = []
            for row in order_book['asks']:
                if len(row) != 3:
                    logger.error(f"Неверный формат строки в asks: {row}")
                    raise ValueError(f"Неверный формат строки в asks: {row}")
                try:
                    asks_data.append([float(row[0]), float(row[1]), int(row[2])])
                except (ValueError, TypeError) as e:
                    logger.error(f"Ошибка преобразования строки в asks: {row}, ошибка: {e}")
                    with open(f"error_asks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", 'w') as f:
                        json.dump(order_book['asks'], f, indent=2)
                    raise ValueError(f"Ошибка преобразования строки в asks: {row}, ошибка: {e}")
            
            bids_data = []
            for row in order_book['bids']:
                if len(row) != 3:
                    logger.error(f"Неверный формат строки в bids: {row}")
                    raise ValueError(f"Неверный формат строки в bids: {row}")
                try:
                    bids_data.append([float(row[0]), float(row[1]), int(row[2])])
                except (ValueError, TypeError) as e:
                    logger.error(f"Ошибка преобразования строки в bids: {row}, ошибка: {e}")
                    with open(f"error_bids_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", 'w') as f:
                        json.dump(order_book['bids'], f, indent=2)
                    raise ValueError(f"Ошибка преобразования строки в bids: {row}, ошибка: {e}")
            
            # Логируем первые несколько строк и их типы для отладки
#            logger.info(f"Первые 5 строк asks_data: {asks_data[:5]}")
#            logger.info(f"Типы первой строки asks_data: {[type(x) for x in asks_data[0]] if asks_data else 'Пусто'}")
#            logger.info(f"Первые 5 строк bids_data: {bids_data[:5]}")
#            logger.info(f"Типы первой строки bids_data: {[type(x) for x in bids_data[0]] if bids_data else 'Пусто'}")
            
            # Создаем DataFrame из списков списков
            asks = pd.DataFrame(asks_data, columns=['price', 'volume', 'count'])
            bids = pd.DataFrame(bids_data, columns=['price', 'volume', 'count'])
            
#            logger.info(f"Типы данных asks DataFrame: {asks.dtypes}")
#            logger.info(f"Типы данных bids DataFrame: {bids.dtypes}")
            
        except Exception as e:
            logger.error(f"Ошибка создания DataFrame: {e}")
            with open(f"error_order_book_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", 'w') as f:
                json.dump(order_book, f, indent=2)
            raise
        
        asks_prices = asks['price'].values
        asks_volumes = asks['volume'].values
        asks_counts = asks['count'].values
        bids_prices = bids['price'].values
        bids_volumes = bids['volume'].values
        bids_counts = bids['count'].values
        all_volumes = np.concatenate([asks_volumes, bids_volumes])
        
        # Логируем размеры и содержимое counts для отладки
#        logger.info(f"Размер asks_counts: {len(asks_counts)}, первые 5 элементов: {asks_counts[:5]}")
#        logger.info(f"Размер bids_counts: {len(bids_counts)}, первые 5 элементов: {bids_counts[:5]}")
        
        # Построение гистограмм для количества ордеров
        asks_counts_hist = build_order_histogram(asks_counts)
        bids_counts_hist = build_order_histogram(bids_counts)
        
        elapsed_prepare = time.time() - start_prepare
        logger.info(f"Время подготовки данных (включая гистограмму): {elapsed_prepare:.4f} секунд")

        # Медиана
        start_median = time.time()
        stats['med_vol_ask'] = np.median(asks_volumes)
        stats['med_vol_bid'] = np.median(bids_volumes)
        stats['med_cnt_ask'] = np.median(asks_counts)
        stats['med_cnt_bid'] = np.median(bids_counts)
        elapsed_median = time.time() - start_median
        logger.info(f"Время вычисления медианы: {elapsed_median:.4f} секунд")

        # Средневзвешенное
        start_weighted = time.time()
        stats['wavg_vol_ask'] = np.average(asks_prices, weights=asks_volumes)
        stats['wavg_vol_bid'] = np.average(bids_prices, weights=bids_volumes)
        stats['wavg_cnt_ask'] = np.average(asks_prices, weights=asks_counts)
        stats['wavg_cnt_bid'] = np.average(bids_prices, weights=bids_counts)
        elapsed_weighted = time.time() - start_weighted
        logger.info(f"Время вычисления средневзвешенного: {elapsed_weighted:.4f} секунд")

        # Энтропия Шеннона
        start_entropy = time.time()
        normalized_volumes = all_volumes / np.sum(all_volumes)
        stats['entropy'] = entropy(normalized_volumes, base=2)
        elapsed_entropy = time.time() - start_entropy
        logger.info(f"Время вычисления энтропии Шеннона: {elapsed_entropy:.4f} секунд")

        # Суммы объемов
        start_totals = time.time()
        stats['tot_vol_ask'] = np.sum(asks_volumes)
        stats['tot_vol_bid'] = np.sum(bids_volumes)
        elapsed_totals = time.time() - start_totals
        logger.info(f"Время вычисления сумм: {elapsed_totals:.4f} секунд")

        # Диапазоны для заданной доли ордеров
        start_range = time.time()
        stats['rng_vol_ask'] = {'min': 0.0, 'max': 0.0}
        stats['rng_vol_bid'] = {'min': 0.0, 'max': 0.0}
        stats['rng_cnt_ask'] = {'min': 0, 'max': 0}
        stats['rng_cnt_bid'] = {'min': 0, 'max': 0}

        if len(asks_volumes) > 0:
            range_vol_ask = find_range(asks_volumes, self.fraction)
            stats['rng_vol_ask'] = {
                'min': asks_prices[range_vol_ask[0]],
                'max': asks_prices[range_vol_ask[1]]
            }

        if len(bids_volumes) > 0:
            range_vol_bid = find_range(bids_volumes, self.fraction)
            stats['rng_vol_bid'] = {
                'min': bids_prices[range_vol_bid[1]],
                'max': bids_prices[range_vol_bid[0]]
            }

        if len(asks_counts_hist) > 0:
            range_cnt_ask = find_range(asks_counts_hist, self.fraction)
            stats['rng_cnt_ask'] = {
                'min': int(range_cnt_ask[0]),
                'max': int(range_cnt_ask[1])
            }

        if len(bids_counts_hist) > 0:
            range_cnt_bid = find_range(bids_counts_hist, self.fraction)
            stats['rng_cnt_bid'] = {
                'min': int(range_cnt_bid[0]),
                'max': int(range_cnt_bid[1])
            }

        elapsed_range = time.time() - start_range
        logger.info(f"Время вычисления диапазонов (fraction={self.fraction}): {elapsed_range:.4f} секунд")

        # K-means для уровней поддержки
        start_kmeans = time.time()
        stats['cent_ask'] = apply_kmeans(asks)
        stats['cent_bid'] = apply_kmeans(bids)
        elapsed_kmeans = time.time() - start_kmeans
        logger.info(f"Время кластеризации K-means: {elapsed_kmeans:.4f} секунд")

        elapsed_total = time.time() - start_total
        logger.info(f"Общее время вычисления статистик: {elapsed_total:.4f} секунд")
        return stats

    def prepare_bigquery_data(self, order_book, stats):
        """Формирование данных для BigQuery."""
        if not stats:
            logger.error("Пустые статистики, невозможно подготовить данные для BigQuery")
            return None
        now = datetime.now()
        return {
            'ts': int(order_book['ts']),
            'date': now.strftime('%Y-%m-%d'),
            'time': now.strftime('%H:%M:%S'),
            'med_vol_ask': stats['med_vol_ask'],
            'med_vol_bid': stats['med_vol_bid'],
            'med_cnt_ask': stats['med_cnt_ask'],
            'med_cnt_bid': stats['med_cnt_bid'],
            'wavg_vol_ask': stats['wavg_vol_ask'],
            'wavg_vol_bid': stats['wavg_vol_bid'],
            'wavg_cnt_ask': stats['wavg_cnt_ask'],
            'wavg_cnt_bid': stats['wavg_cnt_bid'],
            'entropy': stats['entropy'],
            'tot_vol_ask': stats['tot_vol_ask'],
            'tot_vol_bid': stats['tot_vol_bid'],
            'rng_vol_ask': stats['rng_vol_ask'],
            'rng_vol_bid': stats['rng_vol_bid'],
            'rng_cnt_ask': stats['rng_cnt_ask'],
            'rng_cnt_bid': stats['rng_cnt_bid'],
            'cent_ask': stats['cent_ask'],
            'cent_bid': stats['cent_bid']
        }

    def insert_into_bigquery(self, data):
        """Загрузка данных в BigQuery."""
        start_time = time.time()
        try:
            job = self.client.load_table_from_json([data], self.table_ref, job_config=self.job_config)
            job.result()
            elapsed = time.time() - start_time
            logger.info(f"Вставка в BigQuery завершена за {elapsed:.4f} секунд")
        except Exception as e:
            logger.error(f"Ошибка вставки в BigQuery: {e}")
            if self.debug:
                with open(os.path.join(self.script_directory, f"error_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"), 'w') as f:
                    json.dump(data, f, indent=2)
                logger.info(f"Данные с ошибкой сохранены для отладки")

    async def run_test(self):
        """Основной цикл теста с задержкой 60 секунд."""
        async with aiohttp.ClientSession() as session:
            logger.info("Запуск теста производительности")
            while True:
                try:
                    start_cycle = time.time()
                    order_book = await self.fetch_order_book(session)
                    if order_book:
                        logger.info("Стакан успешно получен")
                        stats = self.calculate_statistics(order_book)
                        if stats:
                            logger.info("Статистики успешно вычислены")
                            data = self.prepare_bigquery_data(order_book, stats)
                            if data:
                                logger.info("Данные для BigQuery успешно подготовлены")
                                self.insert_into_bigquery(data)
                            else:
                                logger.warning("Не удалось подготовить данные для BigQuery")
                        else:
                            logger.warning("Не удалось вычислить статистики")
                    else:
                        logger.warning("Не удалось получить стакан")
                    
                    elapsed_cycle = time.time() - start_cycle
                    sleep_time = max(0, 60 - elapsed_cycle)
                    logger.info(f"Цикл завершен за {elapsed_cycle:.4f} секунд, ожидание {sleep_time:.4f} секунд")
                    logger.info(f"-------------------------------------------------------------------------")
                    await asyncio.sleep(sleep_time)
                except Exception as e:
                    logger.error(f"Ошибка цикла: {e}")
                    import traceback
                    logger.error(traceback.format_exc())
                    await asyncio.sleep(10)

async def main():
    try:
        tester = OKXPerformanceTest(debug=True, fraction=0.75)
        await tester.run_test()
    except RuntimeError as e:
        logger.error(f"Program terminated due to initialization error: {e}")
        exit(1)

if __name__ == "__main__":
    asyncio.run(main())
