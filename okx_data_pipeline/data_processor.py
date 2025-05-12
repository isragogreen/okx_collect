import asyncio
import json
import os
import numpy as np
import pandas as pd
from scipy.stats import entropy
from sklearn.cluster import KMeans
from numba import jit
import logging
import time
from datetime import datetime
from okx_data_pipeline.utils import load_buffer_from_disk, save_pending_data

logger = logging.getLogger(__name__)

@jit(nopython=True)
def find_range(weights, fraction=0.75):
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

@jit(nopython=True)
def build_order_histogram(counts):
    if len(counts) == 0:
        return np.zeros(0, dtype=np.float64)
    counts = counts.astype(np.int64)
    max_orders = np.max(counts)
    if max_orders < 0:
        return np.zeros(0, dtype=np.float64)
    hist = np.zeros(max_orders + 1, dtype=np.float64)
    for count in counts:
        hist[count] += 1.0
    return hist

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
    def __init__(self, config, bigquery_client, notifier, queue):
        self.config = config
        self.bigquery_client = bigquery_client
        self.notifier = notifier
        self.queue = queue
        self.script_directory = os.path.dirname(os.path.abspath(__file__))
        self.record_count = 0
        self.pending_data = None
        self.buffer_prefix = self.config.buffer_prefixes['glass']
        asyncio.create_task(load_buffer_from_disk(
            self.script_directory,
            self.bigquery_client,
            self.config.table_ids['glass'],
            self.notifier,
            self.buffer_prefix
        ))

    async def save_pending_data(self):
        return await save_pending_data(self.script_directory, self.pending_data, self.buffer_prefix)

    async def calculate_statistics(self, order_book):
        if not order_book:
            logger.error("Empty order book")
            return None
        start_total = time.time()
        stats = {}
        try:
            if not order_book['asks'] or not order_book['bids']:
                logger.error("Empty asks or bids")
                return None
            asks_data = [[float(row[0]), float(row[1]), int(row[2])] for row in order_book['asks']]
            bids_data = [[float(row[0]), float(row[1]), int(row[2])] for row in order_book['bids']]
            asks = pd.DataFrame(asks_data, columns=['price', 'volume', 'count'])
            bids = pd.DataFrame(bids_data, columns=['price', 'volume', 'count'])
            asks_prices = asks['price'].values
            asks_volumes = asks['volume'].values
            asks_counts = asks['count'].values
            bids_prices = bids['price'].values
            bids_volumes = bids['volume'].values
            bids_counts = bids['count'].values
            all_volumes = np.concatenate([asks_volumes, bids_volumes])
            asks_counts_hist = build_order_histogram(asks_counts)
            bids_counts_hist = build_order_histogram(bids_counts)
            stats['med_vol_ask'] = np.median(asks_volumes)
            stats['med_vol_bid'] = np.median(bids_volumes)
            stats['med_cnt_ask'] = np.median(asks_counts)
            stats['med_cnt_bid'] = np.median(bids_counts)
            stats['wavg_vol_ask'] = np.average(asks_prices, weights=asks_volumes) if np.sum(asks_volumes) > 0 else 0.0
            stats['wavg_vol_bid'] = np.average(bids_prices, weights=bids_volumes) if np.sum(bids_volumes) > 0 else 0.0
            stats['wavg_cnt_ask'] = np.average(asks_prices, weights=asks_counts) if np.sum(asks_counts) > 0 else 0.0
            stats['wavg_cnt_bid'] = np.average(bids_prices, weights=bids_counts) if np.sum(bids_counts) > 0 else 0.0
            if np.sum(all_volumes) == 0:
                stats['entropy'] = 0.0
            else:
                normalized_volumes = all_volumes / np.sum(all_volumes)
                stats['entropy'] = entropy(normalized_volumes, base=2)
            stats['tot_vol_ask'] = np.sum(asks_volumes)
            stats['tot_vol_bid'] = np.sum(bids_volumes)
            stats['rng_vol_ask'] = {'min': 0.0, 'max': 0.0}
            stats['rng_vol_bid'] = {'min': 0.0, 'max': 0.0}
            stats['rng_cnt_ask'] = {'min': 0, 'max': 0}
            stats['rng_cnt_bid'] = {'min': 0, 'max': 0}
            if len(asks_volumes) > 0:
                range_vol_ask = find_range(asks_volumes, self.config.fraction)
                stats['rng_vol_ask'] = {'min': asks_prices[range_vol_ask[0]], 'max': asks_prices[range_vol_ask[1]]}
            if len(bids_volumes) > 0:
                range_vol_bid = find_range(bids_volumes, self.config.fraction)
                stats['rng_vol_bid'] = {'min': bids_prices[range_vol_bid[1]], 'max': bids_prices[range_vol_bid[0]]}
            if len(asks_counts_hist) > 0:
                range_cnt_ask = find_range(asks_counts_hist, self.config.fraction)
                stats['rng_cnt_ask'] = {'min': int(range_cnt_ask[0]), 'max': int(range_cnt_ask[1])}
            if len(bids_counts_hist) > 0:
                range_cnt_bid = find_range(bids_counts_hist, self.config.fraction)
                stats['rng_cnt_bid'] = {'min': int(range_cnt_bid[0]), 'max': int(range_cnt_bid[1])}
            stats['cent_ask'] = apply_kmeans(asks)
            stats['cent_bid'] = apply_kmeans(bids)
            logger.info(f"Statistics computed in {time.time() - start_total:.4f} seconds")
            return stats
        except Exception as e:
            logger.error(f"Error calculating statistics: {e}")
            return None

    def prepare_bigquery_data(self, order_book, stats):
        if not stats:
            return None
        now = datetime.utcfromtimestamp(order_book['ts'] / 1000.0)  # UTC from ts
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

    async def run_test(self):
        while True:
            data = await self.queue.get()
            order_book = data['books_full']
            order_book['ts'] = data['ts']  # Use ts from queue
            stats = await self.calculate_statistics(order_book)
            if stats:
                bq_data = self.prepare_bigquery_data(order_book, stats)
                if bq_data:
                    self.pending_data = bq_data
                    success = await self.bigquery_client.load_data(
                        [bq_data], self.config.table_ids['glass'], notifier=self.notifier
                    )
                    if success:
                        self.record_count += 1
                        self.pending_data = None
                    else:
                        await self.save_pending_data()
            self.queue.task_done()
            
