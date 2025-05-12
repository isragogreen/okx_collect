import pytest
import asyncio
import json
import os
import time
import logging
import aiohttp
from datetime import datetime
from unittest.mock import AsyncMock, patch
from okx_data_pipeline.data_processor import OKXPerformanceTest
from okx_data_pipeline.config import Config
from okx_data_pipeline.bigquery_client import BigQueryClient
from okx_data_pipeline.utils import load_buffer_from_disk, save_pending_data

logger = logging.getLogger(__name__)

@pytest.fixture
def config():
    return Config()

@pytest.fixture
async def queue():
    return asyncio.Queue(maxsize=10)

@pytest.fixture
def mock_notifier():
    notifier = AsyncMock()
    notifier.send_message = AsyncMock()
    notifier.send_exception = AsyncMock()
    return notifier

@pytest.fixture
def mock_bq_client():
    bq_client = AsyncMock(spec=BigQueryClient)
    bq_client.load_data = AsyncMock(return_value=True)
    return bq_client

@pytest.mark.asyncio
async def test_calculate_statistics(config, queue, mock_notifier, mock_bq_client):
    print("Starting test_calculate_statistics")
    processor = OKXPerformanceTest(config, mock_bq_client, mock_notifier, queue)
    
    order_book = {
        "ts": 1747080633206,
        "bids": [[1000.0, 10.0, 1], [999.0, 5.0, 2]],
        "asks": [[1001.0, 10.0, 1], [1002.0, 5.0, 2]]
    }
    
    stats = await processor.calculate_statistics(order_book)
    assert stats is not None
    assert 'med_vol_ask' in stats
    assert stats['med_vol_ask'] == 7.5  # Медиана объемов [10.0, 5.0]
    assert 'med_vol_bid' in stats
    assert stats['med_vol_bid'] == 7.5  # Медиана объемов [10.0, 5.0]
    assert 'entropy' in stats
    assert stats['entropy'] > 0  # Энтропия должна быть положительной
    assert 'cent_ask' in stats
    assert len(stats['cent_ask']) <= 3  # Максимум 3 кластера
    print("Finished test_calculate_statistics")

@pytest.mark.asyncio
async def test_prepare_bigquery_data(config, queue, mock_notifier, mock_bq_client):
    print("Starting test_prepare_bigquery_data")
    processor = OKXPerformanceTest(config, mock_bq_client, mock_notifier, queue)
    
    order_book = {
        "ts": 1747080633206,  # 2025-05-12 20:10:33 UTC
        "bids": [[1000.0, 10.0, 1]],
        "asks": [[1001.0, 10.0, 1]]
    }
    stats = {
        "med_vol_ask": 10.0,
        "med_vol_bid": 10.0,
        "med_cnt_ask": 1.0,
        "med_cnt_bid": 1.0,
        "wavg_vol_ask": 1001.0,
        "wavg_vol_bid": 1000.0,
        "wavg_cnt_ask": 1001.0,
        "wavg_cnt_bid": 1000.0,
        "entropy": 1.0,
        "tot_vol_ask": 10.0,
        "tot_vol_bid": 10.0,
        "rng_vol_ask": {"min": 1001.0, "max": 1001.0},
        "rng_vol_bid": {"min": 1000.0, "max": 1000.0},
        "rng_cnt_ask": {"min": 1, "max": 1},
        "rng_cnt_bid": {"min": 1, "max": 1},
        "cent_ask": [{"price": 1001.0, "vol": 10.0, "cnt": 1.0}],
        "cent_bid": [{"price": 1000.0, "vol": 10.0, "cnt": 1.0}]
    }
    
    bq_data = processor.prepare_bigquery_data(order_book, stats)
    assert bq_data is not None
    assert bq_data['ts'] == 1747080633206
    assert bq_data['date'] == "2025-05-12"
    assert bq_data['time'] == "20:10:33"
    assert bq_data['med_vol_ask'] == 10.0
    assert bq_data['cent_ask'] == stats['cent_ask']
    print("Finished test_prepare_bigquery_data")

@pytest.mark.asyncio
async def test_save_to_disk_on_bigquery_failure(config, queue, mock_notifier, mock_bq_client, tmp_path):
    print("Starting test_save_to_disk_on_bigquery_failure")
    processor = OKXPerformanceTest(config, mock_bq_client, mock_notifier, queue)
    processor.script_directory = str(tmp_path)
    
    mock_bq_client.load_data = AsyncMock(return_value=False)
    
    order_book = {
        "ts": 1747080633206,
        "bids": [[1000.0, 10.0, 1]],
        "asks": [[1001.0, 10.0, 1]]
    }
    
    # Мокаем calculate_statistics для возврата тестовых статистик
    with patch.object(OKXPerformanceTest, 'calculate_statistics', new=AsyncMock(return_value={
        "med_vol_ask": 10.0,
        "med_vol_bid": 10.0,
        "med_cnt_ask": 1.0,
        "med_cnt_bid": 1.0,
        "wavg_vol_ask": 1001.0,
        "wavg_vol_bid": 1000.0,
        "wavg_cnt_ask": 1001.0,
        "wavg_cnt_bid": 1000.0,
        "entropy": 1.0,
        "tot_vol_ask": 10.0,
        "tot_vol_bid": 10.0,
        "rng_vol_ask": {"min": 1001.0, "max": 1001.0},
        "rng_vol_bid": {"min": 1000.0, "max": 1000.0},
        "rng_cnt_ask": {"min": 1, "max": 1},
        "rng_cnt_bid": {"min": 1, "max": 1},
        "cent_ask": [{"price": 1001.0, "vol": 10.0, "cnt": 1.0}],
        "cent_bid": [{"price": 1000.0, "vol": 10.0, "cnt": 1.0}]
    })):
        # Выполняем одну итерацию run_test
        data = {"books_full": order_book, "ts": order_book['ts']}
        await queue.put(data)
        
        async def single_iteration_run_test(self):
            data = await self.queue.get()
            order_book = data['books_full']
            order_book['ts'] = data['ts']
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
        
        with patch.object(OKXPerformanceTest, 'run_test', new=single_iteration_run_test):
            await processor.run_test()
    
    buffer_files = list(tmp_path.glob(f"{config.buffer_prefixes['glass']}_*.json"))
    assert len(buffer_files) == 1, "Buffer file was not created"
    buffer_file = buffer_files[0]
    
    with open(buffer_file, 'r') as f:
        saved_data = json.load(f)
    assert saved_data['ts'] == 1747080633206
    assert saved_data['date'] == "2025-05-12"
    assert 'med_vol_ask' in saved_data
    print("Finished test_save_to_disk_on_bigquery_failure")

@pytest.mark.asyncio
async def test_bigquery_retries_on_failure(config, queue, mock_notifier, mock_bq_client, tmp_path):
    print("Starting test_bigquery_retries_on_failure")
    processor = OKXPerformanceTest(config, mock_bq_client, mock_notifier, queue)
    processor.script_directory = str(tmp_path)
    
    # Симулируем сбой BigQuery на первых двух попытках, успех на третьей
    mock_bq_client.load_data = AsyncMock(side_effect=[False, False, True])
    
    order_book = {
        "ts": 1747080633206,
        "bids": [[1000.0, 10.0, 1]],
        "asks": [[1001.0, 10.0, 1]]
    }
    
    with patch.object(OKXPerformanceTest, 'calculate_statistics', new=AsyncMock(return_value={
        "med_vol_ask": 10.0,
        "med_vol_bid": 10.0,
        "med_cnt_ask": 1.0,
        "med_cnt_bid": 1.0,
        "wavg_vol_ask": 1001.0,
        "wavg_vol_bid": 1000.0,
        "wavg_cnt_ask": 1001.0,
        "wavg_cnt_bid": 1000.0,
        "entropy": 1.0,
        "tot_vol_ask": 10.0,
        "tot_vol_bid": 10.0,
        "rng_vol_ask": {"min": 1001.0, "max": 1001.0},
        "rng_vol_bid": {"min": 1000.0, "max": 1000.0},
        "rng_cnt_ask": {"min": 1, "max": 1},
        "rng_cnt_bid": {"min": 1, "max": 1},
        "cent_ask": [{"price": 1001.0, "vol": 10.0, "cnt": 1.0}],
        "cent_bid": [{"price": 1000.0, "vol": 10.0, "cnt": 1.0}]
    })):
        # Выполняем одну итерацию run_test
        data = {"books_full": order_book, "ts": order_book['ts']}
        await queue.put(data)
        
        async def single_iteration_run_test(self):
            data = await self.queue.get()
            order_book = data['books_full']
            order_book['ts'] = data['ts']
            stats = await self.calculate_statistics(order_book)
            if stats:
                bq_data = self.prepare_bigquery_data(order_book, stats)
                if bq_data:
                    self.pending_data = bq_data
                    success = False
                    for attempt in range(3):
                        logger.info(f"Attempt {attempt + 1} to load data to BigQuery")
                        success = await self.bigquery_client.load_data(
                            [bq_data], self.config.table_ids['glass'], notifier=self.notifier
                        )
                        if success:
                            break
                        await asyncio.sleep(1)  # Задержка между попытками
                    if success:
                        self.record_count += 1
                        self.pending_data = None
                    else:
                        await self.save_pending_data()
            self.queue.task_done()
        
        with patch.object(OKXPerformanceTest, 'run_test', new=single_iteration_run_test):
            await processor.run_test()
    
    buffer_files = list(tmp_path.glob(f"{config.buffer_prefixes['glass']}_*.json"))
    assert len(buffer_files) == 0, "Buffer file was created despite successful retry"
    assert mock_bq_client.load_data.call_count == 3, "Expected 3 load_data calls for retries"
    print("Finished test_bigquery_retries_on_failure")

@pytest.mark.asyncio
async def test_load_buffer_from_disk(config, queue, mock_notifier, mock_bq_client, tmp_path):
    print("Starting test_load_buffer_from_disk")
    processor = OKXPerformanceTest(config, mock_bq_client, mock_notifier, queue)
    processor.script_directory = str(tmp_path)
    
    test_data = {
        "ts": 1747080633206,
        "date": "2025-05-12",
        "time": "20:10:33",
        "med_vol_ask": 10.0,
        "med_vol_bid": 10.0,
        "med_cnt_ask": 1.0,
        "med_cnt_bid": 1.0,
        "wavg_vol_ask": 1001.0,
        "wavg_vol_bid": 1000.0,
        "wavg_cnt_ask": 1001.0,
        "wavg_cnt_bid": 1000.0,
        "entropy": 1.0,
        "tot_vol_ask": 10.0,
        "tot_vol_bid": 10.0,
        "rng_vol_ask": {"min": 1001.0, "max": 1001.0},
        "rng_vol_bid": {"min": 1000.0, "max": 1000.0},
        "rng_cnt_ask": {"min": 1, "max": 1},
        "rng_cnt_bid": {"min": 1, "max": 1},
        "cent_ask": [{"price": 1001.0, "vol": 10.0, "cnt": 1.0}],
        "cent_bid": [{"price": 1000.0, "vol": 10.0, "cnt": 1.0}]
    }
    file_path = tmp_path / f"{config.buffer_prefixes['glass']}_20250512_201033.json"
    with open(file_path, 'w') as f:
        json.dump(test_data, f)
    
    await load_buffer_from_disk(
        str(tmp_path),
        mock_bq_client,
        config.table_ids['glass'],
        mock_notifier,
        config.buffer_prefixes['glass']
    )
    
    mock_bq_client.load_data.assert_called_with([test_data], config.table_ids['glass'], notifier=mock_notifier)
    assert not file_path.exists()
    print("Finished test_load_buffer_from_disk")
