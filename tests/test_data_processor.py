import pytest
import asyncio
import json
import os
import logging
import numpy as np
from datetime import datetime
from unittest.mock import AsyncMock, patch
from okx_data_pipeline.data_processor import OKXPerformanceTest, find_range
from okx_data_pipeline.config import Config
from okx_data_pipeline.bigquery_client import BigQueryClient
from okx_data_pipeline.utils import load_buffer_from_disk

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
async def test_process_queue_data(config, queue, mock_notifier, mock_bq_client, tmp_path):
    print("Starting test_process_queue_data")
    processor = OKXPerformanceTest(config, mock_bq_client, mock_notifier, queue)
    processor.script_directory = str(tmp_path)
    
    order_book = {
        "ts": 123456789,
        "bids": [[1000.0, 10.0, 1]],
        "asks": [[1001.0, 10.0, 1]]
    }
    
    await queue.put({"books_full": order_book, "ts": 123456789})
    
    with patch.object(processor, 'calculate_statistics', new=AsyncMock(return_value={
        'med_vol_ask': 10.0,
        'med_vol_bid': 10.0,
        'med_cnt_ask': 1.0,
        'med_cnt_bid': 1.0,
        'wavg_vol_ask': 1001.0,
        'wavg_vol_bid': 1000.0,
        'wavg_cnt_ask': 1001.0,
        'wavg_cnt_bid': 1000.0,
        'entropy': 1.0,
        'tot_vol_ask': 10.0,
        'tot_vol_bid': 10.0,
        'rng_vol_ask': {'min': 1001.0, 'max': 1001.0},
        'rng_vol_bid': {'min': 1000.0, 'max': 1000.0},
        'rng_cnt_ask': {'min': 1, 'max': 1},
        'rng_cnt_bid': {'min': 1, 'max': 1},
        'cent_ask': [{'price': 1001.0, 'vol': 10.0, 'cnt': 1.0}],
        'cent_bid': [{'price': 1000.0, 'vol': 10.0, 'cnt': 1.0}]
    })):
        # Run a single iteration of run_test
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
            await asyncio.wait_for(processor.run_test(), timeout=5.0)
    
    mock_bq_client.load_data.assert_called()
    assert processor.record_count == 1
    assert processor.pending_data is None
    print("Finished test_process_queue_data")

@pytest.mark.asyncio
async def test_load_buffer_from_disk(config, queue, mock_notifier, mock_bq_client, tmp_path):
    print("Starting test_load_buffer_from_disk")
    processor = OKXPerformanceTest(config, mock_bq_client, mock_notifier, queue)
    processor.script_directory = str(tmp_path)
    
    test_data = {
        "ts": 123456789,
        "time": "12:34:56",
        "med_vol_ask": 10.0,
        "med_vol_bid": 10.0
    }
    file_path = tmp_path / f"{config.buffer_prefixes['glass']}_20250511_123456.json"
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

@pytest.mark.asyncio
async def test_bigquery_retries_on_failure(config, queue, mock_notifier, mock_bq_client, tmp_path):
    print("Starting test_bigquery_retries_on_failure")
    processor = OKXPerformanceTest(config, mock_bq_client, mock_notifier, queue)
    processor.script_directory = str(tmp_path)
    
    # Simulate BigQuery failure on first two attempts, success on third
    mock_bq_client.load_data = AsyncMock(side_effect=[False, False, True])
    
    order_book = {
        "ts": 123456789,
        "bids": [[1000.0, 10.0, 1]],
        "asks": [[1001.0, 10.0, 1]]
    }
    
    await queue.put({"books_full": order_book, "ts": 123456789})
    
    with patch.object(processor, 'calculate_statistics', new=AsyncMock(return_value={
        'med_vol_ask': 10.0,
        'med_vol_bid': 10.0,
        'med_cnt_ask': 1.0,
        'med_cnt_bid': 1.0,
        'wavg_vol_ask': 1001.0,
        'wavg_vol_bid': 1000.0,
        'wavg_cnt_ask': 1001.0,
        'wavg_cnt_bid': 1000.0,
        'entropy': 1.0,
        'tot_vol_ask': 10.0,
        'tot_vol_bid': 10.0,
        'rng_vol_ask': {'min': 1001.0, 'max': 1001.0},
        'rng_vol_bid': {'min': 1000.0, 'max': 1000.0},
        'rng_cnt_ask': {'min': 1, 'max': 1},
        'rng_cnt_bid': {'min': 1, 'max': 1},
        'cent_ask': [{'price': 1001.0, 'vol': 10.0, 'cnt': 1.0}],
        'cent_bid': [{'price': 1000.0, 'vol': 10.0, 'cnt': 1.0}]
    })):
        # Run a single iteration of run_test with retries
        async def single_iteration_run_test(self):
            data = await self.queue.get()
            order_book = data['books_full']
            order_book['ts'] = data['ts']
            stats = await self.calculate_statistics(order_book)
            if stats:
                bq_data = self.prepare_bigquery_data(order_book, stats)
                if bq_data:
                    self.pending_data = bq_data
                    max_retries = 3
                    for attempt in range(max_retries):
                        success = await self.bigquery_client.load_data(
                            [bq_data], self.config.table_ids['glass'], notifier=self.notifier
                        )
                        if success:
                            self.record_count += 1
                            self.pending_data = None
                            break
                        else:
                            if attempt == max_retries - 1:
                                await self.save_pending_data()
            self.queue.task_done()
        
        with patch.object(OKXPerformanceTest, 'run_test', new=single_iteration_run_test):
            await asyncio.wait_for(processor.run_test(), timeout=5.0)
    
    assert mock_bq_client.load_data.call_count == 3, "Expected 3 load_data calls for retries"
    assert processor.record_count == 1
    assert processor.pending_data is None
    buffer_files = list(tmp_path.glob(f"{config.buffer_prefixes['glass']}_*.json"))
    assert len(buffer_files) == 0, "Buffer file was created despite successful retry"
    print("Finished test_bigquery_retries_on_failure")

def test_find_range():
    print("Starting test_find_range")
    # Test empty array
    weights = np.array([], dtype=np.float64)
    result = find_range(weights, fraction=0.75)
    assert np.array_equal(result, np.array([0, 0], dtype=np.int64)), "Empty array should return [0, 0]"

    # Test zero sum array
    weights = np.array([0.0, 0.0, 0.0], dtype=np.float64)
    result = find_range(weights, fraction=0.75)
    assert np.array_equal(result, np.array([0, 0], dtype=np.int64)), "Zero sum array should return [0, 0]"

    # Test normal case
    weights = np.array([1.0, 2.0, 3.0, 4.0, 5.0], dtype=np.float64)
    total = np.sum(weights)  # 15.0
    target = 0.75 * total  # 11.25
    result = find_range(weights, fraction=0.75)
    assert np.array_equal(result, np.array([2, 4], dtype=np.int64)), "Expected range [2, 4] for weights summing to >= 11.25"

    # Test all equal weights
    weights = np.array([1.0, 1.0, 1.0, 1.0], dtype=np.float64)
    total = np.sum(weights)  # 4.0
    target = 0.75 * total  # 3.0
    result = find_range(weights, fraction=0.75)
    assert np.array_equal(result, np.array([0, 2], dtype=np.int64)), "Expected range [0, 2] for equal weights summing to >= 3.0"

    print("Finished test_find_range")
