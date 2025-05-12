import pytest
import asyncio
import json
import os
import time
import logging
import aiohttp
from datetime import datetime
from unittest.mock import AsyncMock, patch
from okx_data_pipeline.data_collector import OKXDataCollector
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
async def test_collect_data_queue_full(config, queue, mock_notifier, mock_bq_client):
    print("Starting test_collect_data_queue_full")
    collector = OKXDataCollector(config, mock_bq_client, mock_notifier, queue)
    
    mock_requests = [
        {'name': 'order_book', 'endpoint': '/api/v5/market/books', 'params': {'instId': config.symbol, 'sz': '25'}},
        {'name': 'candlestick', 'endpoint': '/api/v5/market/candles', 'params': {'instId': config.symbol, 'bar': '1m', 'limit': '1'}},
        {'name': 'ticker', 'endpoint': '/api/v5/market/ticker', 'params': {'instId': config.symbol}},
        {'name': 'books_full', 'endpoint': '/api/v5/market/books-full', 'params': {'instId': config.symbol, 'sz': '5000'}}
    ]
    
    # Заполняем очередь до максимума
    for _ in range(10):
        await queue.put({"books_full": {}, "ts": 1747080633206})
    
    with patch("aiohttp.ClientSession.get") as mock_get, \
         patch("asyncio.sleep", new=AsyncMock(return_value=None)), \
         patch.object(collector, 'requests', mock_requests):
        mock_response = AsyncMock()
        mock_response.status = 200
        
        async def mock_json():
            request_url = mock_get.call_args[0][0]
            request_name = [req['name'] for req in mock_requests if req['endpoint'] in request_url][0]
            print(f"Mocking response for {request_name} with URL: {request_url}")
            ts = 1747080633206  # 2025-05-12 20:10:33 UTC
            if request_name == 'order_book':
                return {
                    "code": "0",
                    "data": [{
                        "ts": ts,
                        "instId": config.symbol,
                        "bids": [[1000.0, 10.0, 0, 1]],
                        "asks": [[1001.0, 10.0, 0, 1]]
                    }]
                }
            elif request_name == 'candlestick':
                return {
                    "code": "0",
                    "data": [[ts, 1000.0, 1002.0, 999.0, 1001.0, 100.0, 1000.0, 10000.0, "0"]]
                }
            elif request_name == 'ticker':
                return {
                    "code": "0",
                    "data": [{
                        "ts": ts,
                        "instId": config.symbol,
                        "instType": "SPOT",
                        "last": 1001.0,
                        "lastSz": 10.0,
                        "askPx": 1002.0,
                        "askSz": 5.0,
                        "bidPx": 1000.0,
                        "bidSz": 5.0,
                        "open24h": 1000.0,
                        "high24h": 1002.0,
                        "low24h": 999.0,
                        "volCcy24h": 10000.0,
                        "vol24h": 100.0,
                        "sodUtc0": 1000.0,
                        "sodUtc8": 1000.0
                    }]
                }
            elif request_name == 'books_full':
                return {
                    "code": "0",
                    "data": [{"ts": ts, "bids": [], "asks": []}]
                }
        
        mock_response.json = AsyncMock(side_effect=mock_json)
        mock_get.return_value.__aenter__.return_value = mock_response
        
        async def single_iteration_collect_data(self):
            async with aiohttp.ClientSession() as session:
                tasks = [self.fetch_data(session, req) for req in self.requests]
                responses = await asyncio.gather(*tasks, return_exceptions=True)
                data_dict = {}
                for response in responses:
                    if isinstance(response, Exception):
                        logger.error(f"Error in request: {response}")
                        continue
                    if response:
                        data_dict[response['name']] = response['data']
                
                if all(key in data_dict for key in ['order_book', 'candlestick', 'ticker', 'books_full']):
                    realtime_data = await self.process_realtime_data(
                        data_dict['order_book'], data_dict['candlestick'], data_dict['ticker']
                    )
                    if realtime_data:
                        self.pending_data = realtime_data
                        success = await self.bigquery_client.load_data(
                            [realtime_data], self.config.table_ids['realtime_data'], notifier=self.notifier
                        )
                        if success:
                            self.record_count += 1
                            self.pending_data = None
                        else:
                            await self.save_pending_data()
                        self.queue.put_nowait({
                            "books_full": data_dict['books_full'][0],
                            "ts": realtime_data['ts']
                        })
        
        with patch.object(OKXDataCollector, 'collect_data', new=single_iteration_collect_data), \
             patch.object(OKXDataCollector, 'process_realtime_data', new=AsyncMock(return_value={
                 "ts": 1747080633206,
                 "date": "2025-05-12",
                 "time": "20:10:33",
                 "data": {
                     "bids": [{"price": 1000.0, "quantity": 10.0, "num_orders": 1}],
                     "asks": [{"price": 1001.0, "quantity": 10.0, "num_orders": 1}],
                     "open": 1000.0,
                     "high": 1002.0,
                     "low": 999.0,
                     "close": 1001.0,
                     "volume": 100.0,
                     "volCcy": 1000.0,
                     "volCcyQuote": 10000.0,
                     "confirm": "0",
                     "instType": "SPOT",
                     "instId": config.symbol,
                     "last": 1001.0,
                     "lastSz": 10.0,
                     "askPx": 1002.0,
                     "askSz": 5.0,
                     "bidPx": 1000.0,
                     "bidSz": 5.0,
                     "open24h": 1000.0,
                     "high24h": 1002.0,
                     "low24h": 999.0,
                     "volCcy24h": 10000.0,
                     "vol24h": 100.0,
                     "sodUtc0": 1000.0,
                     "sodUtc8": 1000.0
                 }
             })):
            with pytest.raises(asyncio.QueueFull):
                await asyncio.wait_for(collector.collect_data(), timeout=10.0)
    
    print("Finished test_collect_data_queue_full")

@pytest.mark.asyncio
async def test_load_buffer_from_disk(config, queue, mock_notifier, mock_bq_client, tmp_path):
    print("Starting test_load_buffer_from_disk")
    collector = OKXDataCollector(config, mock_bq_client, mock_notifier, queue)
    collector.script_directory = str(tmp_path)
    
    test_data = {"ts": 1747080633206, "date": "2025-05-12", "time": "20:10:33", "data": {}}
    file_path = tmp_path / f"{config.buffer_prefixes['realtime_data']}_20250512_201033.json"
    with open(file_path, 'w') as f:
        json.dump(test_data, f)
    
    await load_buffer_from_disk(
        str(tmp_path),
        mock_bq_client,
        config.table_ids['realtime_data'],
        mock_notifier,
        config.buffer_prefixes['realtime_data']
    )
    
    mock_bq_client.load_data.assert_called_with([test_data], config.table_ids['realtime_data'], notifier=mock_notifier)
    assert not file_path.exists()
    print("Finished test_load_buffer_from_disk")

@pytest.mark.asyncio
async def test_save_to_disk_on_bigquery_failure(config, queue, mock_notifier, mock_bq_client, tmp_path):
    print("Starting test_save_to_disk_on_bigquery_failure")
    collector = OKXDataCollector(config, mock_bq_client, mock_notifier, queue)
    collector.script_directory = str(tmp_path)
    
    mock_requests = [
        {'name': 'order_book', 'endpoint': '/api/v5/market/books', 'params': {'instId': config.symbol, 'sz': '25'}},
        {'name': 'candlestick', 'endpoint': '/api/v5/market/candles', 'params': {'instId': config.symbol, 'bar': '1m', 'limit': '1'}},
        {'name': 'ticker', 'endpoint': '/api/v5/market/ticker', 'params': {'instId': config.symbol}},
        {'name': 'books_full', 'endpoint': '/api/v5/market/books-full', 'params': {'instId': config.symbol, 'sz': '5000'}}
    ]
    
    mock_bq_client.load_data = AsyncMock(return_value=False)
    
    with patch("aiohttp.ClientSession.get") as mock_get, \
         patch("asyncio.sleep", new=AsyncMock(return_value=None)), \
         patch.object(collector, 'requests', mock_requests):
        mock_response = AsyncMock()
        mock_response.status = 200
        
        async def mock_json():
            request_url = mock_get.call_args[0][0]
            request_name = [req['name'] for req in mock_requests if req['endpoint'] in request_url][0]
            print(f"Mocking response for {request_name} with URL: {request_url}")
            ts = 1747080633206
            if request_name == 'order_book':
                return {
                    "code": "0",
                    "data": [{
                        "ts": ts,
                        "instId": config.symbol,
                        "bids": [[1000.0, 10.0, 0, 1]],
                        "asks": [[1001.0, 10.0, 0, 1]]
                    }]
                }
            elif request_name == 'candlestick':
                return {
                    "code": "0",
                    "data": [[ts, 1000.0, 1002.0, 999.0, 1001.0, 100.0, 1000.0, 10000.0, "0"]]
                }
            elif request_name == 'ticker':
                return {
                    "code": "0",
                    "data": [{
                        "ts": ts,
                        "instId": config.symbol,
                        "instType": "SPOT",
                        "last": 1001.0,
                        "lastSz": 10.0,
                        "askPx": 1002.0,
                        "askSz": 5.0,
                        "bidPx": 1000.0,
                        "bidSz": 5.0,
                        "open24h": 1000.0,
                        "high24h": 1002.0,
                        "low24h": 999.0,
                        "volCcy24h": 10000.0,
                        "vol24h": 100.0,
                        "sodUtc0": 1000.0,
                        "sodUtc8": 1000.0
                    }]
                }
            elif request_name == 'books_full':
                return {
                    "code": "0",
                    "data": [{"ts": ts, "bids": [], "asks": []}]
                }
        
        mock_response.json = AsyncMock(side_effect=mock_json)
        mock_get.return_value.__aenter__.return_value = mock_response
        
        with patch.object(OKXDataCollector, 'process_realtime_data', new=AsyncMock(return_value={
            "ts": 1747080633206,
            "date": "2025-05-12",
            "time": "20:10:33",
            "data": {
                "bids": [{"price": 1000.0, "quantity": 10.0, "num_orders": 1}],
                "asks": [{"price": 1001.0, "quantity": 10.0, "num_orders": 1}],
                "open": 1000.0,
                "high": 1002.0,
                "low": 999.0,
                "close": 1001.0,
                "volume": 100.0,
                "volCcy": 1000.0,
                "volCcyQuote": 10000.0,
                "confirm": "0",
                "instType": "SPOT",
                "instId": config.symbol,
                "last": 1001.0,
                "lastSz": 10.0,
                "askPx": 1002.0,
                "askSz": 5.0,
                "bidPx": 1000.0,
                "bidSz": 5.0,
                "open24h": 1000.0,
                "high24h": 1002.0,
                "low24h": 999.0,
                "volCcy24h": 10000.0,
                "vol24h": 100.0,
                "sodUtc0": 1000.0,
                "sodUtc8": 1000.0
            }
        })):
            async def single_iteration_collect_data(self):
                async with aiohttp.ClientSession() as session:
                    start_time = time.time()
                    logger.info(f"Starting data collection cycle: {datetime.utcnow()}")
                    tasks = [self.fetch_data(session, req) for req in self.requests]
                    responses = await asyncio.gather(*tasks, return_exceptions=True)
                    data_dict = {}
                    for response in responses:
                        if isinstance(response, Exception):
                            logger.error(f"Error in request: {response}")
                            continue
                        if response:
                            data_dict[response['name']] = response['data']
        
                    if all(key in data_dict for key in ['order_book', 'candlestick', 'ticker', 'books_full']):
                        realtime_data = await self.process_realtime_data(
                            data_dict['order_book'], data_dict['candlestick'], data_dict['ticker']
                        )
                        if realtime_data:
                            self.pending_data = realtime_data
                            success = await self.bigquery_client.load_data(
                                [realtime_data], self.config.table_ids['realtime_data'], notifier=self.notifier
                            )
                            if success:
                                self.record_count += 1
                                self.pending_data = None
                            else:
                                await self.save_pending_data()
                            await self.queue.put({
                                "books_full": data_dict['books_full'][0],
                                "ts": realtime_data['ts']
                            })
                            logger.info("Added books_full to queue")
        
                    elapsed = time.time() - start_time
                    sleep_time = max(0, 1 - elapsed)
                    logger.info(f"Cycle completed in {elapsed:.4f} seconds, sleeping for {sleep_time:.4f} seconds")
                    await asyncio.sleep(sleep_time)
            
            with patch.object(OKXDataCollector, 'collect_data', new=single_iteration_collect_data):
                await asyncio.wait_for(collector.collect_data(), timeout=10.0)
    
    buffer_files = list(tmp_path.glob(f"{config.buffer_prefixes['realtime_data']}_*.json"))
    assert len(buffer_files) == 1, "Buffer file was not created"
    buffer_file = buffer_files[0]
    
    with open(buffer_file, 'r') as f:
        saved_data = json.load(f)
    assert saved_data['ts'] == 1747080633206
    assert saved_data['date'] == "2025-05-12"
    assert 'data' in saved_data
    
    print("Finished test_save_to_disk_on_bigquery_failure")

@pytest.mark.asyncio
async def test_restore_from_disk_to_bigquery(config, queue, mock_notifier, mock_bq_client, tmp_path):
    print("Starting test_restore_from_disk_to_bigquery")
    collector = OKXDataCollector(config, mock_bq_client, mock_notifier, queue)
    collector.script_directory = str(tmp_path)
    
    test_data = {
        "ts": 1747080633206,
        "date": "2025-05-12",
        "time": "20:10:33",
        "data": {
            "bids": [{"price": 1000.0, "quantity": 10.0, "num_orders": 1}],
            "asks": [{"price": 1001.0, "quantity": 10.0, "num_orders": 1}],
            "open": 1000.0,
            "high": 1002.0,
            "low": 999.0,
            "close": 1001.0,
            "volume": 100.0,
            "volCcy": 1000.0,
            "volCcyQuote": 10000.0,
            "confirm": "0",
            "instType": "SPOT",
            "instId": config.symbol,
            "last": 1001.0,
            "lastSz": 10.0,
            "askPx": 1002.0,
            "askSz": 5.0,
            "bidPx": 1000.0,
            "bidSz": 5.0,
            "open24h": 1000.0,
            "high24h": 1002.0,
            "low24h": 999.0,
            "volCcy24h": 10000.0,
            "vol24h": 100.0,
            "sodUtc0": 1000.0,
            "sodUtc8": 1000.0
        }
    }
    file_path = tmp_path / f"{config.buffer_prefixes['realtime_data']}_20250512_201033.json"
    with open(file_path, 'w') as f:
        json.dump(test_data, f)
    
    await load_buffer_from_disk(
        str(tmp_path),
        mock_bq_client,
        config.table_ids['realtime_data'],
        mock_notifier,
        config.buffer_prefixes['realtime_data']
    )
    
    mock_bq_client.load_data.assert_called_with([test_data], config.table_ids['realtime_data'], notifier=mock_notifier)
    assert not file_path.exists()
    print("Finished test_restore_from_disk_to_bigquery")

@pytest.mark.asyncio
async def test_bigquery_retries_on_failure(config, queue, mock_notifier, mock_bq_client, tmp_path):
    print("Starting test_bigquery_retries_on_failure")
    collector = OKXDataCollector(config, mock_bq_client, mock_notifier, queue)
    collector.script_directory = str(tmp_path)
    
    mock_requests = [
        {'name': 'order_book', 'endpoint': '/api/v5/market/books', 'params': {'instId': config.symbol, 'sz': '25'}},
        {'name': 'candlestick', 'endpoint': '/api/v5/market/candles', 'params': {'instId': config.symbol, 'bar': '1m', 'limit': '1'}},
        {'name': 'ticker', 'endpoint': '/api/v5/market/ticker', 'params': {'instId': config.symbol}},
        {'name': 'books_full', 'endpoint': '/api/v5/market/books-full', 'params': {'instId': config.symbol, 'sz': '5000'}}
    ]
    
    # Симулируем сбой BigQuery на первых двух попытках, успех на третьей
    mock_bq_client.load_data = AsyncMock(side_effect=[False, False, True])
    
    with patch("aiohttp.ClientSession.get") as mock_get, \
         patch("asyncio.sleep", new=AsyncMock(return_value=None)), \
         patch.object(collector, 'requests', mock_requests):
        mock_response = AsyncMock()
        mock_response.status = 200
        
        async def mock_json():
            request_url = mock_get.call_args[0][0]
            request_name = [req['name'] for req in mock_requests if req['endpoint'] in request_url][0]
            print(f"Mocking response for {request_name} with URL: {request_url}")
            ts = 1747080633206
            if request_name == 'order_book':
                return {
                    "code": "0",
                    "data": [{
                        "ts": ts,
                        "instId": config.symbol,
                        "bids": [[1000.0, 10.0, 0, 1]],
                        "asks": [[1001.0, 10.0, 0, 1]]
                    }]
                }
            elif request_name == 'candlestick':
                return {
                    "code": "0",
                    "data": [[ts, 1000.0, 1002.0, 999.0, 1001.0, 100.0, 1000.0, 10000.0, "0"]]
                }
            elif request_name == 'ticker':
                return {
                    "code": "0",
                    "data": [{
                        "ts": ts,
                        "instId": config.symbol,
                        "instType": "SPOT",
                        "last": 1001.0,
                        "lastSz": 10.0,
                        "askPx": 1002.0,
                        "askSz": 5.0,
                        "bidPx": 1000.0,
                        "bidSz": 5.0,
                        "open24h": 1000.0,
                        "high24h": 1002.0,
                        "low24h": 999.0,
                        "volCcy24h": 10000.0,
                        "vol24h": 100.0,
                        "sodUtc0": 1000.0,
                        "sodUtc8": 1000.0
                    }]
                }
            elif request_name == 'books_full':
                return {
                    "code": "0",
                    "data": [{"ts": ts, "bids": [], "asks": []}]
                }
        
        mock_response.json = AsyncMock(side_effect=mock_json)
        mock_get.return_value.__aenter__.return_value = mock_response
        
        with patch.object(OKXDataCollector, 'process_realtime_data', new=AsyncMock(return_value={
            "ts": 1747080633206,
            "date": "2025-05-12",
            "time": "20:10:33",
            "data": {
                "bids": [{"price": 1000.0, "quantity": 10.0, "num_orders": 1}],
                "asks": [{"price": 1001.0, "quantity": 10.0, "num_orders": 1}],
                "open": 1000.0,
                "high": 1002.0,
                "low": 999.0,
                "close": 1001.0,
                "volume": 100.0,
                "volCcy": 1000.0,
                "volCcyQuote": 10000.0,
                "confirm": "0",
                "instType": "SPOT",
                "instId": config.symbol,
                "last": 1001.0,
                "lastSz": 10.0,
                "askPx": 1002.0,
                "askSz": 5.0,
                "bidPx": 1000.0,
                "bidSz": 5.0,
                "open24h": 1000.0,
                "high24h": 1002.0,
                "low24h": 999.0,
                "volCcy24h": 10000.0,
                "vol24h": 100.0,
                "sodUtc0": 1000.0,
                "sodUtc8": 1000.0
            }
        })):
            async def single_iteration_collect_data(self):
                async with aiohttp.ClientSession() as session:
                    start_time = time.time()
                    logger.info(f"Starting data collection cycle: {datetime.utcnow()}")
                    tasks = [self.fetch_data(session, req) for req in self.requests]
                    responses = await asyncio.gather(*tasks, return_exceptions=True)
                    data_dict = {}
                    for response in responses:
                        if isinstance(response, Exception):
                            logger.error(f"Error in request: {response}")
                            continue
                        if response:
                            data_dict[response['name']] = response['data']
        
                    if all(key in data_dict for key in ['order_book', 'candlestick', 'ticker', 'books_full']):
                        realtime_data = await self.process_realtime_data(
                            data_dict['order_book'], data_dict['candlestick'], data_dict['ticker']
                        )
                        if realtime_data:
                            self.pending_data = realtime_data
                            success = False
                            for attempt in range(3):
                                logger.info(f"Attempt {attempt + 1} to load data to BigQuery")
                                success = await self.bigquery_client.load_data(
                                    [realtime_data], self.config.table_ids['realtime_data'], notifier=self.notifier
                                )
                                if success:
                                    break
                                await asyncio.sleep(1)  # Задержка между попытками
                            if success:
                                self.record_count += 1
                                self.pending_data = None
                            else:
                                await self.save_pending_data()
                            await self.queue.put({
                                "books_full": data_dict['books_full'][0],
                                "ts": realtime_data['ts']
                            })
                            logger.info("Added books_full to queue")
        
                    elapsed = time.time() - start_time
                    sleep_time = max(0, 1 - elapsed)
                    logger.info(f"Cycle completed in {elapsed:.4f} seconds, sleeping for {sleep_time:.4f} seconds")
                    await asyncio.sleep(sleep_time)
            
            with patch.object(OKXDataCollector, 'collect_data', new=single_iteration_collect_data):
                await asyncio.wait_for(collector.collect_data(), timeout=10.0)
    
    buffer_files = list(tmp_path.glob(f"{config.buffer_prefixes['realtime_data']}_*.json"))
    assert len(buffer_files) == 0, "Buffer file was created despite successful retry"
    assert mock_bq_client.load_data.call_count == 3, "Expected 3 load_data calls for retries"
    
    print("Finished test_bigquery_retries_on_failure")
