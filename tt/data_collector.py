import aiohttp
import asyncio
import json
import os
from datetime import datetime
import pandas as pd
import glob
import time
import random
import logging
import sys

logger = logging.getLogger(__name__)

class OKXDataCollector:
    def __init__(self, api_base_url, symbol, buffer_size, bigquery_client, notifier, debug=False):
        self.api_base_url = api_base_url
        self.symbol = symbol
        self.buffer_size = buffer_size
        self.bigquery_client = bigquery_client
        self.notifier = notifier
        self.debug = debug
        self.buffer = []
        self.script_directory = os.path.dirname(os.path.abspath(__file__))
        self.requests = [
            {'endpoint': '/api/v5/market/books', 'params': {'instId': self.symbol, 'sz': 25}, 'name': 'order_book'},
            {'endpoint': '/api/v5/market/candles', 'params': {'instId': self.symbol, 'bar': '1m', 'limit': 1}, 'name': 'candlestick'},
            {'endpoint': '/api/v5/market/ticker', 'params': {'instId': self.symbol}, 'name': 'ticker'}
        ]
        self.load_buffer_from_disk()

    def load_buffer_from_disk(self):
        try:
            buffer_files = glob.glob(os.path.join(self.script_directory, "okx_buffer_*.json"))
            for file_path in buffer_files:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        self.buffer.extend(data)
                    else:
                        self.buffer.append(data)
                os.remove(file_path)
                logger.info(f"Loaded and removed buffer file: {file_path}")
            logger.info(f"Loaded {len(self.buffer)} records from disk")
            if self.buffer:
                asyncio.create_task(self.attempt_save_buffer())
        except Exception as e:
            logger.error(f"Error loading buffer from disk: {e}")
            asyncio.create_task(self.notifier.send_exception(sys.exc_info()))

    async def attempt_save_buffer(self):
        if not self.buffer:
            logger.info("Buffer is empty, no save required")
            return
        logger.info(f"Attempting to save {len(self.buffer)} records to BigQuery")
        success = await self.bigquery_client.load_data(
            [data], self.config.realtime_table_id, notifier=self.notifier
        )
        
        if success:
            self.buffer.clear()
        else:
            self.save_to_file(self.buffer)
            self.buffer.clear()
            logger.info("Buffer saved to file due to BigQuery failure")

    def save_to_file(self, data):
        filename = f"okx_buffer_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        file_path = os.path.join(self.script_directory, filename)
        try:
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
            logger.info(f"Data saved to {file_path}")
        except Exception as e:
            logger.error(f"Error saving to {file_path}: {e}")
            asyncio.create_task(self.notifier.send_exception(sys.exc_info()))

    async def fetch_data(self, session, request):
        url = f"{self.api_base_url}{request['endpoint']}"
        try:
            async with session.get(url, params=request['params']) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('code') == '0':
                        return {'name': request['name'], 'data': data['data'], 'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                    else:
                        logger.error(f"API error for {request['name']}: {data.get('msg', 'Unknown error')}")
                        return None
                else:
                    logger.error(f"HTTP error for {request['name']}: {response.status}")
                    return None
        except Exception as e:
            logger.error(f"Exception in fetching {request['name']}: {e}")
            await self.notifier.send_exception(sys.exc_info())
            return None

    def process_data(self, order_book, candlestick, ticker):
        try:
            if not (order_book and candlestick and ticker):
                logger.error("Incomplete data for processing")
                return None
            ts = int(ticker[0]['ts'])
            now = datetime.now()
            date_str = now.strftime('%Y-%m-%d')
            time_str = now.strftime('%H:%M:%S')
            bids_df = pd.DataFrame(order_book[0]['bids'][:25], columns=['price', 'quantity', '_', 'num_orders'])
            bids = bids_df[['price', 'quantity', 'num_orders']].astype({
                'price': 'float', 'quantity': 'float', 'num_orders': 'int'
            }).to_dict('records')
            asks_df = pd.DataFrame(order_book[0]['asks'][:25], columns=['price', 'quantity', '_', 'num_orders'])
            asks = asks_df[['price', 'quantity', 'num_orders']].astype({
                'price': 'float', 'quantity': 'float', 'num_orders': 'int'
            }).to_dict('records')
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
            logger.error(f"Error processing data: {e}")
            asyncio.create_task(self.notifier.send_exception(sys.exc_info()))
            return None

    async def collect_data(self, interval=60):
        async with aiohttp.ClientSession() as session:
            while True:
                start_time = datetime.now()
                logger.info(f"Starting data collection cycle: {start_time}")
                tasks = [self.fetch_data(session, req) for req in self.requests]
                responses = await asyncio.gather(*tasks, return_exceptions=True)
                data_dict = {}
                for response in responses:
                    if isinstance(response, Exception):
                        logger.error(f"Error in request: {response}")
                        continue
                    if response:
                        data_dict[response['name']] = response['data']
                if all(key in data_dict for key in ['order_book', 'candlestick', 'ticker']):
                    processed_data = self.process_data(
                        data_dict['order_book'], data_dict['candlestick'], data_dict['ticker']
                    )
                    if processed_data:
                        self.buffer.append(processed_data)
                        logger.info(f"Buffer size: {len(self.buffer)}/{self.buffer_size}")
                        if len(self.buffer) >= self.buffer_size:
                            await self.attempt_save_buffer()
                elapsed = (datetime.now() - start_time).total_seconds()
                await asyncio.sleep(max(0, interval - elapsed))
