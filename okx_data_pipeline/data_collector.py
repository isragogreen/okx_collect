import aiohttp
import asyncio
import json
import os
from datetime import datetime
import pandas as pd
import logging
import time
from okx_data_pipeline.utils import load_buffer_from_disk, save_pending_data

logger = logging.getLogger(__name__)

class OKXDataCollector:
    def __init__(self, config, bigquery_client, notifier, queue):
        self.config = config
        self.bigquery_client = bigquery_client
        self.notifier = notifier
        self.queue = queue
        self.script_directory = os.path.dirname(os.path.abspath(__file__))
        self.record_count = 0
        self.pending_data = None
        self.buffer_prefix = self.config.buffer_prefixes['realtime_data']
        self.requests = [
            {'endpoint': '/api/v5/market/books', 'params': {'instId': self.config.symbol, 'sz': 25}, 'name': 'order_book'},
            {'endpoint': '/api/v5/market/candles', 'params': {'instId': self.config.symbol, 'bar': '1m', 'limit': 1}, 'name': 'candlestick'},
            {'endpoint': '/api/v5/market/ticker', 'params': {'instId': self.config.symbol}, 'name': 'ticker'},
            {'endpoint': '/api/v5/market/books-full', 'params': {'instId': self.config.symbol, 'sz': 5000}, 'name': 'books_full'}
        ]
        asyncio.create_task(load_buffer_from_disk(
            self.script_directory,
            self.bigquery_client,
            self.config.table_ids['realtime_data'],
            self.notifier,
            self.buffer_prefix
        ))

    async def save_pending_data(self):
        return await save_pending_data(self.script_directory, self.pending_data, self.buffer_prefix)

    async def fetch_data(self, session, request):
        url = f"{self.config.api_base_url}{request['endpoint']}"
        try:
            async with session.get(url, params=request['params']) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('code') == '0':
                        return {'name': request['name'], 'data': data['data']}
                    else:
                        logger.error(f"API error for {request['name']}: {data.get('msg', 'Unknown error')}")
                        return None
                else:
                    logger.error(f"HTTP error for {request['name']}: {response.status}")
                    return None
        except Exception as e:
            logger.error(f"Exception in fetching {request['name']}: {e}")
            return None

    async def process_realtime_data(self, order_book, candlestick, ticker):
        try:
            if not (order_book and candlestick and ticker):
                logger.error("Incomplete data for processing")
                return None
            ts = int(ticker[0]['ts'])
            logger.debug(f"Raw ts: {ts}")
            # Проверка: ts должен быть в пределах 2025 года
            if ts < 1735689600000 or ts > 1767225599999:  # 01.01.2025 - 31.12.2025
                logger.error(f"Invalid ts: {ts}")
                return None
            now = datetime.utcfromtimestamp(ts / 1000.0)
            logger.debug(f"Converted time: {now}")
            date_str = now.strftime('%Y-%m-%d')
            time_str = now.strftime('%H:%M:%S')
            logger.debug(f"Formatted date: {date_str}, time: {time_str}")
            logger.debug(f"Candlestick data: {candlestick}")
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
            return None

    async def collect_data(self):
        async with aiohttp.ClientSession() as session:
            while True:
                start_time = time.time()
                system_time = datetime.utcnow()
                logger.debug(f"System time at cycle start: {system_time}")
                logger.info(f"Starting data collection cycle: {datetime.utcnow()}")
                try:
                    tasks = [self.fetch_data(session, req) for req in self.requests]
                    responses = await asyncio.gather(*tasks, return_exceptions=True)
                    data_dict = {}
                    for response in responses:
                        if isinstance(response, Exception):
                            logger.error(f"Error in request: {response}")
                            continue
                        if response:
                            data_dict[response['name']] = response['data']
                    
                    if not all(key in data_dict for key in ['order_book', 'candlestick', 'ticker', 'books_full']):
                        logger.error("Missing data for some endpoints")
                        raise RuntimeError("Incomplete data collection")
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
                        try:
                            await self.queue.put({
                                "books_full": data_dict['books_full'][0],
                                "ts": realtime_data['ts']
                            })
                            logger.info("Added books_full to queue")
                        except asyncio.QueueFull:
                            error_msg = "Queue full, treating as crash"
                            logger.error(error_msg)
                            raise RuntimeError(error_msg)
                except Exception as e:
                    logger.error(f"Data collection cycle failed: {e}")
                elapsed = time.time() - start_time
                sleep_time = max(0, self.config.collect_interval - elapsed)
                logger.info(f"Cycle completed in {elapsed:.4f} seconds, sleeping for {sleep_time:.4f} seconds")
                await asyncio.sleep(sleep_time)
