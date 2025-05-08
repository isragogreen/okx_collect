"""
Main data collector for fetching and processing exchange data.
"""
import asyncio
import logging
import aiohttp
import json
import os
import tempfile
from datetime import datetime
from typing import List, Dict, Any

from config import (
    API_BASE_URL, API_ENDPOINTS, BUFFER_SIZE, WINDOW_SIZE, BUFFER_CAPACITY,
    INTERVALS, MS_PER_MINUTE, MAX_CYCLE_TIME, PROJECT_ID, DATASET_ID, 
    RAW_TABLE_ID, DERIVED_TABLE_ID, CREDENTIALS_PATH, DEBUG
)
from api_client import ExchangeApiClient
from buffer_manager import BufferManager
from bigquery_client import BigQueryClient
from raw_data_processor import process_raw_data
from derived_data_calculator import calculate_derived_data

logger = logging.getLogger(__name__)

class DataCollector:
    """Collects, processes, and stores market data from exchanges."""
    
    def __init__(self, buffer_size: int = BUFFER_SIZE, debug: bool = DEBUG):
        """Initialize the data collector.
        
        Args:
            buffer_size: Number of records to buffer before writing to BigQuery.
            debug: Enable debug mode for additional logging.
        """
        self.buffer_size = buffer_size
        self.debug = debug
        
        # Initialize components
        self.api_client = ExchangeApiClient(API_BASE_URL)
        self.buffer_manager = BufferManager(
            buffer_capacity=BUFFER_CAPACITY,
            window_size=WINDOW_SIZE
        )
        
        # Buffers for data before writing to BigQuery
        self.raw_buffer: List[Dict[str, Any]] = []
        self.derived_buffer: List[Dict[str, Any]] = []
        
        # Initialize BigQuery client
        try:
            self.bigquery_client = BigQueryClient(
                project_id=PROJECT_ID,
                dataset_id=DATASET_ID,
                raw_table_id=RAW_TABLE_ID,
                derived_table_id=DERIVED_TABLE_ID,
                credentials_path=CREDENTIALS_PATH,
                debug=debug
            )
            logger.info("DataCollector initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize DataCollector: {e}")
            raise
    
    async def save_to_file(self, data: Dict[str, Any], prefix: str = 'debug_data') -> None:
        """Save data to a file for debugging.
        
        Args:
            data: Data to save.
            prefix: Prefix for the filename.
        """
        if not self.debug:
            return
            
        filename = f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.json"
        try:
            with open(filename, 'w') as f:
                json.dump(data, f, indent=2)
            logger.info(f"Data saved to {filename}")
        except Exception as e:
            logger.error(f"Error saving to {filename}: {e}")
    
    async def process_cycle(self, cycle_id: int) -> None:
        """Process a single data collection cycle.
        
        Args:
            cycle_id: Identifier for the current cycle.
        """
        start_time = datetime.now()
        logger.info(f"Cycle {cycle_id} started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        async with aiohttp.ClientSession() as session:
            # Fetch data from the exchange
            fetch_start = datetime.now()
            data_dict = await self.api_client.fetch_all_data(session, API_ENDPOINTS)
            fetch_time = (datetime.now() - fetch_start).total_seconds()
            logger.debug(f"Cycle {cycle_id}: Data fetching took {fetch_time:.2f} seconds")
            
            # Check if we have all required data
            if all(key in data_dict for key in ['order_book', 'candlestick', 'ticker']):
                # Process raw data
                process_start = datetime.now()
                raw_data, ts, date_str, time_str, formatted_time = await process_raw_data(
                    data_dict['order_book'],
                    data_dict['candlestick'],
                    data_dict['ticker'],
                    debug=self.debug
                )
                process_time = (datetime.now() - process_start).total_seconds()
                logger.debug(f"Cycle {cycle_id}: Raw data processing took {process_time:.2f} seconds")
                
                if raw_data:
                    self.raw_buffer.append(raw_data)
                    logger.info(f"Cycle {cycle_id}: Added to raw buffer: {len(self.raw_buffer)}/{self.buffer_size} records (ts={ts})")
                
                # Calculate derived data
                if ts:
                    calc_start = datetime.now()
                    derived_data = await calculate_derived_data(
                        order_book=data_dict['order_book'],
                        ticker=data_dict['ticker'],
                        ts=ts,
                        date_str=date_str,
                        time_str=time_str,
                        formatted_time=formatted_time,
                        buffer_manager=self.buffer_manager,
                        intervals=INTERVALS,
                        ms_per_minute=MS_PER_MINUTE
                    )
                    calc_time = (datetime.now() - calc_start).total_seconds()
                    logger.debug(f"Cycle {cycle_id}: Derived data calculation took {calc_time:.2f} seconds")
                    
                    if derived_data:
                        self.derived_buffer.append(derived_data)
                        logger.info(f"Cycle {cycle_id}: Added to derived buffer: {len(self.derived_buffer)}/{self.buffer_size} records (ts={ts})")
                        
                        # Update historical buffer with new data
                        self.buffer_manager.add_to_buffer(derived_data['current_values'], ts)
                    else:
                        logger.warning(f"Cycle {cycle_id}: Failed to calculate derived data")
                    
                # Write to BigQuery if buffer is full
                if len(self.raw_buffer) >= self.buffer_size or len(self.derived_buffer) >= self.buffer_size:
                    bigquery_start = datetime.now()
                    logger.info(f"Cycle {cycle_id}: Buffer full, writing to BigQuery...")
                    await self.bigquery_client.insert_data(self.raw_buffer, self.derived_buffer)
                    bigquery_time = (datetime.now() - bigquery_start).total_seconds()
                    logger.debug(f"Cycle {cycle_id}: BigQuery write took {bigquery_time:.2f} seconds")
                    
                    # Clear buffers after successful write
                    self.raw_buffer.clear()
                    self.derived_buffer.clear()
            else:
                logger.warning(f"Cycle {cycle_id}: Missing required data from API")
            
        # Log cycle time
        elapsed = (datetime.now() - start_time).total_seconds()
        if elapsed > MAX_CYCLE_TIME:
            logger.warning(f"Cycle {cycle_id}: Processing time ({elapsed:.2f}s) exceeded limit ({MAX_CYCLE_TIME}s)")
        else:
            logger.debug(f"Cycle {cycle_id}: Completed in {elapsed:.2f} seconds")
    
    async def collect_data(self, interval: int = 60) -> None:
        """Main data collection loop.
        
        Args:
            interval: Interval between data collection cycles in seconds.
        """
        cycle_id = 0
        
        while True:
            start_time = datetime.now()
            cycle_id += 1
            
            try:
                await self.process_cycle(cycle_id)
            except Exception as e:
                logger.error(f"Cycle {cycle_id}: Error during processing: {e}")
                if self.debug:
                    # Save current buffers if there's an error
                    await self.save_to_file(self.raw_buffer + self.derived_buffer, prefix='error_buffer')
            
            # Calculate sleep time
            elapsed = (datetime.now() - start_time).total_seconds()
            sleep_time = max(0, interval - elapsed)
            await asyncio.sleep(sleep_time)