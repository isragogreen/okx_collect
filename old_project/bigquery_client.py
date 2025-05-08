"""
Client for interacting with Google BigQuery.
"""
import json
import tempfile
import logging
import asyncio
from typing import List, Dict, Any
from google.oauth2 import service_account
from google.cloud import bigquery
from pathlib import Path

logger = logging.getLogger(__name__)

class BigQueryClient:
    """Client for interacting with Google BigQuery."""
    
    def __init__(self, 
                project_id: str, 
                dataset_id: str, 
                raw_table_id: str, 
                derived_table_id: str, 
                credentials_path: Path,
                debug: bool = False):
        """Initialize the BigQuery client.
        
        Args:
            project_id: Google Cloud project ID.
            dataset_id: BigQuery dataset ID.
            raw_table_id: Table ID for raw data.
            derived_table_id: Table ID for derived data.
            credentials_path: Path to service account credentials file.
            debug: Enable debug mode for additional logging.
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.raw_table_id = raw_table_id
        self.derived_table_id = derived_table_id
        self.credentials_path = credentials_path
        self.debug = debug
        
        try:
            self.credentials = service_account.Credentials.from_service_account_file(str(credentials_path))
            self.client = bigquery.Client(credentials=self.credentials, project=project_id)
            
            # Set up table references
            self.raw_table_ref = self.client.dataset(dataset_id).table(raw_table_id)
            self.derived_table_ref = self.client.dataset(dataset_id).table(derived_table_id)
            
            # Define schema for raw table
            raw_schema = [
                bigquery.SchemaField("ts", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("date", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("time", "STRING", mode="NULLABLE"),
                bigquery.SchemaField(
                    "data",
                    "RECORD",
                    mode="REQUIRED",
                    fields=[
                        bigquery.SchemaField(
                            "bids",
                            "RECORD",
                            mode="REPEATED",
                            fields=[
                                bigquery.SchemaField("price", "FLOAT", mode="NULLABLE"),
                                bigquery.SchemaField("quantity", "FLOAT", mode="NULLABLE"),
                                bigquery.SchemaField("num_orders", "INTEGER", mode="NULLABLE"),
                            ],
                        ),
                        bigquery.SchemaField(
                            "asks",
                            "RECORD",
                            mode="REPEATED",
                            fields=[
                                bigquery.SchemaField("price", "FLOAT", mode="NULLABLE"),
                                bigquery.SchemaField("quantity", "FLOAT", mode="NULLABLE"),
                                bigquery.SchemaField("num_orders", "INTEGER", mode="NULLABLE"),
                            ],
                        ),
                        bigquery.SchemaField("open", "FLOAT", mode="NULLABLE"),
                        bigquery.SchemaField("high", "FLOAT", mode="NULLABLE"),
                        bigquery.SchemaField("low", "FLOAT", mode="NULLABLE"),
                        bigquery.SchemaField("close", "FLOAT", mode="NULLABLE"),
                        bigquery.SchemaField("volume", "FLOAT", mode="NULLABLE"),
                        bigquery.SchemaField("volCcy", "FLOAT", mode="NULLABLE"),
                        bigquery.SchemaField("volCcyQuote", "FLOAT", mode="NULLABLE"),
                        bigquery.SchemaField("confirm", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("instType", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("instId", "STRING", mode="NULLABLE"),
                        bigquery.SchemaField("last", "FLOAT", mode="NULLABLE"),
                        bigquery.SchemaField("lastSz", "FLOAT", mode="NULLABLE"),
                        bigquery.SchemaField("askPx", "FLOAT", mode="NULLABLE"),
                        bigquery.SchemaField("askSz", "FLOAT", mode="NULLABLE"),
                        bigquery.SchemaField("bidPx", "FLOAT", mode="NULLABLE"),
                        bigquery.SchemaField("bidSz", "FLOAT", mode="NULLABLE"),
                        bigquery.SchemaField("open24h", "FLOAT", mode="NULLABLE"),
                        bigquery.SchemaField("high24h", "FLOAT", mode="NULLABLE"),
                        bigquery.SchemaField("low24h", "FLOAT", mode="NULLABLE"),
                        bigquery.SchemaField("volCcy24h", "FLOAT", mode="NULLABLE"),
                        bigquery.SchemaField("vol24h", "FLOAT", mode="NULLABLE"),
                        bigquery.SchemaField("sodUtc0", "FLOAT", mode="NULLABLE"),
                        bigquery.SchemaField("sodUtc8", "FLOAT", mode="NULLABLE"),
                    ],
                ),
            ]
            
            # Define schema for derived table
            derived_schema = [
                bigquery.SchemaField("ts", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("formatted_time", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("vwap_asks", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("vwap_bids", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("vwap_orders_asks", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("vwap_orders_bids", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("median_volume_asks", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("median_volume_bids", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("ask_bid_volume_ratio", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("delta_ask_bid_ratio", "FLOAT", mode="REPEATED"),
                bigquery.SchemaField("ask_volume_gradient", "FLOAT", mode="REPEATED"),
                bigquery.SchemaField("bid_volume_gradient", "FLOAT", mode="REPEATED"),
                bigquery.SchemaField("vwap_impulse_asks", "FLOAT", mode="REPEATED"),
                bigquery.SchemaField("vwap_impulse_bids", "FLOAT", mode="REPEATED"),
                bigquery.SchemaField("vwap_ask_gradient", "FLOAT", mode="REPEATED"),
                bigquery.SchemaField("vwap_bid_gradient", "FLOAT", mode="REPEATED"),
                bigquery.SchemaField("median_ask_gradient", "FLOAT", mode="REPEATED"),
                bigquery.SchemaField("median_bid_gradient", "FLOAT", mode="REPEATED"),
                bigquery.SchemaField("vwap_median_distance_asks", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("vwap_median_distance_bids", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("vwap_ask_bid_spread", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("volume_entropy", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("order_imbalance", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField(
                    "fft_peaks_asks",
                    "RECORD",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField("frequency", "FLOAT", mode="NULLABLE"),
                        bigquery.SchemaField("period_min", "FLOAT", mode="NULLABLE"),
                        bigquery.SchemaField("amplitude", "FLOAT", mode="NULLABLE"),
                    ],
                ),
                bigquery.SchemaField(
                    "fft_peaks_bids",
                    "RECORD",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField("frequency", "FLOAT", mode="NULLABLE"),
                        bigquery.SchemaField("period_min", "FLOAT", mode="NULLABLE"),
                        bigquery.SchemaField("amplitude", "FLOAT", mode="NULLABLE"),
                    ],
                ),
                bigquery.SchemaField(
                    "frequency_bands_power",
                    "FLOAT",
                    mode="REPEATED",
                    description="Power in frequency bands: low (0–0.002 Hz), medium (0.002–0.01 Hz), high (>0.01 Hz)",
                ),
                bigquery.SchemaField("dominant_frequency_vwap", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("dominant_period_min", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField(
                    "liquidity_levels",
                    "FLOAT",
                    mode="REPEATED",
                    description="Sum of quantities for top-5, top-10, top-25 levels of asks and bids",
                ),
            ]
            
            # Configure job configs
            self.raw_job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema=raw_schema
            )
            self.derived_job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                schema=derived_schema
            )
            
            # Verify tables exist
            self.client.get_table(self.raw_table_ref)
            self.client.get_table(self.derived_table_ref)
            
            logger.info(f"BigQuery initialized for tables {self.raw_table_ref} and {self.derived_table_ref}")
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery: {e}")
            raise RuntimeError(f"Failed to initialize BigQuery: {e}")
    
    async def insert_data(self, 
                         raw_buffer: List[Dict[str, Any]], 
                         derived_buffer: List[Dict[str, Any]]) -> None:
        """Insert data into BigQuery tables.
        
        Args:
            raw_buffer: Buffer of raw data records.
            derived_buffer: Buffer of derived data records.
        """
        try:
            # Insert raw data
            if raw_buffer:
                await self._insert_into_table(raw_buffer, self.raw_table_ref, self.raw_job_config, "raw")
            
            # Insert derived data
            if derived_buffer:
                await self._insert_into_table(derived_buffer, self.derived_table_ref, self.derived_job_config, "derived")
                
        except Exception as e:
            logger.error(f"Error inserting data into BigQuery: {e}")
            if self.debug:
                self._save_debug_file(raw_buffer + derived_buffer)
    
    async def _insert_into_table(self, 
                               buffer: List[Dict[str, Any]], 
                               table_ref: bigquery.TableReference, 
                               job_config: bigquery.LoadJobConfig,
                               data_type: str) -> None:
        """Insert data into a specific BigQuery table.
        
        Args:
            buffer: Buffer of data records.
            table_ref: Reference to the BigQuery table.
            job_config: Job configuration for the load job.
            data_type: Type of data being inserted (for logging).
        """
        if not buffer:
            return
            
        loop = asyncio.get_event_loop()
        job = await loop.run_in_executor(
            None,
            lambda: self.client.load_table_from_json(
                buffer, table_ref, job_config=job_config
            )
        )
        
        # Wait for the job to complete
        await loop.run_in_executor(None, job.result)
        
        logger.info(f"Successfully loaded {len(buffer)} {data_type} records into {table_ref}")
        buffer.clear()
    
    def _save_debug_file(self, buffer: List[Dict[str, Any]]) -> None:
        """Save buffer to a temporary file for debugging.
        
        Args:
            buffer: Buffer of data records.
        """
        try:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
                for record in buffer:
                    json.dump(record, temp_file)
                    temp_file.write('\n')
                temp_filename = temp_file.name
            logger.info(f"Buffer saved to {temp_filename} for analysis")
        except Exception as ex:
            logger.error(f"Error saving temporary file: {ex}")
