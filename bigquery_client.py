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
            
            # Configure job configs
            self.raw_job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                autodetect=False
            )
            self.derived_job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                autodetect=False
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