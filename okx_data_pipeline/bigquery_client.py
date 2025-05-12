import os
import asyncio
import logging
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core.exceptions import GoogleAPIError

logger = logging.getLogger(__name__)

class BigQueryClient:
    def __init__(self, config, notifier):
        self.config = config
        self.notifier = notifier
        credentials_path = self.config.bigquery_credentials_path
        try:
            if os.path.exists(credentials_path):
                credentials = service_account.Credentials.from_service_account_file(credentials_path)
                logger.info(f"Loaded credentials from {credentials_path}")
            else:
                credentials = None
                logger.info("Credentials file not found, attempting to use Application Default Credentials")
            
            self.client = bigquery.Client(
                credentials=credentials,
                project=self.config.project_id
            )
            self.table_refs = {}
            for table_id in self.config.table_ids.values():
                try:
                    table_ref = self.client.dataset(self.config.dataset_id).table(table_id)
                    self.client.get_table(table_ref)
                    self.table_refs[table_id] = table_ref
                    logger.info(f"Initialized table reference for {table_id}")
                except Exception as e:
                    logger.error(f"Failed to initialize table {table_id}: {e}")
                    raise RuntimeError(f"Failed to initialize table {table_id}: {e}")
            
            self.job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                autodetect=False
            )
            logger.info("BigQuery client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery: {e}")
            raise RuntimeError(f"Failed to initialize BigQuery: {e}")

    async def load_data(self, data, table_id, notifier=None, max_retries=3, initial_delay=1.0):
        try:
            table_ref = self.table_refs[table_id]
        except KeyError:
            logger.error(f"Table ID {table_id} not found in table_refs")
            return False

        last_error = None
        for attempt in range(max_retries):
            logger.debug(f"Attempting to load {len(data)} rows into {table_id}, attempt {attempt + 1}")
            try:
                job = self.client.load_table_from_json(data, table_ref, job_config=self.job_config)
                while not job.done():
                    await asyncio.sleep(0.1)  # Asynchronous polling
                if job.errors:
                    error_msg = f"BigQuery job failed: {job.errors}"
                    logger.error(f"Attempt {attempt + 1}/{max_retries}: {error_msg}")
                    last_error = job.errors
                    if attempt < max_retries - 1:
                        await asyncio.sleep(initial_delay * (2 ** attempt))  # Exponential backoff
                    continue
                logger.info(f"Inserted {len(data)} rows into {table_id}")
                return True
            except GoogleAPIError as e:
                logger.error(f"Attempt {attempt + 1}/{max_retries}: Google API error for {table_id}: {e.code}, {e.message}")
                last_error = str(e)
                if attempt < max_retries - 1:
                    await asyncio.sleep(initial_delay * (2 ** attempt))  # Exponential backoff
                continue
            except Exception as e:
                logger.error(f"Attempt {attempt + 1}/{max_retries}: Exception in load_data for {table_id}: {e}")
                last_error = str(e)
                if attempt < max_retries - 1:
                    await asyncio.sleep(initial_delay * (2 ** attempt))  # Exponential backoff
                continue
        if notifier and last_error:
            await notifier.send_message(f"Failed to insert into {table_id} after {max_retries} attempts: {last_error}")
        logger.error(f"All {max_retries} attempts failed for {table_id}")
        return False
