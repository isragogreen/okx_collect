import os
from google.cloud import bigquery
from google.oauth2 import service_account
import logging

logger = logging.getLogger(__name__)

class BigQueryClient:
    def __init__(self, config, notifier):
        self.config = config
        self.notifier = notifier
        credentials_path = self.config.credentials_path
        try:
            # Check if credentials file exists
            if os.path.exists(credentials_path):
                credentials = service_account.Credentials.from_service_account_file(credentials_path)
                logger.info(f"Loaded credentials from {credentials_path}")
            else:
                # Fallback to Application Default Credentials (ADC) or environment variable
                credentials = None
                logger.info("Credentials file not found, attempting to use Application Default Credentials")
            
            self.client = bigquery.Client(
                credentials=credentials,
                project=config.project_id
            )
            logger.info("BigQuery client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize BigQuery: {e}")
            raise RuntimeError(f"Failed to initialize BigQuery: {e}")

    async def load_data(self, data, table_id, notifier=None):
        try:
            errors = self.client.insert_rows_json(table_id, data)
            if not errors:
                logger.info(f"Inserted {len(data)} rows into {table_id}")
                return True
            else:
                logger.error(f"Errors inserting rows into {table_id}: {errors}")
                if notifier:
                    await notifier.send_message(f"BigQuery insert failed: {errors}")
                return False
        except Exception as e:
            logger.error(f"Exception in load_data: {e}")
            if notifier:
                await notifier.send_exception(sys.exc_info())
            return False
