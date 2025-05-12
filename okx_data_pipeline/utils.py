import os
import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

async def save_pending_data(script_directory, pending_data, buffer_prefix):
    if not pending_data:
        logger.warning("No pending data to save")
        return False
    try:
        now = datetime.now()
        timestamp = now.strftime("%Y%m%d_%H%M%S")
        buffer_filename = f"{buffer_prefix}_{timestamp}.json"
        buffer_filepath = os.path.join(script_directory, buffer_filename)
        with open(buffer_filepath, 'w') as f:
            json.dump(pending_data, f)
        logger.info(f"Saved pending data to {buffer_filepath}")
        return True
    except Exception as e:
        logger.error(f"Failed to save pending data: {e}")
        return False

async def load_buffer_from_disk(script_directory, bigquery_client, table_id, notifier, buffer_prefix):
    try:
        buffer_files = [f for f in os.listdir(script_directory) if f.startswith(buffer_prefix) and f.endswith('.json')]
        for buffer_file in buffer_files:
            buffer_filepath = os.path.join(script_directory, buffer_file)
            try:
                with open(buffer_filepath, 'r') as f:
                    data = json.load(f)
                success = await bigquery_client.load_data([data], table_id, notifier=notifier)
                if success:
                    os.remove(buffer_filepath)
                    logger.info(f"Restored and deleted buffer file {buffer_filepath}")
                else:
                    logger.error(f"Failed to restore buffer file {buffer_filepath}")
            except Exception as e:
                logger.error(f"Error processing buffer file {buffer_filepath}: {e}")
                continue
    except Exception as e:
        logger.error(f"Error loading buffer from disk: {e}")
