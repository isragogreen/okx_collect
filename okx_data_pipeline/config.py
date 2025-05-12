import os

class Config:
    def __init__(self):
        self.api_base_url = os.getenv("OKX_API_BASE_URL", "https://www.okx.com")
        self.symbol = os.getenv("OKX_SYMBOL", "BTC-USDT")
        self.buffer_size = int(os.getenv("BUFFER_SIZE", 10))
        self.collect_interval = int(os.getenv("COLLECT_INTERVAL", 60))
        self.fraction = float(os.getenv("FRACTION", 0.75))
        self.debug = os.getenv("DEBUG", "False").lower() == "true"
        self.project_id = os.getenv("BQ_PROJECT_ID", "wise-trainer-250014")
        self.dataset_id = os.getenv("BQ_DATASET_ID", "wisetrainer250014_test")
        self.table_ids = {
            "realtime_data": os.getenv("BQ_REALTIME_TABLE_ID", "realtime_data"),
            "glass": os.getenv("BQ_GLASS_TABLE_ID", "glass")
        }
        self.buffer_prefixes = {
            "realtime_data": os.getenv("BQ_REALTIME_BUFFER_PREFIX", "okx_realtime_buffer"),
            "glass": os.getenv("BQ_GLASS_BUFFER_PREFIX", "okx_glass_buffer")
        }
        self.bigquery_credentials_path = os.getenv(
            "BQ_CREDENTIALS_PATH",
            os.path.join(os.path.dirname(__file__), "cred", "wise-trainer-250014-917afbf4c8fe.json")
        )
        self.telegram_token = os.getenv("TELEGRAM_TOKEN", "6677036039:AAHbXH8dLbaHHlnA0M9fqN_QFa1TOpCwgwc")
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "5705352522")
