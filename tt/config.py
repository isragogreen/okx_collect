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
        self.realtime_table_id = os.getenv("BQ_REALTIME_TABLE_ID", "realtime_data")
        self.glass_table_id = os.getenv("BQ_GLASS_TABLE_ID", "glass")
        self.bigquery_credentials_path = os.getenv(
            "BQ_CREDENTIALS_PATH",
            os.path.join(os.path.dirname(__file__), "cred", "wise-trainer-250014-917afbf4c8fe.json")
        )
        self.telegram_token = os.getenv("TELEGRAM_TOKEN", "6677036039:AAHbXH8dLbaHHlnA0M9fqN_QFa1TOpCwgwc")
        self.telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID", "5705352522")
