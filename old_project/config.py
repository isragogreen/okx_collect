"""
Configuration settings for the crypto data collector application.
"""
import os
from pathlib import Path

# API Configuration
SYMBOL = "BTC-USDT"
API_BASE_URL = "https://www.okx.com"
API_ENDPOINTS = [
    {
        'endpoint': '/api/v5/market/books',
        'params': {'instId': SYMBOL, 'sz': 400},
        'name': 'order_book'
    },
    {
        'endpoint': '/api/v5/market/candles',
        'params': {'instId': SYMBOL, 'bar': '1m', 'limit': 1},
        'name': 'candlestick'
    },
    {
        'endpoint': '/api/v5/market/ticker',
        'params': {'instId': SYMBOL},
        'name': 'ticker'
    }
]

# Buffer Configuration
BUFFER_SIZE = 10  # Number of records to buffer before writing to BigQuery
WINDOW_SIZE = 600  # Window size for historical data calculations
BUFFER_CAPACITY = 1200  # Total buffer capacity for historical data
INTERVALS = [1, 5, 100]  # Time intervals in minutes for gradient calculations
MS_PER_MINUTE = 60 * 1000  # Milliseconds per minute
MAX_CYCLE_TIME = 55  # Maximum time allowed for a single data collection cycle in seconds

# BigQuery Configuration
PROJECT_ID = 'wise-trainer-250014'
DATASET_ID = 'wisetrainer250014_test'
TABLE_SUFFIX = '_tst'  # Suffix for test tables
RAW_TABLE_ID = f'realtime_data{TABLE_SUFFIX}'
DERIVED_TABLE_ID = f'realtime_data_glass{TABLE_SUFFIX}'

# Paths
SCRIPT_DIR = Path(__file__).parent.absolute()
CREDENTIALS_PATH = SCRIPT_DIR / 'cred' / 'wise-trainer-250014-917afbf4c8fe.json'

# Debug Configuration
DEBUG = True  # Enable debug mode
