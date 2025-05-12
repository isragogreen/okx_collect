OKX Data Pipeline
A modular Python application for collecting and processing OKX exchange data, storing it in Google BigQuery, and sending notifications via Telegram.
Setup

Install dependencies:
pip install -r requirements.txt


Set environment variables (optional):

OKX_API_BASE_URL: OKX API base URL (default: https://www.okx.com)
OKX_SYMBOL: Trading pair (default: BTC-USDT)
TELEGRAM_TOKEN: Telegram bot token
TELEGRAM_CHAT_ID: Telegram chat ID
BQ_CREDENTIALS_PATH: Path to BigQuery credentials JSON
BQ_PROJECT_ID, BQ_DATASET_ID, BQ_REALTIME_TABLE_ID, BQ_GLASS_TABLE_ID: BigQuery settings


Place credentials:

Copy your BigQuery credentials JSON to okx_data_pipeline/cred/wise-trainer-250014-917afbf4c8fe.json or update the path in environment variables.



Running the Pipeline
python main.py

This starts two concurrent pipelines:

Data Collector: Collects order book, candlestick, ticker, and 5000-level order book data every ~60 seconds, storing in BigQuery (realtime_table_id table).
Data Processor: Processes 5000-level order book data from a queue, computes statistics, and stores in BigQuery (glass_table_id table).

Features

Unified Timestamp: Uses ts from ticker[0]['ts'] for both realtime_data and glass tables.
Async Queue: Transfers books_full data from collector to processor via asyncio.Queue (max 10 items).
Error Handling:
Retries BigQuery writes 3 times (5–30s backoff). On failure, saves data to disk and notifies via Telegram.
Retries program crashes 5 times (10–60s backoff), saving data to disk and notifying via Telegram.
Queue overflow treated as a crash, triggers data save and restart.


Data Persistence:
Saves complete data sets to disk (okx_realtime_buffer_*.json, okx_glass_buffer_*.json) only on crash or BigQuery failure.
On startup, loads disk data, writes to BigQuery, deletes only successfully written files.


Notifications:
Sends Telegram messages for crashes, stops (Ctrl+C), and BigQuery failures, including time, reason (e.g., BigQuery error code), and processed record counts.
Example: "❌ Программа крашнулась 2025-05-11 12:34:56. Причина: Queue full. Обработано записей: realtime_data=123, glass=456."



Testing
Run tests using pytest:
pytest tests/

Directory Structure

okx_data_pipeline/: Core modules (config, notifier, data_collector, data_processor, bigquery_client, utils)
tests/: Unit tests
main.py: Entry point
requirements.txt: Dependencies
README.md: This file

Notes

Uses aiohttp for async HTTP requests and google-cloud-bigquery for BigQuery interactions.
No in-memory buffering; data is written to BigQuery immediately or saved to disk on failure.
Cycles are timed to ~60 seconds, accounting for processing time (6–19 seconds).
Files saved on disk are only deleted after successful BigQuery write.

