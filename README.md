# Cryptocurrency Data Collector

A Python application that asynchronously collects market data from cryptocurrency exchanges, processes it, and stores both raw and derived data in Google BigQuery.

## Features

- Asynchronous HTTP requests to fetch market data every minute
- Raw data storage in BigQuery without modification
- Derived data calculation with technical indicators
- Historical data buffer for time-series analysis
- Timestamp-based record correlation between tables

## Project Structure

```
├── config.py                  # Configuration settings
├── api_client.py              # Exchange API client
├── buffer_manager.py          # Historical data buffer manager
├── bigquery_client.py         # Google BigQuery client
├── raw_data_processor.py      # Raw data processing functions
├── derived_data_calculator.py # Derived data calculation functions
├── data_collector.py          # Main data collection orchestrator
├── main.py                    # Application entry point
└── requirements.txt           # Python dependencies
```

## Setup

1. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

2. Set up Google Cloud credentials:
   - Create a service account with BigQuery access
   - Download the JSON credentials file
   - Place it in the `/cred` directory as `wise-trainer-250014-917afbf4c8fe.json`

3. Ensure BigQuery tables exist:
   - Raw data table: `realtime_data_tst`
   - Derived data table: `realtime_data_glass_tst`

## Running the Application

```bash
python main.py
```

## Configuration

Key settings can be modified in `config.py`:

- `BUFFER_SIZE`: Number of records to buffer before writing to BigQuery
- `WINDOW_SIZE`: Size of the sliding window for calculations
- `INTERVALS`: Time intervals (in minutes) for gradient calculations
- `DEBUG`: Enable/disable debug mode

## Data Flow

1. Fetch market data from OKX exchange API
2. Process raw data for storage
3. Calculate derived metrics using current and historical data
4. Buffer data until threshold is reached
5. Write data to BigQuery tables
6. Update historical data buffer for future calculations