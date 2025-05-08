"""
Processor for raw market data.
"""
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, List, Any, Tuple, Optional

logger = logging.getLogger(__name__)

async def process_raw_data(
    order_book: List[Dict[str, Any]],
    candlestick: List[List[Any]],
    ticker: List[Dict[str, Any]],
    debug: bool = False
) -> Tuple[Optional[Dict[str, Any]], Optional[int], Optional[str], Optional[str], Optional[str]]:
    """Process raw market data into structured format.
    
    Args:
        order_book: Order book data from the exchange.
        candlestick: Candlestick data from the exchange.
        ticker: Ticker data from the exchange.
        debug: Enable debug output.
        
    Returns:
        Tuple of (processed_data, timestamp, date_string, time_string, formatted_time)
        or (None, None, None, None, None) if processing fails.
    """
    try:
        if not (order_book and candlestick and ticker):
            logger.error("Incomplete data for processing")
            return None, None, None, None, None

        # Extract timestamp
        ts = int(ticker[0]['ts'])
        if debug:
            logger.debug(f"Processing ts: {ticker[0]['ts']} -> {ts}")

        # Generate time strings
        now = datetime.now()
        date_str = now.strftime('%Y-%m-%d')
        time_str = now.strftime('%H:%M:%S')
        formatted_time = now.strftime('%Y/%m/%d %H:%M:%S')

        # Process bids (top 25 levels)
        bids_df = pd.DataFrame(order_book[0]['bids'][:25], 
                              columns=['price', 'quantity', '_', 'num_orders'])
        bids = bids_df[['price', 'quantity', 'num_orders']].astype({
            'price': 'float',
            'quantity': 'float',
            'num_orders': 'int'
        }).to_dict('records')

        # Process asks (top 25 levels)
        asks_df = pd.DataFrame(order_book[0]['asks'][:25], 
                              columns=['price', 'quantity', '_', 'num_orders'])
        asks = asks_df[['price', 'quantity', 'num_orders']].astype({
            'price': 'float',
            'quantity': 'float',
            'num_orders': 'int'
        }).to_dict('records')

        # Build final data structure
        data = {
            "ts": ts,
            "date": date_str,
            "time": time_str,
            "formatted_time": formatted_time,
            "data": {
                "bids": bids,
                "asks": asks,
                "open": float(candlestick[0][1]),
                "high": float(candlestick[0][2]),
                "low": float(candlestick[0][3]),
                "close": float(candlestick[0][4]),
                "volume": float(candlestick[0][5]),
                "volCcy": float(candlestick[0][6]),
                "volCcyQuote": float(candlestick[0][7]),
                "confirm": str(candlestick[0][8]),
                "instType": str(ticker[0]['instType']),
                "instId": str(ticker[0]['instId']),
                "last": float(ticker[0]['last']),
                "lastSz": float(ticker[0]['lastSz']),
                "askPx": float(ticker[0]['askPx']),
                "askSz": float(ticker[0]['askSz']),
                "bidPx": float(ticker[0]['bidPx']),
                "bidSz": float(ticker[0]['bidSz']),
                "open24h": float(ticker[0]['open24h']),
                "high24h": float(ticker[0]['high24h']),
                "low24h": float(ticker[0]['low24h']),
                "volCcy24h": float(ticker[0]['volCcy24h']),
                "vol24h": float(ticker[0]['vol24h']),
                "sodUtc0": float(ticker[0]['sodUtc0']),
                "sodUtc8": float(ticker[0]['sodUtc8'])
            }
        }
        
        return data, ts, date_str, time_str, formatted_time
    except Exception as e:
        logger.error(f"Error processing raw data: {e}")
        return None, None, None, None, None