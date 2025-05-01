"""
Calculator for derived market data metrics.
"""
import numpy as np
import pandas as pd
import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

async def calculate_derived_data(
    order_book: List[Dict[str, Any]],
    ticker: List[Dict[str, Any]],
    ts: int,
    date_str: str,
    time_str: str,
    formatted_time: str,
    buffer_manager,
    intervals: List[int],
    ms_per_minute: int
) -> Optional[Dict[str, Any]]:
    """Calculate derived metrics from order book and ticker data.
    
    Args:
        order_book: Order book data from the exchange.
        ticker: Ticker data from the exchange.
        ts: Timestamp for the data.
        date_str: Date string.
        time_str: Time string.
        formatted_time: Formatted date and time string.
        buffer_manager: Buffer manager instance for accessing historical data.
        intervals: List of time intervals (in minutes) for gradient calculations.
        ms_per_minute: Milliseconds per minute.
        
    Returns:
        Dictionary of derived metrics or None if calculation fails.
    """
    try:
        # Process order book data (400 levels of asks and bids)
        asks_df = pd.DataFrame(order_book[0]['asks'][:400], 
                              columns=['price', 'quantity', '_', 'num_orders'])
        asks_df = asks_df[['price', 'quantity', 'num_orders']].astype({
            'price': 'float',
            'quantity': 'float',
            'num_orders': 'int'
        })
        
        bids_df = pd.DataFrame(order_book[0]['bids'][:400], 
                              columns=['price', 'quantity', '_', 'num_orders'])
        bids_df = bids_df[['price', 'quantity', 'num_orders']].astype({
            'price': 'float',
            'quantity': 'float',
            'num_orders': 'int'
        })

        # Calculate VWAP (Volume-Weighted Average Price)
        vwap_asks = np.sum(asks_df['price'] * asks_df['quantity']) / np.sum(asks_df['quantity'])
        vwap_bids = np.sum(bids_df['price'] * bids_df['quantity']) / np.sum(bids_df['quantity'])
        
        # Calculate order-weighted VWAP
        vwap_orders_asks = np.sum(asks_df['price'] * asks_df['quantity'] * asks_df['num_orders']) / \
                          np.sum(asks_df['quantity'] * asks_df['num_orders'])
        vwap_orders_bids = np.sum(bids_df['price'] * bids_df['quantity'] * bids_df['num_orders']) / \
                          np.sum(bids_df['quantity'] * bids_df['num_orders'])

        # Calculate volume-weighted median
        def weighted_median(df):
            weights = df['quantity'].values
            values = df['price'].values
            sorted_indices = np.argsort(values)
            sorted_values = values[sorted_indices]
            sorted_weights = weights[sorted_indices]
            cumsum = np.cumsum(sorted_weights)
            midpoint = cumsum[-1] / 2
            median_idx = np.where(cumsum >= midpoint)[0][0]
            return sorted_values[median_idx]

        median_volume_asks = weighted_median(asks_df)
        median_volume_bids = weighted_median(bids_df)

        # Volume metrics
        ask_volume = np.sum(asks_df['quantity'])
        bid_volume = np.sum(bids_df['quantity'])
        ask_bid_volume_ratio = ask_volume / bid_volume if bid_volume != 0 else np.inf

        # Calculate entropy of volume distribution
        total_volume = ask_volume + bid_volume
        volumes = np.concatenate([asks_df['quantity'].values, bids_df['quantity'].values])
        probabilities = volumes / total_volume
        volume_entropy = -np.sum(probabilities * np.log(probabilities + 1e-10))

        # Order imbalance
        order_imbalance = np.sum(asks_df['num_orders']) - np.sum(bids_df['num_orders'])

        # Distance metrics
        vwap_median_distance_asks = vwap_asks - median_volume_asks
        vwap_median_distance_bids = vwap_bids - median_volume_bids
        vwap_ask_bid_spread = vwap_asks - vwap_bids

        # Current values for buffer
        mid_price = (float(ticker[0]['askPx']) + float(ticker[0]['bidPx'])) / 2
        current_values = [
            vwap_asks, vwap_bids, median_volume_asks, median_volume_bids,
            ask_volume, bid_volume, ask_bid_volume_ratio,
            vwap_orders_asks, vwap_orders_bids, mid_price
        ]

        # Calculate gradients using the buffer manager
        gradients = buffer_manager.calculate_gradients(
            current_values, ts, intervals, ms_per_minute
        )
        
        # Calculate impulse metrics
        impulses = {'vwap_asks': {}, 'vwap_bids': {}}
        for interval in intervals:
            # Asks impulse
            if (not np.isnan(gradients['vwap_asks'].get(interval, np.nan)) and 
                not np.isnan(gradients['ask_volume'].get(interval, np.nan)) and
                gradients['ask_volume'].get(interval, 0) != 0):
                impulses['vwap_asks'][interval] = (
                    gradients['vwap_asks'][interval] / 
                    gradients['ask_volume'][interval] / 
                    mid_price
                )
            else:
                impulses['vwap_asks'][interval] = np.nan
            
            # Bids impulse
            if (not np.isnan(gradients['vwap_bids'].get(interval, np.nan)) and 
                not np.isnan(gradients['bid_volume'].get(interval, np.nan)) and
                gradients['bid_volume'].get(interval, 0) != 0):
                impulses['vwap_bids'][interval] = (
                    gradients['vwap_bids'][interval] / 
                    gradients['bid_volume'][interval] / 
                    mid_price
                )
            else:
                impulses['vwap_bids'][interval] = np.nan

        # Prepare derived data for BigQuery
        derived_data = {
            "ts": ts,
            "date": date_str,
            "time": time_str,
            "formatted_time": formatted_time,
            "vwap_asks": float(vwap_asks),
            "vwap_bids": float(vwap_bids),
            "vwap_orders_asks": float(vwap_orders_asks),
            "vwap_orders_bids": float(vwap_orders_bids),
            "median_volume_asks": float(median_volume_asks),
            "median_volume_bids": float(median_volume_bids),
            "ask_bid_volume_ratio": float(ask_bid_volume_ratio),
            "delta_ask_bid_ratio": [float(gradients['ask_bid_volume_ratio'].get(i, np.nan)) for i in intervals],
            "ask_volume_gradient": [float(gradients['ask_volume'].get(i, np.nan)) for i in intervals],
            "bid_volume_gradient": [float(gradients['bid_volume'].get(i, np.nan)) for i in intervals],
            "vwap_impulse_asks": [float(impulses['vwap_asks'].get(i, np.nan)) for i in intervals],
            "vwap_impulse_bids": [float(impulses['vwap_bids'].get(i, np.nan)) for i in intervals],
            "vwap_ask_gradient": [float(gradients['vwap_asks'].get(i, np.nan)) for i in intervals],
            "vwap_bid_gradient": [float(gradients['vwap_bids'].get(i, np.nan)) for i in intervals],
            "median_ask_gradient": [float(gradients['median_volume_asks'].get(i, np.nan)) for i in intervals],
            "median_bid_gradient": [float(gradients['median_volume_bids'].get(i, np.nan)) for i in intervals],
            "vwap_median_distance_asks": float(vwap_median_distance_asks),
            "vwap_median_distance_bids": float(vwap_median_distance_bids),
            "vwap_ask_bid_spread": float(vwap_ask_bid_spread),
            "volume_entropy": float(volume_entropy),
            "order_imbalance": float(order_imbalance),
            "current_values": current_values  # For adding to buffer
        }

        return derived_data
    except Exception as e:
        logger.error(f"Error calculating derived data: {e}")
        return None