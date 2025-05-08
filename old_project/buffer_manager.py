"""
Buffer manager for storing and retrieving historical data.
"""
import logging
import numpy as np
from typing import Tuple, List, Dict, Any

logger = logging.getLogger(__name__)

class BufferManager:
    """Manages data buffers for historical data analysis."""
    
    def __init__(self, buffer_capacity: int, window_size: int, value_dimensions: int = 10):
        """Initialize the buffer manager.
        
        Args:
            buffer_capacity: Maximum number of records the buffer can hold.
            window_size: Size of the sliding window for calculations.
            value_dimensions: Number of values to store for each record.
        """
        self.buffer_capacity = buffer_capacity
        self.window_size = window_size
        self.value_dimensions = value_dimensions
        
        # Initialize buffers
        self.history_buffer = np.zeros((buffer_capacity, value_dimensions), dtype=np.float32)
        self.history_ts = np.zeros(buffer_capacity, dtype=np.int64)
        self.start = 0
        self.end = 0
        
        logger.info(f"Initialized buffer with capacity={buffer_capacity}, window_size={window_size}")
    
    def add_to_buffer(self, values: List[float], ts: int) -> None:
        """Add data to the buffer.
        
        Args:
            values: List of values to add.
            ts: Timestamp for the values.
        """
        if len(values) != self.value_dimensions:
            logger.warning(f"Expected {self.value_dimensions} values but got {len(values)}")
            return
            
        self.history_buffer[self.end] = values
        self.history_ts[self.end] = ts
        self.end += 1
        
        # Adjust window if needed
        if self.end - self.start >= self.window_size:
            self.start += 1
            
        # Reset buffer if it's full
        if self.end >= self.buffer_capacity:
            current_window = self.history_buffer[self.start:self.end]
            current_window_ts = self.history_ts[self.start:self.end]
            window_len = len(current_window)
            
            # Copy window to the beginning
            np.copyto(self.history_buffer[:window_len], current_window)
            np.copyto(self.history_ts[:window_len], current_window_ts)
            
            self.start = 0
            self.end = window_len
            logger.info(f"Buffer rotated: start={self.start}, end={self.end}")
    
    def get_buffer_window(self) -> Tuple[np.ndarray, np.ndarray]:
        """Get the current window of data.
        
        Returns:
            Tuple of (values, timestamps) for the current window.
        """
        return self.history_buffer[self.start:self.end], self.history_ts[self.start:self.end]
    
    def get_value_at_time(self, param_idx: int, target_ts: int) -> float:
        """Get the value for a parameter at a specific timestamp.
        
        Args:
            param_idx: Index of the parameter in the buffer.
            target_ts: Target timestamp to find.
            
        Returns:
            The value at the closest timestamp before target_ts or np.nan if not found.
        """
        for i in range(self.end - self.start - 1, -1, -1):
            idx = self.start + i
            if self.history_ts[idx] <= target_ts:
                return self.history_buffer[idx, param_idx]
        return np.nan
    
    def calculate_gradients(self, 
                           current_values: List[float], 
                           ts: int, 
                           intervals: List[int], 
                           ms_per_minute: int) -> Dict[str, Dict[int, float]]:
        """Calculate gradients for various parameters over specified intervals.
        
        Args:
            current_values: Current parameter values.
            ts: Current timestamp.
            intervals: List of time intervals (in minutes) to calculate gradients for.
            ms_per_minute: Milliseconds per minute.
            
        Returns:
            Dictionary of gradients by parameter name and interval.
        """
        param_names = [
            'vwap_asks', 'vwap_bids', 'median_volume_asks', 'median_volume_bids',
            'ask_volume', 'bid_volume', 'ask_bid_volume_ratio',
            'vwap_orders_asks', 'vwap_orders_bids'
        ]
        
        gradients = {name: {} for name in param_names}
        
        for interval in intervals:
            lag_ms = interval * ms_per_minute
            for idx, name in enumerate(param_names):
                prev_value = self.get_value_at_time(idx, ts - lag_ms)
                if not np.isnan(prev_value):
                    gradients[name][interval] = current_values[idx] - prev_value
                else:
                    gradients[name][interval] = np.nan
        
        return gradients