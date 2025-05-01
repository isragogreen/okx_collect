"""
API client for fetching data from cryptocurrency exchanges.
"""
import logging
import aiohttp
from datetime import datetime
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

class ExchangeApiClient:
    """Client for interacting with cryptocurrency exchange APIs."""
    
    def __init__(self, base_url: str):
        """Initialize the API client.
        
        Args:
            base_url: Base URL for the API.
        """
        self.base_url = base_url
    
    async def fetch_data(self, session: aiohttp.ClientSession, request: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Fetch data from the exchange API.
        
        Args:
            session: HTTP session to use for the request.
            request: Dictionary containing endpoint, parameters, and name.
            
        Returns:
            Dictionary with the response data or None if the request failed.
        """
        url = f"{self.base_url}{request['endpoint']}"
        try:
            async with session.get(url, params=request['params']) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('code') == '0':
                        return {
                            'name': request['name'],
                            'data': data['data'],
                            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        }
                    else:
                        logger.error(f"API error for {request['name']}: {data.get('msg', 'Unknown error')}")
                        return None
                else:
                    logger.error(f"HTTP error for {request['name']}: {response.status}")
                    return None
        except Exception as e:
            logger.error(f"Exception while fetching {request['name']}: {e}")
            return None
    
    async def fetch_all_data(self, 
                            session: aiohttp.ClientSession, 
                            requests: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Fetch all required data from the exchange API.
        
        Args:
            session: HTTP session to use for the requests.
            requests: List of request specifications.
            
        Returns:
            Dictionary mapping data types to their responses.
        """
        import asyncio
        
        tasks = [self.fetch_data(session, req) for req in requests]
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        
        data_dict = {}
        for response in responses:
            if isinstance(response, Exception):
                logger.error(f"Error in request: {response}")
                continue
            if response:
                data_dict[response['name']] = response['data']
            else:
                logger.warning(f"Failed to get data for {response['name'] if response else 'unknown'}")
        
        return data_dict