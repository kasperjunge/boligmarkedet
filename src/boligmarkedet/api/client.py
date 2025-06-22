"""Base API client for Boliga API interactions."""

import httpx
from typing import Any, Dict, Optional, Union
from urllib.parse import urljoin
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from ..config.settings import settings
from ..utils.logging import get_logger
from ..utils.rate_limiter import rate_limiter

logger = get_logger(__name__)


class APIError(Exception):
    """Base exception for API errors."""
    pass


class APIClientError(APIError):
    """Client-side API errors (4xx)."""
    pass


class APIServerError(APIError):
    """Server-side API errors (5xx)."""
    pass


class BoligaAPIClient:
    """Base client for interacting with Boliga API."""
    
    def __init__(self, base_url: Optional[str] = None, timeout: Optional[int] = None):
        """Initialize API client.
        
        Args:
            base_url: Base URL for API. If None, uses settings.
            timeout: Request timeout in seconds. If None, uses settings.
        """
        self.base_url = base_url or settings.api.base_url
        self.timeout = timeout or settings.api.timeout
        self.headers = settings.api.headers.copy()
        
        # Create HTTP client
        self.client = httpx.Client(
            base_url=self.base_url,
            timeout=self.timeout,
            headers=self.headers
        )
        
        logger.info(f"Initialized Boliga API client with base URL: {self.base_url}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.client.close()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, APIServerError))
    )
    def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> httpx.Response:
        """Make HTTP request with rate limiting and retry logic.
        
        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (relative to base URL)
            params: Query parameters
            json: JSON request body
            **kwargs: Additional arguments for httpx request
            
        Returns:
            HTTP response object
            
        Raises:
            APIClientError: For 4xx errors
            APIServerError: For 5xx errors
            APIError: For other errors
        """
        # Apply rate limiting
        rate_limiter.wait_if_needed()
        
        # Make request
        try:
            logger.debug(f"Making {method} request to {endpoint} with params: {params}")
            
            response = self.client.request(
                method=method,
                url=endpoint,
                params=params,
                json=json,
                **kwargs
            )
            
            # Check for errors
            if response.status_code >= 400:
                error_msg = f"API request failed: {response.status_code} {response.reason_phrase}"
                
                if 400 <= response.status_code < 500:
                    logger.error(f"Client error: {error_msg}")
                    raise APIClientError(error_msg)
                elif response.status_code >= 500:
                    logger.error(f"Server error: {error_msg}")
                    raise APIServerError(error_msg)
            
            logger.debug(f"Request successful: {response.status_code}")
            return response
            
        except httpx.TimeoutException as e:
            logger.error(f"Request timeout: {e}")
            raise
        except httpx.RequestError as e:
            logger.error(f"Request error: {e}")
            raise APIError(f"Request failed: {e}")
    
    def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make GET request and return JSON response.
        
        Args:
            endpoint: API endpoint
            params: Query parameters
            
        Returns:
            JSON response as dictionary
        """
        response = self._make_request("GET", endpoint, params=params)
        return response.json()
    
    def post(self, endpoint: str, json: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make POST request and return JSON response.
        
        Args:
            endpoint: API endpoint
            json: JSON request body
            
        Returns:
            JSON response as dictionary
        """
        response = self._make_request("POST", endpoint, json=json)
        return response.json()
    
    def search_active_properties(
        self,
        page_size: int = 50,
        page_index: int = 0,
        **filters
    ) -> Dict[str, Any]:
        """Search for active properties.
        
        Args:
            page_size: Number of results per page
            page_index: Page index (0-based)
            **filters: Additional search filters
            
        Returns:
            API response with property data
        """
        params = {
            'pageSize': page_size,
            'pageIndex': page_index,
            **filters
        }
        
        logger.info(f"Searching active properties: page {page_index}, size {page_size}")
        return self.get('/api/v2/search/results', params=params)
    
    def search_sold_properties(
        self,
        page: int = 1,
        page_size: int = 50,
        **filters
    ) -> Dict[str, Any]:
        """Search for sold properties.
        
        Args:
            page: Page number (1-based)
            page_size: Number of results per page (returned as pageSize in meta)
            **filters: Additional search filters
            
        Returns:
            API response with sold property data
        """
        params = {
            'page': page,
            **filters
        }
        
        logger.info(f"Searching sold properties: page {page}")
        return self.get('/api/v2/sold/search/results', params=params)
    
    def get_property_details(self, property_id: int) -> Dict[str, Any]:
        """Get detailed information for a specific property.
        
        Args:
            property_id: Unique property ID
            
        Returns:
            Detailed property information
        """
        logger.info(f"Getting details for property {property_id}")
        return self.get(f'/api/v2/estate/{property_id}')
    
    def close(self):
        """Close the HTTP client."""
        self.client.close()
        logger.info("API client closed")


class AsyncBoligaAPIClient:
    """Async version of Boliga API client."""
    
    def __init__(self, base_url: Optional[str] = None, timeout: Optional[int] = None):
        """Initialize async API client."""
        self.base_url = base_url or settings.api.base_url
        self.timeout = timeout or settings.api.timeout
        self.headers = settings.api.headers.copy()
        
        # Create async HTTP client
        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=self.timeout,
            headers=self.headers
        )
        
        logger.info(f"Initialized async Boliga API client with base URL: {self.base_url}")
    
    async def __aenter__(self):
        """Async context manager entry."""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.client.aclose()
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10),
        retry=retry_if_exception_type((httpx.TimeoutException, APIServerError))
    )
    async def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> httpx.Response:
        """Make async HTTP request with rate limiting and retry logic."""
        # Apply rate limiting
        await rate_limiter.wait_if_needed_async()
        
        # Make request
        try:
            logger.debug(f"Making async {method} request to {endpoint} with params: {params}")
            
            response = await self.client.request(
                method=method,
                url=endpoint,
                params=params,
                json=json,
                **kwargs
            )
            
            # Check for errors
            if response.status_code >= 400:
                error_msg = f"API request failed: {response.status_code} {response.reason_phrase}"
                
                if 400 <= response.status_code < 500:
                    logger.error(f"Client error: {error_msg}")
                    raise APIClientError(error_msg)
                elif response.status_code >= 500:
                    logger.error(f"Server error: {error_msg}")
                    raise APIServerError(error_msg)
            
            logger.debug(f"Async request successful: {response.status_code}")
            return response
            
        except httpx.TimeoutException as e:
            logger.error(f"Async request timeout: {e}")
            raise
        except httpx.RequestError as e:
            logger.error(f"Async request error: {e}")
            raise APIError(f"Request failed: {e}")
    
    async def get(self, endpoint: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make async GET request and return JSON response."""
        response = await self._make_request("GET", endpoint, params=params)
        return response.json()
    
    async def search_active_properties(
        self,
        page_size: int = 50,
        page_index: int = 0,
        **filters
    ) -> Dict[str, Any]:
        """Async search for active properties."""
        params = {
            'pageSize': page_size,
            'pageIndex': page_index,
            **filters
        }
        
        logger.info(f"Async searching active properties: page {page_index}, size {page_size}")
        return await self.get('/api/v2/search/results', params=params)
    
    async def search_sold_properties(
        self,
        page: int = 1,
        **filters
    ) -> Dict[str, Any]:
        """Async search for sold properties."""
        params = {
            'page': page,
            **filters
        }
        
        logger.info(f"Async searching sold properties: page {page}")
        return await self.get('/api/v2/sold/search/results', params=params)
    
    async def close(self):
        """Close the async HTTP client."""
        await self.client.aclose()
        logger.info("Async API client closed") 