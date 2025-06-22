"""Rate limiting utilities for API requests."""

import asyncio
import time
from dataclasses import dataclass
from typing import Dict, Optional

from ..config.settings import settings
from .logging import get_logger

logger = get_logger(__name__)


@dataclass
class RateLimit:
    """Rate limit configuration."""
    calls: int
    period: float  # seconds
    last_reset: float = 0.0
    current_calls: int = 0
    
    def __post_init__(self):
        if self.last_reset == 0.0:
            self.last_reset = time.time()


class RateLimiter:
    """Rate limiter for API requests."""
    
    def __init__(self, default_delay: float = None):
        """Initialize rate limiter.
        
        Args:
            default_delay: Default delay between requests in seconds
        """
        self.default_delay = default_delay or settings.api.rate_limit_delay
        self.rate_limits: Dict[str, RateLimit] = {}
        self.last_request_time: Optional[float] = None
    
    def add_limit(self, name: str, calls: int, period: float):
        """Add a rate limit rule.
        
        Args:
            name: Name of the rate limit rule
            calls: Number of calls allowed
            period: Time period in seconds
        """
        self.rate_limits[name] = RateLimit(calls=calls, period=period)
        logger.info(f"Added rate limit: {name} - {calls} calls per {period} seconds")
    
    def wait_if_needed(self, rule_name: Optional[str] = None):
        """Wait if rate limit would be exceeded.
        
        Args:
            rule_name: Name of specific rate limit rule to check
        """
        current_time = time.time()
        
        # Check specific rule if provided
        if rule_name and rule_name in self.rate_limits:
            self._check_rule(rule_name, current_time)
        
        # Apply default delay between requests
        if self.last_request_time is not None:
            time_since_last = current_time - self.last_request_time
            if time_since_last < self.default_delay:
                sleep_time = self.default_delay - time_since_last
                logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    def _check_rule(self, rule_name: str, current_time: float):
        """Check and enforce a specific rate limit rule."""
        rule = self.rate_limits[rule_name]
        
        # Reset counter if period has elapsed
        if current_time - rule.last_reset >= rule.period:
            rule.current_calls = 0
            rule.last_reset = current_time
        
        # Check if we need to wait
        if rule.current_calls >= rule.calls:
            wait_time = rule.period - (current_time - rule.last_reset)
            if wait_time > 0:
                logger.info(f"Rate limit reached for {rule_name}. Waiting {wait_time:.2f} seconds")
                time.sleep(wait_time)
                rule.current_calls = 0
                rule.last_reset = time.time()
        
        rule.current_calls += 1
    
    async def wait_if_needed_async(self, rule_name: Optional[str] = None):
        """Async version of wait_if_needed."""
        current_time = time.time()
        
        # Check specific rule if provided
        if rule_name and rule_name in self.rate_limits:
            await self._check_rule_async(rule_name, current_time)
        
        # Apply default delay between requests
        if self.last_request_time is not None:
            time_since_last = current_time - self.last_request_time
            if time_since_last < self.default_delay:
                sleep_time = self.default_delay - time_since_last
                logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
                await asyncio.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    async def _check_rule_async(self, rule_name: str, current_time: float):
        """Async version of _check_rule."""
        rule = self.rate_limits[rule_name]
        
        # Reset counter if period has elapsed
        if current_time - rule.last_reset >= rule.period:
            rule.current_calls = 0
            rule.last_reset = current_time
        
        # Check if we need to wait
        if rule.current_calls >= rule.calls:
            wait_time = rule.period - (current_time - rule.last_reset)
            if wait_time > 0:
                logger.info(f"Rate limit reached for {rule_name}. Waiting {wait_time:.2f} seconds")
                await asyncio.sleep(wait_time)
                rule.current_calls = 0
                rule.last_reset = time.time()
        
        rule.current_calls += 1


# Global rate limiter instance
rate_limiter = RateLimiter() 