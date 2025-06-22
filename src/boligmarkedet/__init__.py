"""Boligmarkedet - Danish property market data scraper."""

__version__ = "0.1.0"

from .main import main
from .config.settings import settings
from .api.client import BoligaAPIClient, AsyncBoligaAPIClient
from .database.models import schema, ActiveProperty, SoldProperty
from .database.operations import PropertyOperations, ScrapingOperations
from .utils.logging import get_logger
from .scrapers import BaseScraper, ActivePropertiesScraper, SoldPropertiesScraper, ScrapingError

__all__ = [
    "main",
    "settings",
    "BoligaAPIClient",
    "AsyncBoligaAPIClient", 
    "schema",
    "ActiveProperty",
    "SoldProperty",
    "PropertyOperations",
    "ScrapingOperations",
    "get_logger",
    "BaseScraper",
    "ActivePropertiesScraper",
    "SoldPropertiesScraper",
    "ScrapingError",
]
