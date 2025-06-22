"""Active properties scraper implementation."""

from datetime import datetime
from typing import Dict, Any, List, Optional, Callable
from dataclasses import asdict

from .base import BaseScraper, ScrapingConfig, ScrapingError
from ..api.client import APIError
from ..database.models import ActiveProperty
from ..utils.logging import get_logger
from ..utils.validators import ValidationResult

logger = get_logger(__name__)


class ActivePropertiesScraper(BaseScraper):
    """Scraper for active properties data."""
    
    def get_scrape_type(self) -> str:
        """Return the scraping type identifier."""
        return 'active'
    
    def _get_validation_function(self) -> Callable[[Dict[str, Any]], ValidationResult]:
        """Return the validation function for active properties."""
        return self.validator.validate_active_property
    
    def _store_data(self, validated_data: List[Dict[str, Any]]) -> Dict[str, int]:
        """Store validated active properties data in the database."""
        # Convert validated data to ActiveProperty objects
        properties = []
        for data in validated_data:
            try:
                # Map API fields to database fields
                property_data = self._map_api_to_db_fields(data)
                properties.append(property_data)
            except Exception as e:
                logger.warning(f"Failed to map property data: {e}")
                continue
        
        # Use bulk upsert for active properties
        return self.db_ops.bulk_upsert_active_properties(properties, self.config.batch_size)
    
    def _scrape_implementation(self, **kwargs) -> Dict[str, Any]:
        """Implement active properties scraping logic."""
        logger.info("Starting active properties scraping")
        
        # Get initial page to determine total pages
        first_page = self._fetch_page(0)
        if not first_page:
            raise ScrapingError("Failed to fetch first page")
        
        # Extract metadata
        meta = first_page.get('meta', {})
        total_results = meta.get('totalResults', 0)
        page_size = meta.get('pageSize', self.config.page_size)
        
        # Calculate total pages
        total_pages = (total_results + page_size - 1) // page_size
        self.progress.total_pages = total_pages
        
        logger.info(f"Found {total_results} active properties across {total_pages} pages")
        
        # Update session with total pages
        if self.session_id:
            self.scraping_ops.update_scraping_progress(
                session_id=self.session_id,
                current_page=0,
                total_pages=total_pages
            )
        
        # Process first page
        first_page_stats = self._process_page(first_page, 0)
        self._update_progress(
            page_increment=1,
            records_processed=first_page_stats['processed'],
            records_inserted=first_page_stats['inserted'],
            records_updated=first_page_stats['updated'],
            records_failed=first_page_stats['failed'],
            api_calls_made=1
        )
        
        # Process remaining pages
        for page_index in range(1, total_pages):
            # Skip if resuming from checkpoint
            if page_index < self.progress.current_page:
                continue
            
            try:
                page_data = self._fetch_page(page_index)
                if not page_data:
                    logger.warning(f"No data returned for page {page_index}")
                    continue
                
                # Process page data
                page_stats = self._process_page(page_data, page_index)
                
                # Update progress
                self._update_progress(
                    page_increment=1,
                    records_processed=page_stats['processed'],
                    records_inserted=page_stats['inserted'],
                    records_updated=page_stats['updated'],
                    records_failed=page_stats['failed'],
                    api_calls_made=1
                )
                
                # Save checkpoint periodically
                if self.config.use_checkpoints and page_index % self.config.checkpoint_interval == 0:
                    self._save_checkpoint({'last_page': page_index})
                
            except Exception as e:
                logger.error(f"Error processing page {page_index}: {e}")
                if not self._handle_api_error(e, page_index, 0):
                    # If error is not retryable, continue to next page
                    self._update_progress(
                        page_increment=1,
                        records_failed=page_size,  # Assume page size failures
                        api_calls_made=1
                    )
                    continue
        
        # Final progress update
        self._log_progress()
        
        return {
            'scrape_type': self.get_scrape_type(),
            'total_pages': total_pages,
            'pages_processed': self.progress.current_page,
            'records_processed': self.progress.records_processed,
            'records_inserted': self.progress.records_inserted,
            'records_updated': self.progress.records_updated,
            'records_failed': self.progress.records_failed,
            'api_calls_made': self.progress.api_calls_made,
            'elapsed_time_minutes': self.progress.elapsed_time / 60,
            'success_rate': (self.progress.records_inserted + self.progress.records_updated) / max(self.progress.records_processed, 1)
        }
    
    def _fetch_page(self, page_index: int, retry_count: int = 0) -> Optional[Dict[str, Any]]:
        """Fetch a single page of active properties.
        
        Args:
            page_index: Zero-based page index
            retry_count: Current retry attempt
            
        Returns:
            API response data or None if failed
        """
        try:
            response = self.api_client.search_active_properties(
                page_size=self.config.page_size,
                page_index=page_index
            )
            return response
            
        except Exception as e:
            if self._handle_api_error(e, page_index, retry_count):
                return self._fetch_page(page_index, retry_count + 1)
            return None
    
    def _process_page(self, page_data: Dict[str, Any], page_index: int) -> Dict[str, int]:
        """Process a single page of data.
        
        Args:
            page_data: Raw API response data
            page_index: Current page index
            
        Returns:
            Dictionary with processing statistics
        """
        # Extract properties from response
        properties = page_data.get('searchResults', [])
        
        if not properties:
            logger.warning(f"No properties found on page {page_index}")
            return {'processed': 0, 'inserted': 0, 'updated': 0, 'failed': 0}
        
        logger.debug(f"Processing {len(properties)} properties from page {page_index}")
        
        # Process the batch
        return self._process_batch(properties, validate=self.config.validate_data)
    
    def _map_api_to_db_fields(self, api_data: Dict[str, Any]) -> Dict[str, Any]:
        """Map API response fields to database fields.
        
        Args:
            api_data: Validated API response data
            
        Returns:
            Dictionary with database field names
        """
        now = datetime.now()
        
        # Basic field mapping
        db_data = {
            'id': api_data.get('id'),
            'price': api_data.get('price'),
            'rooms': api_data.get('rooms'),
            'size': api_data.get('size'),
            'lot_size': api_data.get('lotSize'),
            'build_year': api_data.get('buildYear'),
            'energy_class': api_data.get('energyClass'),
            'city': api_data.get('city'),
            'zip_code': api_data.get('zipCode'),
            'street': api_data.get('street'),
            'latitude': api_data.get('latitude'),
            'longitude': api_data.get('longitude'),
            'days_for_sale': api_data.get('daysForSale'),
            'created_date': api_data.get('createdDate'),
            'property_type': api_data.get('propertyType'),
            'scraped_at': now,
            'updated_at': now,
            'version': 1  # Will be updated by upsert logic if needed
        }
        
        return db_data
    
    def get_active_properties_count(self) -> int:
        """Get the total count of active properties from the API.
        
        Returns:
            Total number of active properties available
        """
        try:
            response = self.api_client.search_active_properties(page_size=1, page_index=0)
            meta = response.get('meta', {})
            return meta.get('totalResults', 0)
        except Exception as e:
            logger.error(f"Failed to get active properties count: {e}")
            return 0
    
    def scrape_sample(self, max_pages: int = 5) -> Dict[str, Any]:
        """Scrape a sample of active properties for testing.
        
        Args:
            max_pages: Maximum number of pages to scrape
            
        Returns:
            Dictionary with scraping results
        """
        logger.info(f"Starting sample scraping of active properties (max {max_pages} pages)")
        
        # Temporarily override total pages for sample scraping
        original_total_pages = self.progress.total_pages
        self.progress.total_pages = max_pages
        
        try:
            # Get first page to validate API
            first_page = self._fetch_page(0)
            if not first_page:
                raise ScrapingError("Failed to fetch first page for sample")
            
            # Start scraping session
            self.session_id = self.scraping_ops.start_scraping_session(
                scrape_type=f"{self.get_scrape_type()}_sample",
                total_pages=max_pages
            )
            
            # Process pages up to max_pages
            for page_index in range(min(max_pages, 10)):  # Safety limit
                try:
                    page_data = self._fetch_page(page_index)
                    if not page_data:
                        logger.warning(f"No data returned for sample page {page_index}")
                        continue
                    
                    page_stats = self._process_page(page_data, page_index)
                    
                    self._update_progress(
                        page_increment=1,
                        records_processed=page_stats['processed'],
                        records_inserted=page_stats['inserted'],
                        records_updated=page_stats['updated'],
                        records_failed=page_stats['failed'],
                        api_calls_made=1
                    )
                    
                except Exception as e:
                    logger.error(f"Error processing sample page {page_index}: {e}")
                    continue
            
            # Complete session
            self.scraping_ops.complete_scraping_session(
                session_id=self.session_id,
                status='completed'
            )
            
            return self.get_progress_summary()
            
        finally:
            # Restore original total pages
            self.progress.total_pages = original_total_pages
            self.api_client.close() 