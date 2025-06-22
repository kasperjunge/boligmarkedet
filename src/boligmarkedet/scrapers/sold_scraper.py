"""Sold properties scraper implementation."""

from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Callable
from dataclasses import asdict

from .base import BaseScraper, ScrapingConfig, ScrapingError
from ..api.client import APIError
from ..database.models import SoldProperty
from ..utils.logging import get_logger
from ..utils.validators import ValidationResult

logger = get_logger(__name__)


class SoldPropertiesScraper(BaseScraper):
    """Scraper for sold properties data."""
    
    def get_scrape_type(self) -> str:
        """Return the scraping type identifier."""
        return 'sold'
    
    def _get_validation_function(self) -> Callable[[Dict[str, Any]], ValidationResult]:
        """Return the validation function for sold properties."""
        return self.validator.validate_sold_property
    
    def _store_data(self, validated_data: List[Dict[str, Any]]) -> Dict[str, int]:
        """Store validated sold properties data in the database."""
        # Convert validated data to SoldProperty objects
        properties = []
        for data in validated_data:
            try:
                # Map API fields to database fields
                property_data = self._map_api_to_db_fields(data)
                properties.append(property_data)
            except Exception as e:
                logger.warning(f"Failed to map property data: {e}")
                continue
        
        # Use bulk upsert for sold properties
        return self.db_ops.bulk_upsert_sold_properties(properties, self.config.batch_size)
    
    def _scrape_implementation(self, 
                             start_date: Optional[datetime] = None,
                             end_date: Optional[datetime] = None,
                             **kwargs) -> Dict[str, Any]:
        """Implement sold properties scraping logic.
        
        Args:
            start_date: Start date for incremental scraping
            end_date: End date for incremental scraping
            **kwargs: Additional scraping parameters
        """
        logger.info("Starting sold properties scraping")
        
        # Build date filter parameters
        date_params = {}
        if start_date:
            date_params['fromDate'] = start_date.strftime('%Y-%m-%d')
        if end_date:
            date_params['toDate'] = end_date.strftime('%Y-%m-%d')
        
        # Get initial page to determine total pages
        first_page = self._fetch_page(1, **date_params)
        if not first_page:
            raise ScrapingError("Failed to fetch first page")
        
        # Extract metadata
        meta = first_page.get('meta', {})
        total_results = meta.get('totalResults', 0)
        page_size = meta.get('pageSize', self.config.page_size)
        
        # Calculate total pages
        total_pages = (total_results + page_size - 1) // page_size
        self.progress.total_pages = total_pages
        
        date_range_str = ""
        if start_date or end_date:
            date_range_str = f" from {start_date.strftime('%Y-%m-%d') if start_date else 'beginning'} to {end_date.strftime('%Y-%m-%d') if end_date else 'now'}"
        
        logger.info(f"Found {total_results} sold properties across {total_pages} pages{date_range_str}")
        
        # Update session with total pages
        if self.session_id:
            self.scraping_ops.update_scraping_progress(
                session_id=self.session_id,
                current_page=0,
                total_pages=total_pages
            )
        
        # Process first page
        first_page_stats = self._process_page(first_page, 1)
        self._update_progress(
            page_increment=1,
            records_processed=first_page_stats['processed'],
            records_inserted=first_page_stats['inserted'],
            records_updated=first_page_stats['updated'],
            records_failed=first_page_stats['failed'],
            api_calls_made=1
        )
        
        # Process remaining pages
        for page_num in range(2, total_pages + 1):
            # Skip if resuming from checkpoint
            if page_num <= self.progress.current_page:
                continue
            
            try:
                page_data = self._fetch_page(page_num, **date_params)
                if not page_data:
                    logger.warning(f"No data returned for page {page_num}")
                    continue
                
                # Process page data
                page_stats = self._process_page(page_data, page_num)
                
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
                if self.config.use_checkpoints and page_num % self.config.checkpoint_interval == 0:
                    checkpoint_data = {
                        'last_page': page_num,
                        'start_date': start_date.isoformat() if start_date else None,
                        'end_date': end_date.isoformat() if end_date else None,
                    }
                    self._save_checkpoint(checkpoint_data)
                
            except Exception as e:
                logger.error(f"Error processing page {page_num}: {e}")
                if not self._handle_api_error(e, page_num, 0):
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
            'success_rate': (self.progress.records_inserted + self.progress.records_updated) / max(self.progress.records_processed, 1),
            'date_range': {
                'start_date': start_date.isoformat() if start_date else None,
                'end_date': end_date.isoformat() if end_date else None
            }
        }
    
    def _fetch_page(self, page_num: int, retry_count: int = 0, **params) -> Optional[Dict[str, Any]]:
        """Fetch a single page of sold properties.
        
        Args:
            page_num: One-based page number
            retry_count: Current retry attempt
            **params: Additional query parameters
            
        Returns:
            API response data or None if failed
        """
        try:
            response = self.api_client.search_sold_properties(
                page=page_num,
                page_size=self.config.page_size,
                **params
            )
            return response
            
        except Exception as e:
            if self._handle_api_error(e, page_num, retry_count):
                return self._fetch_page(page_num, retry_count + 1, **params)
            return None
    
    def _process_page(self, page_data: Dict[str, Any], page_num: int) -> Dict[str, int]:
        """Process a single page of data.
        
        Args:
            page_data: Raw API response data
            page_num: Current page number
            
        Returns:
            Dictionary with processing statistics
        """
        # Extract properties from response
        properties = page_data.get('searchResults', [])
        
        if not properties:
            logger.warning(f"No properties found on page {page_num}")
            return {'processed': 0, 'inserted': 0, 'updated': 0, 'failed': 0}
        
        logger.debug(f"Processing {len(properties)} properties from page {page_num}")
        
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
            'estate_id': api_data.get('estateId'),
            'address': api_data.get('address'),
            'zip_code': api_data.get('zipCode'),
            'price': api_data.get('price'),
            'sold_date': api_data.get('soldDate'),
            'property_type': api_data.get('propertyType'),
            'sale_type': api_data.get('saleType'),
            'sqm_price': api_data.get('sqmPrice'),
            'rooms': api_data.get('rooms'),
            'size': api_data.get('size'),
            'build_year': api_data.get('buildYear'),
            'change': api_data.get('change'),  # Price change percentage
            'latitude': api_data.get('latitude'),
            'longitude': api_data.get('longitude'),
            'city': api_data.get('city'),
            'scraped_at': now,
            'updated_at': now,
            'version': 1  # Will be updated by upsert logic if needed
        }
        
        return db_data
    
    def get_sold_properties_count(self, 
                                start_date: Optional[datetime] = None,
                                end_date: Optional[datetime] = None) -> int:
        """Get the total count of sold properties from the API.
        
        Args:
            start_date: Start date filter
            end_date: End date filter
            
        Returns:
            Total number of sold properties available
        """
        try:
            params = {}
            if start_date:
                params['fromDate'] = start_date.strftime('%Y-%m-%d')
            if end_date:
                params['toDate'] = end_date.strftime('%Y-%m-%d')
            
            response = self.api_client.search_sold_properties(page=1, **params)
            meta = response.get('meta', {})
            return meta.get('totalResults', 0)
        except Exception as e:
            logger.error(f"Failed to get sold properties count: {e}")
            return 0
    
    def scrape_incremental(self, days_back: int = 7) -> Dict[str, Any]:
        """Scrape sold properties from the last N days.
        
        Args:
            days_back: Number of days to go back from today
            
        Returns:
            Dictionary with scraping results
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days_back)
        
        logger.info(f"Starting incremental scraping from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        
        return self.scrape(
            start_date=start_date,
            end_date=end_date,
            resume_from_checkpoint=True
        )
    
    def scrape_sample(self, max_pages: int = 5) -> Dict[str, Any]:
        """Scrape a sample of sold properties for testing.
        
        Args:
            max_pages: Maximum number of pages to scrape
            
        Returns:
            Dictionary with scraping results
        """
        logger.info(f"Starting sample scraping of sold properties (max {max_pages} pages)")
        
        # Temporarily override total pages for sample scraping
        original_total_pages = self.progress.total_pages
        self.progress.total_pages = max_pages
        
        try:
            # Get first page to validate API
            first_page = self._fetch_page(1)
            if not first_page:
                raise ScrapingError("Failed to fetch first page for sample")
            
            # Start scraping session
            self.session_id = self.scraping_ops.start_scraping_session(
                scrape_type=f"{self.get_scrape_type()}_sample",
                total_pages=max_pages
            )
            
            # Process pages up to max_pages
            for page_num in range(1, min(max_pages + 1, 11)):  # Safety limit
                try:
                    page_data = self._fetch_page(page_num)
                    if not page_data:
                        logger.warning(f"No data returned for sample page {page_num}")
                        continue
                    
                    page_stats = self._process_page(page_data, page_num)
                    
                    self._update_progress(
                        page_increment=1,
                        records_processed=page_stats['processed'],
                        records_inserted=page_stats['inserted'],
                        records_updated=page_stats['updated'],
                        records_failed=page_stats['failed'],
                        api_calls_made=1
                    )
                    
                except Exception as e:
                    logger.error(f"Error processing sample page {page_num}: {e}")
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
    
    def _restore_from_checkpoint(self, checkpoint: Dict[str, Any]):
        """Restore scraping state from checkpoint."""
        super()._restore_from_checkpoint(checkpoint)
        
        # Restore date range from checkpoint
        if 'start_date' in checkpoint and checkpoint['start_date']:
            self.start_date = datetime.fromisoformat(checkpoint['start_date'])
        if 'end_date' in checkpoint and checkpoint['end_date']:
            self.end_date = datetime.fromisoformat(checkpoint['end_date'])
    
    def scrape_bulk(self, 
                   batch_size: Optional[int] = None,
                   checkpoint_interval: Optional[int] = None) -> Dict[str, Any]:
        """Scrape all sold properties in bulk (for initial data load).
        
        Args:
            batch_size: Override default batch size
            checkpoint_interval: Override default checkpoint interval
            
        Returns:
            Dictionary with scraping results
        """
        logger.info("Starting bulk scraping of all sold properties")
        
        # Override config for bulk operation
        if batch_size:
            self.config.batch_size = batch_size
        if checkpoint_interval:
            self.config.checkpoint_interval = checkpoint_interval
        
        # Scrape all sold properties (no date filter)
        return self.scrape(resume_from_checkpoint=True) 