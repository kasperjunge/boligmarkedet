"""Base scraper class with common functionality."""

import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Any, List, Optional, Callable, Union
from dataclasses import dataclass, asdict

from ..api.client import BoligaAPIClient, APIError
from ..database.operations import PropertyOperations, ScrapingOperations
from ..utils.logging import get_logger
from ..utils.validators import PropertyValidator, validate_batch_data, ValidationResult

logger = get_logger(__name__)


class ScrapingError(Exception):
    """Base exception for scraping errors."""
    pass


@dataclass
class ScrapingProgress:
    """Represents the current scraping progress."""
    current_page: int
    total_pages: Optional[int]
    records_processed: int
    records_inserted: int
    records_updated: int
    records_failed: int
    api_calls_made: int
    start_time: datetime
    elapsed_time: float
    estimated_remaining: Optional[float] = None


@dataclass
class ScrapingConfig:
    """Configuration for scraping operations."""
    batch_size: int = 100
    page_size: int = 50
    max_retries: int = 3
    retry_delay: float = 1.0
    checkpoint_interval: int = 10  # Save progress every N pages
    max_errors: int = 1000
    validate_data: bool = True
    use_checkpoints: bool = True


class BaseScraper(ABC):
    """Base class for property scrapers."""
    
    def __init__(self, config: Optional[ScrapingConfig] = None):
        """Initialize the scraper.
        
        Args:
            config: Scraping configuration. If None, uses defaults.
        """
        self.config = config or ScrapingConfig()
        self.api_client = BoligaAPIClient()
        self.db_ops = PropertyOperations()
        self.scraping_ops = ScrapingOperations()
        self.validator = PropertyValidator()
        
        # Scraping state
        self.session_id: Optional[int] = None
        self.progress = ScrapingProgress(
            current_page=0,
            total_pages=None,
            records_processed=0,
            records_inserted=0,
            records_updated=0,
            records_failed=0,
            api_calls_made=0,
            start_time=datetime.now(),
            elapsed_time=0.0
        )
        
        # Progress callback
        self.progress_callback: Optional[Callable[[ScrapingProgress], None]] = None
        
        logger.info(f"Initialized {self.__class__.__name__} with config: {asdict(self.config)}")
    
    def set_progress_callback(self, callback: Callable[[ScrapingProgress], None]):
        """Set a callback function to be called on progress updates."""
        self.progress_callback = callback
    
    def scrape(self, 
               resume_from_checkpoint: bool = True,
               **kwargs) -> Dict[str, Any]:
        """Start scraping operation.
        
        Args:
            resume_from_checkpoint: Whether to resume from last checkpoint
            **kwargs: Additional arguments for the scraper
            
        Returns:
            Dictionary with scraping results and statistics
        """
        try:
            # Check for existing checkpoint
            if resume_from_checkpoint and self.config.use_checkpoints:
                checkpoint = self._load_checkpoint()
                if checkpoint:
                    logger.info(f"Resuming from checkpoint: page {checkpoint.get('current_page', 0)}")
                    self._restore_from_checkpoint(checkpoint)
            
            # Start scraping session
            self.session_id = self.scraping_ops.start_scraping_session(
                scrape_type=self.get_scrape_type(),
                total_pages=self.progress.total_pages
            )
            
            # Perform scraping
            results = self._scrape_implementation(**kwargs)
            
            # Complete session
            self.scraping_ops.complete_scraping_session(
                session_id=self.session_id,
                status='completed'
            )
            
            return results
            
        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            
            # Mark session as failed
            if self.session_id:
                self.scraping_ops.complete_scraping_session(
                    session_id=self.session_id,
                    status='failed',
                    error_message=str(e)
                )
            
            # Re-raise the exception
            raise ScrapingError(f"Scraping failed: {e}") from e
        
        finally:
            # Clean up resources
            self.api_client.close()
    
    @abstractmethod
    def get_scrape_type(self) -> str:
        """Return the scraping type identifier."""
        pass
    
    @abstractmethod
    def _scrape_implementation(self, **kwargs) -> Dict[str, Any]:
        """Implement the actual scraping logic."""
        pass
    
    @abstractmethod
    def _get_validation_function(self) -> Callable[[Dict[str, Any]], ValidationResult]:
        """Return the validation function for this scraper."""
        pass
    
    @abstractmethod
    def _store_data(self, validated_data: List[Dict[str, Any]]) -> Dict[str, int]:
        """Store validated data in the database."""
        pass
    
    def _load_checkpoint(self) -> Optional[Dict[str, Any]]:
        """Load the last checkpoint for this scraper."""
        return self.scraping_ops.get_last_checkpoint(self.get_scrape_type())
    
    def _save_checkpoint(self, additional_data: Optional[Dict[str, Any]] = None):
        """Save current progress as checkpoint."""
        if not self.session_id:
            return
        
        checkpoint_data = {
            'current_page': self.progress.current_page,
            'total_pages': self.progress.total_pages,
            'records_processed': self.progress.records_processed,
            'records_inserted': self.progress.records_inserted,
            'records_updated': self.progress.records_updated,
            'records_failed': self.progress.records_failed,
            'api_calls_made': self.progress.api_calls_made,
            'start_time': self.progress.start_time.isoformat(),
            **(additional_data or {})
        }
        
        self.scraping_ops.update_scraping_progress(
            session_id=self.session_id,
            current_page=self.progress.current_page,
            records_processed=self.progress.records_processed,
            records_inserted=self.progress.records_inserted,
            records_updated=self.progress.records_updated,
            records_failed=self.progress.records_failed,
            api_calls_made=self.progress.api_calls_made,
            checkpoint_data=checkpoint_data
        )
    
    def _restore_from_checkpoint(self, checkpoint: Dict[str, Any]):
        """Restore scraping state from checkpoint."""
        self.progress.current_page = checkpoint.get('current_page', 0)
        self.progress.total_pages = checkpoint.get('total_pages')
        self.progress.records_processed = checkpoint.get('records_processed', 0)
        self.progress.records_inserted = checkpoint.get('records_inserted', 0)
        self.progress.records_updated = checkpoint.get('records_updated', 0)
        self.progress.records_failed = checkpoint.get('records_failed', 0)
        self.progress.api_calls_made = checkpoint.get('api_calls_made', 0)
        
        # Restore start time
        if 'start_time' in checkpoint:
            try:
                self.progress.start_time = datetime.fromisoformat(checkpoint['start_time'])
            except ValueError:
                logger.warning("Could not parse start_time from checkpoint, using current time")
                self.progress.start_time = datetime.now()
    
    def _process_batch(self, 
                       batch_data: List[Dict[str, Any]],
                       validate: bool = True) -> Dict[str, int]:
        """Process a batch of data with validation and storage.
        
        Args:
            batch_data: List of raw data records
            validate: Whether to validate data before storage
            
        Returns:
            Dictionary with processing statistics
        """
        stats = {
            'processed': len(batch_data),
            'inserted': 0,
            'updated': 0,
            'failed': 0
        }
        
        if not batch_data:
            return stats
        
        # Validate data if requested
        if validate and self.config.validate_data:
            validation_result = validate_batch_data(
                batch_data,
                self._get_validation_function(),
                max_errors=self.config.max_errors
            )
            
            valid_data = validation_result['valid_records']
            stats['failed'] = validation_result['invalid_count']
            
            if validation_result['invalid_count'] > 0:
                logger.warning(
                    f"Validation failed for {validation_result['invalid_count']} records "
                    f"out of {len(batch_data)}"
                )
        else:
            valid_data = batch_data
        
        # Store valid data
        if valid_data:
            storage_stats = self._store_data(valid_data)
            stats['inserted'] = storage_stats.get('inserted', 0)
            stats['updated'] = storage_stats.get('updated', 0)
            stats['failed'] += storage_stats.get('failed', 0)
        
        return stats
    
    def _update_progress(self, 
                        page_increment: int = 0,
                        records_processed: int = 0,
                        records_inserted: int = 0,
                        records_updated: int = 0,
                        records_failed: int = 0,
                        api_calls_made: int = 0):
        """Update progress tracking."""
        self.progress.current_page += page_increment
        self.progress.records_processed += records_processed
        self.progress.records_inserted += records_inserted
        self.progress.records_updated += records_updated
        self.progress.records_failed += records_failed
        self.progress.api_calls_made += api_calls_made
        
        # Update elapsed time
        self.progress.elapsed_time = (datetime.now() - self.progress.start_time).total_seconds()
        
        # Estimate remaining time
        if self.progress.total_pages and self.progress.current_page > 0:
            pages_per_second = self.progress.current_page / self.progress.elapsed_time
            if pages_per_second > 0:
                remaining_pages = self.progress.total_pages - self.progress.current_page
                self.progress.estimated_remaining = remaining_pages / pages_per_second
        
        # Call progress callback if set
        if self.progress_callback:
            self.progress_callback(self.progress)
        
        # Log progress
        if self.progress.current_page % 10 == 0:  # Log every 10 pages
            self._log_progress()
    
    def _log_progress(self):
        """Log current progress."""
        elapsed_min = self.progress.elapsed_time / 60
        
        progress_msg = (
            f"Progress: Page {self.progress.current_page}"
            f"{f'/{self.progress.total_pages}' if self.progress.total_pages else ''} "
            f"| Processed: {self.progress.records_processed} "
            f"| Inserted: {self.progress.records_inserted} "
            f"| Updated: {self.progress.records_updated} "
            f"| Failed: {self.progress.records_failed} "
            f"| Elapsed: {elapsed_min:.1f}m"
        )
        
        if self.progress.estimated_remaining:
            remaining_min = self.progress.estimated_remaining / 60
            progress_msg += f" | ETA: {remaining_min:.1f}m"
        
        logger.info(progress_msg)
    
    def _handle_api_error(self, error: Exception, page: int, retry_count: int) -> bool:
        """Handle API errors with retry logic.
        
        Args:
            error: The exception that occurred
            page: The page number that failed
            retry_count: Current retry attempt
            
        Returns:
            True if should retry, False otherwise
        """
        if retry_count >= self.config.max_retries:
            logger.error(f"Max retries exceeded for page {page}: {error}")
            return False
        
        # Determine if error is retryable
        if isinstance(error, APIError):
            if "timeout" in str(error).lower():
                logger.warning(f"Timeout error on page {page}, retry {retry_count + 1}")
                time.sleep(self.config.retry_delay * (retry_count + 1))
                return True
            elif "server error" in str(error).lower():
                logger.warning(f"Server error on page {page}, retry {retry_count + 1}")
                time.sleep(self.config.retry_delay * (retry_count + 1))
                return True
        
        logger.error(f"Non-retryable error on page {page}: {error}")
        return False
    
    def get_progress_summary(self) -> Dict[str, Any]:
        """Get a summary of the current scraping progress."""
        return {
            'scrape_type': self.get_scrape_type(),
            'current_page': self.progress.current_page,
            'total_pages': self.progress.total_pages,
            'records_processed': self.progress.records_processed,
            'records_inserted': self.progress.records_inserted,
            'records_updated': self.progress.records_updated,
            'records_failed': self.progress.records_failed,
            'api_calls_made': self.progress.api_calls_made,
            'elapsed_time_minutes': self.progress.elapsed_time / 60,
            'estimated_remaining_minutes': self.progress.estimated_remaining / 60 if self.progress.estimated_remaining else None,
            'success_rate': (self.progress.records_inserted + self.progress.records_updated) / max(self.progress.records_processed, 1),
            'pages_per_minute': (self.progress.current_page / max(self.progress.elapsed_time, 1)) * 60
        } 