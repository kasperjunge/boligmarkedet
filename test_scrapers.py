#!/usr/bin/env python3
"""Test script for the new scraper functionality."""

import logging
from datetime import datetime, timedelta

from src.boligmarkedet import (
    ActivePropertiesScraper, 
    SoldPropertiesScraper, 
    ScrapingError,
    get_logger
)
from src.boligmarkedet.scrapers.base import ScrapingConfig

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = get_logger(__name__)


def test_active_properties_scraper():
    """Test the active properties scraper."""
    logger.info("=== Testing Active Properties Scraper ===")
    
    try:
        # Create scraper with custom config for testing
        config = ScrapingConfig(
            page_size=10,  # Small page size for testing
            batch_size=50,
            checkpoint_interval=2,  # Save checkpoint every 2 pages
            use_checkpoints=True,
            validate_data=True
        )
        
        scraper = ActivePropertiesScraper(config)
        
        # Test getting count first
        logger.info("Getting active properties count...")
        total_count = scraper.get_active_properties_count()
        logger.info(f"Total active properties available: {total_count:,}")
        
        # Test sample scraping
        logger.info("Starting sample scraping (3 pages)...")
        results = scraper.scrape_sample(max_pages=3)
        
        logger.info("Active properties sample scraping results:")
        for key, value in results.items():
            if isinstance(value, float):
                logger.info(f"  {key}: {value:.2f}")
            else:
                logger.info(f"  {key}: {value}")
        
        return True
        
    except Exception as e:
        logger.error(f"Active properties scraper test failed: {e}")
        return False


def test_sold_properties_scraper():
    """Test the sold properties scraper."""
    logger.info("=== Testing Sold Properties Scraper ===")
    
    try:
        # Create scraper with custom config for testing
        config = ScrapingConfig(
            page_size=10,  # Small page size for testing
            batch_size=50,
            checkpoint_interval=2,  # Save checkpoint every 2 pages
            use_checkpoints=True,
            validate_data=True
        )
        
        scraper = SoldPropertiesScraper(config)
        
        # Test getting count first
        logger.info("Getting sold properties count...")
        total_count = scraper.get_sold_properties_count()
        logger.info(f"Total sold properties available: {total_count:,}")
        
        # Test sample scraping
        logger.info("Starting sample scraping (3 pages)...")
        results = scraper.scrape_sample(max_pages=3)
        
        logger.info("Sold properties sample scraping results:")
        for key, value in results.items():
            if isinstance(value, float):
                logger.info(f"  {key}: {value:.2f}")
            else:
                logger.info(f"  {key}: {value}")
        
        return True
        
    except Exception as e:
        logger.error(f"Sold properties scraper test failed: {e}")
        return False


def test_incremental_scraping():
    """Test incremental scraping for sold properties."""
    logger.info("=== Testing Incremental Scraping ===")
    
    try:
        config = ScrapingConfig(
            page_size=5,  # Very small for testing
            batch_size=25,
            use_checkpoints=True,
            validate_data=True
        )
        
        scraper = SoldPropertiesScraper(config)
        
        # Test incremental scraping for last 30 days
        logger.info("Starting incremental scraping (last 30 days)...")
        results = scraper.scrape_incremental(days_back=30)
        
        logger.info("Incremental scraping results:")
        for key, value in results.items():
            if isinstance(value, float):
                logger.info(f"  {key}: {value:.2f}")
            elif key == 'date_range':
                logger.info(f"  {key}: {value}")
            else:
                logger.info(f"  {key}: {value}")
        
        return True
        
    except Exception as e:
        logger.error(f"Incremental scraping test failed: {e}")
        return False


def test_progress_callback():
    """Test progress callback functionality."""
    logger.info("=== Testing Progress Callback ===")
    
    def progress_callback(progress):
        """Example progress callback."""
        elapsed_min = progress.elapsed_time / 60
        logger.info(
            f"Progress Update: Page {progress.current_page} | "
            f"Records: {progress.records_processed} | "
            f"Inserted: {progress.records_inserted} | "
            f"Updated: {progress.records_updated} | "
            f"Failed: {progress.records_failed} | "
            f"Elapsed: {elapsed_min:.1f}m"
        )
    
    try:
        config = ScrapingConfig(
            page_size=5,
            batch_size=20,
            use_checkpoints=False,  # Disable checkpoints for this test
            validate_data=True
        )
        
        scraper = ActivePropertiesScraper(config)
        scraper.set_progress_callback(progress_callback)
        
        logger.info("Starting scraping with progress callback (2 pages)...")
        results = scraper.scrape_sample(max_pages=2)
        
        logger.info("Progress callback test completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Progress callback test failed: {e}")
        return False


def main():
    """Run all scraper tests."""
    logger.info("Starting scraper tests...")
    
    tests = [
        ("Active Properties Scraper", test_active_properties_scraper),
        ("Sold Properties Scraper", test_sold_properties_scraper),
        ("Incremental Scraping", test_incremental_scraping),
        ("Progress Callback", test_progress_callback),
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        logger.info(f"\n{'='*60}")
        logger.info(f"Running test: {test_name}")
        logger.info(f"{'='*60}")
        
        try:
            results[test_name] = test_func()
        except Exception as e:
            logger.error(f"Test {test_name} failed with exception: {e}")
            results[test_name] = False
    
    # Summary
    logger.info(f"\n{'='*60}")
    logger.info("TEST SUMMARY")
    logger.info(f"{'='*60}")
    
    passed = 0
    total = len(results)
    
    for test_name, passed_test in results.items():
        status = "PASSED" if passed_test else "FAILED"
        logger.info(f"{test_name}: {status}")
        if passed_test:
            passed += 1
    
    logger.info(f"\nTests passed: {passed}/{total}")
    
    if passed == total:
        logger.info("All tests passed! 🎉")
        return 0
    else:
        logger.error("Some tests failed! 😞")
        return 1


if __name__ == "__main__":
    exit(main()) 