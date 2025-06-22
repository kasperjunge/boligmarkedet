"""Main application entry point for Boligmarkedet."""

import sys
from pathlib import Path

from .config.settings import settings, ensure_directories
from .utils.logging import setup_logging, get_logger
from .database.models import schema
from .api.client import BoligaAPIClient


def init_application():
    """Initialize the application with necessary setup."""
    # Ensure required directories exist
    ensure_directories()
    
    # Set up logging
    logger = setup_logging()
    logger.info("Starting Boligmarkedet application")
    
    # Initialize database schema
    try:
        schema.create_tables()
        logger.info("Database schema initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize database schema: {e}")
        raise
    
    return logger


def test_api_connection():
    """Test API connection and basic functionality."""
    logger = get_logger(__name__)
    
    try:
        with BoligaAPIClient() as client:
            logger.info("Testing API connection...")
            
            # Test active properties search
            response = client.search_active_properties(page_size=1, page_index=0)
            if response and 'meta' in response:
                total_active = response['meta'].get('totalCount', 0)
                logger.info(f"API test successful - Found {total_active} active properties")
            else:
                logger.warning("API test returned unexpected response format")
            
            # Test sold properties search
            response = client.search_sold_properties(page=1)
            if response and 'meta' in response:
                total_sold = response['meta'].get('totalCount', 0)
                logger.info(f"API test successful - Found {total_sold} sold properties")
            else:
                logger.warning("Sold properties API test returned unexpected response format")
                
    except Exception as e:
        logger.error(f"API connection test failed: {e}")
        raise


def main():
    """Main application function."""
    try:
        # Initialize application
        logger = init_application()
        
        # Test API connection
        test_api_connection()
        
        logger.info("Boligmarkedet application initialized successfully")
        logger.info("Phase 1 setup complete!")
        
        # Print summary
        print("\n" + "="*60)
        print("BOLIGMARKEDET - Phase 1 Setup Complete")
        print("="*60)
        print(f"Database path: {settings.database.path}")
        print(f"Log file: {settings.logging.file_path}")
        print(f"API base URL: {settings.api.base_url}")
        print("="*60)
        print("\nNext steps:")
        print("- Phase 2: Implement data scraping")
        print("- Phase 3: Add bulk load functionality")
        print("- Phase 4: Set up scheduling")
        print("="*60)
        
    except Exception as e:
        logger = get_logger(__name__)
        logger.error(f"Application initialization failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main() 