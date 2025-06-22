"""Main application entry point for Boligmarkedet."""

import sys
from pathlib import Path
import asyncio
from datetime import datetime
from typing import Dict, Any

from .config.settings import settings, ensure_directories
from .utils.logging import setup_logging, get_logger
from .database.models import schema
from .database import (
    DatabaseSchema, 
    property_ops, 
    scraping_ops, 
    data_mgmt,
    migration_manager
)
from .api.client import BoligaAPIClient, AsyncBoligaAPIClient


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


async def initialize_database():
    """Initialize database with schema and migrations."""
    logger = get_logger(__name__)
    logger.info("Initializing database...")
    
    # Create database schema
    schema = DatabaseSchema()
    schema.create_tables()
    
    # Run migrations
    logger.info("Running database migrations...")
    migration_status = migration_manager.get_migration_status()
    logger.info(f"Migration status: {migration_status}")
    
    if migration_status['pending_count'] > 0:
        success = migration_manager.migrate()
        if success:
            logger.info("All migrations applied successfully")
        else:
            logger.error("Some migrations failed")
            return False
    else:
        logger.info("Database is up to date")
    
    return True


async def demonstrate_phase2_operations():
    """Demonstrate Phase 2 database operations."""
    logger = get_logger(__name__)
    logger.info("=== Demonstrating Phase 2: Database Operations ===")
    
    # 1. Test basic CRUD operations
    logger.info("1. Testing CRUD operations...")
    
    # Sample active property data
    sample_active_property = {
        'id': 12345,
        'price': 2500000,
        'rooms': 4.0,
        'size': 120,
        'lot_size': 800,
        'build_year': 2010,
        'energy_class': 'C',
        'city': 'København',
        'zip_code': 2100,
        'street': 'Testgade 1',
        'latitude': 55.6761,
        'longitude': 12.5683,
        'days_for_sale': 30,
        'property_type': 1
    }
    
    # Insert property
    success = property_ops.insert_active_property(sample_active_property)
    logger.info(f"Insert active property: {'Success' if success else 'Failed'}")
    
    # Retrieve property
    retrieved = property_ops.get_active_property(12345)
    logger.info(f"Retrieved property: {retrieved.city if retrieved else 'Not found'}")
    
    # Update property (demonstrate versioning)
    if retrieved:
        sample_active_property['price'] = 2600000  # Price increase
        success = property_ops.upsert_active_property(sample_active_property)
        logger.info(f"Upsert active property: {'Success' if success else 'Failed'}")
        
        # Check version increment
        updated = property_ops.get_active_property(12345)
        if updated:
            logger.info(f"Property version after update: {updated.version}")
    
    # 2. Test bulk operations
    logger.info("2. Testing bulk operations...")
    
    # Sample bulk data
    bulk_properties = []
    for i in range(100, 105):
        bulk_properties.append({
            'id': i,
            'price': 2000000 + (i * 10000),
            'rooms': 3.0,
            'size': 100,
            'city': 'Aarhus',
            'zip_code': 8000,
            'street': f'Bulkgade {i}',
            'property_type': 2
        })
    
    stats = property_ops.bulk_insert_active_properties(bulk_properties)
    logger.info(f"Bulk insert stats: {stats}")
    
    # 3. Test scraping operations (checkpoint/resume functionality)
    logger.info("3. Testing scraping operations with checkpoint functionality...")
    
    # Start a scraping session
    session_id = scraping_ops.start_scraping_session('active', total_pages=10)
    logger.info(f"Started scraping session: {session_id}")
    
    # Update progress (simulate processing)
    for page in range(1, 4):
        scraping_ops.update_scraping_progress(
            session_id=session_id,
            current_page=page,
            records_processed=50,
            records_inserted=45,
            records_updated=3,
            records_failed=2,
            api_calls_made=1,
            checkpoint_data={'last_property_id': page * 1000}
        )
        logger.info(f"Updated progress for page {page}")
    
    # Get checkpoint
    checkpoint = scraping_ops.get_last_checkpoint('active')
    logger.info(f"Last checkpoint: {checkpoint}")
    
    # Complete session
    scraping_ops.complete_scraping_session(session_id, 'completed')
    logger.info("Completed scraping session")
    
    # 4. Test data management operations
    logger.info("4. Testing data management operations...")
    
    # Get statistics
    stats = data_mgmt.get_data_statistics()
    logger.info(f"Database statistics: {stats}")
    
    # Test deduplication (insert duplicate)
    duplicate_property = sample_active_property.copy()
    duplicate_property['version'] = 1  # Lower version
    property_ops.insert_active_property(duplicate_property)
    
    dedup_stats = data_mgmt.deduplicate_active_properties()
    logger.info(f"Deduplication stats: {dedup_stats}")
    
    # 5. Test migration system
    logger.info("5. Testing migration system...")
    
    # Get migration history
    history = migration_manager.get_migration_history()
    logger.info(f"Migration history: {len(history)} migrations")
    
    migration_status = migration_manager.get_migration_status()
    logger.info(f"Current migration status: {migration_status}")
    
    logger.info("=== Phase 2 Operations Demo Complete ===")


async def fetch_sample_data():
    """Fetch sample data to test the system end-to-end."""
    logger = get_logger(__name__)
    logger.info("=== Fetching Sample Data ===")
    
    try:
        # Initialize API client
        async_client = AsyncBoligaAPIClient()
        
        # Fetch a small sample of active properties
        logger.info("Fetching sample active properties...")
        response = await async_client.get_active_properties(
            params={'size': 5, 'page': 1}
        )
        
        if response and 'results' in response:
            properties = response['results']
            logger.info(f"Fetched {len(properties)} active properties")
            
            # Process and insert the sample data
            processed_count = 0
            for prop_data in properties:
                try:
                    # Basic data mapping (would be more sophisticated in real implementation)
                    processed_prop = {
                        'id': prop_data.get('id'),
                        'price': prop_data.get('price'),
                        'rooms': prop_data.get('rooms'),
                        'size': prop_data.get('size'),
                        'city': prop_data.get('city', 'Unknown'),
                        'zip_code': prop_data.get('zipCode', 0),
                        'street': prop_data.get('street', 'Unknown'),
                        'latitude': prop_data.get('latitude'),
                        'longitude': prop_data.get('longitude'),
                        'build_year': prop_data.get('buildYear'),
                        'property_type': prop_data.get('propertyType')
                    }
                    
                    success = property_ops.upsert_active_property(processed_prop)
                    if success:
                        processed_count += 1
                        
                except Exception as e:
                    logger.warning(f"Failed to process property: {e}")
            
            logger.info(f"Successfully processed {processed_count} properties")
            
            # Show final statistics
            final_stats = data_mgmt.get_data_statistics()
            logger.info(f"Final database statistics: {final_stats}")
        
        await async_client.close()
        
    except Exception as e:
        logger.error(f"Error fetching sample data: {e}")


async def main():
    """Main application function."""
    logger = get_logger(__name__)
    logger.info("Starting boligmarkedet application...")
    logger.info(f"Using database: {settings.database.path}")
    logger.info(f"Log level: {settings.logging.level}")
    
    try:
        # Initialize database
        if not await initialize_database():
            logger.error("Database initialization failed")
            return
        
        # Demonstrate Phase 2 operations
        await demonstrate_phase2_operations()
        
        # Fetch and process sample data
        await fetch_sample_data()
        
        logger.info("Application completed successfully")
        
    except Exception as e:
        logger.error(f"Application error: {e}", exc_info=True)
    finally:
        logger.info("Application shutdown")


if __name__ == "__main__":
    asyncio.run(main()) 