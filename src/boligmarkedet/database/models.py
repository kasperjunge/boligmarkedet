"""Database schema definitions for property data."""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, List
from enum import Enum

from ..config.database import db_manager
from ..utils.logging import get_logger

logger = get_logger(__name__)


class PropertyType(Enum):
    """Property type enumeration."""
    HOUSE = 1
    APARTMENT = 2
    TOWNHOUSE = 3
    VILLA = 4
    OTHER = 99


@dataclass
class ActiveProperty:
    """Model for active property data."""
    id: int
    price: int
    rooms: float
    size: int
    lot_size: Optional[int]
    build_year: Optional[int]
    energy_class: Optional[str]
    city: str
    zip_code: int
    street: str
    latitude: Optional[float]
    longitude: Optional[float]
    days_for_sale: Optional[int]
    created_date: Optional[datetime]
    property_type: Optional[int]
    
    # Metadata fields
    scraped_at: datetime
    updated_at: datetime
    version: int = 1


@dataclass
class SoldProperty:
    """Model for sold property data."""
    estate_id: int
    address: str
    zip_code: int
    price: int
    sold_date: datetime
    property_type: Optional[int]
    sale_type: Optional[str]
    sqm_price: Optional[float]
    rooms: Optional[float]
    size: Optional[int]
    build_year: Optional[int]
    change: Optional[float]  # Price change percentage
    latitude: Optional[float]
    longitude: Optional[float]
    city: str
    
    # Metadata fields
    scraped_at: datetime
    updated_at: datetime
    version: int = 1


class DatabaseSchema:
    """Manages database schema creation and updates."""
    
    def __init__(self):
        self.db = db_manager
    
    def create_tables(self):
        """Create all necessary tables."""
        self._create_active_properties_table()
        self._create_sold_properties_table()
        self._create_scraping_metadata_table()
        self._create_indexes()
        logger.info("Database tables created successfully")
    
    def _create_active_properties_table(self):
        """Create table for active properties."""
        sql = """
        CREATE TABLE IF NOT EXISTS active_properties (
            id INTEGER PRIMARY KEY,
            price INTEGER NOT NULL,
            rooms FLOAT,
            size INTEGER,
            lot_size INTEGER,
            build_year INTEGER,
            energy_class VARCHAR(10),
            city VARCHAR(100) NOT NULL,
            zip_code INTEGER NOT NULL,
            street VARCHAR(200) NOT NULL,
            latitude FLOAT,
            longitude FLOAT,
            days_for_sale INTEGER,
            created_date TIMESTAMP,
            property_type INTEGER,
            
            -- Metadata
            scraped_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            version INTEGER NOT NULL DEFAULT 1,
            
            -- Constraints
            CHECK (price > 0),
            CHECK (rooms >= 0),
            CHECK (size > 0),
            CHECK (zip_code BETWEEN 1000 AND 9999),
            CHECK (build_year IS NULL OR build_year BETWEEN 1800 AND 2030),
            CHECK (latitude IS NULL OR latitude BETWEEN -90 AND 90),
            CHECK (longitude IS NULL OR longitude BETWEEN -180 AND 180),
            CHECK (days_for_sale IS NULL OR days_for_sale >= 0)
        )
        """
        self.db.execute_query(sql)
        logger.debug("Created active_properties table")
    
    def _create_sold_properties_table(self):
        """Create table for sold properties."""
        sql = """
        CREATE TABLE IF NOT EXISTS sold_properties (
            estate_id INTEGER PRIMARY KEY,
            address VARCHAR(200) NOT NULL,
            zip_code INTEGER NOT NULL,
            price INTEGER NOT NULL,
            sold_date DATE NOT NULL,
            property_type INTEGER,
            sale_type VARCHAR(50),
            sqm_price FLOAT,
            rooms FLOAT,
            size INTEGER,
            build_year INTEGER,
            change_percent FLOAT,
            latitude FLOAT,
            longitude FLOAT,
            city VARCHAR(100) NOT NULL,
            
            -- Metadata
            scraped_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            version INTEGER NOT NULL DEFAULT 1,
            
            -- Constraints
            CHECK (price > 0),
            CHECK (zip_code BETWEEN 1000 AND 9999),
            CHECK (sqm_price IS NULL OR sqm_price >= 0),
            CHECK (rooms IS NULL OR rooms >= 0),
            CHECK (size IS NULL OR size > 0),
            CHECK (build_year IS NULL OR build_year BETWEEN 1800 AND 2030),
            CHECK (latitude IS NULL OR latitude BETWEEN -90 AND 90),
            CHECK (longitude IS NULL OR longitude BETWEEN -180 AND 180)
        )
        """
        self.db.execute_query(sql)
        logger.debug("Created sold_properties table")
    
    def _create_scraping_metadata_table(self):
        """Create table for tracking scraping runs."""
        sql = """
        CREATE TABLE IF NOT EXISTS scraping_metadata (
            id INTEGER PRIMARY KEY,
            scrape_type VARCHAR(20) NOT NULL, -- 'active' or 'sold'
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP,
            records_processed INTEGER DEFAULT 0,
            records_inserted INTEGER DEFAULT 0,
            records_updated INTEGER DEFAULT 0,
            records_failed INTEGER DEFAULT 0,
            status VARCHAR(20) NOT NULL DEFAULT 'running', -- 'running', 'completed', 'failed'
            error_message TEXT,
            checkpoint_data JSON, -- For resumable operations
            
            -- Additional metadata
            api_calls_made INTEGER DEFAULT 0,
            total_pages INTEGER,
            current_page INTEGER,
            
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
        self.db.execute_query(sql)
        logger.debug("Created scraping_metadata table")
    
    def _create_indexes(self):
        """Create database indexes for better performance."""
        indexes = [
            # Active properties indexes
            "CREATE INDEX IF NOT EXISTS idx_active_zip_code ON active_properties(zip_code)",
            "CREATE INDEX IF NOT EXISTS idx_active_city ON active_properties(city)",
            "CREATE INDEX IF NOT EXISTS idx_active_price ON active_properties(price)",
            "CREATE INDEX IF NOT EXISTS idx_active_size ON active_properties(size)",
            "CREATE INDEX IF NOT EXISTS idx_active_rooms ON active_properties(rooms)",
            "CREATE INDEX IF NOT EXISTS idx_active_scraped_at ON active_properties(scraped_at)",
            "CREATE INDEX IF NOT EXISTS idx_active_location ON active_properties(latitude, longitude)",
            
            # Sold properties indexes
            "CREATE INDEX IF NOT EXISTS idx_sold_zip_code ON sold_properties(zip_code)",
            "CREATE INDEX IF NOT EXISTS idx_sold_city ON sold_properties(city)",
            "CREATE INDEX IF NOT EXISTS idx_sold_price ON sold_properties(price)",
            "CREATE INDEX IF NOT EXISTS idx_sold_date ON sold_properties(sold_date)",
            "CREATE INDEX IF NOT EXISTS idx_sold_size ON sold_properties(size)",
            "CREATE INDEX IF NOT EXISTS idx_sold_scraped_at ON sold_properties(scraped_at)",
            "CREATE INDEX IF NOT EXISTS idx_sold_location ON sold_properties(latitude, longitude)",
            
            # Scraping metadata indexes
            "CREATE INDEX IF NOT EXISTS idx_scraping_type ON scraping_metadata(scrape_type)",
            "CREATE INDEX IF NOT EXISTS idx_scraping_status ON scraping_metadata(status)",
            "CREATE INDEX IF NOT EXISTS idx_scraping_start_time ON scraping_metadata(start_time)",
        ]
        
        for index_sql in indexes:
            self.db.execute_query(index_sql)
        
        logger.debug(f"Created {len(indexes)} database indexes")
    
    def drop_tables(self):
        """Drop all tables (for testing/reset)."""
        tables = ['active_properties', 'sold_properties', 'scraping_metadata']
        for table in tables:
            self.db.execute_query(f"DROP TABLE IF EXISTS {table}")
        logger.info("All tables dropped")
    
    def get_table_info(self, table_name: str) -> List[dict]:
        """Get information about a table's structure."""
        result = self.db.execute_query(f"PRAGMA table_info({table_name})")
        return [dict(row) for row in result] if result else []
    
    def get_table_row_count(self, table_name: str) -> int:
        """Get the number of rows in a table."""
        result = self.db.execute_query(f"SELECT COUNT(*) FROM {table_name}")
        return result[0][0] if result else 0
    
    def vacuum_database(self):
        """Optimize database by running VACUUM."""
        self.db.execute_query("VACUUM")
        logger.info("Database vacuumed")


# Global schema instance
schema = DatabaseSchema() 