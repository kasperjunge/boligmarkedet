"""Database operations for property data management."""

from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple, Union
from dataclasses import asdict
import json
from contextlib import contextmanager

from .models import ActiveProperty, SoldProperty, DatabaseSchema
from ..config.database import db_manager, get_db_connection
from ..utils.logging import get_logger
from ..utils.validators import PropertyValidator, ValidationResult

logger = get_logger(__name__)


class PropertyOperations:
    """Handles CRUD operations for property data."""
    
    def __init__(self):
        self.db = db_manager
        self.validator = PropertyValidator()
    
    # =============================================================================
    # ACTIVE PROPERTIES OPERATIONS
    # =============================================================================
    
    def insert_active_property(self, property_data: Union[ActiveProperty, Dict[str, Any]]) -> bool:
        """Insert a single active property.
        
        Args:
            property_data: Property data as ActiveProperty object or dict
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if isinstance(property_data, dict):
                # Convert dict to ActiveProperty
                property_data = self._dict_to_active_property(property_data)
            
            sql = """
            INSERT INTO active_properties (
                id, price, rooms, size, lot_size, build_year, energy_class,
                city, zip_code, street, latitude, longitude, days_for_sale,
                created_date, property_type, scraped_at, updated_at, version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            params = (
                property_data.id, property_data.price, property_data.rooms,
                property_data.size, property_data.lot_size, property_data.build_year,
                property_data.energy_class, property_data.city, property_data.zip_code,
                property_data.street, property_data.latitude, property_data.longitude,
                property_data.days_for_sale, property_data.created_date,
                property_data.property_type, property_data.scraped_at,
                property_data.updated_at, property_data.version
            )
            
            with self.db.transaction() as conn:
                conn.execute(sql, params)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert active property {property_data.id}: {e}")
            return False
    
    def bulk_insert_active_properties(self, properties: List[Union[ActiveProperty, Dict[str, Any]]], 
                                    batch_size: int = 1000) -> Dict[str, int]:
        """Bulk insert active properties with batching.
        
        Args:
            properties: List of property data
            batch_size: Number of properties to insert per batch
            
        Returns:
            Dict with statistics: inserted, failed, total
        """
        stats = {"inserted": 0, "failed": 0, "total": len(properties)}
        
        # Convert dicts to ActiveProperty objects
        processed_properties = []
        for prop in properties:
            if isinstance(prop, dict):
                try:
                    processed_properties.append(self._dict_to_active_property(prop))
                except Exception as e:
                    logger.warning(f"Failed to process property data: {e}")
                    stats["failed"] += 1
                    continue
            else:
                processed_properties.append(prop)
        
        # Batch insert
        sql = """
        INSERT INTO active_properties (
            id, price, rooms, size, lot_size, build_year, energy_class,
            city, zip_code, street, latitude, longitude, days_for_sale,
            created_date, property_type, scraped_at, updated_at, version
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        for i in range(0, len(processed_properties), batch_size):
            batch = processed_properties[i:i + batch_size]
            batch_params = []
            
            for prop in batch:
                batch_params.append((
                    prop.id, prop.price, prop.rooms, prop.size, prop.lot_size,
                    prop.build_year, prop.energy_class, prop.city, prop.zip_code,
                    prop.street, prop.latitude, prop.longitude, prop.days_for_sale,
                    prop.created_date, prop.property_type, prop.scraped_at,
                    prop.updated_at, prop.version
                ))
            
            try:
                with self.db.transaction() as conn:
                    conn.executemany(sql, batch_params)
                stats["inserted"] += len(batch)
                logger.info(f"Inserted batch of {len(batch)} active properties")
                
            except Exception as e:
                logger.error(f"Failed to insert batch: {e}")
                stats["failed"] += len(batch)
        
        return stats
    
    def bulk_upsert_active_properties(self, properties: List[Union[ActiveProperty, Dict[str, Any]]], 
                                    batch_size: int = 1000) -> Dict[str, int]:
        """Bulk upsert active properties with batching.
        
        Args:
            properties: List of property data
            batch_size: Number of properties to upsert per batch
            
        Returns:
            Dict with statistics: inserted, updated, failed, total
        """
        stats = {"inserted": 0, "updated": 0, "failed": 0, "total": len(properties)}
        
        # Process in batches to avoid memory issues
        for i in range(0, len(properties), batch_size):
            batch = properties[i:i + batch_size]
            
            for prop in batch:
                try:
                    if isinstance(prop, dict):
                        prop_obj = self._dict_to_active_property(prop)
                    else:
                        prop_obj = prop
                    
                    # Check if property exists
                    existing = self.get_active_property(prop_obj.id)
                    
                    if existing:
                        # Update existing property with version increment
                        prop_obj.version = existing.version + 1
                        prop_obj.updated_at = datetime.now()
                        if self.update_active_property(prop_obj):
                            stats["updated"] += 1
                        else:
                            stats["failed"] += 1
                    else:
                        # Insert new property
                        if self.insert_active_property(prop_obj):
                            stats["inserted"] += 1
                        else:
                            stats["failed"] += 1
                            
                except Exception as e:
                    logger.error(f"Failed to upsert active property: {e}")
                    stats["failed"] += 1
            
            # Log progress every batch
            logger.info(f"Processed batch: {i + len(batch)}/{len(properties)} active properties")
        
        return stats
    
    def upsert_active_property(self, property_data: Union[ActiveProperty, Dict[str, Any]]) -> bool:
        """Insert or update an active property (upsert operation).
        
        Args:
            property_data: Property data as ActiveProperty object or dict
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if isinstance(property_data, dict):
                property_data = self._dict_to_active_property(property_data)
            
            # Check if property exists
            existing = self.get_active_property(property_data.id)
            
            if existing:
                # Update existing property with version increment
                property_data.version = existing.version + 1
                property_data.updated_at = datetime.now()
                return self.update_active_property(property_data)
            else:
                # Insert new property
                return self.insert_active_property(property_data)
                
        except Exception as e:
            logger.error(f"Failed to upsert active property {property_data.id}: {e}")
            return False
    
    def update_active_property(self, property_data: Union[ActiveProperty, Dict[str, Any]]) -> bool:
        """Update an existing active property.
        
        Args:
            property_data: Property data as ActiveProperty object or dict
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if isinstance(property_data, dict):
                property_data = self._dict_to_active_property(property_data)
            
            sql = """
            UPDATE active_properties SET
                price = ?, rooms = ?, size = ?, lot_size = ?, build_year = ?,
                energy_class = ?, city = ?, zip_code = ?, street = ?,
                latitude = ?, longitude = ?, days_for_sale = ?, created_date = ?,
                property_type = ?, updated_at = ?, version = ?
            WHERE id = ?
            """
            
            params = (
                property_data.price, property_data.rooms, property_data.size,
                property_data.lot_size, property_data.build_year, property_data.energy_class,
                property_data.city, property_data.zip_code, property_data.street,
                property_data.latitude, property_data.longitude, property_data.days_for_sale,
                property_data.created_date, property_data.property_type,
                property_data.updated_at, property_data.version,
                property_data.id
            )
            
            with self.db.transaction() as conn:
                result = conn.execute(sql, params)
                if result.rowcount == 0:
                    logger.warning(f"No active property found with id {property_data.id}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to update active property {property_data.id}: {e}")
            return False
    
    def get_active_property(self, property_id: int) -> Optional[ActiveProperty]:
        """Get an active property by ID.
        
        Args:
            property_id: Property ID to retrieve
            
        Returns:
            ActiveProperty object or None if not found
        """
        try:
            sql = "SELECT * FROM active_properties WHERE id = ?"
            result = self.db.execute_query(sql, (property_id,))
            
            if result:
                return self._row_to_active_property(result[0])
            return None
            
        except Exception as e:
            logger.error(f"Failed to get active property {property_id}: {e}")
            return None
    
    def delete_active_property(self, property_id: int) -> bool:
        """Delete an active property by ID.
        
        Args:
            property_id: Property ID to delete
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            sql = "DELETE FROM active_properties WHERE id = ?"
            
            with self.db.transaction() as conn:
                result = conn.execute(sql, (property_id,))
                if result.rowcount == 0:
                    logger.warning(f"No active property found with id {property_id}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete active property {property_id}: {e}")
            return False
    
    # =============================================================================
    # SOLD PROPERTIES OPERATIONS
    # =============================================================================
    
    def insert_sold_property(self, property_data: Union[SoldProperty, Dict[str, Any]]) -> bool:
        """Insert a single sold property.
        
        Args:
            property_data: Property data as SoldProperty object or dict
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if isinstance(property_data, dict):
                property_data = self._dict_to_sold_property(property_data)
            
            sql = """
            INSERT INTO sold_properties (
                estate_id, address, zip_code, price, sold_date, property_type,
                sale_type, sqm_price, rooms, size, build_year, change_percent,
                latitude, longitude, city, scraped_at, updated_at, version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            
            params = (
                property_data.estate_id, property_data.address, property_data.zip_code,
                property_data.price, property_data.sold_date, property_data.property_type,
                property_data.sale_type, property_data.sqm_price, property_data.rooms,
                property_data.size, property_data.build_year, property_data.change,
                property_data.latitude, property_data.longitude, property_data.city,
                property_data.scraped_at, property_data.updated_at, property_data.version
            )
            
            with self.db.transaction() as conn:
                conn.execute(sql, params)
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert sold property {property_data.estate_id}: {e}")
            return False
    
    def bulk_insert_sold_properties(self, properties: List[Union[SoldProperty, Dict[str, Any]]], 
                                  batch_size: int = 1000) -> Dict[str, int]:
        """Bulk insert sold properties with batching.
        
        Args:
            properties: List of property data
            batch_size: Number of properties to insert per batch
            
        Returns:
            Dict with statistics: inserted, failed, total
        """
        stats = {"inserted": 0, "failed": 0, "total": len(properties)}
        
        # Convert dicts to SoldProperty objects
        processed_properties = []
        for prop in properties:
            if isinstance(prop, dict):
                try:
                    processed_properties.append(self._dict_to_sold_property(prop))
                except Exception as e:
                    logger.warning(f"Failed to process property data: {e}")
                    stats["failed"] += 1
                    continue
            else:
                processed_properties.append(prop)
        
        # Batch insert
        sql = """
        INSERT INTO sold_properties (
            estate_id, address, zip_code, price, sold_date, property_type,
            sale_type, sqm_price, rooms, size, build_year, change_percent,
            latitude, longitude, city, scraped_at, updated_at, version
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        for i in range(0, len(processed_properties), batch_size):
            batch = processed_properties[i:i + batch_size]
            batch_params = []
            
            for prop in batch:
                batch_params.append((
                    prop.estate_id, prop.address, prop.zip_code, prop.price,
                    prop.sold_date, prop.property_type, prop.sale_type,
                    prop.sqm_price, prop.rooms, prop.size, prop.build_year,
                    prop.change, prop.latitude, prop.longitude, prop.city,
                    prop.scraped_at, prop.updated_at, prop.version
                ))
            
            try:
                with self.db.transaction() as conn:
                    conn.executemany(sql, batch_params)
                stats["inserted"] += len(batch)
                logger.info(f"Inserted batch of {len(batch)} sold properties")
                
            except Exception as e:
                logger.error(f"Failed to insert batch: {e}")
                stats["failed"] += len(batch)
        
        return stats
    
    def bulk_upsert_sold_properties(self, properties: List[Union[SoldProperty, Dict[str, Any]]], 
                                  batch_size: int = 1000) -> Dict[str, int]:
        """Bulk upsert sold properties with batching.
        
        Args:
            properties: List of property data
            batch_size: Number of properties to upsert per batch
            
        Returns:
            Dict with statistics: inserted, updated, failed, total
        """
        stats = {"inserted": 0, "updated": 0, "failed": 0, "total": len(properties)}
        
        # Process in batches to avoid memory issues
        for i in range(0, len(properties), batch_size):
            batch = properties[i:i + batch_size]
            
            for prop in batch:
                try:
                    if isinstance(prop, dict):
                        prop_obj = self._dict_to_sold_property(prop)
                    else:
                        prop_obj = prop
                    
                    # Check if property exists
                    existing = self.get_sold_property(prop_obj.estate_id)
                    
                    if existing:
                        # Update existing property with version increment
                        prop_obj.version = existing.version + 1
                        prop_obj.updated_at = datetime.now()
                        if self.update_sold_property(prop_obj):
                            stats["updated"] += 1
                        else:
                            stats["failed"] += 1
                    else:
                        # Insert new property
                        if self.insert_sold_property(prop_obj):
                            stats["inserted"] += 1
                        else:
                            stats["failed"] += 1
                            
                except Exception as e:
                    logger.error(f"Failed to upsert sold property: {e}")
                    stats["failed"] += 1
            
            # Log progress every batch
            logger.info(f"Processed batch: {i + len(batch)}/{len(properties)} sold properties")
        
        return stats
    
    def upsert_sold_property(self, property_data: Union[SoldProperty, Dict[str, Any]]) -> bool:
        """Insert or update a sold property (upsert operation).
        
        Args:
            property_data: Property data as SoldProperty object or dict
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if isinstance(property_data, dict):
                property_data = self._dict_to_sold_property(property_data)
            
            # Check if property exists
            existing = self.get_sold_property(property_data.estate_id)
            
            if existing:
                # Update existing property with version increment
                property_data.version = existing.version + 1
                property_data.updated_at = datetime.now()
                return self.update_sold_property(property_data)
            else:
                # Insert new property
                return self.insert_sold_property(property_data)
                
        except Exception as e:
            logger.error(f"Failed to upsert sold property {property_data.estate_id}: {e}")
            return False
    
    def update_sold_property(self, property_data: Union[SoldProperty, Dict[str, Any]]) -> bool:
        """Update an existing sold property.
        
        Args:
            property_data: Property data as SoldProperty object or dict
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if isinstance(property_data, dict):
                property_data = self._dict_to_sold_property(property_data)
            
            sql = """
            UPDATE sold_properties SET
                address = ?, zip_code = ?, price = ?, sold_date = ?, property_type = ?,
                sale_type = ?, sqm_price = ?, rooms = ?, size = ?, build_year = ?,
                change_percent = ?, latitude = ?, longitude = ?, city = ?,
                updated_at = ?, version = ?
            WHERE estate_id = ?
            """
            
            params = (
                property_data.address, property_data.zip_code, property_data.price,
                property_data.sold_date, property_data.property_type, property_data.sale_type,
                property_data.sqm_price, property_data.rooms, property_data.size,
                property_data.build_year, property_data.change, property_data.latitude,
                property_data.longitude, property_data.city, property_data.updated_at,
                property_data.version, property_data.estate_id
            )
            
            with self.db.transaction() as conn:
                result = conn.execute(sql, params)
                if result.rowcount == 0:
                    logger.warning(f"No sold property found with id {property_data.estate_id}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to update sold property {property_data.estate_id}: {e}")
            return False
    
    def get_sold_property(self, estate_id: int) -> Optional[SoldProperty]:
        """Get a sold property by estate ID.
        
        Args:
            estate_id: Estate ID to retrieve
            
        Returns:
            SoldProperty object or None if not found
        """
        try:
            sql = "SELECT * FROM sold_properties WHERE estate_id = ?"
            result = self.db.execute_query(sql, (estate_id,))
            
            if result:
                return self._row_to_sold_property(result[0])
            return None
            
        except Exception as e:
            logger.error(f"Failed to get sold property {estate_id}: {e}")
            return None
    
    def delete_sold_property(self, estate_id: int) -> bool:
        """Delete a sold property by estate ID.
        
        Args:
            estate_id: Estate ID to delete
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            sql = "DELETE FROM sold_properties WHERE estate_id = ?"
            
            with self.db.transaction() as conn:
                result = conn.execute(sql, (estate_id,))
                if result.rowcount == 0:
                    logger.warning(f"No sold property found with id {estate_id}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete sold property {estate_id}: {e}")
            return False
    
    # =============================================================================
    # UTILITY METHODS
    # =============================================================================
    
    def _dict_to_active_property(self, data: Dict[str, Any]) -> ActiveProperty:
        """Convert dictionary to ActiveProperty object."""
        now = datetime.now()
        
        return ActiveProperty(
            id=data['id'],
            price=data['price'],
            rooms=data.get('rooms'),
            size=data['size'],
            lot_size=data.get('lot_size'),
            build_year=data.get('build_year'),
            energy_class=data.get('energy_class'),
            city=data['city'],
            zip_code=data['zip_code'],
            street=data['street'],
            latitude=data.get('latitude'),
            longitude=data.get('longitude'),
            days_for_sale=data.get('days_for_sale'),
            created_date=data.get('created_date'),
            property_type=data.get('property_type'),
            scraped_at=data.get('scraped_at', now),
            updated_at=data.get('updated_at', now),
            version=data.get('version', 1)
        )
    
    def _dict_to_sold_property(self, data: Dict[str, Any]) -> SoldProperty:
        """Convert dictionary to SoldProperty object."""
        now = datetime.now()
        
        return SoldProperty(
            estate_id=data['estate_id'],
            address=data['address'],
            zip_code=data['zip_code'],
            price=data['price'],
            sold_date=data['sold_date'],
            property_type=data.get('property_type'),
            sale_type=data.get('sale_type'),
            sqm_price=data.get('sqm_price'),
            rooms=data.get('rooms'),
            size=data.get('size'),
            build_year=data.get('build_year'),
            change=data.get('change'),
            latitude=data.get('latitude'),
            longitude=data.get('longitude'),
            city=data['city'],
            scraped_at=data.get('scraped_at', now),
            updated_at=data.get('updated_at', now),
            version=data.get('version', 1)
        )
    
    def _row_to_active_property(self, row: Tuple) -> ActiveProperty:
        """Convert database row to ActiveProperty object."""
        return ActiveProperty(
            id=row[0], price=row[1], rooms=row[2], size=row[3], lot_size=row[4],
            build_year=row[5], energy_class=row[6], city=row[7], zip_code=row[8],
            street=row[9], latitude=row[10], longitude=row[11], days_for_sale=row[12],
            created_date=row[13], property_type=row[14], scraped_at=row[15],
            updated_at=row[16], version=row[17]
        )
    
    def _row_to_sold_property(self, row: Tuple) -> SoldProperty:
        """Convert database row to SoldProperty object."""
        return SoldProperty(
            estate_id=row[0], address=row[1], zip_code=row[2], price=row[3],
            sold_date=row[4], property_type=row[5], sale_type=row[6],
            sqm_price=row[7], rooms=row[8], size=row[9], build_year=row[10],
            change=row[11], latitude=row[12], longitude=row[13], city=row[14],
            scraped_at=row[15], updated_at=row[16], version=row[17]
        )


class ScrapingOperations:
    """Handles scraping metadata and checkpoint/resume functionality."""
    
    def __init__(self):
        self.db = db_manager
    
    def start_scraping_session(self, scrape_type: str, total_pages: Optional[int] = None) -> int:
        """Start a new scraping session and return the session ID.
        
        Args:
            scrape_type: Type of scraping ('active' or 'sold')
            total_pages: Total number of pages to process
            
        Returns:
            int: Session ID for tracking progress
        """
        sql = """
        INSERT INTO scraping_metadata (
            scrape_type, start_time, total_pages, current_page, status
        ) VALUES (?, ?, ?, ?, ?)
        """
        
        params = (scrape_type, datetime.now(), total_pages, 0, 'running')
        
        with self.db.transaction() as conn:
            conn.execute(sql, params)
            # Get the last inserted row ID
            result = conn.execute("SELECT last_insert_rowid()").fetchone()
            session_id = result[0]
        
        logger.info(f"Started scraping session {session_id} for {scrape_type} properties")
        return session_id
    
    def update_scraping_progress(self, session_id: int, current_page: int, 
                               records_processed: int = 0, records_inserted: int = 0,
                               records_updated: int = 0, records_failed: int = 0,
                               api_calls_made: int = 0, checkpoint_data: Optional[Dict] = None):
        """Update scraping session progress.
        
        Args:
            session_id: Session ID to update
            current_page: Current page being processed
            records_processed: Total records processed
            records_inserted: Total records inserted
            records_updated: Total records updated
            records_failed: Total records failed
            api_calls_made: Total API calls made
            checkpoint_data: Additional checkpoint data as dict
        """
        sql = """
        UPDATE scraping_metadata SET
            current_page = ?,
            records_processed = records_processed + ?,
            records_inserted = records_inserted + ?,
            records_updated = records_updated + ?,
            records_failed = records_failed + ?,
            api_calls_made = api_calls_made + ?,
            checkpoint_data = ?,
            updated_at = ?
        WHERE id = ?
        """
        
        checkpoint_json = json.dumps(checkpoint_data) if checkpoint_data else None
        params = (
            current_page, records_processed, records_inserted, records_updated,
            records_failed, api_calls_made, checkpoint_json, datetime.now(), session_id
        )
        
        with self.db.transaction() as conn:
            conn.execute(sql, params)
    
    def complete_scraping_session(self, session_id: int, status: str = 'completed', 
                                error_message: Optional[str] = None):
        """Mark a scraping session as completed or failed.
        
        Args:
            session_id: Session ID to complete
            status: Final status ('completed' or 'failed')
            error_message: Error message if failed
        """
        sql = """
        UPDATE scraping_metadata SET
            end_time = ?,
            status = ?,
            error_message = ?,
            updated_at = ?
        WHERE id = ?
        """
        
        params = (datetime.now(), status, error_message, datetime.now(), session_id)
        
        with self.db.transaction() as conn:
            conn.execute(sql, params)
        
        logger.info(f"Scraping session {session_id} completed with status: {status}")
    
    def get_last_checkpoint(self, scrape_type: str) -> Optional[Dict[str, Any]]:
        """Get the last checkpoint for a scraping type.
        
        Args:
            scrape_type: Type of scraping ('active' or 'sold')
            
        Returns:
            Dict with checkpoint data or None if no checkpoint found
        """
        sql = """
        SELECT * FROM scraping_metadata 
        WHERE scrape_type = ? AND status = 'running'
        ORDER BY start_time DESC 
        LIMIT 1
        """
        
        result = self.db.execute_query(sql, (scrape_type,))
        
        if result:
            row = result[0]
            checkpoint_data = json.loads(row[9]) if row[9] else {}
            
            return {
                'session_id': row[0],
                'scrape_type': row[1],
                'start_time': row[2],
                'current_page': row[12],
                'total_pages': row[11],
                'records_processed': row[3],
                'records_inserted': row[4],
                'records_updated': row[5],
                'records_failed': row[6],
                'checkpoint_data': checkpoint_data
            }
        
        return None
    
    def get_scraping_statistics(self, scrape_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """Get scraping statistics for all sessions or specific type.
        
        Args:
            scrape_type: Type of scraping to filter by (optional)
            
        Returns:
            List of dictionaries with scraping statistics
        """
        if scrape_type:
            sql = """
            SELECT scrape_type, status, COUNT(*) as session_count,
                   SUM(records_processed) as total_processed,
                   SUM(records_inserted) as total_inserted,
                   SUM(records_updated) as total_updated,
                   SUM(records_failed) as total_failed,
                   SUM(api_calls_made) as total_api_calls,
                   AVG(strftime('%s', end_time) - strftime('%s', start_time)) as avg_duration_seconds
            FROM scraping_metadata 
            WHERE scrape_type = ?
            GROUP BY scrape_type, status
            ORDER BY scrape_type, status
            """
            params = (scrape_type,)
        else:
            sql = """
            SELECT scrape_type, status, COUNT(*) as session_count,
                   SUM(records_processed) as total_processed,
                   SUM(records_inserted) as total_inserted,
                   SUM(records_updated) as total_updated,
                   SUM(records_failed) as total_failed,
                   SUM(api_calls_made) as total_api_calls,
                   AVG(strftime('%s', end_time) - strftime('%s', start_time)) as avg_duration_seconds
            FROM scraping_metadata 
            GROUP BY scrape_type, status
            ORDER BY scrape_type, status
            """
            params = None
        
        results = self.db.execute_query(sql, params)
        
        stats = []
        for row in results:
            stats.append({
                'scrape_type': row[0],
                'status': row[1],
                'session_count': row[2],
                'total_processed': row[3] or 0,
                'total_inserted': row[4] or 0,
                'total_updated': row[5] or 0,
                'total_failed': row[6] or 0,
                'total_api_calls': row[7] or 0,
                'avg_duration_seconds': row[8] or 0
            })
        
        return stats


class DataManagement:
    """Handles data deduplication and cleanup operations."""
    
    def __init__(self):
        self.db = db_manager
    
    def deduplicate_active_properties(self) -> Dict[str, int]:
        """Remove duplicate active properties, keeping the latest version.
        
        Returns:
            Dict with statistics: duplicates_found, duplicates_removed
        """
        # Find duplicates
        sql_find_duplicates = """
        SELECT id, COUNT(*) as count, MAX(version) as max_version
        FROM active_properties 
        GROUP BY id 
        HAVING COUNT(*) > 1
        """
        
        duplicates = self.db.execute_query(sql_find_duplicates)
        
        if not duplicates:
            return {"duplicates_found": 0, "duplicates_removed": 0}
        
        total_removed = 0
        
        for duplicate in duplicates:
            property_id, count, max_version = duplicate
            
            # Remove all versions except the latest
            sql_remove = """
            DELETE FROM active_properties 
            WHERE id = ? AND version < ?
            """
            
            with self.db.transaction() as conn:
                result = conn.execute(sql_remove, (property_id, max_version))
                removed = result.rowcount
                total_removed += removed
                
                logger.info(f"Removed {removed} duplicate versions for property {property_id}")
        
        return {
            "duplicates_found": len(duplicates),
            "duplicates_removed": total_removed
        }
    
    def deduplicate_sold_properties(self) -> Dict[str, int]:
        """Remove duplicate sold properties, keeping the latest version.
        
        Returns:
            Dict with statistics: duplicates_found, duplicates_removed
        """
        # Find duplicates
        sql_find_duplicates = """
        SELECT estate_id, COUNT(*) as count, MAX(version) as max_version
        FROM sold_properties 
        GROUP BY estate_id 
        HAVING COUNT(*) > 1
        """
        
        duplicates = self.db.execute_query(sql_find_duplicates)
        
        if not duplicates:
            return {"duplicates_found": 0, "duplicates_removed": 0}
        
        total_removed = 0
        
        for duplicate in duplicates:
            estate_id, count, max_version = duplicate
            
            # Remove all versions except the latest
            sql_remove = """
            DELETE FROM sold_properties 
            WHERE estate_id = ? AND version < ?
            """
            
            with self.db.transaction() as conn:
                result = conn.execute(sql_remove, (estate_id, max_version))
                removed = result.rowcount
                total_removed += removed
                
                logger.info(f"Removed {removed} duplicate versions for property {estate_id}")
        
        return {
            "duplicates_found": len(duplicates),
            "duplicates_removed": total_removed
        }
    
    def cleanup_old_scraping_sessions(self, days_old: int = 30) -> int:
        """Clean up old completed scraping sessions.
        
        Args:
            days_old: Remove sessions older than this many days
            
        Returns:
            Number of sessions removed
        """
        sql = """
        DELETE FROM scraping_metadata 
        WHERE status IN ('completed', 'failed') 
        AND start_time < datetime('now', '-{} days')
        """.format(days_old)
        
        with self.db.transaction() as conn:
            result = conn.execute(sql)
            removed = result.rowcount
        
        logger.info(f"Cleaned up {removed} old scraping sessions")
        return removed
    
    def get_data_statistics(self) -> Dict[str, Any]:
        """Get comprehensive data statistics.
        
        Returns:
            Dict with various statistics about the data
        """
        stats = {}
        
        # Active properties stats
        active_stats = self.db.execute_query("""
        SELECT 
            COUNT(*) as total_count,
            MIN(price) as min_price,
            MAX(price) as max_price,
            AVG(price) as avg_price,
            MIN(scraped_at) as first_scraped,
            MAX(scraped_at) as last_scraped
        FROM active_properties
        """)[0]
        
        stats['active_properties'] = {
            'total_count': active_stats[0],
            'min_price': active_stats[1],
            'max_price': active_stats[2],
            'avg_price': active_stats[3],
            'first_scraped': active_stats[4],
            'last_scraped': active_stats[5]
        }
        
        # Sold properties stats
        sold_stats = self.db.execute_query("""
        SELECT 
            COUNT(*) as total_count,
            MIN(price) as min_price,
            MAX(price) as max_price,
            AVG(price) as avg_price,
            MIN(scraped_at) as first_scraped,
            MAX(scraped_at) as last_scraped
        FROM sold_properties
        """)[0]
        
        stats['sold_properties'] = {
            'total_count': sold_stats[0],
            'min_price': sold_stats[1],
            'max_price': sold_stats[2],
            'avg_price': sold_stats[3],
            'first_scraped': sold_stats[4],
            'last_scraped': sold_stats[5]
        }
        
        # Database size info
        db_stats = self.db.execute_query("""
        SELECT 
            page_count * page_size as database_size_bytes,
            page_count,
            page_size
        FROM pragma_database_size(), pragma_page_size()
        """)[0]
        
        stats['database'] = {
            'size_bytes': db_stats[0],
            'size_mb': round(db_stats[0] / (1024 * 1024), 2),
            'page_count': db_stats[1],
            'page_size': db_stats[2]
        }
        
        return stats


# Global instances for easy access
property_ops = PropertyOperations()
scraping_ops = ScrapingOperations()
data_mgmt = DataManagement() 