"""Database migrations and schema management."""

from datetime import datetime
from typing import List, Dict, Any, Optional, Callable
from pathlib import Path
import json
from enum import Enum

from ..config.database import db_manager
from ..utils.logging import get_logger

logger = get_logger(__name__)


class MigrationStatus(Enum):
    """Migration status enumeration."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


class Migration:
    """Represents a database migration."""
    
    def __init__(self, version: str, name: str, up_sql: str, down_sql: str = None):
        """Initialize migration.
        
        Args:
            version: Migration version (e.g., "001", "002")
            name: Human-readable migration name
            up_sql: SQL to apply the migration
            down_sql: SQL to rollback the migration (optional)
        """
        self.version = version
        self.name = name
        self.up_sql = up_sql
        self.down_sql = down_sql
        self.created_at = datetime.now()
    
    def __str__(self):
        return f"Migration {self.version}: {self.name}"


class MigrationManager:
    """Manages database migrations and schema versioning."""
    
    def __init__(self):
        self.db = db_manager
        self._migrations: List[Migration] = []
        self._ensure_migrations_table()
    
    def _ensure_migrations_table(self):
        """Create migrations tracking table if it doesn't exist."""
        sql = """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            id INTEGER PRIMARY KEY,
            version VARCHAR(10) NOT NULL UNIQUE,
            name VARCHAR(200) NOT NULL,
            status VARCHAR(20) NOT NULL DEFAULT 'pending',
            applied_at TIMESTAMP,
            rolled_back_at TIMESTAMP,
            execution_time_ms INTEGER,
            error_message TEXT,
            checksum VARCHAR(64), -- Hash of migration SQL for integrity checking
            
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        self.db.execute_query(sql)
        logger.debug("Ensured schema_migrations table exists")
    
    def register_migration(self, migration: Migration):
        """Register a migration for execution.
        
        Args:
            migration: Migration object to register
        """
        self._migrations.append(migration)
        logger.debug(f"Registered migration: {migration}")
    
    def add_migration(self, version: str, name: str, up_sql: str, down_sql: str = None):
        """Add a new migration.
        
        Args:
            version: Migration version
            name: Migration name
            up_sql: SQL to apply migration
            down_sql: SQL to rollback migration
        """
        migration = Migration(version, name, up_sql, down_sql)
        self.register_migration(migration)
    
    def get_pending_migrations(self) -> List[Migration]:
        """Get list of pending migrations.
        
        Returns:
            List of migrations that haven't been applied
        """
        applied_versions = self._get_applied_versions()
        pending = []
        
        for migration in sorted(self._migrations, key=lambda m: m.version):
            if migration.version not in applied_versions:
                pending.append(migration)
        
        return pending
    
    def get_migration_status(self) -> Dict[str, Any]:
        """Get current migration status.
        
        Returns:
            Dict with migration status information
        """
        applied_versions = self._get_applied_versions()
        pending_migrations = self.get_pending_migrations()
        
        # Get latest applied migration
        latest_applied = None
        if applied_versions:
            sql = """
            SELECT version, name, applied_at 
            FROM schema_migrations 
            WHERE status = 'completed'
            ORDER BY version DESC 
            LIMIT 1
            """
            result = self.db.execute_query(sql)
            if result:
                latest_applied = {
                    'version': result[0][0],
                    'name': result[0][1],
                    'applied_at': result[0][2]
                }
        
        return {
            'total_migrations': len(self._migrations),
            'applied_count': len(applied_versions),
            'pending_count': len(pending_migrations),
            'latest_applied': latest_applied,
            'pending_migrations': [
                {'version': m.version, 'name': m.name} 
                for m in pending_migrations
            ]
        }
    
    def migrate(self, target_version: Optional[str] = None) -> bool:
        """Apply pending migrations up to target version.
        
        Args:
            target_version: Stop at this version (optional, applies all if None)
            
        Returns:
            bool: True if all migrations successful, False otherwise
        """
        pending_migrations = self.get_pending_migrations()
        
        if not pending_migrations:
            logger.info("No pending migrations to apply")
            return True
        
        # Filter migrations up to target version
        if target_version:
            pending_migrations = [
                m for m in pending_migrations 
                if m.version <= target_version
            ]
        
        success_count = 0
        
        for migration in pending_migrations:
            logger.info(f"Applying migration: {migration}")
            
            if self._apply_migration(migration):
                success_count += 1
            else:
                logger.error(f"Failed to apply migration: {migration}")
                break
        
        logger.info(f"Applied {success_count}/{len(pending_migrations)} migrations")
        return success_count == len(pending_migrations)
    
    def rollback(self, target_version: str) -> bool:
        """Rollback migrations to target version.
        
        Args:
            target_version: Rollback to this version
            
        Returns:
            bool: True if rollback successful, False otherwise
        """
        applied_versions = self._get_applied_versions()
        
        # Find migrations to rollback (in reverse order)
        migrations_to_rollback = []
        for migration in reversed(sorted(self._migrations, key=lambda m: m.version)):
            if migration.version in applied_versions and migration.version > target_version:
                migrations_to_rollback.append(migration)
        
        if not migrations_to_rollback:
            logger.info(f"No migrations to rollback to version {target_version}")
            return True
        
        success_count = 0
        
        for migration in migrations_to_rollback:
            logger.info(f"Rolling back migration: {migration}")
            
            if self._rollback_migration(migration):
                success_count += 1
            else:
                logger.error(f"Failed to rollback migration: {migration}")
                break
        
        logger.info(f"Rolled back {success_count}/{len(migrations_to_rollback)} migrations")
        return success_count == len(migrations_to_rollback)
    
    def _apply_migration(self, migration: Migration) -> bool:
        """Apply a single migration.
        
        Args:
            migration: Migration to apply
            
        Returns:
            bool: True if successful, False otherwise
        """
        start_time = datetime.now()
        
        try:
            # Record migration start
            self._record_migration_start(migration)
            
            # Execute migration SQL
            with self.db.transaction() as conn:
                # Split SQL by semicolons and execute each statement
                statements = [stmt.strip() for stmt in migration.up_sql.split(';') if stmt.strip()]
                for statement in statements:
                    conn.execute(statement)
            
            # Calculate execution time
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            
            # Record successful completion
            self._record_migration_completion(migration, execution_time)
            
            logger.info(f"Successfully applied migration {migration.version}: {migration.name}")
            return True
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Failed to apply migration {migration.version}: {error_msg}")
            
            # Record failure
            self._record_migration_failure(migration, error_msg)
            return False
    
    def _rollback_migration(self, migration: Migration) -> bool:
        """Rollback a single migration.
        
        Args:
            migration: Migration to rollback
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not migration.down_sql:
            logger.error(f"No rollback SQL defined for migration {migration.version}")
            return False
        
        try:
            # Execute rollback SQL
            with self.db.transaction() as conn:
                statements = [stmt.strip() for stmt in migration.down_sql.split(';') if stmt.strip()]
                for statement in statements:
                    conn.execute(statement)
            
            # Update migration record
            sql = """
            UPDATE schema_migrations 
            SET status = 'rolled_back', rolled_back_at = ?, updated_at = ?
            WHERE version = ?
            """
            self.db.execute_query(sql, (datetime.now(), datetime.now(), migration.version))
            
            logger.info(f"Successfully rolled back migration {migration.version}: {migration.name}")
            return True
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Failed to rollback migration {migration.version}: {error_msg}")
            return False
    
    def _record_migration_start(self, migration: Migration):
        """Record migration start in database."""
        checksum = self._calculate_checksum(migration.up_sql)
        
        sql = """
        INSERT OR REPLACE INTO schema_migrations 
        (version, name, status, checksum, created_at, updated_at)
        VALUES (?, ?, 'running', ?, ?, ?)
        """
        
        now = datetime.now()
        self.db.execute_query(sql, (
            migration.version, migration.name, checksum, now, now
        ))
    
    def _record_migration_completion(self, migration: Migration, execution_time_ms: float):
        """Record successful migration completion."""
        sql = """
        UPDATE schema_migrations 
        SET status = 'completed', applied_at = ?, execution_time_ms = ?, updated_at = ?
        WHERE version = ?
        """
        
        now = datetime.now()
        self.db.execute_query(sql, (
            now, int(execution_time_ms), now, migration.version
        ))
    
    def _record_migration_failure(self, migration: Migration, error_message: str):
        """Record migration failure."""
        sql = """
        UPDATE schema_migrations 
        SET status = 'failed', error_message = ?, updated_at = ?
        WHERE version = ?
        """
        
        self.db.execute_query(sql, (error_message, datetime.now(), migration.version))
    
    def _get_applied_versions(self) -> List[str]:
        """Get list of applied migration versions."""
        sql = """
        SELECT version FROM schema_migrations 
        WHERE status = 'completed'
        ORDER BY version
        """
        
        results = self.db.execute_query(sql)
        return [row[0] for row in results] if results else []
    
    def _calculate_checksum(self, sql: str) -> str:
        """Calculate checksum for migration SQL."""
        import hashlib
        return hashlib.sha256(sql.encode()).hexdigest()
    
    def get_migration_history(self) -> List[Dict[str, Any]]:
        """Get complete migration history.
        
        Returns:
            List of migration records with details
        """
        sql = """
        SELECT version, name, status, applied_at, rolled_back_at, 
               execution_time_ms, error_message, created_at
        FROM schema_migrations 
        ORDER BY version
        """
        
        results = self.db.execute_query(sql)
        
        history = []
        for row in results:
            history.append({
                'version': row[0],
                'name': row[1],
                'status': row[2],
                'applied_at': row[3],
                'rolled_back_at': row[4],
                'execution_time_ms': row[5],
                'error_message': row[6],
                'created_at': row[7]
            })
        
        return history


class DefaultMigrations:
    """Default migrations for the boligmarkedet database."""
    
    @staticmethod
    def register_all(migration_manager: MigrationManager):
        """Register all default migrations.
        
        Args:
            migration_manager: MigrationManager instance to register migrations with
        """
        
        # Migration 001: Initial schema
        migration_manager.add_migration(
            version="001",
            name="Create initial property tables",
            up_sql="""
            -- Create active properties table
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
                CHECK (rooms IS NULL OR rooms >= 0),
                CHECK (size IS NULL OR size > 0),
                CHECK (zip_code BETWEEN 1000 AND 9999),
                CHECK (build_year IS NULL OR build_year BETWEEN 1800 AND 2030),
                CHECK (latitude IS NULL OR latitude BETWEEN -90 AND 90),
                CHECK (longitude IS NULL OR longitude BETWEEN -180 AND 180),
                CHECK (days_for_sale IS NULL OR days_for_sale >= 0)
            );
            
            -- Create sold properties table
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
            );
            """,
            down_sql="""
            DROP TABLE IF EXISTS sold_properties;
            DROP TABLE IF EXISTS active_properties;
            """
        )
        
        # Migration 002: Create indexes
        migration_manager.add_migration(
            version="002",
            name="Create database indexes",
            up_sql="""
            -- Active properties indexes
            CREATE INDEX IF NOT EXISTS idx_active_zip_code ON active_properties(zip_code);
            CREATE INDEX IF NOT EXISTS idx_active_city ON active_properties(city);
            CREATE INDEX IF NOT EXISTS idx_active_price ON active_properties(price);
            CREATE INDEX IF NOT EXISTS idx_active_size ON active_properties(size);
            CREATE INDEX IF NOT EXISTS idx_active_build_year ON active_properties(build_year);
            CREATE INDEX IF NOT EXISTS idx_active_property_type ON active_properties(property_type);
            CREATE INDEX IF NOT EXISTS idx_active_scraped_at ON active_properties(scraped_at);
            CREATE INDEX IF NOT EXISTS idx_active_location ON active_properties(latitude, longitude);
            
            -- Sold properties indexes
            CREATE INDEX IF NOT EXISTS idx_sold_zip_code ON sold_properties(zip_code);
            CREATE INDEX IF NOT EXISTS idx_sold_city ON sold_properties(city);
            CREATE INDEX IF NOT EXISTS idx_sold_price ON sold_properties(price);
            CREATE INDEX IF NOT EXISTS idx_sold_size ON sold_properties(size);
            CREATE INDEX IF NOT EXISTS idx_sold_build_year ON sold_properties(build_year);
            CREATE INDEX IF NOT EXISTS idx_sold_property_type ON sold_properties(property_type);
            CREATE INDEX IF NOT EXISTS idx_sold_date ON sold_properties(sold_date);
            CREATE INDEX IF NOT EXISTS idx_sold_scraped_at ON sold_properties(scraped_at);
            CREATE INDEX IF NOT EXISTS idx_sold_location ON sold_properties(latitude, longitude);
            """,
            down_sql="""
            -- Drop sold properties indexes
            DROP INDEX IF EXISTS idx_sold_location;
            DROP INDEX IF EXISTS idx_sold_scraped_at;
            DROP INDEX IF EXISTS idx_sold_date;
            DROP INDEX IF EXISTS idx_sold_property_type;
            DROP INDEX IF EXISTS idx_sold_build_year;
            DROP INDEX IF EXISTS idx_sold_size;
            DROP INDEX IF EXISTS idx_sold_price;
            DROP INDEX IF EXISTS idx_sold_city;
            DROP INDEX IF EXISTS idx_sold_zip_code;
            
            -- Drop active properties indexes
            DROP INDEX IF EXISTS idx_active_location;
            DROP INDEX IF EXISTS idx_active_scraped_at;
            DROP INDEX IF EXISTS idx_active_property_type;
            DROP INDEX IF EXISTS idx_active_build_year;
            DROP INDEX IF EXISTS idx_active_size;
            DROP INDEX IF EXISTS idx_active_price;
            DROP INDEX IF EXISTS idx_active_city;
            DROP INDEX IF EXISTS idx_active_zip_code;
            """
        )
        
        # Migration 003: Create scraping metadata table
        migration_manager.add_migration(
            version="003",
            name="Create scraping metadata table",
            up_sql="""
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
            );
            
            -- Create index for scraping metadata
            CREATE INDEX IF NOT EXISTS idx_scraping_type_status 
            ON scraping_metadata(scrape_type, status);
            CREATE INDEX IF NOT EXISTS idx_scraping_start_time 
            ON scraping_metadata(start_time);
            """,
            down_sql="""
            DROP INDEX IF EXISTS idx_scraping_start_time;
            DROP INDEX IF EXISTS idx_scraping_type_status;
            DROP TABLE IF EXISTS scraping_metadata;
            """
        )


# Global migration manager instance
migration_manager = MigrationManager()

# Register default migrations
DefaultMigrations.register_all(migration_manager) 