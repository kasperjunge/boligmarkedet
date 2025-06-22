"""Database module for boligmarkedet."""

from .models import (
    ActiveProperty,
    SoldProperty,
    PropertyType,
    DatabaseSchema
)

from .operations import (
    PropertyOperations,
    ScrapingOperations,
    DataManagement,
    property_ops,
    scraping_ops,
    data_mgmt
)

from .migrations import (
    Migration,
    MigrationManager,
    MigrationStatus,
    migration_manager
)

__all__ = [
    # Models
    "ActiveProperty",
    "SoldProperty", 
    "PropertyType",
    "DatabaseSchema",
    
    # Operations
    "PropertyOperations",
    "ScrapingOperations", 
    "DataManagement",
    "property_ops",
    "scraping_ops",
    "data_mgmt",
    
    # Migrations
    "Migration",
    "MigrationManager",
    "MigrationStatus", 
    "migration_manager"
] 