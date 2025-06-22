"""Database connection management for DuckDB."""

import duckdb
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Optional

from .settings import settings


class DatabaseManager:
    """Manages DuckDB database connections and setup."""
    
    def __init__(self, db_path: Optional[str] = None):
        """Initialize database manager.
        
        Args:
            db_path: Path to database file. If None, uses settings.
        """
        self.db_path = db_path or settings.database.path
        self._connection: Optional[duckdb.DuckDBPyConnection] = None
        
        # Ensure database directory exists
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
    
    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create database connection."""
        if self._connection is None:
            self._connection = duckdb.connect(self.db_path)
            self._setup_database()
        return self._connection
    
    def _setup_database(self):
        """Set up database with initial configuration."""
        conn = self.get_connection()
        
        # Configure DuckDB for better performance
        conn.execute("PRAGMA memory_limit='2GB'")
        conn.execute("PRAGMA threads=4")
        
    def close(self):
        """Close database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None
    
    @contextmanager
    def transaction(self) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """Context manager for database transactions."""
        conn = self.get_connection()
        try:
            conn.begin()
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
    
    def execute_query(self, query: str, parameters: Optional[tuple] = None):
        """Execute a query and return results."""
        conn = self.get_connection()
        if parameters:
            return conn.execute(query, parameters).fetchall()
        return conn.execute(query).fetchall()
    
    def execute_many(self, query: str, parameters_list: list):
        """Execute a query with multiple parameter sets."""
        conn = self.get_connection()
        return conn.executemany(query, parameters_list)
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


# Global database manager instance
db_manager = DatabaseManager()


@contextmanager
def get_db_connection():
    """Context manager for getting database connections."""
    try:
        yield db_manager.get_connection()
    finally:
        pass  # Connection is managed by the database manager 