"""Configuration management for Boligmarkedet project."""

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class DatabaseConfig:
    """Database configuration settings."""
    path: str = "data/boligmarkedet.duckdb"
    connection_timeout: int = 30
    max_connections: int = 5


@dataclass
class APIConfig:
    """API configuration settings."""
    base_url: str = "https://api.boliga.dk"
    timeout: int = 30
    max_retries: int = 3
    rate_limit_delay: float = 1.0
    headers: dict = None
    
    def __post_init__(self):
        if self.headers is None:
            self.headers = {
                "Accept": "application/json, text/plain, */*",
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
            }


@dataclass
class ScrapingConfig:
    """Scraping configuration settings."""
    active_properties_page_size: int = 50
    sold_properties_page_size: int = 50
    checkpoint_interval: int = 1000  # Records between checkpoints
    batch_size: int = 1000  # Records per batch insert
    max_workers: int = 4  # For concurrent operations


@dataclass
class LoggingConfig:
    """Logging configuration settings."""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file_path: Optional[str] = "logs/boligmarkedet.log"
    max_bytes: int = 10 * 1024 * 1024  # 10MB
    backup_count: int = 5


@dataclass
class Settings:
    """Main settings class containing all configuration."""
    database: DatabaseConfig
    api: APIConfig
    scraping: ScrapingConfig
    logging: LoggingConfig
    
    @classmethod
    def from_env(cls) -> "Settings":
        """Create settings from environment variables with defaults."""
        return cls(
            database=DatabaseConfig(
                path=os.getenv("DB_PATH", "data/boligmarkedet.duckdb"),
                connection_timeout=int(os.getenv("DB_CONNECTION_TIMEOUT", "30")),
                max_connections=int(os.getenv("DB_MAX_CONNECTIONS", "5"))
            ),
            api=APIConfig(
                base_url=os.getenv("API_BASE_URL", "https://api.boliga.dk"),
                timeout=int(os.getenv("API_TIMEOUT", "30")),
                max_retries=int(os.getenv("API_MAX_RETRIES", "3")),
                rate_limit_delay=float(os.getenv("API_RATE_LIMIT_DELAY", "1.0"))
            ),
            scraping=ScrapingConfig(
                active_properties_page_size=int(os.getenv("ACTIVE_PAGE_SIZE", "50")),
                sold_properties_page_size=int(os.getenv("SOLD_PAGE_SIZE", "50")),
                checkpoint_interval=int(os.getenv("CHECKPOINT_INTERVAL", "1000")),
                batch_size=int(os.getenv("BATCH_SIZE", "1000")),
                max_workers=int(os.getenv("MAX_WORKERS", "4"))
            ),
            logging=LoggingConfig(
                level=os.getenv("LOG_LEVEL", "INFO"),
                format=os.getenv("LOG_FORMAT", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
                file_path=os.getenv("LOG_FILE_PATH", "logs/boligmarkedet.log"),
                max_bytes=int(os.getenv("LOG_MAX_BYTES", str(10 * 1024 * 1024))),
                backup_count=int(os.getenv("LOG_BACKUP_COUNT", "5"))
            )
        )


# Global settings instance
settings = Settings.from_env()


def ensure_directories():
    """Ensure all necessary directories exist."""
    directories = [
        Path(settings.database.path).parent,
        Path(settings.logging.file_path).parent if settings.logging.file_path else None
    ]
    
    for directory in directories:
        if directory:
            directory.mkdir(parents=True, exist_ok=True) 