"""
Configuration for the UI dashboard.
"""

import os
from typing import Dict, Any

class UIConfig:
    """Dashboard configuration."""
    
    # Flask app settings
    FLASK_HOST = os.getenv("SDMS_UI_HOST", "127.0.0.1")
    FLASK_PORT = int(os.getenv("SDMS_UI_PORT", 5000))
    FLASK_DEBUG = os.getenv("SDMS_UI_DEBUG", "False").lower() == "true"
    
    # Kafka settings
    KAFKA_BROKER = os.getenv("SDMS_KAFKA_BROKER", "localhost:9092")
    KAFKA_BUFFER_SIZE = int(os.getenv("SDMS_KAFKA_BUFFER_SIZE", 100))  # Keep last N events per topic
    KAFKA_CONSUMER_TIMEOUT_MS = 1000  # Timeout for consumer poll
    KAFKA_CONSUMER_GROUP_PREFIX = "sdms_ui"
    
    # UI refresh settings
    DEFAULT_REFRESH_INTERVAL_MS = 1000  # Auto-refresh interval in milliseconds
    
    # Database settings
    SQLITE_DB_PATH = os.getenv("SDMS_SQLITE_DB", "data/static_tables.db")
    
    # Logging
    LOG_LEVEL = os.getenv("SDMS_LOG_LEVEL", "INFO")
    
    @classmethod
    def to_dict(cls) -> Dict[str, Any]:
        """Export configuration as dictionary."""
        return {
            "flask_host": cls.FLASK_HOST,
            "flask_port": cls.FLASK_PORT,
            "flask_debug": cls.FLASK_DEBUG,
            "kafka_broker": cls.KAFKA_BROKER,
            "kafka_buffer_size": cls.KAFKA_BUFFER_SIZE,
            "refresh_interval_ms": cls.DEFAULT_REFRESH_INTERVAL_MS,
        }


# Default configuration instance
config = UIConfig()
