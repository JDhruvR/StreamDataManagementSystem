"""
Kafka configuration module.

Supports both persistent and in-memory modes:
- Persistent (default): Messages stored to disk with configurable retention
- In-memory (ephemeral): Messages deleted immediately after consumption (retention.ms=1)
"""

from typing import Dict, Any


class KafkaConfig:
    """Kafka broker and topic configuration."""
    
    BROKER_DEFAULT = 'localhost:9092'
    
    # In-memory (ephemeral) mode configuration
    IN_MEMORY_CONFIG = {
        'log.retention.ms': 1,  # Delete messages immediately
        'log.segment.bytes': 1048576,  # 1MB segments
        'log.cleanup.policy': 'delete',
        'log.retention.check.interval.ms': 300000,  # Check every 5 minutes
    }
    
    # Persistent mode configuration (default)
    PERSISTENT_CONFIG = {
        'log.retention.ms': 604800000,  # 7 days
        'log.segment.bytes': 1073741824,  # 1GB segments
        'log.cleanup.policy': 'delete',
        'log.retention.check.interval.ms': 300000,
    }
    
    def __init__(self, broker: str = BROKER_DEFAULT, persistence: bool = False):
        """
        Initialize Kafka configuration.
        
        Args:
            broker: Kafka broker address (default: localhost:9092)
            persistence: If False, messages are ephemeral (in-memory). If True, persisted to disk.
        """
        self.broker = broker
        self.persistence = persistence
    
    def get_broker(self) -> str:
        """Get broker address."""
        return self.broker
    
    def is_persistent(self) -> bool:
        """Check if persistence is enabled."""
        return self.persistence
    
    def get_producer_config(self) -> Dict[str, Any]:
        """Get producer configuration."""
        return {
            'bootstrap_servers': self.broker,
            'acks': 'all',
            'retries': 3,
            'compression_type': 'gzip',
        }
    
    def get_consumer_config(self, group_id: str) -> Dict[str, Any]:
        """Get consumer configuration."""
        return {
            'bootstrap_servers': self.broker,
            'group_id': group_id,
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': False,
            'session_timeout_ms': 30000,
        }
    
    def get_topic_config(self) -> Dict[str, Any]:
        """Get topic configuration based on persistence mode."""
        if self.persistence:
            return self.PERSISTENT_CONFIG
        else:
            return self.IN_MEMORY_CONFIG
    
    def get_mode_description(self) -> str:
        """Get human-readable mode description."""
        mode = "Persistent (disk storage)" if self.persistence else "In-Memory (ephemeral)"
        retention_ms = self.get_topic_config()['log.retention.ms']
        return f"{mode} - Retention: {retention_ms}ms"


# Global default config instance
_default_config = KafkaConfig(persistence=False)


def set_default_config(broker: str = KafkaConfig.BROKER_DEFAULT, persistence: bool = False):
    """Set the default Kafka configuration globally."""
    global _default_config
    _default_config = KafkaConfig(broker=broker, persistence=persistence)


def get_default_config() -> KafkaConfig:
    """Get the default Kafka configuration."""
    return _default_config
