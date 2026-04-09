"""
Kafka configuration module.

Message queue is intentionally ephemeral (non-persistent):
- Messages are retained for 1ms
- No persistence mode is exposed
"""

from typing import Dict, Any


class KafkaConfig:
    """Kafka broker and topic configuration."""
    
    BROKER_DEFAULT = 'localhost:9092'
    
    # Ephemeral mode configuration
    EPHEMERAL_CONFIG = {
        'log.retention.ms': 1,  # Delete messages immediately
        'log.segment.bytes': 1048576,  # 1MB segments
        'log.cleanup.policy': 'delete',
        'log.retention.check.interval.ms': 300000,  # Check every 5 minutes
    }

    def __init__(self, broker: str = BROKER_DEFAULT):
        """
        Initialize Kafka configuration.
        
        Args:
            broker: Kafka broker address (default: localhost:9092)
        """
        self.broker = broker
    
    def get_broker(self) -> str:
        """Get broker address."""
        return self.broker
    
    def is_persistent(self) -> bool:
        """Queue persistence is disabled by design."""
        return False
    
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
        """Get topic configuration (always ephemeral)."""
        return self.EPHEMERAL_CONFIG
    
    def get_mode_description(self) -> str:
        """Get human-readable mode description."""
        mode = "In-Memory (ephemeral)"
        retention_ms = self.get_topic_config()['log.retention.ms']
        return f"{mode} - Retention: {retention_ms}ms"


# Global default config instance
_default_config = KafkaConfig()


def set_default_config(broker: str = KafkaConfig.BROKER_DEFAULT):
    """Set the default Kafka configuration globally."""
    global _default_config
    _default_config = KafkaConfig(broker=broker)


def get_default_config() -> KafkaConfig:
    """Get the default Kafka configuration."""
    return _default_config
