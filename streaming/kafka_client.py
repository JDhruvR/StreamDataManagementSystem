import json
from kafka import KafkaProducer, KafkaConsumer
from streaming.kafka_config import get_default_config


class StreamProducer:
    def __init__(self, bootstrap_servers=None, topic=None, config=None):
        """
        Initialize Kafka producer.
        
        Args:
            bootstrap_servers: List of broker addresses (deprecated, use config instead)
            topic: Topic name (not used, kept for compatibility)
            config: KafkaConfig instance (uses default if not provided)
        """
        if config is None:
            config = get_default_config()
        
        if bootstrap_servers is None:
            bootstrap_servers = [config.get_broker()]
        
        producer_config = config.get_producer_config()
        producer_config['bootstrap_servers'] = bootstrap_servers
        
        self.producer = KafkaProducer(
            **producer_config,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.config = config

    def send(self, topic, data):
        """Send message to topic."""
        self.producer.send(topic, data)
        self.producer.flush()


class StreamConsumer:
    def __init__(self, topic, bootstrap_servers=None, group_id=None, config=None):
        """
        Initialize Kafka consumer.
        
        Args:
            topic: Topic to consume from
            bootstrap_servers: List of broker addresses (deprecated, use config instead)
            group_id: Consumer group ID
            config: KafkaConfig instance (uses default if not provided)
        """
        if config is None:
            config = get_default_config()
        
        if bootstrap_servers is None:
            bootstrap_servers = [config.get_broker()]
        
        consumer_config = config.get_consumer_config(group_id or 'default_group')
        consumer_config['bootstrap_servers'] = bootstrap_servers
        consumer_config['auto_offset_reset'] = 'earliest'  # Read from start of stream
        
        self.consumer = KafkaConsumer(
            topic,
            **consumer_config,
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )
        self.config = config

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.consumer).value
