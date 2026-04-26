"""
Background Kafka consumer for listening to output streams.

Runs in a separate thread and buffers events as they arrive from Kafka topics.
"""

import threading
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

from streaming.kafka_client import StreamConsumer
from streaming.kafka_config import KafkaConfig, get_default_config
from ui.data_buffer import QueryOutputBuffer


logger = logging.getLogger(__name__)


class KafkaOutputConsumer:
    """
    Background Kafka consumer that listens to output streams and buffers events.
    
    Runs separate threads for each topic and automatically buffers recent events in memory.
    """
    
    def __init__(self, output_buffer: QueryOutputBuffer, kafka_broker: str = "localhost:9092", buffer_size: int = 100):
        """
        Initialize Kafka consumer.
        
        Args:
            output_buffer: QueryOutputBuffer instance for storing events
            kafka_broker: Kafka broker address (host:port)
            buffer_size: Max events per topic in buffer
        """
        self.output_buffer = output_buffer
        self.kafka_broker = kafka_broker
        self.buffer_size = buffer_size
        
        self.running = False
        self.consumer_threads: Dict[str, threading.Thread] = {}
        self.lock = threading.Lock()
        self.topics: List[str] = []
        
        logger.info(f"KafkaOutputConsumer initialized with broker: {kafka_broker}")
    
    def subscribe_to_topics(self, topics: List[str]) -> None:
        """
        Subscribe to one or more Kafka topics.
        
        Args:
            topics: List of topic names to subscribe to
        """
        with self.lock:
            new_topics = [t for t in topics if t and t not in self.topics]
            if new_topics:
                self.topics.extend(new_topics)
                logger.info(f"Added topics: {new_topics}")
                
                # If consumer is running, start new consumer threads for new topics
                if self.running:
                    for topic in new_topics:
                        self._start_topic_consumer(topic)
    
    def start(self) -> None:
        """Start the background consumer threads."""
        if self.running:
            logger.warning("KafkaOutputConsumer already running")
            return
        
        if not self.topics:
            logger.warning("No topics to consume, call subscribe_to_topics first")
            return
        
        self.running = True
        
        with self.lock:
            for topic in self.topics:
                self._start_topic_consumer(topic)
        
        logger.info(f"KafkaOutputConsumer started for {len(self.topics)} topics")
    
    def _start_topic_consumer(self, topic: str) -> None:
        """Start a consumer thread for a specific topic."""
        if topic in self.consumer_threads:
            return
        
        thread = threading.Thread(
            target=self._consume_topic,
            args=(topic,),
            daemon=True,
            name=f"kafka_consumer_{topic}"
        )
        thread.start()
        self.consumer_threads[topic] = thread
        logger.info(f"Started consumer thread for topic: {topic}")
    
    def stop(self) -> None:
        """Stop all background consumer threads."""
        self.running = False
        
        with self.lock:
            for topic, thread in self.consumer_threads.items():
                if thread.is_alive():
                    thread.join(timeout=2)
            self.consumer_threads.clear()
        
        logger.info("KafkaOutputConsumer stopped")
    
    def _consume_topic(self, topic: str) -> None:
        """Consume messages from a specific topic (runs in background thread)."""
        try:
            logger.info(f"Connecting to Kafka topic: {topic}")
            
            # Create Kafka config
            config = get_default_config()
            config.broker = self.kafka_broker
            
            # Create consumer for this topic
            group_id = f"sdms_ui_{topic}_{datetime.utcnow().timestamp()}"
            consumer = StreamConsumer(topic=topic, group_id=group_id, config=config)
            
            logger.info(f"Connected to topic '{topic}', group_id: {group_id}")
            
            # Poll for messages
            while self.running:
                try:
                    for msg in consumer:
                        if not self.running:
                            break
                        
                        if isinstance(msg, dict):
                            # Add to buffer under topic name
                            self.output_buffer.add_event(topic, msg)
                            logger.debug(f"Buffered event from topic '{topic}': {list(msg.keys())}")
                        else:
                            logger.warning(f"Unexpected message type from {topic}: {type(msg)}")
                except Exception as e:
                    logger.error(f"Error consuming from topic '{topic}': {e}")
                    if self.running:
                        # Wait before reconnecting
                        import time
                        time.sleep(2)
                        continue
            
            consumer.consumer.close()
            logger.info(f"Closed consumer for topic '{topic}'")
        except Exception as e:
            logger.error(f"KafkaOutputConsumer error for topic '{topic}': {e}")
        finally:
            self.running = False
    
    def is_running(self) -> bool:
        """Check if consumer is running."""
        return self.running
    
    def get_buffer_stats(self) -> Dict[str, Any]:
        """Get current buffer statistics."""
        return {
            "running": self.running,
            "topics": list(self.topics),
            "buffer_stats": self.output_buffer.get_all_stats(),
        }

