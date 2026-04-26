"""
In-memory event buffer for streaming data from Kafka topics.

Maintains a rolling buffer of recent events for each output topic/query.
"""

from collections import deque
from threading import Lock
from typing import Dict, List, Any, Deque
from datetime import datetime


class EventBuffer:
    """Circular buffer to store recent events from a Kafka topic."""
    
    def __init__(self, max_size: int = 100):
        """
        Initialize event buffer.
        
        Args:
            max_size: Maximum number of events to keep in buffer
        """
        self.max_size = max_size
        self.events: Deque[Dict[str, Any]] = deque(maxlen=max_size)
        self.lock = Lock()
        self.last_update = None
    
    def add_event(self, event: Dict[str, Any]) -> None:
        """
        Add an event to the buffer.
        
        Args:
            event: Event dict (typically from Kafka)
        """
        with self.lock:
            # Add metadata
            event_with_meta = dict(event)
            if "_sdms_received_at" not in event_with_meta:
                event_with_meta["_sdms_received_at"] = datetime.utcnow().isoformat()
            
            self.events.append(event_with_meta)
            self.last_update = datetime.utcnow().isoformat()
    
    def get_all_events(self) -> List[Dict[str, Any]]:
        """
        Get all events currently in buffer.
        
        Returns:
            List of events (newest last)
        """
        with self.lock:
            return list(self.events)
    
    def get_recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get most recent N events.
        
        Args:
            limit: Number of events to retrieve
        
        Returns:
            List of events (newest last)
        """
        with self.lock:
            all_events = list(self.events)
            return all_events[-limit:] if all_events else []
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get buffer statistics.
        
        Returns:
            Dict with count, capacity, and last update timestamp
        """
        with self.lock:
            return {
                "count": len(self.events),
                "capacity": self.max_size,
                "last_update": self.last_update,
            }
    
    def clear(self) -> None:
        """Clear all events from buffer."""
        with self.lock:
            self.events.clear()
            self.last_update = None


class QueryOutputBuffer:
    """
    Manages event buffers for all output streams/queries.
    
    Maps query name or output topic to an EventBuffer.
    """
    
    def __init__(self, buffer_size: int = 100):
        """
        Initialize query output buffer manager.
        
        Args:
            buffer_size: Max events per output stream
        """
        self.buffer_size = buffer_size
        self.buffers: Dict[str, EventBuffer] = {}
        self.lock = Lock()
    
    def get_buffer(self, query_or_topic: str) -> EventBuffer:
        """
        Get or create buffer for a query/topic.
        
        Args:
            query_or_topic: Query name or output topic name
        
        Returns:
            EventBuffer instance
        """
        with self.lock:
            if query_or_topic not in self.buffers:
                self.buffers[query_or_topic] = EventBuffer(self.buffer_size)
            return self.buffers[query_or_topic]
    
    def add_event(self, query_or_topic: str, event: Dict[str, Any]) -> None:
        """
        Add event to a specific query/topic buffer.
        
        Args:
            query_or_topic: Query name or output topic
            event: Event dict
        """
        buffer = self.get_buffer(query_or_topic)
        buffer.add_event(event)
    
    def get_all_queries(self) -> List[str]:
        """Get all tracked query/topic names."""
        with self.lock:
            return list(self.buffers.keys())
    
    def get_query_events(self, query_or_topic: str, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get events for a specific query/topic.
        
        Args:
            query_or_topic: Query name or output topic
            limit: Max events to return
        
        Returns:
            List of events
        """
        buffer = self.get_buffer(query_or_topic)
        return buffer.get_recent_events(limit)
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all query buffers."""
        with self.lock:
            stats = {}
            for query_name, buffer in self.buffers.items():
                stats[query_name] = buffer.get_stats()
            return stats
