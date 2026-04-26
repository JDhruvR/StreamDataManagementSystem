"""
Stream Data Management System UI Module.

Provides a Flask-based web dashboard for visualizing continuous query outputs
in real-time and historical data stored in SQLite.
"""

from ui.app import UIApp, create_app
from ui.config import config
from ui.data_buffer import QueryOutputBuffer, EventBuffer
from ui.kafka_consumer import KafkaOutputConsumer
from ui.db_service import DatabaseService


__all__ = [
    "UIApp",
    "create_app",
    "config",
    "QueryOutputBuffer",
    "EventBuffer",
    "KafkaOutputConsumer",
    "DatabaseService",
]
