"""
Flask application for the Stream Data Management System UI dashboard.

Provides REST API endpoints for:
- Listing active queries and their metadata
- Retrieving live streamed data from Kafka
- Retrieving historical data from SQLite
- Dashboard HTML rendering
"""

import logging
from typing import Dict, Any, Optional
from flask import Flask, render_template, jsonify
from flask_cors import CORS

from ui.config import config
from ui.data_buffer import QueryOutputBuffer
from ui.kafka_consumer import KafkaOutputConsumer
from ui.db_service import DatabaseService
from core.execution.schema_registry import get_global_registry


# Configure logging
logging.basicConfig(level=config.LOG_LEVEL)
logger = logging.getLogger(__name__)


class UIApp:
    """Stream Data Management System UI Application."""
    
    def __init__(self):
        """Initialize the UI application."""
        self.flask_app = Flask(__name__, 
                               template_folder="ui/templates",
                               static_folder="ui/static")
        
        # Enable CORS for API endpoints
        CORS(self.flask_app)
        
        # Initialize components
        self.output_buffer = QueryOutputBuffer(buffer_size=config.KAFKA_BUFFER_SIZE)
        self.kafka_consumer = KafkaOutputConsumer(
            self.output_buffer,
            kafka_broker=config.KAFKA_BROKER,
            buffer_size=config.KAFKA_BUFFER_SIZE
        )
        self.db_service = DatabaseService(config.SQLITE_DB_PATH)
        self.registry = get_global_registry()
        
        # Register routes
        self._register_routes()
        
        logger.info("UIApp initialized")
    
    def _register_routes(self) -> None:
        """Register all Flask routes and API endpoints."""
        
        @self.flask_app.route("/", methods=["GET"])
        def dashboard():
            """Serve the dashboard HTML."""
            try:
                return render_template("dashboard.html")
            except Exception as e:
                logger.error(f"Error rendering dashboard: {e}")
                return {"error": str(e)}, 500
        
        @self.flask_app.route("/api/config", methods=["GET"])
        def get_config():
            """Get UI configuration."""
            return jsonify(config.to_dict())
        
        @self.flask_app.route("/api/status", methods=["GET"])
        def get_status():
            """Get system status."""
            try:
                active_schema = self._get_active_schema_info()
                return jsonify({
                    "ok": True,
                    "kafka_consumer_running": self.kafka_consumer.is_running(),
                    "active_schema": active_schema,
                    "buffer_stats": self.kafka_consumer.get_buffer_stats(),
                })
            except Exception as e:
                logger.error(f"Error getting status: {e}")
                return {"error": str(e)}, 500
        
        @self.flask_app.route("/api/queries", methods=["GET"])
        def list_queries():
            """
            Get all active queries with metadata.
            
            Returns:
            {
                "queries": [
                    {
                        "name": "query_name",
                        "input_stream": "input_topic",
                        "output_stream": "output_topic",
                        "output_topic": "kafka_topic_name",
                        "sql": "SELECT ... FROM ...",
                    },
                    ...
                ],
                "total": 3,
            }
            """
            try:
                engine = self._get_active_engine()
                if not engine:
                    return jsonify({"queries": [], "total": 0, "message": "No active schema"})
                
                queries = engine.get_queries()
                output_streams = engine.get_output_streams()
                
                query_list = []
                for query in queries:
                    output_stream_name = query.get("output_stream")
                    output_topic = output_streams.get(output_stream_name, {}).get("topic", output_stream_name)
                    
                    query_info = {
                        "name": query.get("name"),
                        "input_stream": query.get("input_stream"),
                        "output_stream": output_stream_name,
                        "output_topic": output_topic,
                        "sql": query.get("query_plan", {}).get("raw", ""),
                        "input_streams": query.get("input_streams", []),
                    }
                    query_list.append(query_info)
                
                return jsonify({
                    "queries": query_list,
                    "total": len(query_list),
                    "schema_name": engine.get_schema_name(),
                })
            except Exception as e:
                logger.error(f"Error listing queries: {e}")
                return {"error": str(e)}, 500
        
        @self.flask_app.route("/api/query/<query_name>/live", methods=["GET"])
        def get_query_live_data(query_name: str):
            """
            Get recent live data for a query from Kafka buffer.
            
            Query params:
                limit: Number of events to return (default: 50, max: 200)
            
            Returns:
            {
                "query_name": "query_name",
                "data": [...],
                "count": 42,
                "buffer_info": {...},
            }
            """
            try:
                from flask import request
                limit = int(request.args.get("limit", 50))
                limit = min(max(limit, 1), 200)  # Clamp between 1 and 200
                
                # Get the output topic for this query
                engine = self._get_active_engine()
                if not engine:
                    return {"error": "No active schema"}, 404
                
                queries = {q["name"]: q for q in engine.get_queries()}
                if query_name not in queries:
                    return {"error": f"Query '{query_name}' not found"}, 404
                
                output_stream_name = queries[query_name].get("output_stream")
                output_streams = engine.get_output_streams()
                output_topic = output_streams.get(output_stream_name, {}).get("topic", output_stream_name)
                
                # Get events from buffer
                events = self.output_buffer.get_query_events(output_topic, limit)
                buffer_stats = self.output_buffer.get_buffer(output_topic).get_stats()
                
                return jsonify({
                    "query_name": query_name,
                    "output_topic": output_topic,
                    "data": events,
                    "count": len(events),
                    "buffer_info": buffer_stats,
                })
            except Exception as e:
                logger.error(f"Error getting live data for query '{query_name}': {e}")
                return {"error": str(e)}, 500
        
        @self.flask_app.route("/api/query/<query_name>/history", methods=["GET"])
        def get_query_history_data(query_name: str):
            """
            Get historical data for a query from SQLite.
            
            Query params:
                table: Table name (optional, defaults to query name)
                limit: Number of rows to return (default: 100, max: 1000)
            
            Returns:
            {
                "query_name": "query_name",
                "table_name": "table_name",
                "data": [...],
                "count": 42,
                "schema": [...],
                "row_count": 1000,
            }
            """
            try:
                from flask import request
                table_name = request.args.get("table", query_name)
                limit = int(request.args.get("limit", 100))
                limit = min(max(limit, 1), 1000)  # Clamp between 1 and 1000
                
                # Check if table exists
                if not self.db_service.table_exists(table_name):
                    return {
                        "query_name": query_name,
                        "table_name": table_name,
                        "error": f"Table '{table_name}' not found",
                        "available_tables": self.db_service.list_tables(),
                    }, 404
                
                # Query the table
                rows = self.db_service.query_table(table_name, limit)
                schema = self.db_service.get_table_schema(table_name)
                row_count = self.db_service.get_row_count(table_name)
                
                return jsonify({
                    "query_name": query_name,
                    "table_name": table_name,
                    "data": rows,
                    "count": len(rows),
                    "schema": schema,
                    "row_count": row_count,
                })
            except Exception as e:
                logger.error(f"Error getting history data for query '{query_name}': {e}")
                return {"error": str(e)}, 500
        
        @self.flask_app.route("/api/schema", methods=["GET"])
        def get_schema_info():
            """Get current active schema information."""
            try:
                engine = self._get_active_engine()
                if not engine:
                    return jsonify({"schema": None, "message": "No active schema"})
                
                window_config = engine.get_window_config()
                
                return jsonify({
                    "schema_name": engine.get_schema_name(),
                    "window_config": window_config,
                    "input_streams": {
                        name: {
                            "name": name,
                            "topic": cfg.get("topic"),
                            "schema": cfg.get("schema"),
                        }
                        for name, cfg in engine.get_input_streams().items()
                    },
                    "output_streams": {
                        name: {
                            "name": name,
                            "topic": cfg.get("topic"),
                            "schema": cfg.get("schema"),
                        }
                        for name, cfg in engine.get_output_streams().items()
                    },
                    "query_count": len(engine.get_queries()),
                })
            except Exception as e:
                logger.error(f"Error getting schema info: {e}")
                return {"error": str(e)}, 500
        
        @self.flask_app.route("/api/database/tables", methods=["GET"])
        def list_database_tables():
            """List all tables in the SQLite database."""
            try:
                tables = self.db_service.list_tables()
                return jsonify({
                    "tables": tables,
                    "count": len(tables),
                })
            except Exception as e:
                logger.error(f"Error listing database tables: {e}")
                return {"error": str(e)}, 500
        
        @self.flask_app.route("/api/health", methods=["GET"])
        def health_check():
            """Health check endpoint."""
            return jsonify({
                "status": "healthy",
                "kafka_consumer": "running" if self.kafka_consumer.is_running() else "stopped",
            })
        
        # Error handlers
        @self.flask_app.errorhandler(404)
        def not_found(error):
            return {"error": "Not found"}, 404
        
        @self.flask_app.errorhandler(500)
        def internal_error(error):
            return {"error": "Internal server error"}, 500
        
        logger.info("All routes registered")
    
    def _get_active_engine(self):
        """Get the currently active execution engine from registry."""
        try:
            schemas = self.registry.list_schemas()
            if not schemas:
                return None
            
            # Get first active schema (in production, track which one is active)
            schema_name = list(schemas.keys())[0]
            return self.registry.get_schema(schema_name)
        except Exception as e:
            logger.error(f"Error getting active engine: {e}")
            return None
    
    def _get_active_schema_info(self) -> Optional[Dict[str, Any]]:
        """Get info about the active schema."""
        try:
            engine = self._get_active_engine()
            if not engine:
                return None
            
            return {
                "name": engine.get_schema_name(),
                "queries": len(engine.get_queries()),
                "input_streams": len(engine.get_input_streams()),
                "output_streams": len(engine.get_output_streams()),
            }
        except Exception:
            return None
    
    def initialize_kafka_consumer(self) -> None:
        """Initialize and start the Kafka consumer."""
        try:
            engine = self._get_active_engine()
            if not engine:
                logger.warning("No active engine, skipping Kafka consumer initialization")
                return
            
            # Get all output topics from queries
            output_streams = engine.get_output_streams()
            topics = [cfg.get("topic") for cfg in output_streams.values()]
            topics = [t for t in topics if t]  # Filter out empty values
            
            if not topics:
                logger.warning("No output topics found, skipping Kafka consumer")
                return
            
            self.kafka_consumer.subscribe_to_topics(topics)
            self.kafka_consumer.start()
            logger.info(f"Kafka consumer initialized with topics: {topics}")
        except Exception as e:
            logger.error(f"Error initializing Kafka consumer: {e}")
    
    def get_flask_app(self) -> Flask:
        """Get the Flask application object."""
        return self.flask_app
    
    def start(self, host: str = None, port: int = None, debug: bool = False) -> None:
        """
        Start the Flask server.
        
        Args:
            host: Host to bind to (default: config.FLASK_HOST)
            port: Port to bind to (default: config.FLASK_PORT)
            debug: Enable debug mode
        """
        host = host or config.FLASK_HOST
        port = port or config.FLASK_PORT
        
        logger.info(f"Starting Flask server on {host}:{port}")
        self.flask_app.run(host=host, port=port, debug=debug)
    
    def shutdown(self) -> None:
        """Shutdown the application."""
        logger.info("Shutting down UI application")
        self.kafka_consumer.stop()


def create_app() -> Flask:
    """Factory function to create the Flask application."""
    ui_app = UIApp()
    return ui_app.get_flask_app()
