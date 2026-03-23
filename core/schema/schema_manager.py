"""
Schema Manager: Load, validate, and manage streaming data schemas.

Schema JSON structure:
{
  "schema_name": "pollution_monitoring_v1",
  "window_size": 10,
  "window_unit": "seconds",
  "velocity": {
    "type": "count",
    "value": 100
  },
  "input_streams": [
    {
      "name": "pollution_stream",
      "topic": "pollution_stream",
      "schema": {
        "timestamp": "STRING",
        "sensor_id": "STRING",
        "pollutant": "STRING",
        "value": "FLOAT"
      }
    }
  ],
  "continuous_queries": [
    {
      "name": "high_pollution_alert",
      "input_stream": "pollution_stream",
      "output_stream": "high_pollution_alerts",
      "query": "SELECT sensor_id, AVG(value) FROM pollution_stream WHERE value > 50.0"
    }
  ],
  "output_streams": [
    {
      "name": "high_pollution_alerts",
      "topic": "high_pollution_alerts",
      "schema": {
        "sensor_id": "STRING",
        "avg_value": "FLOAT"
      }
    }
  ]
}
"""

import json
import os
from typing import Dict, List, Any, Optional


class SchemaValidationError(Exception):
    """Raised when schema validation fails."""
    pass


class SchemaManager:
    """Load, validate, and manage streaming data schemas."""
    
    REQUIRED_FIELDS = ['schema_name', 'window_size', 'window_unit', 'velocity', 
                       'input_streams', 'continuous_queries', 'output_streams']
    VALID_WINDOW_UNITS = ['seconds', 'minutes', 'hours']
    VALID_VELOCITY_TYPES = ['count', 'time']
    VALID_DATA_TYPES = ['STRING', 'INT', 'FLOAT', 'TIMESTAMP', 'BOOLEAN']
    
    def __init__(self):
        self.schema = None
        self.schema_path = None
    
    def load_from_file(self, filepath: str) -> Dict[str, Any]:
        """Load schema from JSON file."""
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"Schema file not found: {filepath}")
        
        try:
            with open(filepath, 'r') as f:
                self.schema = json.load(f)
            self.schema_path = filepath
            self.validate()
            print(f"✓ Schema loaded from {filepath}")
            return self.schema
        except json.JSONDecodeError as e:
            raise SchemaValidationError(f"Invalid JSON in schema file: {e}")
    
    def load_from_input(self) -> Dict[str, Any]:
        """Load schema from interactive terminal input."""
        print("\n=== Schema Input ===")
        print("Provide schema JSON path or paste JSON directly.")
        print("Option 1: Enter file path")
        print("Option 2: Paste JSON (type 'END' on new line when done)")
        print()
        
        user_input = input("Enter file path or 'paste': ").strip()
        
        if user_input.lower() == 'paste':
            json_lines = []
            print("Paste JSON (type 'END' on new line to finish):")
            while True:
                line = input()
                if line.strip() == 'END':
                    break
                json_lines.append(line)
            
            try:
                self.schema = json.loads('\n'.join(json_lines))
                self.validate()
                print("✓ Schema loaded from input")
                return self.schema
            except json.JSONDecodeError as e:
                raise SchemaValidationError(f"Invalid JSON: {e}")
        else:
            return self.load_from_file(user_input)
    
    def validate(self) -> None:
        """Validate schema structure and references."""
        if self.schema is None:
            raise SchemaValidationError("No schema loaded")
        
        # Check required top-level fields
        for field in self.REQUIRED_FIELDS:
            if field not in self.schema:
                raise SchemaValidationError(f"Missing required field: {field}")
        
        # Validate window_size
        if not isinstance(self.schema['window_size'], (int, float)) or self.schema['window_size'] <= 0:
            raise SchemaValidationError("window_size must be a positive number")
        
        # Validate window_unit
        if self.schema['window_unit'] not in self.VALID_WINDOW_UNITS:
            raise SchemaValidationError(f"window_unit must be one of {self.VALID_WINDOW_UNITS}")
        
        # Validate velocity
        velocity = self.schema['velocity']
        if not isinstance(velocity, dict):
            raise SchemaValidationError("velocity must be a dict with 'type' and 'value'")
        if 'type' not in velocity or 'value' not in velocity:
            raise SchemaValidationError("velocity must have 'type' and 'value' fields")
        if velocity['type'] not in self.VALID_VELOCITY_TYPES:
            raise SchemaValidationError(f"velocity type must be one of {self.VALID_VELOCITY_TYPES}")
        if not isinstance(velocity['value'], (int, float)) or velocity['value'] <= 0:
            raise SchemaValidationError("velocity value must be a positive number")
        
        # Validate input_streams
        self._validate_input_streams()
        
        # Validate output_streams
        self._validate_output_streams()
        
        # Validate continuous_queries
        self._validate_continuous_queries()
    
    def _validate_input_streams(self) -> None:
        """Validate input_streams section."""
        input_streams = self.schema.get('input_streams', [])
        
        if not isinstance(input_streams, list) or len(input_streams) == 0:
            raise SchemaValidationError("input_streams must be a non-empty list")
        
        stream_names = set()
        for stream in input_streams:
            if not isinstance(stream, dict):
                raise SchemaValidationError("Each input stream must be a dict")
            
            if 'name' not in stream or 'topic' not in stream or 'schema' not in stream:
                raise SchemaValidationError("Input stream must have 'name', 'topic', 'schema'")
            
            stream_name = stream['name']
            if stream_name in stream_names:
                raise SchemaValidationError(f"Duplicate input stream name: {stream_name}")
            stream_names.add(stream_name)
            
            # Validate schema fields
            if not isinstance(stream['schema'], dict):
                raise SchemaValidationError(f"Stream {stream_name} schema must be a dict")
            
            for col_name, col_type in stream['schema'].items():
                if col_type not in self.VALID_DATA_TYPES:
                    raise SchemaValidationError(
                        f"Stream {stream_name}, column {col_name}: invalid type {col_type}"
                    )
        
        self._input_stream_names = stream_names
    
    def _validate_output_streams(self) -> None:
        """Validate output_streams section."""
        output_streams = self.schema.get('output_streams', [])
        
        if not isinstance(output_streams, list):
            raise SchemaValidationError("output_streams must be a list")
        
        stream_names = set()
        for stream in output_streams:
            if not isinstance(stream, dict):
                raise SchemaValidationError("Each output stream must be a dict")
            
            if 'name' not in stream or 'topic' not in stream or 'schema' not in stream:
                raise SchemaValidationError("Output stream must have 'name', 'topic', 'schema'")
            
            stream_name = stream['name']
            if stream_name in stream_names:
                raise SchemaValidationError(f"Duplicate output stream name: {stream_name}")
            stream_names.add(stream_name)
            
            if not isinstance(stream['schema'], dict):
                raise SchemaValidationError(f"Stream {stream_name} schema must be a dict")
            
            for col_name, col_type in stream['schema'].items():
                if col_type not in self.VALID_DATA_TYPES:
                    raise SchemaValidationError(
                        f"Stream {stream_name}, column {col_name}: invalid type {col_type}"
                    )
        
        self._output_stream_names = stream_names
    
    def _validate_continuous_queries(self) -> None:
        """Validate continuous_queries section."""
        continuous_queries = self.schema.get('continuous_queries', [])
        
        if not isinstance(continuous_queries, list):
            raise SchemaValidationError("continuous_queries must be a list")
        
        for query in continuous_queries:
            if not isinstance(query, dict):
                raise SchemaValidationError("Each query must be a dict")
            
            required = ['name', 'input_stream', 'output_stream', 'query']
            for field in required:
                if field not in query:
                    raise SchemaValidationError(f"Query missing required field: {field}")
            
            # Validate stream references
            input_stream = query['input_stream']
            if input_stream not in self._input_stream_names:
                raise SchemaValidationError(
                    f"Query '{query['name']}': input_stream '{input_stream}' not defined"
                )
            
            output_stream = query['output_stream']
            if output_stream not in self._output_stream_names:
                raise SchemaValidationError(
                    f"Query '{query['name']}': output_stream '{output_stream}' not defined"
                )
    
    def save_to_file(self, filepath: str) -> None:
        """Save current schema to JSON file."""
        if self.schema is None:
            raise SchemaValidationError("No schema to save")
        
        os.makedirs(os.path.dirname(filepath) or '.', exist_ok=True)
        
        with open(filepath, 'w') as f:
            json.dump(self.schema, f, indent=2)
        
        print(f"✓ Schema saved to {filepath}")
    
    def get_schema(self) -> Dict[str, Any]:
        """Get loaded schema dict."""
        if self.schema is None:
            raise SchemaValidationError("No schema loaded")
        return self.schema
    
    def get_schema_name(self) -> str:
        """Get schema name."""
        if self.schema is None:
            raise SchemaValidationError("No schema loaded")
        return self.schema['schema_name']
    
    def get_window_config(self) -> Dict[str, Any]:
        """Get window configuration (size, unit, velocity)."""
        if self.schema is None:
            raise SchemaValidationError("No schema loaded")
        
        return {
            'window_size': self.schema['window_size'],
            'window_unit': self.schema['window_unit'],
            'velocity': self.schema['velocity']
        }
    
    def get_input_streams(self) -> List[Dict[str, Any]]:
        """Get input streams list."""
        if self.schema is None:
            raise SchemaValidationError("No schema loaded")
        return self.schema['input_streams']
    
    def get_output_streams(self) -> List[Dict[str, Any]]:
        """Get output streams list."""
        if self.schema is None:
            raise SchemaValidationError("No schema loaded")
        return self.schema['output_streams']
    
    def get_continuous_queries(self) -> List[Dict[str, Any]]:
        """Get continuous queries list."""
        if self.schema is None:
            raise SchemaValidationError("No schema loaded")
        return self.schema['continuous_queries']
