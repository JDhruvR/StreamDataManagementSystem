"""
Schema Registry: Manage multiple concurrent schemas and their execution engines.

Allows multiple schemas to run independently, each with their own:
- Input streams
- Output streams
- Continuous queries
- Window/velocity configuration
"""

from typing import Dict, Any, Optional
from core.execution.engine import ExecutionEngine


class SchemaRegistry:
    """Registry for managing multiple deployed schemas."""
    
    def __init__(self):
        """Initialize registry."""
        self.schemas = {}  # schema_name -> {schema_config, engine}
    
    def register_schema(self, schema_dict: Dict[str, Any]) -> ExecutionEngine:
        """
        Register and deploy a new schema.
        
        Args:
            schema_dict: Schema configuration dict from SchemaManager
        
        Returns:
            ExecutionEngine instance for the schema
        
        Raises:
            ValueError: If schema_name already exists or schema is invalid
        """
        schema_name = schema_dict.get('schema_name')
        
        if not schema_name:
            raise ValueError("Schema must have 'schema_name' field")
        
        if schema_name in self.schemas:
            raise ValueError(f"Schema '{schema_name}' already registered")
        
        # Create and initialize engine for this schema
        try:
            engine = ExecutionEngine(schema_dict)
        except Exception as e:
            raise ValueError(f"Failed to deploy schema '{schema_name}': {e}")
        
        # Store schema and engine
        self.schemas[schema_name] = {
            'config': schema_dict,
            'engine': engine
        }
        
        print(f"✓ Schema registered: {schema_name}")
        return engine
    
    def get_engine(self, schema_name: str) -> ExecutionEngine:
        """
        Get the execution engine for a schema.
        
        Args:
            schema_name: Name of the schema
        
        Returns:
            ExecutionEngine instance
        
        Raises:
            ValueError: If schema not found
        """
        if schema_name not in self.schemas:
            raise ValueError(f"Schema '{schema_name}' not found")
        
        return self.schemas[schema_name]['engine']
    
    def get_schema_config(self, schema_name: str) -> Dict[str, Any]:
        """
        Get the configuration dict for a schema.
        
        Args:
            schema_name: Name of the schema
        
        Returns:
            Schema configuration dict
        
        Raises:
            ValueError: If schema not found
        """
        if schema_name not in self.schemas:
            raise ValueError(f"Schema '{schema_name}' not found")
        
        return self.schemas[schema_name]['config']
    
    def unregister_schema(self, schema_name: str) -> None:
        """
        Unregister and remove a schema.
        
        Args:
            schema_name: Name of the schema
        
        Raises:
            ValueError: If schema not found
        """
        if schema_name not in self.schemas:
            raise ValueError(f"Schema '{schema_name}' not found")
        
        del self.schemas[schema_name]
        print(f"✓ Schema unregistered: {schema_name}")
    
    def list_schemas(self) -> Dict[str, Dict[str, Any]]:
        """
        List all registered schemas with metadata.
        
        Returns:
            Dict mapping schema_name to metadata (input_streams, queries, etc.)
        """
        result = {}
        for name, data in self.schemas.items():
            engine = data['engine']
            result[name] = {
                'input_streams': list(engine.get_input_streams().keys()),
                'output_streams': list(engine.get_output_streams().keys()),
                'continuous_queries': [q['name'] for q in engine.get_queries()],
                'window_config': engine.get_window_config()
            }
        return result
    
    def process_event(self, schema_name: str, stream_name: str, event: Dict[str, Any]) -> None:
        """
        Process an event on a specific schema and input stream.
        
        Args:
            schema_name: Name of the schema
            stream_name: Name of the input stream
            event: Event data as dict
        
        Raises:
            ValueError: If schema or stream not found
        """
        engine = self.get_engine(schema_name)
        engine.process_event(stream_name, event)


# Global registry instance
_global_registry = SchemaRegistry()


def get_global_registry() -> SchemaRegistry:
    """Get the global schema registry instance."""
    return _global_registry


def reset_global_registry() -> None:
    """Reset the global registry (useful for testing)."""
    global _global_registry
    _global_registry = SchemaRegistry()
