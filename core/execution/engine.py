"""
Execution Engine: Process streaming data through operator pipelines.

Schema-based design: All continuous queries are defined in the schema.
No ad-hoc queries. Window size and velocity are fixed per schema.
"""

from core.parser.sql_parser import parse_sql
from core.execution.operators import FilterOperator, WindowOperator, AggregateOperator, SinkOperator, JoinOperator
from core.storage.table import storage
from typing import Dict, List, Any, Optional, Callable


class ExecutionEngine:
    """
    Execute continuous queries defined in a schema.
    
    Receives events from input streams and routes them through operator pipelines.
    Window size and velocity are fixed per schema, not per query.
    """
    
    def __init__(self, schema: Optional[Dict[str, Any]] = None):
        """
        Initialize execution engine.
        
        Args:
            schema: Schema dict from SchemaManager (optional, can initialize_from_schema later)
        """
        self.schema = schema
        self.input_streams = {}  # stream_name -> stream_config
        self.output_streams = {}  # stream_name -> stream_config
        self.queries = []  # List of query pipelines: {name, input_stream, output_stream, pipeline}
        self.tables = {}  # table_name -> schema
        self.window_config = None  # {window_size, window_unit, velocity}
        
        if schema:
            self.initialize_from_schema(schema)
    
    def initialize_from_schema(self, schema: Dict[str, Any]) -> None:
        """
        Initialize engine from schema configuration.
        
        Builds operator pipelines for all continuous queries in the schema.
        Window size, unit, and velocity are applied globally to all queries.
        
        Args:
            schema: Schema dict with input_streams, output_streams, continuous_queries, etc.
        
        Raises:
            ValueError: If schema is invalid
        """
        self.schema = schema
        
        # Store window configuration (global for all queries)
        self.window_config = {
            'window_size': schema['window_size'],
            'window_unit': schema['window_unit'],
            'velocity': schema['velocity']
        }
        
        # Register input streams
        for stream in schema.get('input_streams', []):
            self._register_input_stream(stream)
        
        # Register output streams
        for stream in schema.get('output_streams', []):
            self._register_output_stream(stream)
        
        # Build pipelines for continuous queries
        for query_config in schema.get('continuous_queries', []):
            self._deploy_continuous_query(query_config)
    
    def _register_input_stream(self, stream_config: Dict[str, Any]) -> None:
        """Register an input stream."""
        stream_name = stream_config['name']
        self.input_streams[stream_name] = stream_config
        print(f"✓ Input stream registered: {stream_name} (topic: {stream_config['topic']})")
    
    def _register_output_stream(self, stream_config: Dict[str, Any]) -> None:
        """Register an output stream."""
        stream_name = stream_config['name']
        self.output_streams[stream_name] = stream_config
        print(f"✓ Output stream registered: {stream_name} (topic: {stream_config['topic']})")
    
    def _deploy_continuous_query(self, query_config: Dict[str, Any]) -> None:
        """
        Deploy a continuous query from schema.
        
        Creates operator pipeline:
        Filter -> Window -> Aggregate -> Sink
        """
        query_name = query_config['name']
        input_stream_name = query_config['input_stream']
        output_stream_name = query_config['output_stream']
        query_text = query_config['query']
        
        # Validate streams exist
        if input_stream_name not in self.input_streams:
            raise ValueError(f"Query '{query_name}': input_stream '{input_stream_name}' not found")
        if output_stream_name not in self.output_streams:
            raise ValueError(f"Query '{query_name}': output_stream '{output_stream_name}' not found")
        
        # Parse query
        try:
            parse_result = parse_sql(query_text)
            if not parse_result or len(parse_result) == 0:
                raise ValueError(f"Query parse failed: {query_text}")
            
            query_plan = parse_result[0]
            if query_plan.get('type') != 'select_query':
                raise ValueError("Only SELECT queries supported for continuous queries")
        
        except Exception as e:
            raise ValueError(f"Failed to parse query '{query_name}': {e}")
        
        # Build operator pipeline with schema-level window/velocity config
        pipeline = self._build_pipeline(query_plan, output_stream_name)
        
        # Store query metadata
        self.queries.append({
            'name': query_name,
            'input_stream': input_stream_name,
            'output_stream': output_stream_name,
            'pipeline': pipeline,
            'query_plan': query_plan
        })
        
        print(f"✓ Continuous query deployed: {query_name}")
        print(f"   {input_stream_name} -> {output_stream_name}")
    
    def _build_pipeline(self, query_plan: Dict[str, Any], output_stream_name: str) -> Any:
        """
        Build operator pipeline from query plan.
        
        Pipeline stages (built in reverse order):
        1. Sink (final stage)
        2. Aggregate (if aggregation in query)
        3. Window (using schema-level config)
        4. Filter (using WHERE clause)
        5. Join (if INNER JOIN specified)
        """
        # Sink operator (final stage)
        output_topic = self.output_streams[output_stream_name]['topic']
        pipeline = SinkOperator(target_stream=output_stream_name, target_topic=output_topic)
        
        # Aggregate operator (if needed)
        select_list = query_plan.get('select', [])
        agg_config = None
        group_by = []
        
        if isinstance(select_list, list):
            for item in select_list:
                if isinstance(item, dict) and 'func' in item:
                    agg_config = item
                else:
                    group_by.append(item)
        
        if agg_config:
            pipeline = AggregateOperator(
                agg_config,
                group_by_fields=group_by,
                next_op=pipeline,
                select_list=select_list,
            )
        
        # Window operator (using schema-level config, NOT query-level)
        window_config = {
            'type': 'TUMBLING',  # Default to tumbling
            'size': self.window_config['window_size'],
            'unit': self.window_config['window_unit']
        }
        pipeline = WindowOperator(window_config, next_op=pipeline)
        
        # Filter operator (if WHERE clause exists)
        if 'where' in query_plan:
            pipeline = FilterOperator(query_plan['where'], next_op=pipeline)

        # Join operator (currently supports stream -> table INNER JOIN)
        if 'join' in query_plan:
            join_cfg = query_plan['join']
            join_type = join_cfg.get('join_type', 'INNER')
            if join_type != 'INNER':
                raise ValueError(f"Unsupported join type: {join_type}")

            pipeline = JoinOperator(
                table_name=join_cfg['table'],
                left_field=join_cfg['left_field'],
                right_field=join_cfg['right_field'],
                next_op=pipeline,
            )
        
        return pipeline
    
    def process_event(self, stream_name: str, event: Dict[str, Any]) -> None:
        """
        Process an event from an input stream.
        
        Routes event to all queries listening on that stream.
        
        Args:
            stream_name: Name of input stream
            event: Event data as dict
        
        Raises:
            ValueError: If stream_name not found
        """
        if stream_name not in self.input_streams:
            raise ValueError(f"Unknown input stream: {stream_name}")
        
        # Route to all queries listening on this stream
        for query in self.queries:
            if query['input_stream'] == stream_name:
                query['pipeline'].process(event)
    
    def get_input_streams(self) -> Dict[str, Dict[str, Any]]:
        """Get all registered input streams."""
        return self.input_streams
    
    def get_output_streams(self) -> Dict[str, Dict[str, Any]]:
        """Get all registered output streams."""
        return self.output_streams
    
    def get_queries(self) -> List[Dict[str, Any]]:
        """Get all deployed queries."""
        return self.queries
    
    def get_schema_name(self) -> str:
        """Get schema name."""
        if self.schema is None:
            return "unknown"
        return self.schema.get('schema_name', 'unknown')
    
    def get_window_config(self) -> Dict[str, Any]:
        """Get window configuration."""
        return self.window_config

