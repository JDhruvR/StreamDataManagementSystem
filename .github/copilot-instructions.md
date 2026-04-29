# Copilot Instructions for StreamDataManagementSystem

## Writing Guidelines for This Project

CRITICAL RULES:
- Keep writing concise and direct. No verbose explanations.
- Never use emojis in files or terminal output. Never.
- Document must be brief, practical, focused on implementation.
- Avoid marketing language, abstractions, or flowery descriptions.

## Project Overview

This is a streaming data processing system that routes events through operator pipelines defined in JSON schemas. Data flows: Kafka producer -> JSON schema -> schema registry -> operator chain -> Kafka output/storage.

## Quick Start

### Environment Setup
```bash
# Install dependencies
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# Start Kafka (separate terminal windows):
# Terminal 1: Zookeeper
kafka_2.13-3.6.1/bin/zookeeper-server-start.sh kafka_2.13-3.6.1/config/zookeeper.properties

# Terminal 2: Kafka broker
kafka_2.13-3.6.1/bin/kafka-server-start.sh kafka_2.13-3.6.1/config/server.properties

# Terminal 3: Run sensor simulator
python -m sensors.pollution_sensor

# Terminal 4: Run interactive CLI (v1.1+)
python -m examples.cli
```

### Running Tests
```bash
pytest -q
pytest -v
```

## Architecture

### Six-Stage Pipeline (v1.1+)

1. **Data Generation** (`sensors/pollution_sensor.py`, `sensors/weather_sensor.py`)
   - Simulates IoT sensor readings (pollution, weather)
   - Publishes to Kafka topics: `pollution_stream`, `weather_stream`
   - JSON format: `{"sensor_id": "...", "timestamp": "...", "value": ...}`

2. **Schema Configuration** (`schemas/*.json`)
   - JSON defines all aspects: input/output streams, queries, window, velocity
   - Loaded via SchemaManager
   - Validated before deployment

3. **Schema Registry & Deployment** (`core/execution/schema_registry.py`)
   - Registers schemas with ExecutionEngine
   - Supports multiple concurrent schemas
   - Isolated stream and query namespaces per schema

4. **Query Parsing** (`core/parser/`)
   - **grammar.lark**: SQL-like DSL (SELECT, WHERE, JOIN, GROUP BY, aggregates)
   - **sql_parser.py**: SQLTransformer converts Lark tree to structured AST
   - Output: Dictionary with query type, fields, conditions, joins

5. **Execution Engine** (`core/execution/`)
   - **engine.py**: ExecutionEngine orchestrates operator pipeline per query
      - `initialize_from_schema()`: Builds all operator chains from schema
      - `process_event()`: Routes event by stream membership to all queries
   - **operators.py**: Chain-of-responsibility pattern
      - Abstract `Operator` base class with `process()` method
      - Concrete operators: FilterOperator, JoinOperator, WindowOperator, AggregateOperator, ProjectionOperator, SinkOperator
      - Each operator calls `self.next_op.process()` if conditions met

6. **Output & Storage**
   - **Kafka**: SinkOperator writes to output topics
   - **SQLite**: Reference tables in `data/static_tables.db` via TableManager
   - **CLI**: Interactive results display

### Key Data Flow

```
Sensors → Kafka Topics → Schema Registry → Execution Engine
                                               ↓
                                   Operator Chains (per query)
                                   ├─ Filter → Window → Aggregate → Sink → Kafka
                                   ├─ Join → Filter → Projection → Sink → Kafka
                                   └─ ...
                                               ↓
                                   Output Topics + Reference Tables
```

## Code Conventions

### Naming Conventions
- **Files**: `lowercase_with_underscores.py`
- **Classes**: `PascalCaseOperator` (e.g., `FilterOperator`, `AggregateOperator`)
- **Functions/Methods**: `snake_case()`
- **Constants**: `UPPERCASE_SNAKE_CASE`
- **Kafka Topics**: `lowercase_snake_case` (e.g., `pollution_stream`)

### Design Patterns

1. **Operator Pattern**: Abstract base with `process(event)` method
   - Each operator transforms or evaluates the event
   - Operators chain together, calling `self.next_op.process()`
   - Used for: Filter, Window, Aggregate, Sink operations

2. **Pipeline Chain**: Operators hold reference to next operator
   - Easy to compose complex queries
   - Each operator independent and testable
   - New operators extend `Operator` class

3. **Parser with Lark**: Grammar-driven SQL parsing
   - Define syntax in `grammar.lark`
   - Implement transformer in `sql_parser.py` (extends Lark's Transformer)
   - Returns AST-like dictionary structure

4. **Callback Pattern**: Sinks use callbacks for output
   - Example: `alert_callback` passed to SinkOperator
   - Allows flexible output without tight coupling

## File Structure

```
core/
  ├── schema/             # Schema loading/validation (SchemaManager)
  ├── execution/          # Engine, operators, registry
  │   ├── engine.py
  │   ├── operators.py
  │   └── schema_registry.py
  └── parser/             # SQL parsing (grammar.lark, sql_parser.py)
streaming/
  ├── kafka_client.py     # Kafka producer/consumer
  └── kafka_config.py     # Kafka configuration
sensors/
  ├── pollution_sensor.py
  └── weather_sensor.py
examples/
  ├── cli.py              # Interactive schema/query/table management
  └── run_system.py       # Non-interactive runner
schemas/
  ├── pollution_schema.json
  ├── pollution2.json
  └── stream_join_demo.json
data/
  └── static_tables.db    # SQLite reference tables
tests/
  └── test_*.py           # Unit & integration tests
docs/
  ├── setup/              # Installation guides
  ├── guides/             # Architecture & extension
  └── lessons/            # History & decisions
```

## Important Implementation Notes

### Parser & Query Execution
- **Grammar file**: `core/parser/grammar.lark` defines supported SQL syntax
- **Transformation**: `SQLTransformer` in `sql_parser.py` converts parse tree to execution AST
- **AST Format**: Dictionary with `type`, `select`, `from`, `where`, `join` fields
- **Schema-based**: All queries pre-defined in schema (no ad-hoc execution API)

### Operator Chain Execution
- **Initialization**: `ExecutionEngine.initialize_from_schema()` builds all operator chains from schema
- **Processing**: `process_event(schema_name, stream_name, event)` routes event to relevant queries
- **Termination**: SinkOperator writes to Kafka or callback
- **Conditional Flow**: Operators only call `self.next_op.process()` if event passes their condition

### Kafka Integration
- **Producer** (sensors): Publishes raw sensor readings
- **Consumer** (streaming/kafka_client.py): Background thread decodes JSON, feeds to engine
- **Serialization**: JSON for message format
- **Topics**: Multiple input/output topics per schema, defined in JSON config
- **Mode**: Ephemeral (no persistence by default, retention.ms=1)

### Schema & Registry
- **SchemaManager** (`core/schema/schema_manager.py`): Loads/validates JSON schemas
- **SchemaRegistry** (`core/execution/schema_registry.py`): Manages schema lifecycle
- **Multiple Schemas**: Run concurrently with isolated namespaces
- **Event Routing**: Engine routes by stream membership, not query name

## Known Limitations

- **Stream Joins**: Stream-stream and stream-table INNER JOINs are fully supported
- **GROUP BY**: Simplified implementation, not all use cases supported
- **SLIDING windows**: Use TUMBLING by default
- **Error Handling**: Basic validation, limited recovery
- **State**: No operator state persistence across restarts
- **Event-time**: Processing-time only (watermarking not supported)

## Extension Points

### Adding New Operators
1. Create new class in `core/execution/operators.py` extending `Operator`
2. Implement `process(self, event)` method
3. Call `self.next_op.process(event)` if event matches condition
4. Update parser grammar and SQLTransformer if needed

### Adding New Syntax
1. Update `core/parser/grammar.lark` with new grammar rule
2. Add handler method to `SQLTransformer` in `sql_parser.py`
3. Update AST structure to include new field
4. Update ExecutionEngine to handle new AST field

### Adding Sensor Types
1. Create new file in `sensors/` extending producer pattern
2. Publish to appropriate Kafka topic
3. Update consumer to handle new message format if needed

## Dependencies

Key libraries:
- **kafka-python-ng** (2.2.2): Kafka client
- **lark** (1.1.7): Parser framework for grammar-driven parsing
- **pandas** (2.2.1): DataFrame operations for windowing/aggregation
- **pytest** (8.1.1): Testing framework

See `requirements.txt` for full list.

## Documentation References

- **docs/setup.md**: Installation, prerequisites, running guides
- **docs/guides.md**: Architecture, extending system, join reference, schema format
- **docs/lessons.md**: Release notes, design decisions, implementation status, feature summary
- **examples/cli.py**: Working interactive deployment example
- **examples/run_system.py**: Non-interactive runner example

## Common Tasks

### Running a Query (via CLI)
```bash
python -m examples.cli
```

At prompt:
```
load schemas/pollution2.json
status
query
query> SELECT sensor_id, AVG(value) FROM pollution_stream WHERE value > 50
query_name> avg_query
output_stream> output
save
```

### Creating a Schema
```json
{
  "schema_name": "my_schema",
  "window_size": 10,
  "window_unit": "seconds",
  "velocity": {"type": "count", "value": 100},
  "input_streams": [
    {"name": "my_stream", "topic": "my_topic", "schema": {"id": "STRING", "value": "FLOAT"}}
  ],
  "continuous_queries": [
    {
      "name": "my_query",
      "input_stream": "my_stream",
      "output_stream": "output",
      "query": "SELECT id, AVG(value) FROM my_stream WHERE value > 0"
    }
  ],
  "output_streams": [
    {"name": "output", "topic": "output", "schema": {"id": "STRING", "AVG(value)": "FLOAT"}}
  ]
}
```

### Stream-to-Stream Join
```sql
SELECT pollution.sensor_id, pollution.value, weather.humidity
FROM pollution_stream INNER JOIN weather_stream ON sensor_id = sensor_id
WHERE pollution.value > 50
```

### Stream-to-Table Join
```sql
SELECT pollution.value, sensor.name
FROM pollution_stream INNER JOIN sensors ON sensor_id = id
```

### Debugging
- Check schema validation: `status` in CLI
- View operator chain: Print `engine.queries[query_name]` object
- Trace event flow: Add print statements in operator `process()` methods
- Test parsing: `python -c "from core.parser.sql_parser import SQLParser; p = SQLParser(); print(p.parse('SELECT * FROM stream'))"`
