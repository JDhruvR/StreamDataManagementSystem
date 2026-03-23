# Copilot Instructions for StreamDataManagementSystem

## Project Overview

This is an **Extensible Streaming Data Management System** that processes real-time data from IoT sensors using a custom SQL-like query language. The architecture follows a four-stage pipeline: Data Generation → Kafka → Query Parsing → Execution Engine → Output.

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

# Terminal 4: Run system
python -m examples.run_system
```

### Running Tests
```bash
# Single test file
pytest test_ast.py -v

# All tests (currently minimal coverage)
pytest -v

# Tests with output
pytest -s
```

## Architecture

### Four-Stage Pipeline

1. **Data Generation** (`sensors/pollution_sensor.py`)
   - Simulates IoT sensor readings for pollution data
   - Publishes to Kafka topic: `pollution_stream`
   - JSON format: `{"sensor_id": "...", "timestamp": "...", "pollutant": "...", "value": ...}`

2. **Message Streaming** (`streaming/kafka_client.py`)
   - Kafka producer: Wraps kafka-python-ng for message publishing
   - Kafka consumer: Background thread (run_kafka_consumer) feeds messages to engine
   - Topic: `pollution_stream` (default)

3. **Query Parsing** (`core/parser/`)
   - **grammar.lark**: Defines SQL-like DSL grammar (SELECT, WHERE, WINDOW, GROUP BY, etc.)
   - **sql_parser.py**: SQLTransformer converts Lark tree to AST with operators and options
   - Output: Dictionary with keys `operators` (list), `options` (dict), and other metadata

4. **Execution Engine** (`core/execution/`)
   - **engine.py**: ExecutionEngine orchestrates the pipeline
     - `execute_query()`: Takes parsed AST and sets up operator chain
     - `process_event()`: Processes incoming Kafka message through operator chain
   - **operators.py**: Chain-of-responsibility pattern
     - Abstract `Operator` base class with `process()` method
     - Concrete operators: `FilterOperator`, `WindowOperator`, `AggregateOperator`, `SinkOperator`
     - Each operator calls `self.next_op.process()` if conditions met

5. **Storage & Output** (`core/storage/table.py`)
   - TableManager handles SQLite-based result persistence
   - SinkOperator writes aggregated results to tables
   - Alert callbacks invoke on matching results

### Key Data Flow

```
Sensor → Kafka Topic → Background Consumer Thread → ExecutionEngine
                                                             ↓
                                              Operator Chain (Filter → Window → Aggregate → Sink)
                                                             ↓
                                                    Storage or Callback
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

### File Structure

```
core/
  ├── parser/           # SQL parsing (grammar.lark, sql_parser.py)
  ├── execution/        # Engine & operators (engine.py, operators.py)
  └── storage/          # SQLite management (table.py)
streaming/
  └── kafka_client.py   # Kafka producer/consumer wrappers
sensors/
  └── pollution_sensor.py  # Mock sensor data generator
examples/
  └── run_system.py     # End-to-end demo
```

## Important Implementation Notes

### Parser & Query Execution
- **Grammar file**: `core/parser/grammar.lark` defines supported SQL syntax
- **Transformation**: `SQLTransformer` in `sql_parser.py` converts parse tree to execution AST
- **AST Format**: Dictionary with `operators` list, `options` dict, and other metadata
- **Options syntax**: `WITH (key="value")` in SELECT statement

### Operator Chain Execution
- **Initialization**: `ExecutionEngine.execute_query()` builds operator chain from AST
- **Processing**: Each event flows through chain via `process()` calls
- **Termination**: SinkOperator is final stage (no next_op)
- **Conditional Flow**: Operators only call `self.next_op.process()` if event passes their condition

### Kafka Integration
- **Producer** (sensors): Publishes raw sensor readings
- **Consumer** (engine): Runs in background thread, decodes JSON, calls `engine.process_event()`
- **Serialization**: JSON for message format
- **Topic**: `pollution_stream` (configurable via query options)

### Storage & Results
- **SQLite**: TableManager uses built-in Python sqlite3
- **Result Tables**: Created per query, named by user (e.g., `pollution_alerts`)
- **Aggregation**: Results written via SinkOperator callback to TableManager

## Known Limitations

- **No JOIN support**: Queries work on single streams only
- **GROUP BY**: Simplified implementation, not all use cases supported
- **WINDOW**: SLIDING windows use simplified time handling
- **Error Handling**: Basic try/catch, limited recovery
- **State**: No persisted operator state across restarts
- **Test Coverage**: Only `test_ast.py` with minimal tests; add comprehensive test suite as needed

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

- `walkthrough.md`: Step-by-step guide on how the system works
- `implementationPlan.txt`: Design specification and scope
- `examples/run_system.py`: Working end-to-end example to understand flow

## Common Tasks

### Running a Query
```python
from core.parser.sql_parser import SQLParser
from core.execution.engine import ExecutionEngine
from streaming.kafka_client import KafkaConsumer

parser = SQLParser()
ast = parser.parse("SELECT * FROM pollution_stream WHERE value > 50")

engine = ExecutionEngine(ast)
consumer = KafkaConsumer(topic="pollution_stream")

for message in consumer:
    engine.process_event(message)
```

### Adding an Alert Callback
```python
def my_alert(result):
    print(f"Alert: {result}")

engine.sink_callback = my_alert
```

### Debugging
- Enable logging in Kafka consumer with print statements in `run_kafka_consumer()`
- Check operator chain order via `engine.operators` list
- Print event as it flows through operators in `process()` method
