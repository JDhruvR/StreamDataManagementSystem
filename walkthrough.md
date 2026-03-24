# StreamDataManagementSystem v1.0 - Schema-Based Streaming Engine

A complete walkthrough of the schema-based architecture, how continuous queries work, and how to extend the system.

---

## How the System Works (End-to-End)

The v1.0 architecture operates on a **schema-based model** where all streaming deployments are defined in JSON configuration files. Here's the complete flow:

### Stage 1: Schema Configuration (The Blueprint)
* **File:** `schemas/pollution_schema.json` (or any custom schema file)
* **What happens:** 
  - You define a schema JSON file specifying:
    - **Input streams**: Where data comes from (Kafka topics)
    - **Output streams**: Where results go (Kafka topics or storage)
    - **Continuous queries**: Pre-defined queries that run on schedule
    - **Window config**: Global window size and time unit for all queries
    - **Velocity config**: Event batch size or time-based triggers
  - Example:
    ```json
    {
      "schema_name": "pollution_monitoring_v1",
      "window_size": 10,
      "window_unit": "seconds",
      "velocity": {"type": "count", "value": 100},
      "input_streams": [...],
      "continuous_queries": [...],
      "output_streams": [...]
    }
    ```

### Stage 2: Schema Loading & Validation (The Parser)
* **File:** `core/schema/schema_manager.py`
* **What happens:**
  - `SchemaManager` loads your JSON schema from a file
  - Validates structure: streams exist, queries reference correct streams
  - Validates data types, window units, velocity configuration
  - Example code:
    ```python
    from core.schema.schema_manager import SchemaManager
    manager = SchemaManager()
    schema = manager.load_from_file('schemas/pollution_schema.json')
    ```

### Stage 3: Schema Deployment (The Registry)
* **File:** `core/execution/schema_registry.py`
* **What happens:**
  - `SchemaRegistry` registers the validated schema
  - Creates an `ExecutionEngine` instance for that schema
  - Supports multiple schemas running concurrently
  - Example code:
    ```python
    from core.execution.schema_registry import get_global_registry
    registry = get_global_registry()
    engine = registry.register_schema(schema)
    ```

### Stage 4: Query Pipeline Construction (The Operators)
* **Files:** `core/execution/engine.py` & `core/execution/operators.py`
* **What happens:**
  - For each continuous query in the schema, engine builds an operator chain:
    1. **FilterOperator**: Checks WHERE conditions (e.g., `value > 50`)
    2. **WindowOperator**: Buffers events for the schema's window duration
    3. **AggregateOperator**: Applies aggregate functions (AVG, SUM, COUNT, etc.)
    4. **SinkOperator**: Writes results to output stream/table
  - All queries in the schema share the same window size (defined globally)
  - Example pipeline for "SELECT sensor_id, AVG(value) FROM stream WHERE value > 50":
    ```
    Event Input → FilterOperator (value > 50?) → WindowOperator (10s buffer)
    → AggregateOperator (AVG) → SinkOperator (output stream)
    ```

### Stage 5: Data Ingestion (The Sensor & Kafka)
* **Files:** `sensors/pollution_sensor.py`, `streaming/kafka_client.py`
* **What happens:**
  - Sensor simulator generates JSON events every second
  - Publishes to Kafka topic (e.g., `pollution_stream`)
  - Kafka runs in **in-memory mode** by default (ephemeral, no persistence)
  - See: `streaming/kafka_config.py` - retention.ms=1

### Stage 6: Event Consumption & Processing
* **File:** `examples/run_system.py`
* **What happens:**
  - Background thread runs `StreamConsumer` listening to each input stream topic
  - New events are immediately fed to the engine
  - Engine routes event through all continuous queries listening on that stream
  - Results flow through the operator chain
  - Sink outputs results to Kafka output topic or callback
  - Example:
    ```python
    registry.process_event(schema_name, stream_name, event)
    ```

---

## How to Extend the System

### A. Create a New Continuous Query

Add a new query to your schema JSON:
```json
"continuous_queries": [
  {
    "name": "extreme_pollution",
    "input_stream": "pollution_stream",
    "output_stream": "extreme_alerts",
    "query": "SELECT sensor_id, MAX(value) FROM pollution_stream WHERE value > 150"
  }
]
```

Then reload the schema. The engine automatically:
- Parses the query (parser removes old WINDOW syntax - it's now global)
- Builds the operator chain using schema's window_size
- Deploys alongside other queries

### B. Add a New Operator Type

To add custom processing (e.g., `DeduplicateOperator`):

1. **Create the operator** in `core/execution/operators.py`:
   ```python
   class DeduplicateOperator(Operator):
       def __init__(self, next_op=None):
           self.seen = set()
           self.next_op = next_op
       
       def process(self, event):
           event_key = (event.get('sensor_id'), event.get('value'))
           if event_key not in self.seen:
               self.seen.add(event_key)
               if self.next_op:
                   self.next_op.process(event)
   ```

2. **Wire it into the pipeline** in `core/execution/engine.py` `_build_pipeline()`:
   ```python
   # Add before Filter
   pipeline = DeduplicateOperator(next_op=pipeline)
   ```

### C. Add Support for New SQL Syntax

If you want to add a new clause (e.g., `HAVING`, `ORDER BY`):

1. **Update grammar** (`core/parser/grammar.lark`):
   ```lark
   select_query: "SELECT" select_list "FROM" identifier (where_clause)? (having_clause)?
   having_clause: "HAVING" condition
   ```

2. **Update transformer** (`core/parser/sql_parser.py`):
   ```python
   def having_clause(self, items):
       return items[0]
   
   def select_query(self, items):
       query = {
           "type": "select_query",
           "select": items[0],
           "from": items[1]
       }
       if len(items) > 2:
           for item in items[2:]:
               if "field" in item and "operator" in item:
                   query["where"] = item
               elif "having" in item:
                   query["having"] = item
       return query
   ```

3. **Handle in engine** (`core/execution/engine.py` `_build_pipeline()`):
   ```python
   if 'having' in query_plan:
       pipeline = HavingOperator(query_plan['having'], next_op=pipeline)
   ```

### D. Support Multiple Input Streams per Query

**Already supported!** Create a query that reads from any stream in the schema:
```json
{
  "name": "multi_stream_alert",
  "input_stream": "pollution_stream",  // Can be any input stream
  "output_stream": "alerts",
  "query": "SELECT * FROM pollution_stream WHERE value > 100"
}
```

Schema ensures stream exists before deploying.

### E. Swap Kafka for Alternative Message Queue

1. Create new client wrapper in `streaming/alternative_client.py`
2. Update `run_system.py` to use new client instead of `StreamConsumer`
3. Maintain same interface: `.send()` and `.receive()` methods

---

## Configuration & Customization

### Kafka Mode (In-Memory vs. Persistent)

Default is **in-memory (ephemeral)**. To enable persistence:
```python
from streaming.kafka_config import KafkaConfig, set_default_config

set_default_config(persistence=True)  # Now messages persist to disk
```

### Window Configuration

Global per schema - all queries share the same:
```json
{
  "window_size": 30,           // Integer time units
  "window_unit": "seconds",    // "seconds" | "minutes" | "hours"
  "velocity": {
    "type": "count",           // "count" or "time"
    "value": 500               // Batch size or time interval
  }
}
```

### Velocity Modes

- **Count-based**: Trigger after N events (currently implemented)
- **Time-based**: Trigger after N seconds (extensible, not yet implemented)

---

## Limitations & Future Enhancements

### Current Limitations (v1.0)
- No stream joins (single input per query)
- GROUP BY partially supported (simplified)
- SLIDING windows simplified (TUMBLING default)
- No query state persistence across restarts
- Single deployment per schema (no query updates without restart)

### Future Enhancements
- Stream join support
- Hot-reload queries without restart
- Advanced state management
- Performance optimization for high-throughput
- Monitoring and metrics collection
- Schema versioning and migration

---

## Quick Reference

### Run the System
```bash
# 1. Start Kafka (see setup.txt)
# 2. Run sensor
python -m sensors.pollution_sensor

# 3. Run system (loads schemas/pollution_schema.json by default)
python -m examples.run_system
```

### Key Files
- **Schema**: `schemas/pollution_schema.json`
- **Schema Manager**: `core/schema/schema_manager.py`
- **Registry**: `core/execution/schema_registry.py`
- **Engine**: `core/execution/engine.py`
- **Operators**: `core/execution/operators.py`
- **Kafka Config**: `streaming/kafka_config.py`
- **Main App**: `examples/run_system.py`

### Example Query Flow
```
JSON Schema → SchemaManager.load() → SchemaRegistry.register()
→ ExecutionEngine.initialize_from_schema()
→ For each query: parse → build operator chain
→ Kafka consumer starts reading input stream
→ Events: stream → FilterOp → WindowOp → AggregateOp → SinkOp → output
```

---

For more details, see `v_1.txt` for release notes and API changes.
