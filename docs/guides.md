# Architecture and System Design

Complete walkthrough of the schema-based architecture and how continuous queries work.

---

## System Overview (End-to-End)

The system operates on a **schema-based model** where all streaming deployments are defined in JSON configuration files.

### Stage 1: Schema Configuration (The Blueprint)

**File:** `schemas/pollution_schema.json` or custom schema file

You define a schema JSON specifying:
- **Input streams**: Where data comes from (Kafka topics)
- **Output streams**: Where results go (Kafka topics)
- **Continuous queries**: Pre-defined queries that run on schedule
- **Window config**: Global window size and time unit for all queries
- **Velocity config**: Event batch size or time-based triggers

Example:
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

**File:** `core/schema/schema_manager.py`

`SchemaManager` loads your JSON schema and validates:
- Structure: streams exist, queries reference correct streams
- Data types, window units, velocity configuration

```python
from core.schema.schema_manager import SchemaManager
manager = SchemaManager()
schema = manager.load_from_file('schemas/pollution_schema.json')
```

### Stage 3: Schema Deployment (The Registry)

**File:** `core/execution/schema_registry.py`

`SchemaRegistry` registers validated schema and creates an `ExecutionEngine` instance. Supports multiple schemas running concurrently.

```python
from core.execution.schema_registry import get_global_registry
registry = get_global_registry()
engine = registry.register_schema(schema)
```

### Stage 4: Query Pipeline Construction (The Operators)

**Files:** `core/execution/engine.py` & `core/execution/operators.py`

For each continuous query, engine builds an operator chain:

1. **JoinOperator** (optional):
   - stream -> stream INNER JOIN (window-aware)
   - stream -> SQLite reference table INNER JOIN
2. **FilterOperator** (optional): Checks WHERE conditions (e.g., `value > 50`)
3. **WindowOperator** + **AggregateOperator** (for aggregate queries)
4. **ProjectionOperator** (for non-aggregate SELECT field lists)
5. **SinkOperator**: Writes results to output stream/table

All queries in the schema share the same window size (defined globally).

Example pipeline for `SELECT sensor_id, AVG(value) FROM stream WHERE value > 50`:
```
Event Input → FilterOperator (value > 50?) → WindowOperator (10s buffer)
→ AggregateOperator (AVG) → SinkOperator (output stream)
```

### Stage 5: Data Ingestion (The Sensor & Kafka)

**Files:** `sensors/pollution_sensor.py`, `streaming/kafka_client.py`

Sensor simulator generates JSON events every second and publishes to Kafka topic (e.g., `pollution_stream`).

Kafka runs in **in-memory mode** by default (ephemeral, no persistence). See: `streaming/kafka_config.py` - `retention.ms=1`.

### Stage 6: Event Consumption & Processing

**File:** `examples/run_system.py`

Background thread runs `StreamConsumer` listening to each input stream topic.
- New events fed to the engine
- Engine routes event through all continuous queries listening on that stream
- Results flow through the operator chain
- Sink outputs results to Kafka output topic

```python
registry.process_event(schema_name, stream_name, event)
```

---

## Schema Configuration Reference

### Basic Structure

```json
{
  "schema_name": "string",
  "window_size": number,
  "window_unit": "seconds|minutes|hours",
  "velocity": {"type": "count|time", "value": number},
  "input_streams": [...],
  "continuous_queries": [...],
  "output_streams": [...]
}
```

### Window and Velocity

Global per schema - all queries share:

```json
{
  "window_size": 30,
  "window_unit": "seconds",
  "velocity": {
    "type": "count",        // or "time"
    "value": 500            // batch size or interval seconds
  }
}
```

- **Count-based**: Trigger after N events
- **Time-based**: Trigger after N seconds

### Input Streams

```json
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
]
```

### Continuous Queries

```json
"continuous_queries": [
  {
    "name": "avg_pollution",
    "input_stream": "pollution_stream",
    "output_stream": "pollution_out",
    "query": "SELECT sensor_id, AVG(value) FROM pollution_stream WHERE value > 50"
  }
]
```

Supported syntax:
- `SELECT` with field lists or aggregates (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`)
- `FROM` single input stream
- `WHERE` optional conditions
- `INNER JOIN` with another stream or reference table

### Output Streams

```json
"output_streams": [
  {
    "name": "pollution_out",
    "topic": "pollution_out",
    "schema": {
      "sensor_id": "STRING",
      "AVG(value)": "FLOAT"
    }
  }
]
```

---

## How to Extend the System

### A. Create a New Continuous Query

Add to your schema JSON:

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

Then reload schema. Engine automatically:
- Parses the query
- Builds operator chain using schema's window_size
- Deploys alongside other queries

### B. Add a New Operator Type

To add custom processing (e.g., `DeduplicateOperator`):

1. **Create operator** in `core/execution/operators.py`:

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

2. **Wire into pipeline** in `core/execution/engine.py` `_build_pipeline()`:

```python
pipeline = DeduplicateOperator(next_op=pipeline)
```

### C. Add New SQL Syntax

To add a new clause (e.g., `HAVING`, `ORDER BY`):

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

### D. Stream-to-Table Join

**Supported:** stream -> table `INNER JOIN` in continuous queries.

```json
{
  "name": "joined_alert",
  "input_stream": "pollution_stream",
  "output_stream": "alerts",
  "query": "SELECT AVG(value), name FROM pollution_stream INNER JOIN sensors ON sensor_id = id"
}
```

Join execution reads rows from `data/static_tables.db`.

Field name collision: right-side (table) fields prefixed as `table_<field>`.

### E. Stream-to-Stream Join

**Supported:** stream -> stream `INNER JOIN` in continuous queries.

If JOIN target matches an input stream name, engine creates stream-stream join pipeline.

```json
{
  "name": "pollution_weather",
  "input_stream": "pollution_stream",
  "output_stream": "joined_out",
  "query": "SELECT sensor_id, value, humidity FROM pollution_stream INNER JOIN weather_stream ON sensor_id = sensor_id"
}
```

Behavior notes:
- Join window uses schema `window_size/window_unit` (processing time)
- Events buffered per stream inside window; matches emit joined events
- Field name collision: right-stream fields prefixed as `right_<field>`

### F. Replace Message Queue

1. Create new client wrapper in `streaming/alternative_client.py`
2. Update `run_system.py` to use new client instead of `StreamConsumer`
3. Maintain same interface: `.send()` and `.receive()` methods

---

## Key Files Reference

- **Schema**: `schemas/pollution_schema.json`
- **Schema Manager**: `core/schema/schema_manager.py`
- **Registry**: `core/execution/schema_registry.py`
- **Engine**: `core/execution/engine.py`
- **Operators**: `core/execution/operators.py`
- **Kafka Config**: `streaming/kafka_config.py`
- **Main App**: `examples/run_system.py`

---

## Limitations & Future Work

### Current Limitations

- GROUP BY partially supported (simplified)
- SLIDING windows simplified (TUMBLING default)
- No query state persistence across restarts
- No hot-reload pipeline orchestration (CLI supports interactive add/query/save)

### Future Enhancements

- Hot-reload queries without restart
- Advanced state management
- Performance optimization for high-throughput
- Monitoring and metrics collection
- Schema versioning and migration
- Stronger schema-time validation for JOIN targets and field compatibility
