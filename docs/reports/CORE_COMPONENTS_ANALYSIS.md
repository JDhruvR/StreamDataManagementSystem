# StreamDataManagementSystem - Core Components Analysis

## System Overview
Stream Data Management System is a schema-first streaming data processing engine that processes real-time event streams using predefined SQL-style continuous queries. It supports stream aggregations, stream-to-stream joins, and stream-to-reference-table joins via Apache Kafka.

---

## 1. MAJOR MODULES AND THEIR RESPONSIBILITIES

### 1.1 Core Schema Module (`core/schema/`)
**Purpose**: Schema loading, validation, and management

#### SchemaManager (`core/schema/schema_manager.py`)
- **Responsibilities**:
  - Load schemas from JSON files or interactive input
  - Validate schema structure (required fields, data types, references)
  - Manage schema lifecycle (load, save, retrieve)
  - Validate stream and query definitions
  
- **Key Methods**:
  - `load_from_file(filepath)` - Load schema from JSON
  - `load_from_input()` - Interactive schema input
  - `validate()` - Full schema validation
  - `get_schema_name()`, `get_window_config()`, `get_input_streams()`, etc.

### 1.2 Execution Engine Module (`core/execution/`)
**Purpose**: Query execution, event processing, and operator pipelines

#### ExecutionEngine (`core/execution/engine.py`)
- **Responsibilities**:
  - Initialize execution pipelines from schema configurations
  - Register input/output streams
  - Deploy continuous queries as operator chains
  - Route incoming events to appropriate query pipelines
  
- **Key Methods**:
  - `initialize_from_schema(schema)` - Build operator pipelines
  - `process_event(stream_name, event)` - Route event through pipelines

#### SchemaRegistry (`core/execution/schema_registry.py`)
- **Responsibilities**:
  - Manage multiple concurrent schemas
  - Deploy, replace, unregister schemas
  - Route events to correct schema's engine
  - Maintain global registry instance (singleton)

### 1.3 Query Parser Module (`core/parser/`)
**Purpose**: Parse and transform SQL queries into execution plans

#### SQL Parser (`core/parser/sql_parser.py`)
- Parse SQL SELECT statements using Lark parser
- Extract query components (SELECT, FROM, WHERE, JOIN, GROUP BY)
- Transform parse tree into query plan dictionaries
- Supported: COUNT, SUM, AVG, MAX, MIN aggregations

### 1.4 Operators Module (`core/execution/operators.py`)
**Purpose**: Data transformation and event processing operators

#### Operator Types:
1. **FilterOperator** - WHERE condition filtering
2. **WindowOperator** - Tumbling/sliding windows
3. **ProjectionOperator** - SELECT column selection
4. **AggregateOperator** - Compute aggregations with SQLite state
5. **StreamStreamJoinOperator** - Join two input streams
6. **JoinOperator** - Join stream with reference table
7. **SinkOperator** - Output to Kafka or callback

### 1.5 Storage Module (`core/storage/`)
**Purpose**: Persistent data storage management

- **TableManager** - SQLite static reference tables
- **ReferenceTableStore** - Manage dimension tables (CRUD operations)

### 1.6 Streaming Module (`streaming/`)
**Purpose**: Apache Kafka integration

- **KafkaConfig** - Configuration (ephemeral mode: 1ms retention)
- **StreamProducer** - Publish to Kafka topics
- **StreamConsumer** - Subscribe and consume from topics

### 1.7 UI Module (`ui/`)
**Purpose**: Web-based dashboard and REST API

- **UIApp** - Flask application with REST endpoints
- **QueryOutputBuffer** - Circular buffer for live data
- **KafkaOutputConsumer** - Background consumer thread

### 1.8 CLI Module (`examples/cli.py`)
**Purpose**: Interactive command-line interface for runtime schema management

Commands: load, status, query, table, ui

### 1.9 Sensors Module (`sensors/`)
**Purpose**: Demo data producers

- pollution_sensor.py
- vehicle_sensor.py
- weather_sensor.py
- signal_sensor.py

---

## 2. DEPENDENCIES BETWEEN COMPONENTS

### Dependency Tree

```
CLI/UI/Examples (top-level entry points)
    ├─→ SchemaManager (load/validate schemas)
    ├─→ SchemaRegistry (manage engines)
    ├─→ ExecutionEngine (query execution)
    │   ├─→ SQL Parser (parse queries)
    │   └─→ Operators (process events)
    │       ├─→ ReferenceTableStore (joins)
    │       └─→ StreamProducer (output)
    ├─→ StreamConsumer (input events)
    ├─→ KafkaConfig (Kafka configuration)
    └─→ ReferenceTableStore (static tables)
```

### Import Graph

- **ExecutionEngine** imports: SQL Parser, all Operators
- **Operators** lazy-import: StreamProducer, ReferenceTableStore
- **CLI** imports: SchemaManager, SchemaRegistry, SQL Parser, ReferenceTableStore, StreamConsumer
- **UI** imports: SchemaManager, SchemaRegistry, KafkaOutputConsumer, DatabaseService
- **Sensors** import: StreamProducer

### External Dependencies
```
kafka-python-ng==2.2.2
lark==1.1.7
pandas==2.2.1
pytest==8.1.1
flask==3.0.0
flask-cors==4.0.0
plotly==5.17.0
```

---

## 3. INTERFACES/CONTRACTS BETWEEN COMPONENTS

### Operator Interface
```python
class Operator(ABC):
    @abstractmethod
    def process(self, event):
        """Process event(s)"""
        pass
```

### Schema Contract (JSON)
```json
{
  "schema_name": "string",
  "window_size": number,
  "window_unit": "seconds|minutes|hours",
  "window_type": "tumbling|sliding",
  "velocity": {"type": "count|time", "value": number},
  "input_streams": [{"name": str, "topic": str, "schema": {...}}],
  "output_streams": [{"name": str, "topic": str, "schema": {...}}],
  "continuous_queries": [{"name": str, "input_stream": str, "output_stream": str, "query": str}]
}
```

### Event Contract
```python
# Input/Output events are JSON dicts
event = {
    "field1": value1,
    "field2": value2,
    ...
}
```

### Query Plan Contract
```python
query_plan = {
    'type': 'select_query',
    'from': 'stream_name',
    'select': ['*'] or [col_names],
    'where': {'field': 'col', 'operator': '>', 'value': val},
    'join': {'join_type': 'INNER', 'table': 'name', ...},
    'group_by': ['col1', 'col2']
}
```

---

## 4. INITIALIZATION ORDER AND BOOTSTRAP SEQUENCE

### System Startup (run_system.py)

```
1. Load Schema
   └─→ SchemaManager.load_from_file()
   └─→ SchemaManager.validate()

2. Deploy Schema
   └─→ registry.register_schema(schema)
   └─→ ExecutionEngine.initialize_from_schema()
       ├─→ Register input streams
       ├─→ Register output streams
       └─→ Deploy continuous queries
           ├─→ parse_sql(query)
           └─→ _build_pipeline() → chain operators

3. Start Kafka Consumers
   └─→ For each input_stream:
       ├─→ Thread(run_kafka_consumer)
       ├─→ StreamConsumer(topic, group_id)
       └─→ registry.process_event(schema_name, stream_name, event)

4. Main Loop
   └─→ Keep main thread alive
   └─→ Consumers run in background
   └─→ Graceful shutdown on Ctrl+C
```

### Query Pipeline Chain Example

For: `SELECT sensor_id, AVG(value) FROM pollution_stream WHERE value > 50 GROUP BY sensor_id`

```
Event (raw)
    ↓
[FilterOperator] WHERE value > 50
    ↓ (if pass)
[WindowOperator] Groups into 10-second windows
    ↓ (window emits)
[AggregateOperator] Computes AVG(value), groups by sensor_id
    ↓
[SinkOperator] Writes to pollution_out topic
    ↓
Kafka Output Topic
```

### CLI Bootstrap

```
1. Parse arguments
2. Initialize SchemaManager, SchemaRegistry, ReferenceTableStore
3. REPL loop:
   a. "load <file>" → register schema
   b. "status" → list schemas
   c. "query <sql>" → deploy ad-hoc query
   d. "table" → manage reference tables
   e. "ui" → launch Flask app
```

### UI Bootstrap

```
1. Create Flask app
2. Initialize QueryOutputBuffer, KafkaOutputConsumer, DatabaseService
3. If SDMS_ACTIVE_SCHEMA_PATH set:
   └─→ Load and register schema
4. Register Flask routes
5. Start background KafkaOutputConsumer thread
6. Run Flask on 127.0.0.1:5000
```

---

## 5. CONFIGURATION AND ENVIRONMENT SETUP

### Environment Variables

```
SDMS_KAFKA_BROKER          # Default: localhost:9092
SDMS_UI_HOST               # Default: 127.0.0.1
SDMS_UI_PORT               # Default: 5000
SDMS_UI_DEBUG              # Default: False
SDMS_ACTIVE_SCHEMA_PATH    # Path to active schema JSON
SDMS_LOG_LEVEL             # Default: INFO
SDMS_SQLITE_DB             # Default: data/static_tables.db
```

### Database Configuration

- **Aggregate State**: `data/aggregate_states.db` (SQLite)
- **Reference Tables**: `data/static_tables.db` (SQLite)
- **Kafka**: localhost:9092 (ephemeral mode, 1ms retention)

### Data Types Supported

```
STRING      TEXT values
INT         Integer values
FLOAT       Floating-point values
TIMESTAMP   Date/time values
BOOLEAN     True/False values
```

---

## 6. COMPONENT INTERACTION GUIDE

### Event Processing Flow

```
Kafka Topic
    ├─→ StreamConsumer
    ├─→ JSON deserialize
    ├─→ registry.process_event()
    ├─→ engine.process_event()
    └─→ for each query:
        ├─→ [Filter] WHERE condition
        ├─→ [Window] Buffer & emit
        ├─→ [Aggregate] Compute metrics
        ├─→ [Join] Match with other stream/table
        ├─→ [Projection] Select columns
        └─→ [Sink] Output to Kafka topic
```

### Stream-to-Stream Join

```
Stream1 Event               Stream2 Event
    ├─→ StreamStreamJoinOperator ←┤
        ├─→ Match on condition
        ├─→ Buffer event
        ├─→ Join all at window close (tumbling)
        ├─→ Join immediately (sliding)
        ├─→ Merge events
        └─→ Emit to Sink
```

### Stream-to-Table Join

```
Stream Event
    ├─→ JoinOperator
    ├─→ Extract join field value
    ├─→ Query reference table
    ├─→ For each matching row: merge & emit
    └─→ If no matches: no output (INNER JOIN)
```

### Aggregation State Management

```
Window of Events
    ├─→ Partition by GROUP BY fields
    ├─→ For each group:
    │   ├─→ Load previous state from SQLite
    │   ├─→ Compute local aggregates
    │   ├─→ Merge with previous state
    │   ├─→ Store merged state in SQLite
    │   └─→ Emit result
    └─→ Output to Sink
```

---

## 7. ERROR HANDLING AND RECOVERY

| Error Type | Location | Recovery |
|-----------|----------|----------|
| Schema Validation | SchemaManager.validate() | User fixes JSON |
| Query Parsing | parse_sql() | User fixes SQL |
| Kafka Connection | StreamConsumer/Producer | Start Kafka & retry |
| Stream Not Found | ExecutionEngine._deploy_continuous_query() | Fix stream name |
| Join Condition | Operator.process() | Log error, skip event |
| Database | SQLite operations | Log error, continue |

---

## 8. LIFECYCLE AND SHUTDOWN

### Startup Sequence
1. Load schema (validate structure)
2. Deploy schema (build engine)
3. Start consumer threads (listen for events)
4. Maintain running state

### Graceful Shutdown
1. Signal all consumer threads
2. Close Kafka consumers
3. Close database connections
4. Exit process

### Schema Redeployment
- CLI: `load new_schema.json`
- Registry: `replace_schema()` updates engine
- New queries start immediately
- Old queries remain active

---

## Summary: Component Responsibilities

| Component | Type | Purpose | Key Responsibility |
|-----------|------|---------|-------------------|
| SchemaManager | Manager | Schema lifecycle | Load, validate, persist |
| ExecutionEngine | Engine | Query execution | Build pipelines, route events |
| SchemaRegistry | Registry | Multi-schema state | Manage concurrent engines |
| SQL Parser | Parser | Query analysis | Transform SQL to plan |
| Operators | Pipeline | Data transformation | Filter, window, aggregate, join |
| Kafka Client | Connector | Messaging | Produce/consume events |
| Reference Tables | Storage | Dimension data | Persist and query joins |
| UI App | Web | Dashboard | Display results, manage schemas |
| CLI | Interface | User interaction | Command loop, runtime deployment |

---

## Architecture Diagram

```
USER INTERFACES
├─ CLI (examples/cli.py)
├─ Web UI (ui/app.py)
└─ Sensors (sensors/*.py)

SCHEMA & REGISTRY LAYER
├─ SchemaManager
└─ SchemaRegistry (singleton)

QUERY EXECUTION LAYER
├─ ExecutionEngine
├─ SQL Parser
└─ Operator Pipeline
   ├─ SinkOperator
   ├─ AggregateOperator
   ├─ WindowOperator
   ├─ FilterOperator
   ├─ StreamStreamJoinOperator/JoinOperator
   └─ ProjectionOperator

STORAGE & STREAMING LAYER
├─ StreamProducer (Kafka output)
├─ StreamConsumer (Kafka input)
├─ KafkaConfig
├─ ReferenceTableStore (SQLite)
└─ TableManager (SQLite)

EXTERNAL
└─ Apache Kafka Broker (localhost:9092)
```

