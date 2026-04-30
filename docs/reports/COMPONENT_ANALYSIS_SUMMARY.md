# StreamDataManagementSystem - Component Analysis Summary

**Date**: April 30, 2026  
**Analysis Type**: Core Components, Dependencies, Interfaces, Initialization, Configuration

---

## Quick Reference

### Files Generated
1. **CORE_COMPONENTS_ANALYSIS.md** - Detailed responsibilities and roles of each component
2. **DEPENDENCY_MAP.md** - Complete dependency graph, dataflow, and interaction sequences

### System Architecture Overview

**Stream Data Management System** is a schema-first streaming engine that:
- Processes real-time events from Apache Kafka using predefined SQL-style queries
- Supports aggregations, stream-to-stream joins, and stream-to-table joins
- Manages multiple concurrent schemas with independent execution engines
- Provides web UI and CLI for runtime schema deployment

---

## 1. NINE MAJOR COMPONENTS (Roles & Responsibilities)

| Component | Module | Purpose | Key Responsibility |
|-----------|--------|---------|------------------|
| **SchemaManager** | `core/schema/` | Schema lifecycle management | Load from JSON, validate structure, persist |
| **ExecutionEngine** | `core/execution/` | Query pipeline execution | Build operator chains, route events |
| **SchemaRegistry** | `core/execution/` | Multi-schema state management | Manage concurrent engines (singleton) |
| **SQL Parser** | `core/parser/` | Query parsing & transformation | Parse SQL → query plan (uses Lark) |
| **Operators** | `core/execution/` | Data transformation pipeline | Filter, window, aggregate, join, output |
| **Kafka Client** | `streaming/` | Message broker integration | Produce/consume events (kafka-python-ng) |
| **ReferenceTableStore** | `core/storage/` | Dimension table persistence | CRUD operations on SQLite |
| **UI App** | `ui/` | Web dashboard & REST API | Display results, manage schemas (Flask) |
| **CLI** | `examples/` | Interactive command interface | Runtime schema deployment |

---

## 2. DEPENDENCY STRUCTURE

### Hierarchy (Call Graph)

```
USER INTERFACES (CLI, UI, Sensors)
    ↓
SCHEMA & REGISTRY (SchemaManager, SchemaRegistry)
    ↓
EXECUTION ENGINE (ExecutionEngine, SQL Parser)
    ↓
OPERATORS (Filter, Window, Aggregate, Join, Sink)
    ↓
STORAGE & STREAMING (Kafka, SQLite, ReferenceTableStore)
    ↓
EXTERNAL SYSTEMS (Kafka Broker, SQLite Files)
```

### Import Dependencies

**Key imports flow**:
- CLI/UI → SchemaManager, SchemaRegistry, SQL Parser, Kafka
- ExecutionEngine → SQL Parser, All Operators
- Operators → ReferenceTableStore, StreamProducer (lazy import)
- Sensors → StreamProducer

**External deps**: kafka-python-ng, lark, flask, pandas, plotly

---

## 3. CORE INTERFACES & CONTRACTS

### 1. Operator Interface (ABC)
```python
class Operator(ABC):
    @abstractmethod
    def process(self, event):
        """Process event(s) through pipeline"""
```

### 2. Schema Contract (JSON)
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

### 3. Event Contract
- Input events: JSON dict from Kafka topic
- Output events: JSON dict written to output topic
- Schema: Defined in stream configuration

### 4. Query Plan Contract
```python
{
  'type': 'select_query',
  'from': 'stream_name',
  'select': [...] or '*',
  'where': {...} or None,
  'join': {...} or None,
  'group_by': [...] or None
}
```

### 5. Singleton Patterns
- **get_global_registry()** → SchemaRegistry (one per system)
- **get_default_config()** → KafkaConfig (one per system)

---

## 4. INITIALIZATION & BOOTSTRAP SEQUENCES

### System Startup (run_system.py)

```
1. Schema Loading
   └─→ SchemaManager.load_from_file()
   └─→ SchemaManager.validate()

2. Schema Deployment  
   └─→ registry.register_schema(schema_dict)
   └─→ ExecutionEngine.initialize_from_schema()
       ├─→ Register input/output streams
       ├─→ For each continuous_query:
       │   ├─→ parse_sql(query_text)
       │   └─→ _build_pipeline(query_plan)
       │       └─→ Chain operators (next_op pointers)

3. Start Kafka Consumers
   └─→ For each input_stream:
       ├─→ Thread(run_kafka_consumer)
       ├─→ StreamConsumer(topic, group_id)
       └─→ registry.process_event()

4. Main Loop
   └─→ Keep processes alive
   └─→ Graceful shutdown on Ctrl+C
```

### Query Pipeline Example

For: `SELECT sensor_id, AVG(value) FROM pollution_stream WHERE value > 50 GROUP BY sensor_id`

```
Raw Event → [Filter WHERE value > 50]
         → [Window 10 sec (tumbling)]
         → [Aggregate AVG by sensor_id]
         → [Sink to pollution_out topic]
         → Output Kafka Topic
```

### CLI Bootstrap

```
1. Initialize SchemaManager, SchemaRegistry, ReferenceTableStore
2. REPL loop:
   - "load <file>" → register schema
   - "query <sql>" → deploy ad-hoc query
   - "status" → list schemas
   - "table" → manage reference tables
   - "ui" → launch Flask app
```

---

## 5. CONFIGURATION & ENVIRONMENT

### Environment Variables
```
SDMS_KAFKA_BROKER              Default: localhost:9092
SDMS_UI_HOST                   Default: 127.0.0.1
SDMS_UI_PORT                   Default: 5000
SDMS_ACTIVE_SCHEMA_PATH        Path to active schema JSON
SDMS_SQLITE_DB                 Default: data/static_tables.db
SDMS_UI_DEBUG                  Default: False
SDMS_LOG_LEVEL                 Default: INFO
```

### Database Locations
- **Aggregate State**: `data/aggregate_states.db` (SQLite)
- **Reference Tables**: `data/static_tables.db` (SQLite)
- **Kafka**: localhost:9092 (ephemeral, 1ms retention)

### Data Types
STRING | INT | FLOAT | TIMESTAMP | BOOLEAN

---

## 6. EVENT PROCESSING FLOW

### From Kafka to Output

```
Input Event
    ├─→ StreamConsumer.next() [Kafka]
    ├─→ JSON deserialize
    ├─→ registry.process_event(schema_name, stream_name, event)
    ├─→ engine.process_event(stream_name, event)
    └─→ For each query listening on stream_name:
        ├─→ [1] StreamStreamJoinOperator (optional)
        ├─→ [2] FilterOperator (WHERE condition)
        ├─→ [3] WindowOperator (buffer & emit)
        ├─→ [4] AggregateOperator (compute metrics)
        ├─→ [5] JoinOperator (stream-to-table join)
        ├─→ [6] ProjectionOperator (SELECT columns)
        └─→ [7] SinkOperator (output to Kafka)
```

### Join Types

**Stream-to-Stream Join**:
- Buffers events from both streams in window
- Matches on join condition
- Merges matched events (prefixed duplicates: right_col)
- Emits at window close (tumbling) or immediately (sliding)

**Stream-to-Table Join**:
- Queries reference table in SQLite
- For each matching row: merge and emit
- INNER JOIN: no output if no matches

### Aggregation with State

```
Group events by GROUP BY fields
    ├─→ Load previous state from SQLite
    ├─→ Compute local aggregates
    ├─→ Merge with previous state
    ├─→ Store merged state in SQLite
    └─→ Emit result
```

---

## 7. OPERATOR CHAIN ARCHITECTURE

### 7 Operator Types

1. **FilterOperator** - WHERE condition (>, <, =, >=, <=, !=)
2. **WindowOperator** - Tumbling/sliding windows with velocity control
3. **ProjectionOperator** - SELECT column filtering
4. **AggregateOperator** - SUM, AVG, MAX, MIN, COUNT with GROUP BY
5. **StreamStreamJoinOperator** - Join two input streams
6. **JoinOperator** - Join stream with reference table
7. **SinkOperator** - Output to Kafka or callback

### Operator Chaining Pattern

```
pipeline = SinkOperator(...)
pipeline = AggregateOperator(..., next_op=pipeline)
pipeline = WindowOperator(..., next_op=pipeline)
pipeline = FilterOperator(..., next_op=pipeline)

query['pipeline'] = pipeline

# Processing:
pipeline.process(event)  # Calls first operator
```

---

## 8. ERROR HANDLING & RECOVERY

| Error | Location | Recovery |
|-------|----------|----------|
| SchemaValidationError | SchemaManager.validate() | User fixes JSON |
| Lark ParseError | parse_sql() | User fixes SQL |
| Kafka ConnectionError | StreamConsumer/Producer | Start Kafka, retry |
| Stream Not Found | ExecutionEngine._deploy_continuous_query() | Fix stream name |
| Join Condition Error | Operator.process() | Log, skip event |
| SQLite Error | Operator.process() | Log, continue |

---

## 9. CONCURRENCY MODEL

### Threading

```
Main Thread (CLI/run_system.py)
    ├─→ REPL loop (CLI) or main loop (run_system.py)
    ├─→ Spawns consumer threads (one per input stream)
    └─→ Handles graceful shutdown

Consumer Threads (one per input stream)
    └─→ StreamConsumer.next() [blocking]
    └─→ registry.process_event() [blocking]

UI Background Thread (optional)
    ├─→ Flask main thread (HTTP requests)
    └─→ KafkaOutputConsumer thread (daemon)
        └─→ StreamConsumer.next() [blocking]
```

### Synchronization

- StreamConsumer.next(): Blocking Kafka poll
- SQLite: `check_same_thread=False` (multi-threaded access safe)
- QueryOutputBuffer: Thread-safe queue operations
- Global registry: Shared read-only after initialization

---

## 10. STATE PERSISTENCE

### SQLite Storage

```
Aggregate State DB (data/aggregate_states.db)
    └─→ Per query: agg_state_{query_name}
        ├─→ group_key TEXT PRIMARY KEY (JSON of GROUP BY values)
        └─→ state_data TEXT (JSON {SUM_X_sum, AVG_Y_count, ...})

Reference Tables DB (data/static_tables.db)
    └─→ User-created tables: sensors, vehicles, ...
        └─→ Columns as defined via CLI

Join State DB (data/stream_stream_join.db - dynamic)
    └─→ Join buffers for stream-to-stream joins
```

### In-Memory State

```
WindowOperator
    ├─→ buffer: deque of (timestamp, event) tuples
    ├─→ window_start_time: float
    └─→ events_since_emit: int

StreamStreamJoinOperator
    ├─→ left_buffer: deque
    ├─→ right_buffer: deque
    └─→ window_start_time: float
```

---

## 11. KEY DESIGN DECISIONS

### Schema-First Approach
- All queries defined upfront in JSON schema
- No ad-hoc query deployment (except CLI)
- Window/velocity global per schema, not per query

### Ephemeral Kafka Mode
- 1ms message retention (design choice for demo)
- Messages consumed immediately or lost
- No persistence layer exposed

### Operator Pipeline Pattern
- Chain of Responsibility design pattern
- Each operator independent, composable
- Backward chaining for initialization (end→start)

### Singleton Registry
- One global SchemaRegistry per system
- Multiple schemas can run concurrently
- Thread-safe after initialization

---

## 12. QUICK START PATHS

### Path 1: System Mode (run_system.py)
```bash
python examples/run_system.py
# Loads schema, starts consumers, processes continuously
```

### Path 2: CLI Mode (examples/cli.py)
```bash
python -m examples.cli
sdms> load schemas/pollution2.json
sdms> status
sdms> query SELECT sensor_id, AVG(value) FROM pollution_stream WHERE value > 50
sdms> ui
# Launches Flask dashboard
```

### Path 3: Sensor + UI
```bash
# Terminal 1: Sensor producer
python -m sensors.pollution_sensor

# Terminal 2: CLI with schema deployment
python -m examples.cli
sdms> load schemas/pollution2.json

# Terminal 3: Web UI
./run_ui.sh  # Opens http://127.0.0.1:5000
```

---

## 13. FILE LOCATIONS

```
StreamDataManagementSystem/
├── core/
│   ├── schema/              ← SchemaManager
│   ├── execution/           ← ExecutionEngine, SchemaRegistry, Operators
│   ├── parser/              ← SQL Parser & Lark grammar
│   └── storage/             ← ReferenceTableStore, TableManager
├── streaming/               ← Kafka integration (Producer/Consumer/Config)
├── ui/                      ← Flask app, buffer, consumer
├── sensors/                 ← Demo data producers
├── examples/                ← CLI & run_system entry points
├── schemas/                 ← Example JSON schema files
├── data/                    ← SQLite databases (created at runtime)
├── tests/                   ← Pytest test suite
├── CORE_COMPONENTS_ANALYSIS.md    ← Main documentation
├── DEPENDENCY_MAP.md               ← Interaction sequences
└── README.md                       ← Setup & quickstart
```

---

## 14. TESTING THE SYSTEM

```bash
# Unit tests
pytest -q

# Integration tests (requires Kafka running)
# Terminal 1: Zookeeper
cd kafka_2.13-3.6.1
./bin/zookeeper-server-start.sh config/zookeeper.properties

# Terminal 2: Kafka
./bin/kafka-server-start.sh config/server.properties

# Terminal 3: Sensor
python -m sensors.pollution_sensor

# Terminal 4: CLI
python -m examples.cli
load schemas/pollution2.json

# Terminal 5: View output
./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic pollution_out
```

---

## 15. EXTENSION POINTS

### Add New Operator
1. Create class inheriting from `Operator`
2. Implement `process(self, event)` method
3. Update `ExecutionEngine._build_pipeline()` to instantiate
4. Update `grammar.lark` if new SQL syntax needed

### Add New Data Source
1. Create producer class in `sensors/` or `streaming/`
2. Use `StreamProducer.send(topic, event)` to emit
3. Register topic in schema's `input_streams`

### Add New Storage Backend
1. Create class with `create_table()`, `insert()`, `query()` methods
2. Update `ReferenceTableStore` or create new storage class
3. Update operators to use new backend

---

## Summary

**StreamDataManagementSystem** is a well-architected streaming engine with:
- **Clear separation of concerns** (schema, execution, storage, streaming)
- **Extensible operator pipeline** (chain of responsibility)
- **Multi-schema capability** (singleton registry)
- **Complete bootstrap sequence** (schema → engine → queries → processing)
- **Comprehensive state management** (SQLite + in-memory buffers)
- **User-friendly interfaces** (CLI + Web UI)

The system is production-ready for streaming analytics scenarios requiring SQL-style transformations and joins.

---

**For detailed analysis, see**:
- `CORE_COMPONENTS_ANALYSIS.md` - Component roles and responsibilities
- `DEPENDENCY_MAP.md` - Dependency graph and interaction sequences

