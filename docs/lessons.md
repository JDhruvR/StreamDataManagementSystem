# History, Decisions, and Features

Complete history of StreamDataManagementSystem development, design decisions made, and current feature set.

---

## Release History

### v1.0 - Schema-Based Foundation

**Features Added:**

1. **Schema-Based Configuration**
   - JSON schema files define all aspects of streaming deployment
   - Schema includes input streams, output streams, and continuous queries
   - Load schemas from files or interactive terminal input
   - Save and validate schemas before deployment
   - Location: `core/schema/schema_manager.py`

2. **Multiple Schemas Support**
   - Run multiple schemas concurrently in same engine
   - Each schema has isolated input/output streams and queries
   - No cross-schema interference or naming conflicts
   - SchemaRegistry manages schema lifecycle
   - Location: `core/execution/schema_registry.py`

3. **Continuous Queries (Pre-Defined Only)**
   - Queries declared in schema, not ad-hoc
   - Query maps input stream → processing pipeline → output stream
   - Single query: one input stream, one output stream
   - Window size and velocity fixed per schema (apply to all queries)
   - No per-query window parameters

4. **Kafka In-Memory Mode (Default)**
   - Messages ephemeral by default (`retention.ms=1`)
   - No persistence to disk
   - Aligns with philosophy of not saving input streams
   - Configurable: can enable persistence if needed
   - Location: `streaming/kafka_config.py`

5. **Stream Management**
   - Input streams registered independently
   - Streams without queries supported (can dump data)
   - Multiple input/output streams per schema
   - Events routed by stream name, not query name

6. **Velocity Configuration**
   - Flexible velocity type (count-based, time-based extensible)
   - Defined per schema in JSON: `{"type": "count", "value": 100}`
   - Applies globally to all queries in schema

**Features Removed / Changed:**

1. **Ad-Hoc Query Support Removed**
   - No more inline query execution via `run_system.py`
   - `ExecutionEngine.execute_query()` removed
   - `ExecutionEngine.handle_statement()` removed
   - Queries must be pre-defined in schema

2. **Per-Query Window Parameters Removed**
   - Grammar: WINDOW clause removed from SELECT statement
   - Window size/unit no longer inline with query
   - All queries in schema share schema-level window config
   - `core/parser/grammar.lark` updated

3. **Hardcoded DDL Removed**
   - No more DDL in Python code
   - Stream and table definitions moved to schema JSON
   - `examples/run_system.py` refactored to load schemas

4. **Kafka Persistence Disabled by Default**
   - Previous: Messages persisted to disk by default
   - Current: `retention.ms=1` (ephemeral)
   - Consistent with stream input philosophy

**API Changes (v1.0):**

ExecutionEngine:
- REMOVED: `execute_query(query_plan, output_callback)`
- REMOVED: `handle_statement(sql_text, callback)`
- REMOVED: `execute_ddl(statement)`
- NEW: `initialize_from_schema(schema)`
- CHANGED: `process_event(source_stream, event)` now requires stream_name, validates stream exists

Parser (Grammar):
- REMOVED: `window_clause: "WINDOW" window_type "(" literal literal_unit ")"`
- Query syntax: `SELECT select_list FROM identifier [WHERE condition]`
- No window parameters inline

StreamProducer/StreamConsumer:
- ADDED parameter: `config` (KafkaConfig instance)
- BACKWARDS COMPATIBLE: `bootstrap_servers` still works

### v1.1 - Interactive Deployment & Joins

**New Features:**

1. **Interactive CLI for Runtime Deployment**
   - Create/load schema
   - Create input streams at runtime
   - Deploy continuous SELECT queries
   - Manage persistent tables (SQLite)
   - Run stream-to-table INNER JOIN
   - Location: `examples/cli.py`

2. **Persistent Schema Updates (SQLite)**
   - Schemas saved to JSON files
   - Schemas can be reloaded/replaced with same name
   - Table CRUD operations on persistent SQLite database
   - Location: `data/static_tables.db`

3. **Stream-to-Table INNER JOIN**
   - Join stream with persistent reference table
   - Query pattern: `SELECT ... FROM stream INNER JOIN table ON stream_field = table_field [WHERE ...]`
   - Table data read from `data/static_tables.db`
   - Field collision handled with `table_` prefix

4. **Stream-to-Stream INNER JOIN** (NEW)
   - Join two input streams in same schema
   - Window-aware processing-time buffers
   - Query pattern: `SELECT ... FROM stream1 INNER JOIN stream2 ON field1 = field2 [WHERE ...]`
   - Field collision handled with `right_` prefix

5. **Query Projection Fixes**
   - Non-aggregate SELECT now properly projects field lists
   - Output schema inference from field selections

6. **Time-Based Velocity Triggering** (NEW)
   - Count mode: emit after N events
   - Time mode: emit every T seconds
   - Window operator implements both triggering strategies
   - Configured per schema

**File Structure (v1.1):**

NEW DIRECTORIES:
- `core/schema/` - Schema management
- `schemas/` - Schema configuration files

NEW FILES:
- `core/schema/schema_manager.py` - Schema loading/validation
- `core/execution/schema_registry.py` - Multi-schema management
- `streaming/kafka_config.py` - Kafka configuration
- `schemas/pollution_schema.json` - Example schema
- `examples/cli.py` - Interactive CLI
- `tests/test_stream_stream_join.py` - Stream join tests
- `tests/test_window_velocity.py` - Velocity semantics tests

MODIFIED FILES:
- `core/execution/engine.py` - Schema-based, multi-input routing
- `core/execution/operators.py` - Join operators, velocity-aware window
- `core/parser/grammar.lark` - Removed WINDOW clause
- `core/parser/sql_parser.py` - Updated transformer
- `streaming/kafka_client.py` - Added config parameter

---

## Architecture Principles Applied

1. **Single-Responsibility Operators**
   - Each operator owns one processing stage: filter, projection, window, aggregate, join, sink
   - Clear contracts between operators
   - Composable pipeline construction

2. **Deterministic Event Routing**
   - Engine routes events by registered stream membership per deployed query
   - No ad-hoc routing; all queries pre-defined in schema
   - Predictable behavior across multiple schemas

3. **Schema-Level Invariants**
   - Window and velocity are global per schema for consistent query behavior
   - All queries in schema share same window size/unit
   - Simplifies reasoning about timing and batching

4. **Explicit Contracts**
   - Parser emits structured plans (type + fields)
   - Engine maps plan type to concrete pipeline topology
   - No implicit behavior; all processing stages visible in code

5. **Prefix-Based Collision Handling**
   - Right-side stream fields use stable prefixes: `right_<field>`
   - Reference table fields use stable prefixes: `table_<field>`
   - Predictable naming for field access in downstream systems

---

## Design Decisions

### Decision 1: No Ad-Hoc Queries

**Rationale:** Pre-defined schema-based queries enable:
- Reproducible deployments (schema is versioned)
- No runtime parsing errors (validated at load time)
- Consistent metrics and monitoring per schema
- Simplified query lifecycle management

**Trade-off:** Less flexibility vs. more predictability.

### Decision 2: Global Window Per Schema

**Rationale:** Simpler semantics and implementation:
- All queries in schema process same time windows
- No inter-query window coordination needed
- Easier to reason about event ordering
- Single configuration point per deployment

**Trade-off:** Less granular control vs. easier understanding.

### Decision 3: Ephemeral Kafka (No Persistence by Default)

**Rationale:** Aligns with streaming principles:
- Input streams are transient data sources
- Persistent state kept in reference tables (SQLite), not Kafka
- Reduces disk I/O and storage overhead
- Simplifies queue configuration (one mode, not two)

**Trade-off:** Cannot replay input stream history unless explicitly captured.

### Decision 4: Stream-Stream Join Uses Processing Time

**Rationale:** Practical for real-time use cases:
- Processing time windows are deterministic (clock-based)
- No need for complex watermarking or late arrivals
- Matches window operator's existing timing model
- Events buffered per stream inside window duration

**Trade-off:** Event-time join semantics not supported (future enhancement).

### Decision 5: Prefix-Based Field Collision Resolution

**Rationale:** Stable, predictable, simple:
- No user configuration needed for join output schema
- Field names consistent across runs
- Clear origin of fields in result

**Trade-off:** User cannot customize output names (future enhancement).

---

## Completed Engineering Work

### Execution Engine
- Multi-input query routing for stream-stream joins
- Schema-based initialization and lifecycle management
- Event-to-query routing by stream membership

### Stream-Stream Join Operator
- Processing-time bounded buffers per stream
- Join key matching with configurable columns
- Collision prefix handling (right_*)

### Window Operator Enhancements
- Velocity-aware triggering (count + time modes)
- Proper emission timing for both aggregate and non-aggregate queries

### CLI Features
- Interactive schema creation
- Runtime query deployment
- Persistent table management (CRUD)
- Schema reload/replace support
- Output schema inference for joins

### Testing
- Window and velocity semantics (`tests/test_window_velocity.py`)
- Stream-stream join operator (`tests/test_stream_stream_join.py`)
- Engine routing for multi-input queries (`tests/test_engine_stream_stream_join.py`)
- CLI schema inference (`tests/test_cli_schema_inference.py`)

---

## Open Engineering Items (Priority Order)

1. **Add Schema-Time Validation for JOINs**
   - Validate JOIN target exists (stream or table)
   - Validate join key field compatibility
   - Fail fast on schema load, not at runtime

2. **Add Configurable Output Aliasing**
   - User-defined names for joined fields
   - Alternative to fixed `table_` / `right_` prefixes
   - Schema-level alias configuration

3. **Expand Integration Tests**
   - Mixed aggregate + join + filter plans
   - Multi-stream scenarios with complex WHERE clauses
   - Kafka consumer group management

4. **Add Explicit Resource Cleanup**
   - Lifecycle controls for consumer threads
   - Proper shutdown of operators
   - Buffer management for stream-stream joins

---

## Known Limitations

- GROUP BY only partially supported (simplified)
- SLIDING windows simplified (TUMBLING default)
- No query state persistence across restarts
- No hot-reload pipeline orchestration (CLI supports interactive add/query/save)
- Event-time joins not supported (processing time only)
- No schema versioning or migration tools

---

## Testing Status

Test suite: `pytest -q`

Currently passing (7 tests):
- Window velocity semantics
- Stream-stream join operator behavior
- Engine routing for stream-stream joins
- CLI schema inference for joins
- Basic integration scenarios

---

## Future Enhancements

1. **Hot-Reload Queries**
   - Change schema without restart
   - Graceful query transition

2. **Advanced State Management**
   - Stateful operators for complex logic
   - State snapshots for recovery

3. **Performance Optimization**
   - High-throughput buffer management
   - Operator parallelization

4. **Monitoring & Metrics**
   - Event flow tracking
   - Latency histograms
   - Query throughput

5. **Schema Versioning**
   - Version-aware deployments
   - Breaking change detection
   - Migration scripts

6. **Event-Time Processing**
   - Watermark support
   - Late arrival handling
   - Session windows

---

## Feature Summary (Current)

### Core Capabilities

| Feature | Status | Notes |
|---------|--------|-------|
| Schema-based configuration | ✓ Complete | JSON format, file-based |
| Continuous SELECT queries | ✓ Complete | Pre-defined, no ad-hoc |
| WHERE filtering | ✓ Complete | Basic conditions |
| Aggregations (COUNT, SUM, AVG, MIN, MAX) | ✓ Complete | Window-aware |
| Projections (SELECT field list) | ✓ Complete | Non-aggregate queries |
| Stream-to-Stream INNER JOIN | ✓ Complete | Processing-time windows |
| Stream-to-Table INNER JOIN | ✓ Complete | SQLite reference tables |
| Persistent Reference Tables | ✓ Complete | SQLite, CRUD via CLI |
| Multiple Schemas (Concurrent) | ✓ Complete | Isolated deployments |
| Interactive CLI | ✓ Complete | Schema/query/table management |
| Count-Based Velocity | ✓ Complete | Emit after N events |
| Time-Based Velocity | ✓ Complete | Emit every T seconds |
| In-Memory Kafka | ✓ Complete | Ephemeral by default |

### Query Support

| Feature | Status | Example |
|---------|--------|---------|
| SELECT * | ✓ | `SELECT * FROM stream` |
| SELECT fields | ✓ | `SELECT id, value FROM stream` |
| SELECT aggregates | ✓ | `SELECT id, AVG(value) FROM stream` |
| WHERE conditions | ✓ | `... WHERE value > 50` |
| INNER JOIN (stream-stream) | ✓ | `... JOIN weather ON sensor_id = sensor_id` |
| INNER JOIN (stream-table) | ✓ | `... JOIN sensors ON id = id` |
| GROUP BY | ⚠ Partial | Simplified implementation |
| SLIDING windows | ⚠ Simplified | Uses TUMBLING by default |

### Runtime Management

| Feature | Status |
|---------|--------|
| Load schema from file | ✓ Complete |
| Create schema interactively | ✓ Complete |
| Deploy query at runtime | ✓ Complete |
| Save schema to file | ✓ Complete |
| Replace active schema | ✓ Complete |
| Table create/read/update/delete | ✓ Complete |
| Consumer topic monitoring | ✓ Complete |
| Schema validation | ✓ Complete |

### Data Types

Supported: STRING, FLOAT, INT, BOOLEAN, DOUBLE

### Kafka Integration

- Multiple input streams per schema
- Multiple output streams per schema
- Topic-to-stream binding
- Ephemeral message retention
- Bootstrap server configuration

---

## File Organization

```
StreamDataManagementSystem/
├── README.md                          # Project overview
├── requirements.txt                   # Python dependencies
├── docs/
│   ├── setup/                        # Installation & running guides
│   │   └── README.md
│   ├── guides/                       # Architecture & extension
│   │   └── README.md
│   └── lessons/                      # History & decisions
│       └── README.md (this file)
├── core/
│   ├── schema/
│   │   └── schema_manager.py
│   ├── execution/
│   │   ├── engine.py
│   │   ├── operators.py
│   │   └── schema_registry.py
│   └── parser/
│       ├── grammar.lark
│       └── sql_parser.py
├── streaming/
│   ├── kafka_client.py
│   └── kafka_config.py
├── sensors/
│   ├── pollution_sensor.py
│   └── weather_sensor.py
├── examples/
│   ├── cli.py
│   └── run_system.py
├── schemas/
│   ├── pollution_schema.json
│   ├── pollution2.json
│   └── stream_join_demo.json
├── data/
│   └── static_tables.db               # SQLite reference tables
└── tests/
    ├── test_*.py
    └── ...
```

---

End of History and Decisions. All knowledge preserved. No loss.
