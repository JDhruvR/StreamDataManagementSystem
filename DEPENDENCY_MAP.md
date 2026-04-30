# Dependency Map and Component Interaction Guide

## 1. COMPONENT DEPENDENCY GRAPH

### Hierarchy (Top-Down)

```
┌─────────────────────────────────────────┐
│   USER ENTRY POINTS                     │
│ ┌──────────────┐  ┌────────┐  ┌──────┐ │
│ │ CLI          │  │ UI App │  │Sensors│ │
│ │ (cli.py)     │  │(app.py)│  │       │ │
│ └──────┬───────┘  └────┬───┘  └───┬──┘ │
└────────┼─────────────────┼──────────┼───┘
         │                 │          │
┌────────▼─────────────────▼──────────▼──┐
│   CONFIGURATION LAYER                   │
│ ┌────────────────────────────────────┐ │
│ │ KafkaConfig (streaming/)           │ │
│ │ UIConfig (ui/)                     │ │
│ │ Environment Variables              │ │
│ └────────────────────────────────────┘ │
└────────────────────────────────────────┘
         │
┌────────▼─────────────────────────────────┐
│   SCHEMA & EXECUTION LAYER               │
│ ┌─────────────────────────────────────┐ │
│ │ SchemaManager (core/schema/)        │ │
│ ├─────────────────────────────────────┤ │
│ │ SchemaRegistry (core/execution/)    │ │
│ │ (singleton: get_global_registry())  │ │
│ ├─────────────────────────────────────┤ │
│ │ ExecutionEngine (core/execution/)   │ │
│ │ (created per schema)                │ │
│ └─────────────────────────────────────┘ │
└────────┬───────────────────────────────┘
         │
┌────────▼─────────────────────────────────┐
│   QUERY PROCESSING LAYER                │
│ ┌─────────────────────────────────────┐ │
│ │ SQL Parser (core/parser/)           │ │
│ │ + Grammar (grammar.lark)            │ │
│ └──────────────┬──────────────────────┘ │
│                │                        │
│ ┌──────────────▼──────────────────────┐ │
│ │ Operator Pipeline                   │ │
│ │ (core/execution/operators.py)       │ │
│ │ ┌──────────────────────────────┐   │ │
│ │ │ Base: Operator (ABC)         │   │ │
│ │ ├──────────────────────────────┤   │ │
│ │ │ FilterOperator               │   │ │
│ │ │ WindowOperator               │   │ │
│ │ │ ProjectionOperator           │   │ │
│ │ │ AggregateOperator            │   │ │
│ │ │ StreamStreamJoinOperator     │   │ │
│ │ │ JoinOperator                 │   │ │
│ │ │ SinkOperator                 │   │ │
│ │ └──────────────────────────────┘   │ │
│ └──────────────┬──────────────────────┘ │
└────────────────┼───────────────────────┘
                 │
┌────────────────▼───────────────────────┐
│   STORAGE & I/O LAYER                  │
│ ┌──────────────────────────────────┐  │
│ │ Kafka Integration                │  │
│ │ ┌──────────────────────────────┐ │  │
│ │ │ StreamProducer               │ │  │
│ │ │ StreamConsumer               │ │  │
│ │ │ KafkaConfig                  │ │  │
│ │ └──────────────────────────────┘ │  │
│ ├──────────────────────────────────┤  │
│ │ SQLite Storage                   │  │
│ │ ┌──────────────────────────────┐ │  │
│ │ │ ReferenceTableStore          │ │  │
│ │ │ TableManager                 │ │  │
│ │ │ DatabaseService (UI)         │ │  │
│ │ └──────────────────────────────┘ │  │
│ └──────────────────────────────────┘  │
└─────────────────────────────────────────┘
         │
┌────────▼──────────────────┐
│   EXTERNAL SYSTEMS        │
│ ┌──────────────────────┐  │
│ │ Apache Kafka Broker  │  │
│ │ (localhost:9092)     │  │
│ └──────────────────────┘  │
└───────────────────────────┘
```

---

## 2. DETAILED IMPORT DEPENDENCIES

### examples/cli.py imports:
```python
from core.schema.SchemaManager
from core.execution.schema_registry import get_global_registry()
from core.parser.sql_parser import parse_sql()
from core.storage.reference_tables import ReferenceTableStore
from streaming.kafka_client import StreamConsumer
from streaming.kafka_config import set_default_config, get_default_config()
```

### examples/run_system.py imports:
```python
from core.schema.SchemaManager
from core.execution.schema_registry import get_global_registry()
from streaming.kafka_client import StreamConsumer
from streaming.kafka_config import get_default_config()
```

### ui/app.py imports:
```python
from ui.config import config
from ui.data_buffer import QueryOutputBuffer
from ui.kafka_consumer import KafkaOutputConsumer
from ui.db_service import DatabaseService
from core.schema.SchemaManager
from core.execution.schema_registry import get_global_registry()
from flask import Flask, render_template, jsonify, Response
from flask_cors import CORS
```

### core/execution/engine.py imports:
```python
from core.parser.sql_parser import parse_sql()
from core.execution.operators import (
    AggregateOperator,
    FilterOperator,
    JoinOperator,
    ProjectionOperator,
    SinkOperator,
    StreamStreamJoinOperator,
    WindowOperator,
)
```

### core/execution/schema_registry.py imports:
```python
from core.execution.engine import ExecutionEngine
```

### core/execution/operators.py imports:
```python
# Lazy imports inside methods:
# from core.storage.table import storage
# from streaming.kafka_client import StreamProducer

# Standard library:
import sqlite3
import json
import threading
from abc import ABC, abstractmethod
```

### sensors/*.py imports:
```python
from streaming.kafka_client import StreamProducer
```

---

## 3. OBJECT INSTANTIATION GRAPH

### Global Singletons

```
get_global_registry() → SchemaRegistry (singleton)
    │
    └─→ Contains: schemas = {
        schema_name: {
            'config': schema_dict,
            'engine': ExecutionEngine instance
        }
    }

get_default_config() → KafkaConfig (singleton)
    │
    └─→ broker: str
    └─→ producer_config, consumer_config, topic_config

set_default_config(broker) → Updates global KafkaConfig
```

### Per-Schema Instances

```
SchemaManager.load_from_file(path) → Schema dict

registry.register_schema(schema_dict) → Creates:
    │
    ├─→ ExecutionEngine(schema_dict)
    │   ├─→ Stores: input_streams, output_streams
    │   ├─→ For each continuous_query:
    │   │   ├─→ parse_sql(query_text)
    │   │   │   └─→ Returns: query_plan dict
    │   │   └─→ _build_pipeline(query_plan)
    │   │       ├─→ Creates SinkOperator (Kafka output)
    │   │       ├─→ Creates AggregateOperator (optional)
    │   │       ├─→ Creates WindowOperator (optional)
    │   │       ├─→ Creates FilterOperator (optional)
    │   │       ├─→ Creates StreamStreamJoinOperator or JoinOperator
    │   │       └─→ Chains with next_op pointers
    │   │
    │   └─→ Stores queries list with pipelines
    │
    └─→ registry.schemas[schema_name] stores {config, engine}
```

### Per-Query Event Processing

```
event = StreamConsumer.next() → JSON dict
    │
    ├─→ registry.process_event(schema_name, stream_name, event)
    │
    ├─→ engine.process_event(stream_name, event)
    │
    ├─→ for query in engine.queries:
    │   if stream_name in query['input_streams']:
    │       query['pipeline'].process(event)
    │           │
    │           └─→ Operator chain:
    │               ├─→ op1.process(event)
    │               │   └─→ if match: op2.process(event)
    │               ├─→ op2.process(event)
    │               │   └─→ if match: op3.process(event)
    │               └─→ ... (chain continues)
    │                   └─→ SinkOperator.process(final_event)
    │                       └─→ StreamProducer.send(output_topic, final_event)
    │
    └─→ Output Kafka topic
```

---

## 4. DATA FLOW SEQUENCES

### Initialization Sequence

```
Start Process
    │
    ├─→ [1] Load Configuration
    │   ├─→ Read environment variables
    │   ├─→ KafkaConfig.__init__(broker)
    │   └─→ UIConfig (if UI)
    │
    ├─→ [2] Initialize Components
    │   ├─→ SchemaManager = SchemaManager()
    │   ├─→ registry = get_global_registry()
    │   └─→ config = get_default_config()
    │
    ├─→ [3] Load Schema
    │   ├─→ SchemaManager.load_from_file(path)
    │   ├─→ SchemaManager.validate()
    │   └─→ Returns: validated schema_dict
    │
    ├─→ [4] Deploy Schema
    │   ├─→ registry.register_schema(schema_dict)
    │   ├─→ ExecutionEngine.__init__(schema_dict)
    │   ├─→ engine.initialize_from_schema()
    │   │   ├─→ _register_input_streams()
    │   │   ├─→ _register_output_streams()
    │   │   └─→ For each continuous_query:
    │   │       ├─→ parse_sql(query_text)
    │   │       └─→ _build_pipeline(query_plan)
    │   │           └─→ Chain operators
    │   └─→ Returns: ExecutionEngine
    │
    ├─→ [5] Start Consumers (if run_system.py)
    │   ├─→ For each input_stream:
    │   │   ├─→ Thread(run_kafka_consumer)
    │   │   ├─→ StreamConsumer(topic, group_id, config)
    │   │   └─→ Begin consuming
    │   └─→ Threads run in background
    │
    └─→ [6] Main Loop
        ├─→ Keep process alive
        └─→ Graceful shutdown on Ctrl+C
```

### Query Execution Sequence (Per Event)

```
Event: {"timestamp": "2026-04-30T10:00:00", "sensor_id": "s1", "value": 75.5}
    │
    ├─→ [1] Receive from Kafka
    │   └─→ StreamConsumer.next()
    │   └─→ JSON deserialize
    │
    ├─→ [2] Route to Registry
    │   └─→ registry.process_event("pollution2", "pollution_stream", event)
    │
    ├─→ [3] Route to Engine
    │   └─→ engine.process_event("pollution_stream", event)
    │
    ├─→ [4] Process Through Pipeline
    │   │
    │   ├─→ Query: SELECT sensor_id, AVG(value) FROM pollution_stream WHERE value > 50 GROUP BY sensor_id
    │   │
    │   ├─→ [4a] StreamStreamJoinOperator (if INNER JOIN)
    │   │   ├─→ Not applicable for this query
    │   │   └─→ Pass to next operator
    │   │
    │   ├─→ [4b] FilterOperator (WHERE value > 50)
    │   │   ├─→ Check: 75.5 > 50? YES
    │   │   ├─→ Pass: call next_op.process(event)
    │   │   └─→ Continue
    │   │
    │   ├─→ [4c] WindowOperator
    │   │   ├─→ Add event to buffer: {timestamp: now, event: {...}}
    │   │   ├─→ Elapsed time since window start: 2.5 sec
    │   │   ├─→ Window size: 10 sec
    │   │   ├─→ Check velocity: type=time, value=10 sec
    │   │   ├─→ Time since last emit: 8.5 sec
    │   │   ├─→ Velocity condition NOT met (need 10 sec)
    │   │   └─→ Do not emit yet
    │   │
    │   └─→ Continue buffering...
    │       (Next 7.5 seconds of events)
    │
    ├─→ [5] Window Emit (after 10 seconds)
    │   │
    │   ├─→ WindowOperator.emit_window()
    │   │   └─→ Collect all buffered events into list
    │   │   └─→ Call next_op.process(events_list)
    │   │
    │   ├─→ [5a] AggregateOperator
    │   │   ├─→ Group events by GROUP BY fields (sensor_id)
    │   │   │   ├─→ Group "s1": [{...}, {...}, ...]
    │   │   │   ├─→ Group "s2": [{...}, {...}, ...]
    │   │   │   └─→ etc.
    │   │   │
    │   │   ├─→ For group "s1":
    │   │   │   ├─→ Load state from SQLite
    │   │   │   │   └─→ SELECT state_data FROM agg_state_avg_pollution WHERE group_key = ["s1"]
    │   │   │   │   └─→ Returns: {AVG_value_count: 50, AVG_value_sum: 3750}
    │   │   │   │
    │   │   │   ├─→ Compute local aggregates on window events
    │   │   │   │   ├─→ Count: 12 events
    │   │   │   │   ├─→ Sum: 900
    │   │   │   │   └─→ Average: 900/12 = 75.0
    │   │   │   │
    │   │   │   ├─→ Merge with previous state
    │   │   │   │   ├─→ New count: 50 + 12 = 62
    │   │   │   │   ├─→ New sum: 3750 + 900 = 4650
    │   │   │   │   ├─→ New average: 4650 / 62 = 75.0
    │   │   │   │
    │   │   │   ├─→ Store to SQLite
    │   │   │   │   └─→ INSERT/UPDATE agg_state_avg_pollution
    │   │   │   │       SET state_data = JSON{count: 62, sum: 4650, avg: 75.0}
    │   │   │   │
    │   │   │   └─→ Create result event
    │   │   │       └─→ {sensor_id: "s1", AVG(value): 75.0}
    │   │   │
    │   │   └─→ Call next_op.process(result_event)
    │   │
    │   ├─→ [5b] JoinOperator (if stream-to-table join)
    │   │   ├─→ Not applicable for this query
    │   │   └─→ Pass to next operator
    │   │
    │   ├─→ [5c] ProjectionOperator (if SELECT specified columns)
    │   │   ├─→ Not applicable (already in result_event)
    │   │   └─→ Pass to next operator
    │   │
    │   └─→ [5d] SinkOperator
    │       ├─→ Receive result_event: {sensor_id: "s1", AVG(value): 75.0}
    │       ├─→ StreamProducer.send("pollution_out", result_event)
    │       │   ├─→ JSON serialize: '{"sensor_id": "s1", "AVG(value)": 75.0}'
    │       │   ├─→ Send to Kafka topic: pollution_out
    │       │   └─→ Kafka confirms receipt
    │       │
    │       └─→ Optional: callback or SQLite write
    │
    ├─→ [6] Output Consumed (UI/Monitoring)
    │   │
    │   ├─→ KafkaOutputConsumer (background thread)
    │   │   ├─→ Consume from pollution_out topic
    │   │   ├─→ Deserialize: {sensor_id: "s1", AVG(value): 75.0}
    │   │   ├─→ QueryOutputBuffer.append(event)
    │   │   └─→ Keep last 100 events
    │   │
    │   └─→ UI /api/data endpoint
    │       ├─→ GET /api/data?topic=pollution_out
    │       ├─→ QueryOutputBuffer.get_latest(100)
    │       └─→ Return JSON: [{...}, {...}, ...]
    │
    └─→ [7] Event Processing Complete
```

---

## 5. CONCURRENCY MODEL

### Main Thread
- Entry point (CLI or run_system.py)
- Maintains REPL loop (CLI) or main loop (run_system.py)
- Handles graceful shutdown

### Consumer Threads (run_system.py)
```
Main Thread
    │
    ├─→ Thread 1: run_kafka_consumer("pollution_stream")
    │   └─→ StreamConsumer.next() (blocking)
    │   └─→ registry.process_event() (blocking)
    │   └─→ Event pipeline processing (blocking)
    │
    ├─→ Thread 2: run_kafka_consumer("vehicle_stream")
    │   └─→ Same pattern
    │
    └─→ Main loop (sleep, keep alive)
```

### UI Background Thread
```
UIApp
    │
    ├─→ Main Flask thread (accepts HTTP requests)
    │
    └─→ KafkaOutputConsumer thread (daemon)
    │   └─→ StreamConsumer.next() (blocking)
    │   └─→ QueryOutputBuffer.append() (thread-safe)
    │   └─→ Continue listening
```

### Synchronization Points
- **StreamConsumer.next()**: Blocking call
- **SQLite connections**: `check_same_thread=False` allows multi-threaded access
- **QueryOutputBuffer**: Thread-safe queue operations
- **Global registry**: Shared state accessed by multiple threads

---

## 6. STATE PERSISTENCE

### SQLite Databases

```
data/aggregate_states.db
    └─→ Tables per query:
        ├─→ agg_state_avg_pollution (from query name)
        ├─→ agg_state_max_vehicle
        └─→ etc.
    
    Schema per table:
    ├─→ group_key TEXT PRIMARY KEY
    │   └─→ JSON of GROUP BY field values
    └─→ state_data TEXT
        └─→ JSON {SUM_value_sum: X, SUM_value_count: Y, ...}

data/static_tables.db
    └─→ Reference tables:
        ├─→ sensors (id INT, name TEXT, location TEXT, ...)
        ├─→ vehicles (vehicle_id INT, model TEXT, ...)
        └─→ etc.

data/stream_stream_join.db (created dynamically)
    └─→ Join state per query:
        ├─→ left_buffer (stream_side, timestamp, event_data)
        ├─→ right_buffer
        └─→ etc.
```

### In-Memory State (Operators)

```
WindowOperator:
    └─→ buffer: deque [(timestamp, event), ...]
    └─→ window_start_time: float
    └─→ events_since_emit: int

StreamStreamJoinOperator:
    └─→ left_buffer: deque [(timestamp, event), ...]
    └─→ right_buffer: deque [(timestamp, event), ...]
    └─→ window_start_time: float

AggregateOperator:
    └─→ State loaded/stored from SQLite per window
```

---

## 7. CONFIGURATION FLOW

```
Environment Variables
    ├─→ SDMS_KAFKA_BROKER (→ KafkaConfig)
    ├─→ SDMS_UI_HOST, SDMS_UI_PORT (→ UIConfig)
    ├─→ SDMS_ACTIVE_SCHEMA_PATH (→ UIApp bootstrap)
    └─→ SDMS_SQLITE_DB (→ Storage paths)

KafkaConfig.get_default_config()
    ├─→ broker: localhost:9092
    ├─→ producer_config: {acks: all, retries: 3, compression: gzip}
    ├─→ consumer_config: {auto_offset_reset: earliest, enable_auto_commit: false}
    └─→ topic_config: {retention: 1ms (ephemeral)}

Schema JSON
    ├─→ window_size, window_unit, window_type (global)
    ├─→ velocity (global)
    ├─→ input_streams (defines Kafka topics & schemas)
    ├─→ output_streams (defines output Kafka topics & schemas)
    └─→ continuous_queries (defines SQL transformations)
```

---

## 8. ERROR PROPAGATION

```
High-Level Error → Low-Level Handling

SchemaValidationError
    ├─→ Raised in: SchemaManager.validate()
    ├─→ Caught in: CLI or run_system.py
    └─→ Action: Display to user, ask for corrected schema

Lark ParseError (parsing error)
    ├─→ Raised in: parse_sql()
    ├─→ Caught in: ExecutionEngine._deploy_continuous_query()
    ├─→ Wrapped as: ValueError
    └─→ Action: Display to user, ask for corrected SQL

ValueError (schema/stream mismatch)
    ├─→ Raised in: ExecutionEngine._deploy_continuous_query()
    ├─→ Caught in: SchemaRegistry.register_schema()
    └─→ Action: Display to user, fix schema references

Kafka ConnectionError
    ├─→ Raised in: StreamConsumer/Producer.__init__()
    ├─→ Caught in: run_kafka_consumer or operator
    ├─→ Action: Print error, continue
    └─→ User Action: Start Kafka, restart system

SQLite OperationalError
    ├─→ Raised in: Operator.process() (during SQL operation)
    ├─→ Caught in: Try-except in operator
    ├─→ Action: Log error, skip event
    └─→ Effect: Single event loss, no cascade

Event Processing Error
    ├─→ Raised in: Operator.process()
    ├─→ Caught in: Try-except wrapper
    └─→ Action: Log, drop event, continue
```

