# Setup and Installation

## Prerequisites

- Python 3.7+
- Apache Kafka 3.6.1
- Java (required for Kafka)
- Virtual environment tool (venv or conda)

## Installation

### 1. Clone and Setup Python Environment

```bash
cd StreamDataManagementSystem
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Download Kafka

```bash
wget https://archive.apache.org/dist/kafka/3.6.1/kafka_2.13-3.6.1.tgz
tar -xzf kafka_2.13-3.6.1.tgz
```

Kafka should now be in `kafka_2.13-3.6.1/` at project root.

---

## Running the System

All commands run from repository root.

### Start Infrastructure (2 terminals)

**Terminal 1 - Zookeeper:**
```bash
cd kafka_2.13-3.6.1
bin/zookeeper-server-start.sh config/zookeeper.properties
```

**Terminal 2 - Kafka Broker:**
```bash
cd kafka_2.13-3.6.1
bin/kafka-server-start.sh config/server.properties
```

### Start Producers (1-2 terminals)

**Terminal 3 - Pollution Sensor (required):**
```bash
python -m sensors.pollution_sensor
```

**Terminal 4 - Weather Sensor (optional, for stream-stream join demo):**
```bash
python -m sensors.weather_sensor
```

### Start Interactive CLI (required)

**Terminal 5:**
```bash
python -m examples.cli
```

At `sdms>` prompt, you can:
- Load pre-built schemas
- Create new schemas interactively
- Deploy queries at runtime
- Manage persistent reference tables

### View Output

**Terminal 6 (optional - consume results):**
```bash
cd kafka_2.13-3.6.1
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <output_topic>
```

Replace `<output_topic>` with the topic name from your schema (e.g., `pollution_out`).

---

## Demo Workflows

### Demo A: Stream-to-Table Join

**In CLI:**
```
load schemas/pollution2.json
status
```

**Create reference table:**
```
table_create
table_name> sensors
columns> id:STRING,name:STRING

table_insert
table_name> sensors
row_json> {"id":"s1","name":"sensor_1"}

table_insert
table_name> sensors
row_json> {"id":"s2","name":"sensor_2"}

table_insert
table_name> sensors
row_json> {"id":"s3","name":"sensor_3"}
```

**Consume joined output (Terminal 6):**
```bash
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic pollution_out
```

### Demo B: Stream-to-Stream Join

**In CLI:**
```
load schemas/stream_join_demo.json
status
```

**Consume joined output (Terminal 6):**
```bash
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic joined_out
```

Join output appears when:
- `sensor_id` matches across `pollution_stream` and `weather_stream`
- Pollution event passes `value > 50` filter

---

## Creating Custom Schemas

### Via CLI (Interactive)

```
create
schema_name> my_schema

add_input
input_stream_name> my_stream
topic [my_stream]>
columns> id:STRING,value:FLOAT

query
query> SELECT id, AVG(value) FROM my_stream WHERE value > 0
query_name> my_query
output_stream> my_output
output_topic [my_output]>

save
```

### Via JSON File

Copy and modify `schemas/pollution2.json`:

```json
{
  "schema_name": "custom_schema",
  "window_size": 10,
  "window_unit": "seconds",
  "velocity": {"type": "count", "value": 100},
  "input_streams": [
    {
      "name": "my_stream",
      "topic": "my_topic",
      "schema": {"id": "STRING", "value": "FLOAT"}
    }
  ],
  "continuous_queries": [
    {
      "name": "my_query",
      "input_stream": "my_stream",
      "output_stream": "my_output",
      "query": "SELECT id, AVG(value) FROM my_stream WHERE value > 0"
    }
  ],
  "output_streams": [
    {
      "name": "my_output",
      "topic": "my_output",
      "schema": {"id": "STRING", "AVG(value)": "FLOAT"}
    }
  ]
}
```

Then in CLI: `load schemas/custom_schema.json`

---

## Configuration

### Default Behavior

- **Kafka Mode**: In-memory/ephemeral (no persistence to disk)
- **Window**: Configurable per schema (default 10 seconds)
- **Velocity**: Count or time-based triggering (default count mode)
- **Join Support**: Both stream-stream and stream-table INNER JOINs

### Persistent Tables

Reference tables stored in `data/static_tables.db` (SQLite).

Table commands in CLI:
- `table_create` - Create new table
- `table_insert` - Add rows
- `table_update` - Modify rows
- `table_delete` - Remove rows
- `table_select` - Query table
- `table_list` - List all tables
- `table_schema` - Show table structure

---

## Troubleshooting

### Kafka won't start

- Verify Java is installed: `java -version`
- Check Kafka folder exists: `ls kafka_2.13-3.6.1/`
- Ensure port 9092 is free

### No data flowing

1. Verify sensor is running and printing `Sent: ...`
2. Check schema is loaded: `status` in CLI
3. Verify topic name: compare CLI status with consumer topic
4. Wait 10-40 seconds (window/velocity delay)

### CLI says "unknown command"

- Use `help` to list available commands
- Enter one command per prompt
- Respond to sub-prompts one line at a time

### "Socket already in use"

- Kill existing Kafka/Zookeeper processes
- Or change ports in `kafka_2.13-3.6.1/config/`

### No output in consumer

- Consumer may need `--from-beginning` flag to see old messages
- Or create new output topic for fresh demo

---

## Stop Everything

Press `Ctrl+C` in each terminal (consumer, CLI, producers, Kafka, Zookeeper).

---

## Running Tests

```bash
pytest -q
```

Tests cover:
- Window and velocity semantics
- Stream-stream and stream-table joins
- Schema validation and CLI inference
