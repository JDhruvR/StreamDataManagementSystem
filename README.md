# StreamDataManagementSystem

Schema-first streaming engine with continuous SQL-style queries, stream joins, and SQLite reference-table joins.

## Easiest Run (Demo in 4 terminals)

### 0) Install dependencies
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 1) Start Kafka
Terminal A:
```bash
cd kafka_2.13-3.6.1
bin/zookeeper-server-start.sh config/zookeeper.properties
```

Terminal B:
```bash
cd kafka_2.13-3.6.1
bin/kafka-server-start.sh config/server.properties
```

### 2) Start sensor producer
Terminal C:
```bash
python -m sensors.pollution_sensor
```

### 2b) (For stream-stream join demo) Start weather producer
Terminal D:
```bash
python -m sensors.weather_sensor
```

### 3) Start CLI
Terminal E:
```bash
python -m examples.cli
```

At `sdms>` prompt:
```text
load schemas/pollution2.json
status
```

### 4) Watch output topic
Open another terminal (or reuse one):
```bash
cd kafka_2.13-3.6.1
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic pollution_out
```

You should now see processed output events.

## Notes

- Message queue mode is ephemeral-only (non-persistent) in runtime config.
- Window and velocity are schema-level settings.
- Supported joins:
  - stream -> stream INNER JOIN
  - stream -> SQLite table INNER JOIN

## Manual Stream-Stream Join Demo

1. Start `python -m sensors.pollution_sensor` and `python -m sensors.weather_sensor` in separate terminals.
2. Start CLI: `python -m examples.cli`
3. In CLI:
```text
load schemas/stream_join_demo.json
status
```
4. Consume join output:
```bash
cd kafka_2.13-3.6.1
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic joined_out
```

## Run tests

```bash
pytest -q
```
