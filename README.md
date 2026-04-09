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

### 3) Start CLI
Terminal D:
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

## Run tests

```bash
pytest -q
```
