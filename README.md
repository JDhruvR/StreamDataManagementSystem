# StreamDataManagementSystem

Schema-first streaming engine with continuous SQL-style queries, stream joins, and SQLite reference-table joins.

## What It Does

Process event streams in real-time using pre-defined schemas. Define input streams, write SQL queries, and watch results flow to output topics. Supports both stream-to-stream and stream-to-table joins.

## Quick Start

### Prerequisites

- Python 3.7+
- Apache Kafka 3.6.1
- Java

### Setup (one-time)

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Run Demo

Terminal 1 (Zookeeper):
```bash
cd kafka_2.13-3.6.1
bin/zookeeper-server-start.sh config/zookeeper.properties
```

Terminal 2 (Kafka):
```bash
cd kafka_2.13-3.6.1
bin/kafka-server-start.sh config/server.properties
```

Terminal 3 (Sensor producer):
```bash
python -m sensors.pollution_sensor
```

Terminal 4 (Interactive CLI):
```bash
python -m examples.cli
```

At `sdms>` prompt:
```
load schemas/pollution2.json
status
```

Terminal 5 (View results):
```bash
cd kafka_2.13-3.6.1
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic pollution_out
```

## Key Features

- Schema-based configuration (JSON)
- Continuous SELECT queries only (no ad-hoc)
- Stream-to-stream INNER JOIN
- Stream-to-table INNER JOIN
- Persistent SQLite reference tables
- Window and velocity controls per schema
- Interactive CLI for runtime schema/query deployment

## Documentation

- [Setup & Running](docs/setup.md) - Installation, prerequisites, troubleshooting
- [Architecture & Guides](docs/guides.md) - System design, how to extend, JOIN reference
- [History & Decisions](docs/lessons.md) - Release notes, design decisions, feature summary

## Tests

```bash
pytest -q
```
Run these from inside StreamDataManagementSystem/:

Terminal 1:

cd kafka_2.13-3.6.1 && ./bin/zookeeper-server-start.sh config/zookeeper.properties

Terminal 2:

cd kafka_2.13-3.6.1 && ./bin/kafka-server-start.sh config/server.properties

Terminal 3:

source venv/bin/activate && python -m sensors.pollution_sensor

Terminal 4:

source venv/bin/activate && python -m examples.cli
Then type: load schemas/pollution2.json

Terminal 5:

cd kafka_2.13-3.6.1 && ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic pollution_out --from-beginning

Terminal 6:

./run_ui.sh
Then open http://127.0.0.1:5000 in browser.
