# How To Run SDMS (Most Updated)

This guide covers both demo modes:
- Demo A: stream -> table join (`pollution2.json`)
- Demo B: stream -> stream join (`stream_join_demo.json`)

Run commands from repository root: `StreamDataManagementSystem`.

## 0) One-time setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 1) Start infrastructure

### Terminal 1 - Zookeeper
```bash
cd kafka_2.13-3.6.1
bin/zookeeper-server-start.sh config/zookeeper.properties
```

### Terminal 2 - Kafka broker
```bash
cd kafka_2.13-3.6.1
bin/kafka-server-start.sh config/server.properties
```

## 2) Start producers

### Terminal 3 - Pollution producer
```bash
python -m sensors.pollution_sensor
```

### Terminal 4 - Weather producer
```bash
python -m sensors.weather_sensor
```

## 3) Start CLI

### Terminal 5
```bash
python -m examples.cli
```

Note: once you are at `sdms>`, run only CLI commands (do not run shell commands there).

## Demo A: stream -> table join

In CLI:
```text
load schemas/pollution2.json
status
```

Create/populate join table:
```text
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

### Terminal 6 - output consumer
```bash
cd kafka_2.13-3.6.1
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic pollution_out
```

## Demo B: stream -> stream join

In CLI:
```text
load schemas/stream_join_demo.json
status
```

### Terminal 6 - joined output consumer
```bash
cd kafka_2.13-3.6.1
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic joined_out
```

Joined output appears when:
- `sensor_id` matches across `pollution_stream` and `weather_stream`
- pollution event passes `value > 50`

## Troubleshooting

If no output appears in 20-40 seconds:
1. In CLI run `status` and verify expected schema is active.
2. Verify both producers are printing `Sent: ...`.
3. Keep consumer open; join conditions may take a few events to match.

If CLI says unknown command:
- use `help`
- enter one command per prompt
- then respond to sub-prompts one line at a time

## Stop everything

Press `Ctrl+C` in each terminal (consumer, CLI, producers, Kafka, Zookeeper).
