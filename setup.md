# StreamDataManagementSystem Current Setup Instructions (v1.1+)

## Prerequisites
- Python 3.7+
- Apache Kafka 3.6.1
- Virtual environment

## Quick Start

### 1. Install Python Dependencies
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Download and Setup Kafka
```bash
wget https://archive.apache.org/dist/kafka/3.6.1/kafka_2.13-3.6.1.tgz
tar -xzf kafka_2.13-3.6.1.tgz
cd kafka_2.13-3.6.1
```

### 3. Start Kafka (in separate terminals)

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

### 4. Run Sensor Simulator

**Terminal 3:**
```bash
python -m sensors.pollution_sensor
```

### 5. Run the System

**Terminal 4:**
```bash
python -m examples.run_system
```

### 6. Optional: Use Interactive CLI (recommended for current workflow)
```bash
python -m examples.cli
```

## Configuration

### Default Behavior
- **Schema**: `examples/run_system.py` uses `schemas/pollution_schema.json` by default; `examples/cli.py` supports `load` and `create`
- **Kafka Mode**: In-memory (ephemeral, no persistence)
- **Window**: 10 seconds
- **Velocity**: Count and time modes are both executed by window operator
- **JOIN support**: Stream -> stream and stream -> SQLite table `INNER JOIN` are supported

### Create Custom Schema
1. Copy `schemas/pollution_schema.json` to create a new schema file
2. Modify the JSON configuration (input/output streams, queries, window size, velocity)
3. Run system with your schema

Example usage:
```bash
# Will prompt for schema input or file path
python -m examples.run_system
```

## Requirements

See `requirements.txt` for all dependencies:
- kafka-python-ng==2.2.2
- lark==1.1.7
- pandas==2.2.1
- pytest==8.1.1

## Troubleshooting

**Kafka not starting?**
- Ensure Java is installed
- Check Kafka broker is running on localhost:9092

**Schema validation error?**
- Verify JSON syntax in schema file
- Check all stream references (input_streams, output_streams)
- Ensure continuous queries reference valid streams

**No events flowing?**
- Verify sensor is running (Terminal 3)
- Check Kafka topics exist: `kafka-topics.sh --list --bootstrap-server localhost:9092`
- Check consumer group offset

## Key Files
- `requirements.txt` - Python dependencies
- `schemas/` - Schema configuration files
- `examples/run_system.py` - Non-interactive runner
- `examples/cli.py` - Interactive schema/query/table management
- `sensors/pollution_sensor.py` - Test data generator
- `v_1.txt` - Release notes and documentation
- `v_1_1.txt` - Current run guide
- `walkthrough.md` - System architecture guide
