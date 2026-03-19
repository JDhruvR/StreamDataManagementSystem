import json
import threading
import time
from streaming.kafka_client import StreamConsumer
from core.execution.engine import ExecutionEngine

def output_callback(event):
    print(f"\\n>>> [ALERT] High Pollution Detected: {event}\\n")

def run_kafka_consumer(engine, topic, source_stream_name):
    print(f"Starting Kafka consumer for topic '{topic}'...")
    try:
        consumer = StreamConsumer(topic=topic, group_id='query_engine_group')
        for msg in consumer:
            # Feed data into the execution engine
            engine.process_event(source_stream_name, msg)
    except Exception as e:
        print(f"Kafka consumer error: {e}")
        print("Please ensure Kafka is running and `sensors/pollution_sensor.py` is producing data.")

def main():
    engine = ExecutionEngine()

    # 1. Provide DDL to create stream and table
    print("Initializing System...")
    ddl_statements = """
    CREATE STREAM pollution_stream (
        timestamp STRING,
        sensor_id STRING,
        pollutant STRING,
        value FLOAT
    ) WITH (topic="pollution_stream")

    CREATE TABLE high_pollution_table (
        sensor_id STRING,
        avg_value FLOAT
    )
    """
    engine.handle_statement(ddl_statements.strip())

    # 2. Provide a Continuous Query
    print("\\nDeploying Continuous Query...")
    # Find average pollution per window for sensors that read very high values over 50.
    query = """
    SELECT sensor_id, AVG(value)
    FROM pollution_stream
    WINDOW TUMBLING (10 SECONDS)
    WHERE value > 50.0
    """
    # The callback will print output. In reality, engine would sink to table or stream
    engine.handle_statement(query.strip(), callback=output_callback)

    # 3. Start a background thread to consume from Kafka and feed the engine
    topic = "pollution_stream"
    consumer_thread = threading.Thread(
        target=run_kafka_consumer, 
        args=(engine, topic, "pollution_stream"),
        daemon=True
    )
    consumer_thread.start()

    print("\\nSystem is running. Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\\nShutting down system...")

if __name__ == "__main__":
    main()
