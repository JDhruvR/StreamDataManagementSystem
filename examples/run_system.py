"""
StreamDataManagementSystem: Main application.

Schema-based streaming data processing using continuous queries.
All queries are pre-defined in schema configuration files.
"""

import json
import threading
import time
from core.schema.schema_manager import SchemaManager
from core.execution.schema_registry import get_global_registry
from streaming.kafka_client import StreamConsumer
from streaming.kafka_config import get_default_config


def output_callback(event):
    """Default output callback for alerts."""
    print(f"\n>>> [ALERT] {json.dumps(event, indent=2)}\n")


def run_kafka_consumer(schema_name, stream_name, registry, topic, group_id):
    """
    Consume messages from Kafka topic and feed to engine.
    
    Runs in a background thread for each input stream.
    """
    print(f"  Consumer started for {schema_name}/{stream_name} (topic: {topic})")
    
    try:
        config = get_default_config()
        consumer = StreamConsumer(
            topic=topic,
            group_id=group_id,
            config=config
        )
        
        for msg in consumer:
            try:
                registry.process_event(schema_name, stream_name, msg)
            except Exception as e:
                print(f"  Error processing event: {e}")
    
    except Exception as e:
        print(f"  Kafka consumer error for {stream_name}: {e}")
        print(f"  Make sure Kafka is running and {stream_name} is producing data.")


def main():
    """Main entry point."""
    print("=" * 60)
    print("StreamDataManagementSystem v1.0 - Schema-Based")
    print("=" * 60)
    print()
    
    # Step 1: Load schema
    print("Step 1: Load Schema")
    print("-" * 60)
    
    schema_manager = SchemaManager()
    
    # Try to load example schema first, or prompt for input
    try:
        schema = schema_manager.load_from_file('schemas/pollution_schema.json')
        print()
    except FileNotFoundError:
        print("  Using interactive schema input...")
        try:
            schema = schema_manager.load_from_input()
            print()
        except Exception as e:
            print(f"  Error loading schema: {e}")
            return
    except Exception as e:
        print(f"  Error: {e}")
        return
    
    schema_name = schema_manager.get_schema_name()
    print(f"Loaded schema: {schema_name}")
    print()
    
    # Step 2: Deploy schema to registry
    print("Step 2: Deploy Schema")
    print("-" * 60)
    
    registry = get_global_registry()
    
    try:
        engine = registry.register_schema(schema)
    except Exception as e:
        print(f"  Error deploying schema: {e}")
        return
    
    print()
    
    # Print schema summary
    print(f"Schema Summary:")
    print(f"  Window: {schema['window_size']} {schema['window_unit']}")
    print(f"  Velocity: {schema['velocity']}")
    
    input_streams = engine.get_input_streams()
    output_streams = engine.get_output_streams()
    queries = engine.get_queries()
    
    print(f"  Input Streams: {', '.join(input_streams.keys())}")
    print(f"  Output Streams: {', '.join(output_streams.keys())}")
    print(f"  Continuous Queries: {len(queries)}")
    for q in queries:
        print(f"    - {q['name']}: {q['input_stream']} -> {q['output_stream']}")
    print()
    
    # Step 3: Start Kafka consumers
    print("Step 3: Starting Kafka Consumers")
    print("-" * 60)
    
    config = get_default_config()
    print(f"Kafka Mode: {config.get_mode_description()}")
    print(f"Broker: {config.get_broker()}")
    print()
    
    consumers = []
    for stream_name, stream_config in input_streams.items():
        topic = stream_config['topic']
        group_id = f"{schema_name}_{stream_name}_consumer"
        
        # Start consumer in background thread
        thread = threading.Thread(
            target=run_kafka_consumer,
            args=(schema_name, stream_name, registry, topic, group_id),
            daemon=True
        )
        thread.start()
        consumers.append(thread)
    
    print()
    print("=" * 60)
    print("System Running. Press Ctrl+C to stop.")
    print("=" * 60)
    print()
    
    try:
        # Keep main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n\nShutting down...")
        print("✓ System stopped")


if __name__ == '__main__':
    main()
