import random
import time
from datetime import datetime

from streaming.kafka_client import StreamProducer


SENSOR_IDS = ["s1", "s2", "s3", "s4"]
ZONES = ["north", "south", "east", "west"]


def generate_reading():
    return {
        "timestamp": datetime.now().isoformat(),
        "sensor_id": random.choice(SENSOR_IDS),
        "humidity": round(random.uniform(20.0, 90.0), 2),
        "zone": random.choice(ZONES),
    }


def main():
    producer = StreamProducer()
    topic = "weather_stream"
    print(f"Starting to stream weather data to topic '{topic}'...")
    try:
        while True:
            data = generate_reading()
            producer.send(topic, data)
            print(f"Sent: {data}")
            time.sleep(random.uniform(0.5, 2.0))
    except KeyboardInterrupt:
        print("Stopping weather sensor simulator.")
    except Exception as e:
        print(f"Failed to produce data: {e}")
        print("Make sure Kafka is running on localhost:9092")


if __name__ == "__main__":
    main()
