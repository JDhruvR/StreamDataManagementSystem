import time
import random
import datetime
from streaming.kafka_client import StreamProducer

SENSOR_IDS = ['s1', 's2', 's3', 's4']
POLLUTANTS = ['PM2.5', 'PM10', 'CO2', 'NO2']

def generate_reading():
    sensor_id = random.choice(SENSOR_IDS)
    pollutant = random.choice(POLLUTANTS)
    
    # Generate realistic values based on pollutant
    if pollutant == 'PM2.5':
        value = random.uniform(5.0, 150.0)
    elif pollutant == 'PM10':
        value = random.uniform(10.0, 200.0)
    elif pollutant == 'CO2':
        value = random.uniform(300.0, 600.0)
    else:  # NO2
        value = random.uniform(10.0, 80.0)
    
    return {
        "timestamp": datetime.datetime.now().isoformat(),
        "sensor_id": sensor_id,
        "pollutant": pollutant,
        "value": round(value, 2)
    }

def main():
    producer = StreamProducer()
    topic = 'pollution_stream'
    print(f"Starting to stream pollution data to topic '{topic}'...")
    try:
        while True:
            data = generate_reading()
            producer.send(topic, data)
            print(f"Sent: {data}")
            time.sleep(random.uniform(0.5, 2.0))
    except KeyboardInterrupt:
        print("Stopping sensor simulator.")
    except Exception as e:
        print(f"Failed to produce data: {e}")
        print("Make sure Kafka is running on localhost:9092")

if __name__ == "__main__":
    main()
