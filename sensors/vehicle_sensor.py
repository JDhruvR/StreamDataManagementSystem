import time
import random
import datetime
from streaming.kafka_client import StreamProducer

JUNCTION_IDS   = ['J1', 'J2', 'J3', 'J4', 'J5']
VEHICLE_TYPES  = ['car', 'truck', 'bus', 'motorcycle', 'ambulance']

# Realistic base speeds per junction (matching their zone speed limits)
JUNCTION_SPEED_PROFILE = {
    'J1': (20, 45),   # school zone — vehicles often overshoot
    'J2': (30, 65),   # commercial — busy but fast
    'J3': (60, 110),  # highway — high speed
    'J4': (25, 55),   # residential
    'J5': (10, 35),   # hospital zone
}

def generate_vehicle_event():
    junction_id = random.choice(JUNCTION_IDS)
    speed_min, speed_max = JUNCTION_SPEED_PROFILE[junction_id]
    return {
        "timestamp":    datetime.datetime.now().isoformat(),
        "vehicle_id":   f"VH-{random.randint(1000, 9999)}",
        "junction_id":  junction_id,
        "speed":        round(random.uniform(speed_min, speed_max), 2),
        "vehicle_type": random.choice(VEHICLE_TYPES),
        "lane":         random.randint(1, 4)
    }

def main():
    producer = StreamProducer()
    topic = 'vehicle_stream'
    print(f"Starting vehicle sensor stream → topic '{topic}' ...")
    count = 0
    try:
        while True:
            event = generate_vehicle_event()
            producer.send(topic, event)
            count += 1
            print(f"[{count}] Sent vehicle event: {event['vehicle_id']} @ {event['junction_id']} "
                  f"speed={event['speed']} km/h  type={event['vehicle_type']}")
            time.sleep(random.uniform(0.3, 1.5))
    except KeyboardInterrupt:
        print("Vehicle sensor stopped.")
    except Exception as e:
        print(f"Producer error: {e}")

if __name__ == "__main__":
    main()