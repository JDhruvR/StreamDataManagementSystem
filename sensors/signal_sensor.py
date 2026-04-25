import time
import random
import datetime
from streaming.kafka_client import StreamProducer

JUNCTION_IDS   = ['J1', 'J2', 'J3', 'J4', 'J5']
SIGNAL_PHASES  = ['GREEN', 'YELLOW', 'RED']

# Congestion weights per junction: (low_weight, high_weight)
# Higher weight on high side = more likely to be congested
CONGESTION_PROFILE = {
    'J1': (4, 6),    # school zone — moderate-high
    'J2': (3, 7),    # commercial — often congested
    'J3': (7, 3),    # highway — mostly free
    'J4': (5, 5),    # residential — balanced
    'J5': (6, 4),    # hospital — moderate
}

def generate_signal_event(junction_id, current_phase):
    low_w, high_w = CONGESTION_PROFILE[junction_id]
    congestion = random.choices(
        range(1, 11),
        weights=[low_w if i < 5 else high_w for i in range(10)]
    )[0]
    phase_duration = {'GREEN': 30, 'YELLOW': 5, 'RED': 25}[current_phase]
    return {
        "timestamp":      datetime.datetime.now().isoformat(),
        "junction_id":    junction_id,
        "signal_phase":   current_phase,
        "phase_duration": phase_duration,
        "congestion_level": congestion
    }

def main():
    producer = StreamProducer()
    topic = 'signal_stream'
    print(f"Starting signal controller stream → topic '{topic}' ...")

    # Each junction cycles through phases independently
    junction_phases = {jid: random.choice(SIGNAL_PHASES) for jid in JUNCTION_IDS}
    phase_cycle = ['GREEN', 'YELLOW', 'RED']
    count = 0

    try:
        while True:
            for junction_id in JUNCTION_IDS:
                event = generate_signal_event(junction_id, junction_phases[junction_id])
                producer.send(topic, event)
                count += 1
                print(f"[{count}] Signal @ {junction_id}: phase={event['signal_phase']} "
                      f"congestion={event['congestion_level']}/10")

                # Cycle phase occasionally
                if random.random() < 0.15:
                    current_idx = phase_cycle.index(junction_phases[junction_id])
                    junction_phases[junction_id] = phase_cycle[(current_idx + 1) % 3]

            time.sleep(random.uniform(1.0, 3.0))
    except KeyboardInterrupt:
        print("Signal controller stopped.")
    except Exception as e:
        print(f"Producer error: {e}")

if __name__ == "__main__":
    main()