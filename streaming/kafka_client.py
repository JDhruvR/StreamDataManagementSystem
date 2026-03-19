import json
from kafka import KafkaProducer, KafkaConsumer

class StreamProducer:
    def __init__(self, bootstrap_servers=['localhost:9092']):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def send(self, topic, data):
        self.producer.send(topic, data)
        # Flush is useful for scripts and simulators to ensure it's sent immediately
        # In high throughput, we might flush less often
        self.producer.flush()

class StreamConsumer:
    def __init__(self, topic, bootstrap_servers=['localhost:9092'], group_id=None):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8'))
        )

    def __iter__(self):
        return self

    def __next__(self):
        return next(self.consumer).value
