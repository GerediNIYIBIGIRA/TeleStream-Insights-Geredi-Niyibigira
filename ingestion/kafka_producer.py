"""
A minimal Kafka producer scaffold. Uses kafka-python if available. The producer reads
sample JSON lines from a file or generator and sends them to a topic. This is a template
for how to implement streaming producers in the ingestion layer.
"""
import json
import time
from typing import Iterable, Dict

try:
    from kafka import KafkaProducer
except Exception:
    KafkaProducer = None


class SimpleKafkaProducer:
    def __init__(self, bootstrap_servers: str = "localhost:9092"):
        self.bootstrap = bootstrap_servers
        if KafkaProducer is None:
            raise RuntimeError("kafka-python is not installed")
        self.producer = KafkaProducer(
            bootstrap_servers=[self.bootstrap],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def send(self, topic: str, record: Dict):
        future = self.producer.send(topic, record)
        result = future.get(timeout=10)
        return result


def generate_example_events(n=10):
    for i in range(n):
        yield {"event_id": i, "value": f"example-{i}", "ts": int(time.time())}


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", required=True)
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--count", type=int, default=10)
    args = parser.parse_args()

    prod = None
    try:
        prod = SimpleKafkaProducer(args.bootstrap)
        for e in generate_example_events(args.count):
            print(f"Sending {e}")
            prod.send(args.topic, e)
            time.sleep(0.1)
    except RuntimeError:
        print("Kafka library not available. This is a scaffold for production use.")