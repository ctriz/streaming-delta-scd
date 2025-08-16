from kafka import KafkaProducer
import json
import time
import random

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

while True:
    message = {
        "id": random.randint(1000, 9999),
        "timestamp": time.time(),
        "value": round(random.uniform(10, 100), 2)
    }
    producer.send("sample-topic", message)
    print(f"Sent: {message}")
    time.sleep(1)
