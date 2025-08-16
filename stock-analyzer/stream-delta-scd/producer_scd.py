# producer_scd.py
import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer

TOPIC_NAME = "employees"
BOOTSTRAP_SERVERS = "localhost:9092"

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

countries = ["US", "IN"]
names = ["Alice", "Akash", "Neha"]

while True:
    msg = {
        "name": random.choice(names),
        "age": random.randint(20, 60),
        "country": random.choice(countries),
        "event_time": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    }
    producer.send(TOPIC_NAME, msg)
    print("Produced:", msg)
    time.sleep(2)
