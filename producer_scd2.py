# producer_scd2_continuous.py
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

topic = "test_topic_scd2"

names = ["Alice", "Bob", "Charlie", "David", "Eve"]
countries = ["US", "UK", "DE", "IN", "FR"]

print("Starting Continuous SCD2 Producer...")

try:
    while True:
        # Randomly pick a name
        name = random.choice(names)

        # Either update existing person's age/country slightly or keep same
        age = random.randint(25, 60)
        country = random.choice(countries)

        event = {
            "name": name,
            "age": age,
            "country": country,
            "event_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        }

        producer.send(topic, event)
        print(f"Sent: {event}")

        # Wait 1 second before next record
        time.sleep(1)

except KeyboardInterrupt:
    print("Producer stopped by user")

finally:
    producer.close()
