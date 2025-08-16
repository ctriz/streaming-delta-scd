from kafka import KafkaProducer
import json, time, random
from kafka.errors import NoBrokersAvailable
from datetime import datetime
# Kafka configuration
# Use the Docker service name for the Kafka broker
# KAFKA_BOOTSTRAP = "kafka:9092"  # Uncomment this line if running in Docker with a service named 'kafka'

# Use the Docker service name for the Kafka broker
KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_NAME = "test_topic" 

#Retry logic to handle Kafka not being ready yet.
print("Kafka producer is attempting to connect to the broker with retry logic.")
producer = None
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        print("Successfully connected to Kafka.")
    except NoBrokersAvailable:
        print("Kafka broker not yet available, retrying in 5 seconds...")
        time.sleep(5)

# Sample data to send
countries = ["US", "IN", "UK", "DE"]
names = ["Alice", "Bob", "Charlie", "David", "Eve"]

print(f"Starting to send messages to topic '{TOPIC_NAME}'...")


try:
    while True:
       msg = {
        "name": random.choice(names),
        "age": random.randint(20, 60),
        "country": random.choice(countries),
        "event_time": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    }
       producer.send(TOPIC_NAME, msg)
       print(f"Sent message: {msg}")
       time.sleep(1)
except KeyboardInterrupt:
    print("Stopping producer.")
finally:
    producer.close()
