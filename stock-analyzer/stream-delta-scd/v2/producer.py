from kafka import KafkaProducer
import json, time, random
from kafka.errors import NoBrokersAvailable

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

base_messages = [
    {"fname": "Alice", "age": 30, "country": "IN"},
    {"fname": "Bob", "age": 25, "country": "US"},
    {"fname": "Charlie", "age": 35, "country": "UK"},
    {"fname": "Doug", "age": 40, "country": "CA"},
    {"fname": "Ebony", "age": 28, "country": "IN"},
    {"fname": "Fiona", "age": 32, "country": "US"},
    {"fname": "George", "age": 29, "country": "UK"},
]

print(f"Starting to send messages to topic '{TOPIC_NAME}'...")

message_id = 100

try:
    while True:
        msg_data = random.choice(base_messages)
        message = {
            "id": message_id,
            "fname": msg_data["fname"],
            # Add random integer between 1 and 5 to age
            "age": msg_data["age"] + random.randint(1, 5),
            "country": msg_data["country"]
        }
        producer.send(TOPIC_NAME, message)
        print(f"Sent message: {message}")
        message_id += 1
        time.sleep(1)
except KeyboardInterrupt:
    print("Stopping producer.")
finally:
    producer.close()
