from kafka import KafkaProducer
import json
import time
import random

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_NAME = "test_topic" 

# Create a Kafka producer instance
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    # The value_serializer converts a Python dictionary to a JSON string and then encodes it to bytes
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# A list of base messages to be sent. We will modify these in the loop.
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

# Use a counter to create unique IDs for each message.
message_id = 100

try:
    while True:
        # Select a random base message from the list
        msg_data = random.choice(base_messages)
        
        # Create a new message with a unique ID and a slight age variation
        message = {
            "id": message_id,
            "fname": msg_data["fname"],
            # Add a small random number to the age to simulate a changing value
            "age": msg_data["age"] + random.randint(-2, 2),
            "country": msg_data["country"]
        }
        
        # Send the message to the Kafka topic
        producer.send(TOPIC_NAME, value=message)
        
        print(f"Sent: {message}")
        
        # Increment the message ID for the next message
        message_id += 1
        
        # Wait for 1 second before sending the next message to simulate a stream
        time.sleep(3)

except KeyboardInterrupt:
    print("Stopping producer...")
finally:
    # Ensure all messages are sent before closing the producer
    producer.flush()
    producer.close()
    print("Producer closed.")
