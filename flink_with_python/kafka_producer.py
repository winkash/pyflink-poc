import json
import os
import time

from kafka import KafkaProducer


def create_kafka_producer(bootstrap_servers):
    """
    Create a Kafka producer.
    """
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    return producer


def send_messages(producer, topic, messages, total_messages=10):
    """
    Send a fixed number of messages to a Kafka topic using a for loop.
    """
    for i in range(total_messages):
        # Select the message from the list in a cyclic manner
        message = messages[i % len(messages)]
        producer.send(topic, message)
        print(f"Sent: {message}")
        time.sleep(1)  # Simulate a delay between messages


if __name__ == "__main__":
    # Kafka configuration
    bootstrap_servers = [os.environ["KAFKA_BROKER"]]  # Adjust this to your Kafka server configuration
    topic = os.environ["KAFKA_TOPIC"]

    # Messages to send
    messages = [
        {"message": "Hello Kafka"},
        {"message": "This is a test message"},
        {"message": "Python kafka-python producer"}
    ]

    # Create the producer
    producer = create_kafka_producer(bootstrap_servers)

    # Send messages
    send_messages(producer, topic, messages)

    # Ensure all messages are sent before closing
    producer.flush()
