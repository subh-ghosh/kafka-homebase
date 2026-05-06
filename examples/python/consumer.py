import os
from confluent_kafka import Consumer

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
TOPIC = os.environ.get("KAFKA_TOPIC", "app1.events")
USERNAME = os.environ["KAFKA_USERNAME"]
PASSWORD = os.environ["KAFKA_PASSWORD"]
CA_LOCATION = os.environ.get("KAFKA_CA_LOCATION", "./ca.crt")
GROUP = os.environ.get("KAFKA_GROUP", "app1")

consumer = Consumer(
    {
        "bootstrap.servers": BOOTSTRAP,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "SCRAM-SHA-512",
        "sasl.username": USERNAME,
        "sasl.password": PASSWORD,
        "ssl.ca.location": CA_LOCATION,
        "group.id": GROUP,
        "auto.offset.reset": "earliest",
    }
)

consumer.subscribe([TOPIC])
print(f"Consuming {TOPIC}...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"error: {msg.error()}")
            continue
        print(f"{msg.topic()}[{msg.partition()}]@{msg.offset()}: {msg.value().decode('utf-8')}")
finally:
    consumer.close()
