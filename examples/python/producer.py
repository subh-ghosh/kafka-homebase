import os
import json
from confluent_kafka import Producer

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]  # e.g. kafka.yourdomain.com:9092
TOPIC = os.environ.get("KAFKA_TOPIC", "app1.events")
USERNAME = os.environ["KAFKA_USERNAME"]
PASSWORD = os.environ["KAFKA_PASSWORD"]
CA_LOCATION = os.environ.get("KAFKA_CA_LOCATION", "./ca.crt")

producer = Producer(
    {
        "bootstrap.servers": BOOTSTRAP,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "SCRAM-SHA-512",
        "sasl.username": USERNAME,
        "sasl.password": PASSWORD,
        "ssl.ca.location": CA_LOCATION,
        "client.id": "python-producer",
    }
)

def delivery_callback(err, msg):
    if err is not None:
        print(f"delivery failed: {err}")
    else:
        print(f"delivered to {msg.topic()}[{msg.partition()}] @ offset {msg.offset()}")

payload = {"hello": "world"}
producer.produce(TOPIC, value=json.dumps(payload).encode("utf-8"), callback=delivery_callback)
producer.flush(10)
