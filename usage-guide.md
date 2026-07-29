# 📘 StreamBase Client Integration Guide

Welcome to **StreamBase**! This guide details how to connect your application (Python, Node.js, Java, Go, or CLI) to the StreamBase managed Apache Kafka® cluster.

---

## 1. Core Connection Parameters

Every client application requires the following configuration properties:

| Parameter | Value / Setting | Notes |
| :--- | :--- | :--- |
| **Bootstrap Server** | `broker.subartaghosh.co.in:9092` | Production SASL_SSL Kafka Endpoint |
| **Security Protocol** | `SASL_SSL` | Enforces encrypted SSL TLS transport |
| **SASL Mechanism** | `SCRAM-SHA-512` | Cryptographic password authentication |
| **SASL Username** | `<your-github-username>` | Generated automatically upon GitHub login |
| **SASL Password** | `<YOUR_PASSWORD>` | Revealed in your StreamBase dashboard |
| **Default Topic** | `<your-github-username>.events` | Scoped topic with 3 partitions |

> [!IMPORTANT]
> **Topic Naming Rule**: All your topics must start with your GitHub handle as a prefix (e.g. `your-github-username.my-topic`). ACL permissions restrict write access to topics matching your prefix.

---

## 2. Python Examples

### A. Using `kafka-python`
```bash
pip install kafka-python
```

```python
from kafka import KafkaConsumer, KafkaProducer
import ssl

ssl_ctx = ssl.create_default_context()

# --- PRODUCER ---
producer = KafkaProducer(
    bootstrap_servers='broker.subartaghosh.co.in:9092',
    security_protocol='SASL_SSL',
    sasl_mechanism='SCRAM-SHA-512',
    sasl_plain_username='your-github-username',
    sasl_plain_password='YOUR_PASSWORD',
    ssl_context=ssl_ctx
)

producer.send('your-github-username.events', b'Hello from Python!')
producer.flush()
print("Sent message successfully!")

# --- CONSUMER ---
consumer = KafkaConsumer(
    'your-github-username.events',
    bootstrap_servers='broker.subartaghosh.co.in:9092',
    security_protocol='SASL_SSL',
    sasl_mechanism='SCRAM-SHA-512',
    sasl_plain_username='your-github-username',
    sasl_plain_password='YOUR_PASSWORD',
    ssl_context=ssl_ctx,
    auto_offset_reset='earliest',
    group_id='your-github-username-group'
)

for msg in consumer:
    print(f"Received: {msg.value.decode('utf-8')}")
```

---

## 3. Node.js Example (`kafkajs`)

```bash
npm install kafkajs
```

```javascript
const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['broker.subartaghosh.co.in:9092'],
  ssl: true,
  sasl: {
    mechanism: 'scram-sha-512',
    username: 'your-github-username',
    password: 'YOUR_PASSWORD'
  }
});

async function run() {
  const producer = kafka.producer();
  await producer.connect();
  await producer.send({
    topic: 'your-github-username.events',
    messages: [{ value: JSON.stringify({ event: 'user_signup', ts: Date.now() }) }]
  });
  console.log('Message sent!');
  await producer.disconnect();
}

run().catch(console.error);
```

---

## 4. CLI Console Tools

You can produce and consume directly using official Kafka CLI scripts:

```bash
# 1. Create client.properties file
cat > client.properties << EOF
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="your-github-username" password="YOUR_PASSWORD";
EOF

# 2. Consume messages
kafka-console-consumer.sh \
  --bootstrap-server broker.subartaghosh.co.in:9092 \
  --consumer.config client.properties \
  --topic your-github-username.events \
  --from-beginning

# 3. Produce messages
echo "Hello StreamBase" | kafka-console-producer.sh \
  --bootstrap-server broker.subartaghosh.co.in:9092 \
  --producer.config client.properties \
  --topic your-github-username.events
```

---

## ⚖️ Legal Notice

*Apache®, Apache Kafka®, Kafka®, and the Kafka logo are registered trademarks of The Apache Software Foundation. StreamBase is not affiliated with or endorsed by The Apache Software Foundation.*
