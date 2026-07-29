# How to Use Your Kafka Cluster

Once your Kafka broker is running on your cloud instance, it is secured using **TLS (v1.3)** for encryption and **SASL/SCRAM** for authentication. This guide provides detailed examples of how to connect and use your Kafka cluster from various languages and tools.

## 1. Connection Details

To connect to your broker, you will always need these parameters:

- **Bootstrap Server:** `<YOUR_PUBLIC_IP_OR_DOMAIN>:9092`
- **Security Protocol:** `SASL_SSL`
- **SASL Mechanism:** `SCRAM-SHA-512`
- **Username:** `admin` *(or a specific app user created via provision_app.sh)*
- **Password:** `<YOUR_ADMIN_PASSWORD>` *(the password you set during the installation)*
- **TLS Certificate:** You must provide the truststore (`kafka.truststore.p12`) or a PEM file depending on the client library.

> [!IMPORTANT]
> The `kafka.truststore.p12` file (and its auto-generated password) is required for Java applications. You can find this password in `/etc/kafka/secrets/keystore.pass` on your server. Other languages (like Python or Node.js) may require you to export this `.p12` file into a `.pem` file, or you can temporarily disable certificate validation for development.

---

## 2. Using Python (`confluent-kafka`)

`confluent-kafka` is the recommended Python client. Since it uses `librdkafka` under the hood, it requires a PEM file for the CA certificate if you enforce strict TLS validation. For quick development, you can disable strict hostname checking.

**Install:**
```bash
pip install confluent-kafka
```

**Producer Example:**
```python
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': '<YOUR_PUBLIC_IP_OR_DOMAIN>:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'SCRAM-SHA-512',
    'sasl.username': 'admin',
    'sasl.password': '<YOUR_ADMIN_PASSWORD>',
    # If using a self-signed cert without a PEM, you can bypass strict checks for dev:
    'enable.ssl.certificate.verification': False
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Send a message
producer.produce('my-test-topic', key='key1', value='Hello Kafka!', callback=delivery_report)
producer.flush()
```

---

## 3. Using Node.js (`kafkajs`)

`kafkajs` is a pure JavaScript client for Node.js.

**Install:**
```bash
npm install kafkajs
```

**Producer & Consumer Example:**
```javascript
const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['<YOUR_PUBLIC_IP_OR_DOMAIN>:9092'],
  ssl: {
    rejectUnauthorized: false // Set to true in prod with proper CA
  },
  sasl: {
    mechanism: 'scram-sha-512',
    username: 'admin',
    password: '<YOUR_ADMIN_PASSWORD>'
  },
})

const run = async () => {
  // Produce
  const producer = kafka.producer()
  await producer.connect()
  await producer.send({
    topic: 'my-test-topic',
    messages: [
      { value: 'Hello Kafka from Node!' },
    ],
  })
  console.log("Message sent!")
  await producer.disconnect()

  // Consume
  const consumer = kafka.consumer({ groupId: 'test-group' })
  await consumer.connect()
  await consumer.subscribe({ topic: 'my-test-topic', fromBeginning: true })

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(`Received: ${message.value.toString()}`)
    },
  })
}

run().catch(console.error)
```

---

## 4. Using Java (Spring Boot or standard `kafka-clients`)

Java applications can natively read the `kafka.truststore.p12` file that is generated during installation.

**Properties Configuration (`application.properties`):**
```properties
spring.kafka.bootstrap-servers=<YOUR_PUBLIC_IP_OR_DOMAIN>:9092

spring.kafka.properties.security.protocol=SASL_SSL
spring.kafka.properties.sasl.mechanism=SCRAM-SHA-512
spring.kafka.properties.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="<YOUR_ADMIN_PASSWORD>";

spring.kafka.ssl.trust-store-location=file:/path/to/your/kafka.truststore.p12
spring.kafka.ssl.trust-store-password=<YOUR_TRUSTSTORE_PASSWORD>
spring.kafka.ssl.trust-store-type=PKCS12
```

---

## 5. Using Kafka Command Line Tools

If you have Kafka installed locally on your machine, you can use the built-in shell scripts to manage your cluster. 

You must pass a `client.properties` file to the tools so they know how to authenticate.

**Create a Topic:**
```bash
kafka-topics.sh --bootstrap-server <YOUR_PUBLIC_IP_OR_DOMAIN>:9092 \
  --command-config client.properties \
  --create --topic my-test-topic --partitions 3 --replication-factor 1
```

**Produce Messages from CLI:**
```bash
kafka-console-producer.sh --bootstrap-server <YOUR_PUBLIC_IP_OR_DOMAIN>:9092 \
  --producer.config client.properties \
  --topic my-test-topic
```

**Consume Messages from CLI:**
```bash
kafka-console-consumer.sh --bootstrap-server <YOUR_PUBLIC_IP_OR_DOMAIN>:9092 \
  --consumer.config client.properties \
  --topic my-test-topic --from-beginning
```

---

## 6. Best Practices for Production

1. **Do not use the `admin` user in your apps:** The `admin` user should only be used to create topics and provision new users. 
2. **Create App-Specific Users:** SSH into your EC2 instance and run the provision script to create a limited user for each app. Example:
   ```bash
   sudo bash scripts/provision_app.sh --app ecommerce-backend --topic ecommerce.events --partitions 3
   ```
   This restricts the `ecommerce-backend` user so they can *only* read/write to topics starting with `ecommerce.events`.
3. **Handle Certificates Properly:** For production, convert your `truststore.p12` into a PEM file and enforce `rejectUnauthorized: true` (Node) or `enable.ssl.certificate.verification: true` (Python).
