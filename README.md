# ⚡ StreamBase — Free Kafka-Compatible Message Broker for Students & Developers

<p align="center">
  <strong>Production-Grade, SASL/SSL-Secured Managed Apache Kafka® Infrastructure for Learning & Building.</strong>
</p>

<p align="center">
  <a href="https://streambase.subartaghosh.co.in"><img src="https://img.shields.io/badge/Live_Portal-streambase.subartaghosh.co.in-FF3B30?style=for-the-badge&logo=apachekafka&logoColor=white" /></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-FFE500?style=for-the-badge&logo=open-source-initiative&logoColor=black" /></a>
  <img src="https://img.shields.io/badge/Status-Production_Live-1DB954?style=for-the-badge&logo=statuspage&logoColor=white" />
  <img src="https://img.shields.io/badge/Architecture-KRaft_Mode-0055FF?style=for-the-badge&logo=docker&logoColor=white" />
</p>

---

## 🚀 What is StreamBase?

**StreamBase** is an open-source, multi-tenant Kafka cloud platform engineered to eliminate the expensive $50+/month cost barriers of commercial cloud brokers (such as Confluent Cloud or AWS MSK) for students, educators, open-source contributors, and independent developers.

With **StreamBase**, developers authenticate via GitHub OAuth and instantly receive:
- 🔐 **Dedicated SASL/SSL Kafka Credentials** (`SCRAM-SHA-512` encryption on Port 9092)
- 📦 **3-Partition Custom Topics** scoped under their GitHub handle (`<github-handle>.events`)
- ⚡ **1-Click Quick Connect Code Snippets** for Python, Node.js, Java, and CLI
- 📊 **Real-time Web Management Dashboard** featuring storage quota meters (125 MB), consumer group monitoring, and interactive produce/consume stream viewers
- 🛡 **Multi-Tenant Isolation** with ACL principal enforcement and bandwidth rate-limiting (1 MB/sec producer/consumer throttling)

---

## ✨ Features

- **Zero Credit Card Required**: Instant onboarding via GitHub OAuth.
- **Production Encryption**: Full `SASL_SSL` connection protocol using `SCRAM-SHA-512` authentication.
- **3-Partition Topics**: Real distributed topic partition support out of the box.
- **Live Stream Inspector**: Read and inspect live messages directly from the web portal.
- **Interactive Producer**: Send test JSON or string messages to your topics straight from the web UI.
- **Storage & Quota Management**: Built-in automated retention and disk quota monitoring.
- **Neo-Brutalist UI**: High-contrast, responsive interface built with modern web aesthetics.

---

## 🏗 System Architecture

StreamBase is built as a decoupled, high-performance distributed streaming control plane:

```
                  ┌─────────────────────────────────────────┐
                  │          StreamBase Web Portal          │
                  │   https://streambase.subartaghosh.co.in │
                  └────────────────────┬────────────────────┘
                                       │
                                 GitHub OAuth
                                       │
                  ┌────────────────────▼────────────────────┐
                  │    Express Control Plane & API Engine   │
                  │  (Rate-Limiting, DB, ACL Provisioner)  │
                  └────────────────────┬────────────────────┘
                                       │
                               Port 9094 (Internal)
                                       │
┌──────────────────────────────────────▼──────────────────────────────────────┐
│                            Apache Kafka® Broker                             │
│                  SASL/SSL Encrypted Endpoint (Port 9092)                    │
│            scram-sha-512 · ACL Enforced · 3-Partition Default Topics         │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Core Tech Stack
- **Streaming Core**: Native Apache Kafka 3.7+ running in **KRaft mode** (ZooKeeper-less).
- **Security Protocols**: `SASL_SSL` encryption with `SCRAM-SHA-512` authentication & ACL principal scoping.
- **Control Plane Engine**: Node.js, Express, SQLite (`db.sqlite`), Async Execution Queue (`p-queue`).
- **Frontend Architecture**: Neo-Brutalist UI system (Space Grotesk, DM Mono, CSS Design Tokens).
- **Production Host**: AWS EC2 instance, systemd service management, Cloudflare TLS proxy.

---

## 💻 Quick Connect Code Snippets

### 🐍 Python (`kafka-python` / `confluent-kafka`)
```python
from kafka import KafkaConsumer, KafkaProducer
import ssl

ssl_ctx = ssl.create_default_context()

# Consumer Example
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
    print(msg.value.decode('utf-8'))
```

### 🟨 Node.js (`kafkajs`)
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

const producer = kafka.producer();
await producer.connect();
await producer.send({
  topic: 'your-github-username.events',
  messages: [{ value: 'Hello StreamBase!' }]
});
```

---

## 🔧 Self-Hosting & Deployment Guide

To deploy your own instance of **StreamBase** on an Ubuntu EC2 or Linux VM:

### 1. Clone & Set Up Environment
```bash
git clone https://github.com/subh-ghosh/kafka-homebase.git
cd kafka-homebase/portal
npm install
```

### 2. Configure Environment Variables
Create a systemd unit or `.env` file with:
```env
PORT=80
GITHUB_CLIENT_ID=your_github_oauth_client_id
GITHUB_CLIENT_SECRET=your_github_oauth_client_secret
ADMIN_USER=admin
ADMIN_PASS=your_secure_admin_password
```

### 3. Start Control Plane Service
```bash
node index.js
```

---

## 📂 Repository Structure

- `portal/` — Node.js Express API control plane, SQLite database layer, and frontend web portal (`index.html`, `admin.html`).
- `kafka/` — Kafka broker configuration files (`server.properties`, systemd unit templates).
- `scripts/` — Automated installation, KRaft initialization, cert generation, and user provisioning scripts.
- `docs/` — Comprehensive cloud deployment guides (AWS EC2 & Oracle Cloud Ubuntu).
- `examples/` — Minimal runnable producer/consumer scripts for Python, Node.js, and Java.

---

## ⚖️ Legal & Trademark Notice

*Apache®, Apache Kafka®, Kafka®, and the Kafka logo are registered trademarks of The Apache Software Foundation in the United States and/or other countries. StreamBase is not affiliated with, endorsed by, or sponsored by The Apache Software Foundation.*

---

## 📜 License

Distributed under the **MIT License**. See [`LICENSE`](file:///h:/Kafka/LICENSE) for details.
