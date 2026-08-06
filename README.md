# ⚡ StreamBase — Free Managed Kafka Platform for Students & Developers

<p align="center">
  <strong>Production-Grade, SASL/SSL-Secured Multi-Tenant Apache Kafka® Infrastructure.</strong>
</p>

<p align="center">
  <a href="https://streambase.subartaghosh.co.in"><img src="https://img.shields.io/badge/Live_Portal-streambase.subartaghosh.co.in-FF3B30?style=for-the-badge&logo=apachekafka&logoColor=white" /></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-FFE500?style=for-the-badge&logo=open-source-initiative&logoColor=black" /></a>
  <img src="https://img.shields.io/badge/Status-Production_Live-1DB954?style=for-the-badge&logo=statuspage&logoColor=white" />
  <img src="https://img.shields.io/badge/Architecture-KRaft_Mode-0055FF?style=for-the-badge&logo=docker&logoColor=white" />
  <img src="https://img.shields.io/badge/npm_audit-0_vulnerabilities-00C853?style=for-the-badge&logo=snyk&logoColor=white" />
</p>

---

## 🚀 What is StreamBase?

**StreamBase** is an open-source, multi-tenant Kafka cloud platform that eliminates the $50+/month cost barrier of commercial brokers (Confluent Cloud, AWS MSK) for students and developers.

Developers authenticate via GitHub OAuth and instantly receive isolated, production-grade Kafka credentials — no credit card, no manual setup required.

---

## 📊 Measured Production Performance

All metrics measured against the live production endpoint (`streambase.subartaghosh.co.in`) over HTTPS:

| Endpoint | avg | p50 | p95 |
|---|---|---|---|
| `GET /api/config` | **230ms** | 229ms | 243ms |
| `GET /` (landing page) | **330ms** | 268ms | 892ms |
| Auth rejection (`/api/user`, no token) | **229ms** | 228ms | 233ms |
| 20 concurrent burst (`/api/config`) | **100% success** in 1,074ms total | avg 641ms | p95 1,066ms |

> Optimized via gzip compression (level 6), `Cache-Control` headers (`s-maxage=300` Cloudflare edge cache), and ETag-based static file caching — reducing API latency **66%** (682ms → 230ms avg) and landing page load **77%** (1,461ms → 330ms avg).

---

## ✨ Features

- **Zero Credit Card Required** — Instant onboarding via GitHub OAuth
- **Production Encryption** — `SASL_SSL` + `SCRAM-SHA-512` authentication on Port 9092
- **Multi-Tenant ACL Isolation** — Per-user Kafka principal scoping; no cross-tenant data access possible
- **Bandwidth Throttling** — 1 MB/sec producer + consumer quota per tenant via `kafka-configs.sh` dynamic overrides
- **Storage Quota Enforcement** — 125 MB cap per user with automated ACL write-revocation on breach, restored on recovery
- **3-Partition Topics** — Real distributed partition support out of the box
- **Live Stream Inspector** — Read and inspect live Kafka messages directly from the browser
- **Interactive Producer** — Send test JSON or string messages to topics from the web UI
- **Multi-Layer Rate Limiting** — 10 registrations/hr · 50 mutations/15min · 200 reads/15min · 30 admin/min

---

## 🛡 Security

- **0 npm vulnerabilities** (`npm audit` clean)
- **CORS locked** to `https://streambase.subartaghosh.co.in` — no wildcard `*` origin
- **5-minute GitHub token cache** — prevents GitHub API rate-limit exhaustion on repeated requests
- **Shell injection prevention** — all usernames validated against `/^user_[a-f0-9]{8}$/` before any `kafka-*.sh` invocation
- **Admin routes** protected by HTTP Basic Auth with a dedicated rate limiter (30 req/min)
- **Cloudflare TLS proxy** — EC2 instance not directly exposed to the internet

---

## 🏗 System Architecture

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
                  │  gzip · Rate-Limiting · SQLite · ACLs  │
                  └────────────────────┬────────────────────┘
                                       │
                               Port 9094 (Internal PLAINTEXT)
                                       │
┌──────────────────────────────────────▼──────────────────────────────────────┐
│                            Apache Kafka® Broker                             │
│                  SASL/SSL Encrypted Public Endpoint (Port 9092)             │
│            SCRAM-SHA-512 · ACL Enforced · 3-Partition Default Topics        │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Core Tech Stack

| Layer | Technology |
|---|---|
| Streaming Core | Apache Kafka 3.7+ in **KRaft mode** (ZooKeeper-free) |
| Security | `SASL_SSL` + `SCRAM-SHA-512` + per-principal ACL scoping |
| Control Plane | Node.js · Express · SQLite · `p-queue` (concurrency-safe shell operations) |
| Performance | `compression` middleware (gzip level 6) · Cloudflare edge caching |
| Infrastructure | AWS EC2 · systemd service · Cloudflare TLS reverse proxy |

---

## 💻 Quick Connect

### 🐍 Python (`kafka-python`)
```python
from kafka import KafkaConsumer
import ssl

ssl_ctx = ssl.create_default_context()

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

## 🔧 Self-Hosting

```bash
git clone https://github.com/subh-ghosh/kafka-homebase.git
cd kafka-homebase/portal
npm install
```

Required environment variables:
```env
PORT=80
GITHUB_CLIENT_ID=your_github_oauth_client_id
GITHUB_CLIENT_SECRET=your_github_oauth_client_secret
ADMIN_USER=admin
ADMIN_PASS=your_secure_admin_password
```

```bash
node index.js
```

---

## 🧪 Running the Test Suite

```bash
cd portal
GITHUB_TOKEN=your_gho_token ADMIN_PASS=your_admin_pass node loadtest.js
```

Tests cover: public routes, GitHub OAuth token validation, topic CRUD, message produce/consume, account management, and admin endpoints.

---

## 📂 Repository Structure

| Path | Description |
|---|---|
| `portal/index.js` | Node.js Express control plane — API routes, auth, ACL provisioning |
| `portal/db.js` | SQLite database layer |
| `portal/public/` | Frontend portal (HTML/CSS/JS, served by Express) |
| `portal/loadtest.js` | Full API endpoint test suite |
| `kafka/` | Kafka broker configuration (`server.properties`, systemd templates) |
| `scripts/` | KRaft init, TLS cert generation, user provisioning automation |
| `docs/` | AWS EC2 & Oracle Cloud deployment guides |
| `examples/` | Minimal producer/consumer scripts for Python, Node.js, Java |

---

## ⚖️ Legal

*Apache®, Apache Kafka®, and Kafka® are registered trademarks of The Apache Software Foundation. StreamBase is not affiliated with, endorsed by, or sponsored by The Apache Software Foundation.*

---

## 📜 License

Distributed under the **MIT License**. See [`LICENSE`](LICENSE) for details.
