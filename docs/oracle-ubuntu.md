# Oracle Cloud Always Free (Ubuntu) — Personal Kafka

This guide installs a **single-node** Kafka broker (KRaft) on an Ubuntu VM.

## 0) VM checklist

- Ubuntu 22.04 (recommended)
- A public IPv4 address (or a domain pointing to it)
- Disk: at least ~30GB

## 1) Open only the needed ports

- `22/tcp` (SSH) — ideally restricted to your IP
- `9092/tcp` (Kafka clients) — open to the internet *only after* TLS+SASL are enabled

## 2) Install Kafka

On the VM:

```bash
git clone <YOUR_GITHUB_REPO_URL>
cd <repo>

sudo bash scripts/ubuntu_install_kafka.sh
```

## 3) Bootstrap (local-only, no public listener)

This initializes KRaft storage and starts Kafka only on localhost.

```bash
sudo bash scripts/bootstrap_local_plaintext.sh
```

## 4) Create the first admin user (still local-only)

This creates the initial `admin` SCRAM credential while Kafka is only reachable on `127.0.0.1`.

```bash
sudo bash scripts/create_admin_user.sh
```

## 5) Generate TLS material

Pick a hostname (recommended): `kafka.yourdomain.com`

```bash
sudo bash scripts/generate_tls.sh kafka.yourdomain.com
```

This writes certs into `secrets/` on the VM.

## 6) Switch to secure public mode

```bash
sudo bash scripts/enable_secure_public.sh kafka.yourdomain.com
```

At this point you can open `9092` publicly.

## 7) Create admin.properties for CLI tools

Kafka CLI tooling (topic/user/ACL commands) needs a config file with your admin login.

```bash
sudo bash scripts/write_admin_properties.sh
```

## 8) Onboard an app

Example: create user `app1`, topic prefix `app1.` and a topic `app1.events`.

```bash
sudo bash scripts/provision_app.sh \
  --app app1 \
  --topic app1.events \
  --partitions 3 \
  --replication 1
```

## Notes

- Remember that Oracle Always Free is limited to 2 instances.
- Keep `auto.create.topics.enable=false` (prevents abuse).
- Use topic prefixes per app (`app1.*`) and ACLs.



- For higher reliability you need multiple brokers (not free).
