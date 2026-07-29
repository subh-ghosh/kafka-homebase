# Personal Kafka (self-hosted)

Run your **own** Kafka broker on a small cloud VM (recommended: **Oracle Cloud Always Free** or **AWS EC2 Free Tier**) and use it from any of your Python / Node / Java projects.

This repo targets a **single-node** Kafka setup in **KRaft mode** (no ZooKeeper). It’s meant for learning and small projects.

## What you get

- A repeatable install for an Ubuntu VM (systemd service)
- A bootstrap → secure setup flow (so you don’t expose an open broker)
- Public endpoint with **TLS + SASL/SCRAM** (username/password)
- A provisioning script to onboard new apps (create user + topics + ACLs)
- Minimal producer/consumer examples for Python, Node, and Java

## Reality check (important)

- A single VM is **not HA**. If the VM dies/reboots, Kafka goes down.
- Do **not** let browsers/mobile apps connect directly to Kafka.
  - Your backend services connect to Kafka.
  - End users talk to your backend over HTTP/WebSocket.

## Quick start (high level)

1. Create a cloud VM running Ubuntu 22.04 (e.g., Oracle Always Free or AWS EC2 t2.micro).
2. Clone this repo on the VM.
3. Run the installer script.
4. Run the bootstrap script to initialize KRaft storage.
5. Create the first `admin` user (still local-only).
6. Generate TLS certs and switch to the secure config.
7. Write `admin.properties` and onboard app users/topics.

Detailed steps: see [docs/oracle-ubuntu.md](file:///h:/Kafka/docs/oracle-ubuntu.md) or [docs/aws-ubuntu.md](file:///h:/Kafka/docs/aws-ubuntu.md)

If you encounter issues during setup, you can run the [repair.sh](file:///h:/Kafka/repair.sh) script to fully reset and re-initialize the installation securely.

## Folder map

- `docs/usage-guide.md` — **Start here:** code examples for Python, Node, and Java
- `docs/` — step-by-step VM setup
- `kafka/` — Kafka configs + systemd unit template
- `scripts/` — install/bootstrap/provision scripts
- `examples/` — Python/Node/Java client samples

## License

MIT (see LICENSE)
