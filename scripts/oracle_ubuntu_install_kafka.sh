#!/usr/bin/env bash
set -euo pipefail

KAFKA_VERSION_DEFAULT="3.7.0"
SCALA_VERSION_DEFAULT="2.13"

KAFKA_VERSION="${KAFKA_VERSION:-$KAFKA_VERSION_DEFAULT}"
SCALA_VERSION="${SCALA_VERSION:-$SCALA_VERSION_DEFAULT}"

if [[ $EUID -ne 0 ]]; then
  echo "Run as root: sudo bash $0" >&2
  exit 1
fi

apt-get update
apt-get install -y --no-install-recommends \
  ca-certificates curl jq openssl \
  openjdk-17-jre-headless \
  net-tools

id -u kafka >/dev/null 2>&1 || useradd --system --home /var/lib/kafka --shell /usr/sbin/nologin kafka

mkdir -p /opt
if [[ ! -d /opt/kafka ]]; then
  tmpdir="$(mktemp -d)"
  trap 'rm -rf "$tmpdir"' EXIT

  tgz="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
  url="https://downloads.apache.org/kafka/${KAFKA_VERSION}/${tgz}"
  echo "Downloading $url"
  curl -fL "$url" -o "$tmpdir/$tgz"
  tar -xzf "$tmpdir/$tgz" -C "$tmpdir"
  mv "$tmpdir/kafka_${SCALA_VERSION}-${KAFKA_VERSION}" /opt/kafka
fi

mkdir -p /etc/kafka /etc/kafka/secrets /var/lib/kafka/logs /var/log/kafka
chown -R kafka:kafka /var/lib/kafka /var/log/kafka
chmod 750 /etc/kafka/secrets

# Install default (local-only) config
cp -f kafka/config/server.properties.local /etc/kafka/server.properties
chown root:root /etc/kafka/server.properties
chmod 644 /etc/kafka/server.properties

# Install systemd unit
cp -f kafka/systemd/kafka.service /etc/systemd/system/kafka.service
systemctl daemon-reload
systemctl enable kafka.service

echo "Installed Kafka to /opt/kafka"
echo "Next: sudo bash scripts/bootstrap_local_plaintext.sh"
