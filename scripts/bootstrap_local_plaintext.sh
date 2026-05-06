#!/usr/bin/env bash
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
  echo "Run as root: sudo bash $0" >&2
  exit 1
fi

CONFIG="/etc/kafka/server.properties"

# Format KRaft storage (idempotent)
CLUSTER_ID_FILE="/etc/kafka/cluster.id"
if [[ -f "$CLUSTER_ID_FILE" ]]; then
  CLUSTER_ID="$(cat "$CLUSTER_ID_FILE")"
else
  CLUSTER_ID="$(/opt/kafka/bin/kafka-storage.sh random-uuid)"
  echo "$CLUSTER_ID" > "$CLUSTER_ID_FILE"
  chmod 600 "$CLUSTER_ID_FILE"
fi

/opt/kafka/bin/kafka-storage.sh format -t "$CLUSTER_ID" -c "$CONFIG" --ignore-formatted

systemctl restart kafka.service
sleep 2
systemctl --no-pager --full status kafka.service | sed -n '1,25p'

echo "Kafka started locally on 127.0.0.1:9092 (PLAINTEXT)"
