#!/usr/bin/env bash
set -euo pipefail

PUBLIC_HOST="${1:-}"
if [[ -z "$PUBLIC_HOST" ]]; then
  echo "Usage: sudo bash $0 <public-hostname-or-ip>" >&2
  exit 1
fi

if [[ $EUID -ne 0 ]]; then
  echo "Run as root: sudo bash $0 <public-hostname-or-ip>" >&2
  exit 1
fi

SECRETS_DIR="/etc/kafka/secrets"
PASS_FILE="$SECRETS_DIR/keystore.pass"
if [[ ! -f "$PASS_FILE" ]]; then
  echo "Missing $PASS_FILE. Run: sudo bash scripts/generate_tls.sh $PUBLIC_HOST" >&2
  exit 1
fi
PASS="$(cat "$PASS_FILE")"

TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT

sed \
  -e "s/__PUBLIC_HOST__/${PUBLIC_HOST}/g" \
  -e "s/__KEYSTORE_PASSWORD__/${PASS}/g" \
  -e "s/__KEY_PASSWORD__/${PASS}/g" \
  -e "s/__TRUSTSTORE_PASSWORD__/${PASS}/g" \
  kafka/config/server.properties.secure > "$TMP"

cp -f "$TMP" /etc/kafka/server.properties
chmod 640 /etc/kafka/server.properties
chown root:root /etc/kafka/server.properties

systemctl restart kafka.service
sleep 2
systemctl --no-pager --full status kafka.service | sed -n '1,25p'

echo "Kafka is now configured for SASL_SSL on :9092 (public)"
echo "Next: sudo bash scripts/write_admin_properties.sh"
