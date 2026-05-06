#!/usr/bin/env bash
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
  echo "Run as root: sudo bash $0" >&2
  exit 1
fi

if grep -q "^listeners=.*SASL_SSL" /etc/kafka/server.properties 2>/dev/null; then
  echo "Kafka is already in secure SASL_SSL mode." >&2
  echo "This script is intended to be run BEFORE enabling SASL, during local PLAINTEXT bootstrap." >&2
  echo "To rotate admin credentials later, use kafka-configs.sh with /etc/kafka/admin.properties." >&2
  exit 1
fi

read -r -p "Admin password (will not echo): " -s ADMIN_PASS
echo
if [[ -z "$ADMIN_PASS" ]]; then
  echo "Empty password not allowed" >&2
  exit 1
fi

# Create/Update SCRAM credential for admin
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server 127.0.0.1:9092 \
  --alter \
  --add-config "SCRAM-SHA-512=[iterations=4096,password=${ADMIN_PASS}]" \
  --entity-type users \
  --entity-name admin

ADMIN_PASS_FILE="/etc/kafka/admin.pass"
echo "$ADMIN_PASS" > "$ADMIN_PASS_FILE"
chmod 600 "$ADMIN_PASS_FILE"

echo "Admin user created/updated (SCRAM-SHA-512)."
echo "Saved password (root-only) at: $ADMIN_PASS_FILE"
echo "Next: generate TLS + enable secure mode, then run scripts/write_admin_properties.sh"
