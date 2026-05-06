#!/usr/bin/env bash
set -euo pipefail

if [[ $EUID -ne 0 ]]; then
  echo "Run as root: sudo bash $0" >&2
  exit 1
fi

SECRETS_DIR="/etc/kafka/secrets"
PASS_FILE="$SECRETS_DIR/keystore.pass"
TRUSTSTORE="$SECRETS_DIR/kafka.truststore.p12"

if [[ ! -f "$PASS_FILE" || ! -f "$TRUSTSTORE" ]]; then
  echo "Missing TLS truststore. Run scripts/generate_tls.sh then scripts/enable_secure_public.sh" >&2
  exit 1
fi

DEFAULT_PASS_FILE="/etc/kafka/admin.pass"
if [[ -f "$DEFAULT_PASS_FILE" ]]; then
  read -r -p "Use password from $DEFAULT_PASS_FILE? [Y/n] " yn
  yn=${yn:-Y}
  if [[ "$yn" =~ ^[Yy]$ ]]; then
    ADMIN_PASS="$(cat "$DEFAULT_PASS_FILE")"
  fi
fi

if [[ -z "${ADMIN_PASS:-}" ]]; then
  read -r -p "Admin password (will not echo): " -s ADMIN_PASS
  echo
fi

if [[ -z "$ADMIN_PASS" ]]; then
  echo "Empty password not allowed" >&2
  exit 1
fi

ADMIN_PROPS="/etc/kafka/admin.properties"
cat > "$ADMIN_PROPS" <<EOF
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username=\"admin\" password=\"${ADMIN_PASS}\";
ssl.truststore.type=PKCS12
ssl.truststore.location=${TRUSTSTORE}
ssl.truststore.password=$(cat "$PASS_FILE")
EOF

chmod 600 "$ADMIN_PROPS"

echo "Wrote $ADMIN_PROPS"
echo "You can now run scripts/provision_app.sh"
