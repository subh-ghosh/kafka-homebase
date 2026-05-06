#!/usr/bin/env bash
set -euo pipefail

HOSTNAME="${1:-}"
if [[ -z "$HOSTNAME" ]]; then
  echo "Usage: sudo bash $0 <public-hostname>" >&2
  exit 1
fi

if [[ $EUID -ne 0 ]]; then
  echo "Run as root: sudo bash $0 <public-hostname>" >&2
  exit 1
fi

OUT_DIR="/etc/kafka/secrets"
mkdir -p "$OUT_DIR"
chmod 750 "$OUT_DIR"

CA_KEY="$OUT_DIR/ca.key"
CA_CERT="$OUT_DIR/ca.crt"
SERVER_KEY="$OUT_DIR/server.key"
SERVER_CSR="$OUT_DIR/server.csr"
SERVER_CERT="$OUT_DIR/server.crt"
SERVER_P12="$OUT_DIR/kafka.server.p12"
TRUSTSTORE_P12="$OUT_DIR/kafka.truststore.p12"

# Passwords are stored only in /etc/kafka/secrets/keystore.pass (root-only)
PASS_FILE="$OUT_DIR/keystore.pass"
if [[ -f "$PASS_FILE" ]]; then
  PASSWORD="$(cat "$PASS_FILE")"
else
  PASSWORD="$(openssl rand -base64 24)"
  echo "$PASSWORD" > "$PASS_FILE"
  chmod 600 "$PASS_FILE"
fi

# Create CA (if missing)
if [[ ! -f "$CA_KEY" || ! -f "$CA_CERT" ]]; then
  openssl genrsa -out "$CA_KEY" 4096
  openssl req -x509 -new -nodes -key "$CA_KEY" -sha256 -days 3650 \
    -subj "/CN=personal-kafka-ca" \
    -out "$CA_CERT"
fi

# Server cert with SAN
cat > "$OUT_DIR/openssl.cnf" <<EOF
[ req ]
default_bits       = 2048
distinguished_name = req_distinguished_name
req_extensions     = req_ext
prompt = no

[ req_distinguished_name ]
CN = ${HOSTNAME}

[ req_ext ]
subjectAltName = @alt_names

[ alt_names ]
DNS.1 = ${HOSTNAME}
EOF

openssl genrsa -out "$SERVER_KEY" 2048
openssl req -new -key "$SERVER_KEY" -out "$SERVER_CSR" -config "$OUT_DIR/openssl.cnf"
openssl x509 -req -in "$SERVER_CSR" -CA "$CA_CERT" -CAkey "$CA_KEY" -CAcreateserial \
  -out "$SERVER_CERT" -days 825 -sha256 -extensions req_ext -extfile "$OUT_DIR/openssl.cnf"

# Create PKCS12 keystore
openssl pkcs12 -export \
  -in "$SERVER_CERT" \
  -inkey "$SERVER_KEY" \
  -certfile "$CA_CERT" \
  -out "$SERVER_P12" \
  -password "pass:$PASSWORD" \
  -name kafka

# Create PKCS12 truststore containing CA cert
rm -f "$TRUSTSTORE_P12"
keytool -importcert -noprompt \
  -alias personal-kafka-ca \
  -file "$CA_CERT" \
  -keystore "$TRUSTSTORE_P12" \
  -storetype PKCS12 \
  -storepass "$PASSWORD"

chown -R root:kafka "$OUT_DIR"
chmod 640 "$OUT_DIR"/*.crt "$OUT_DIR"/*.p12 2>/dev/null || true
chmod 600 "$OUT_DIR"/*.key "$PASS_FILE" 2>/dev/null || true

echo "TLS generated in $OUT_DIR"
echo "CA cert for clients: $CA_CERT"
echo "Keystore/truststore password stored at: $PASS_FILE"
