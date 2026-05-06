#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<EOF
Usage: sudo bash $0 --app <name> --topic <topic> [--partitions N] [--replication N]

Creates:
- SCRAM user <app>
- topic <topic>
- ACLs allowing <app> to Read/Write/Create/Describe only for topics prefixed with "<app>."

Requires: /etc/kafka/admin.properties created by create_admin_user.sh
EOF
}

APP=""
TOPIC=""
PARTITIONS="3"
REPLICATION="1"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --app) APP="$2"; shift 2;;
    --topic) TOPIC="$2"; shift 2;;
    --partitions) PARTITIONS="$2"; shift 2;;
    --replication) REPLICATION="$2"; shift 2;;
    -h|--help) usage; exit 0;;
    *) echo "Unknown arg: $1"; usage; exit 1;;
  esac
done

if [[ $EUID -ne 0 ]]; then
  echo "Run as root: sudo bash $0 ..." >&2
  exit 1
fi

if [[ -z "$APP" || -z "$TOPIC" ]]; then
  usage
  exit 1
fi

if [[ "$TOPIC" != "$APP".* ]]; then
  echo "For safety, topic must start with '${APP}.' (got: $TOPIC)" >&2
  exit 1
fi

ADMIN_PROPS="/etc/kafka/admin.properties"
if [[ ! -f "$ADMIN_PROPS" ]]; then
  echo "Missing $ADMIN_PROPS. Run scripts/create_admin_user.sh first." >&2
  exit 1
fi

read -r -p "Password for app user '${APP}' (will not echo): " -s APP_PASS
echo
if [[ -z "$APP_PASS" ]]; then
  echo "Empty password not allowed" >&2
  exit 1
fi

# Create/Update SCRAM credential
/opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server 127.0.0.1:9092 \
  --command-config "$ADMIN_PROPS" \
  --alter \
  --add-config "SCRAM-SHA-512=[iterations=4096,password=${APP_PASS}]" \
  --entity-type users \
  --entity-name "$APP"

# Create topic
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server 127.0.0.1:9092 \
  --command-config "$ADMIN_PROPS" \
  --create --if-not-exists \
  --topic "$TOPIC" \
  --partitions "$PARTITIONS" \
  --replication-factor "$REPLICATION"

# ACLs for topic prefix <app>.
PREFIX="${APP}."

/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server 127.0.0.1:9092 \
  --command-config "$ADMIN_PROPS" \
  --add \
  --allow-principal "User:$APP" \
  --operation Read --operation Write --operation Describe --operation Create \
  --topic "$PREFIX" \
  --resource-pattern-type prefixed

# Consumer group access (optional but usually needed)
/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server 127.0.0.1:9092 \
  --command-config "$ADMIN_PROPS" \
  --add \
  --allow-principal "User:$APP" \
  --operation Read --operation Describe \
  --group "$APP" \
  --resource-pattern-type prefixed

echo "Provisioned app '$APP'"
echo "- Topic: $TOPIC"
echo "- Username: $APP"
echo "- Password: (the one you entered)"
echo "- Bootstrap: SASL_SSL://<public-host>:9092"
