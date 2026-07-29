#!/usr/bin/env bash
set -e
echo "Stopping Kafka..."
sudo systemctl stop kafka.service || true

echo "Wiping Kafka state..."
sudo rm -rf /var/lib/kafka/logs/*
sudo rm -rf /etc/kafka/secrets/*

echo "Restoring local plaintext config..."
sudo cp /home/ubuntu/kafka/config/server.properties.local /etc/kafka/server.properties
sudo chown kafka:kafka /etc/kafka/server.properties
sudo chmod 640 /etc/kafka/server.properties

echo "Bootstrapping..."
sudo bash /home/ubuntu/scripts/bootstrap_local_plaintext.sh

echo "Waiting for Kafka to start..."
sleep 10

echo "Creating Admin SCRAM credential..."
sudo /opt/kafka/bin/kafka-configs.sh --bootstrap-server 127.0.0.1:9092 --alter --add-config "SCRAM-SHA-512=[iterations=4096,password=admin]" --entity-type users --entity-name admin
echo "admin" | sudo tee /etc/kafka/admin.pass > /dev/null
sudo chmod 600 /etc/kafka/admin.pass
sudo chown root:root /etc/kafka/admin.pass

echo "Generating TLS..."
sudo bash /home/ubuntu/scripts/generate_tls.sh kafka.subartaghosh.co.in

echo "Enabling secure public mode..."
sudo bash /home/ubuntu/scripts/enable_secure_public.sh kafka.subartaghosh.co.in

echo "Writing admin properties..."
sudo bash /home/ubuntu/scripts/write_admin_properties.sh

echo "Checking Kafka status..."
sudo systemctl is-active kafka.service
