#!/usr/bin/env bash
set -e

echo "=> Copying portal files to EC2..."
scp -i kafka-aws-key.pem -r portal ubuntu@18.212.227.199:/home/ubuntu/

echo "=> Setting up portal on EC2..."
ssh -i kafka-aws-key.pem ubuntu@18.212.227.199 << 'EOF'
  set -e
  
  # Install Node.js if not installed
  if ! command -v node &> /dev/null; then
    curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
    sudo apt-get install -y nodejs
  fi

  # Setup portal directory
  cd /home/ubuntu/portal
  npm install

  # Create systemd service
  cat << 'UNIT' | sudo tee /etc/systemd/system/kafka-portal.service
[Unit]
Description=Kafka Self-Service Portal
After=network.target

[Service]
Environment=PORT=80
# Environment=INVITE_CODE=mysecretcode
Type=simple
User=root
WorkingDirectory=/home/ubuntu/portal
ExecStart=/usr/bin/node index.js
Restart=on-failure

[Install]
WantedBy=multi-user.target
UNIT

  # Start the service
  sudo systemctl daemon-reload
  sudo systemctl enable kafka-portal
  sudo systemctl restart kafka-portal

  echo "=> Portal is now running on port 80!"
EOF
