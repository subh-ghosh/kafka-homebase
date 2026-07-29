#!/usr/bin/env bash
set -e

echo "=> Setting up portal on EC2..."
ssh -i kafka-aws-key.pem ubuntu@18.212.227.199 << 'EOF'
  set -e
  
  cd kafka-homebase
  git pull
  sudo cp -r portal /opt/portal
  cd /opt/portal
  
  # Install Node.js if not installed
  if ! command -v node &> /dev/null; then
    curl -fsSL https://deb.nodesource.com/setup_18.x | sudo -E bash -
    sudo apt-get install -y nodejs
  fi

  sudo npm install

  # Create systemd service
  cat << 'UNIT' | sudo tee /etc/systemd/system/kafka-portal.service
[Unit]
Description=Kafka Self-Service Portal
After=network.target

[Service]
Environment=PORT=80
Environment=GITHUB_CLIENT_ID=$GITHUB_CLIENT_ID
Environment=GITHUB_CLIENT_SECRET=$GITHUB_CLIENT_SECRET
Type=simple
User=root
WorkingDirectory=/opt/portal
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
