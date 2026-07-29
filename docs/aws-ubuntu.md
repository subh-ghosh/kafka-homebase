# AWS EC2 Free Tier (Ubuntu) — Personal Kafka

This guide installs a **single-node** Kafka broker (KRaft) on an Amazon Web Services (AWS) EC2 Ubuntu VM.

## 0) AWS EC2 checklist

- Log into the [AWS Management Console](https://console.aws.amazon.com/).
- Navigate to **EC2 > Instances** and click **Launch Instances**.
- Name: `kafka-broker`
- OS Image: Select **Ubuntu** and choose **Ubuntu Server 22.04 LTS (HVM)** (Ensure it says "Free tier eligible").
- Instance Type: **t2.micro** or **t3.micro** (Free tier eligible).
- Key Pair: Create a new key pair (e.g., `kafka-aws-key`). **Download the `.pem` file and keep it safe**.
- Storage: 10 to 30 GB (Free tier allows up to 30GB of EBS storage).
- Launch the instance.

## 1) Open only the needed ports (Security Groups)

While the VM is launching, configure the Security Group:
1. Under **Network and Security > Security Groups**, find the group attached to your new instance.
2. Edit **Inbound Rules**:
   - `SSH` (Port `22`) — ideally restricted to your IP (My IP).
   - `Custom TCP` (Port `9092`) — Source: `0.0.0.0/0` (for Kafka client access).

## 2) Connect and Install Kafka

Find your instance's Public IPv4 address in the EC2 dashboard. Connect via SSH:

```bash
# On your local machine (Linux/Mac/WSL)
chmod 400 kafka-aws-key.pem
ssh -i "kafka-aws-key.pem" ubuntu@<YOUR_PUBLIC_IP>
```

On the VM:

```bash
git clone <YOUR_GITHUB_REPO_URL>
cd <repo>

sudo bash scripts/ubuntu_install_kafka.sh
```

## 3) Bootstrap (local-only, no public listener)

This initializes KRaft storage and starts Kafka only on localhost.

```bash
sudo bash scripts/bootstrap_local_plaintext.sh
```

## 4) Create the first admin user (still local-only)

This creates the initial `admin` SCRAM credential while Kafka is only reachable on `127.0.0.1`.

```bash
sudo bash scripts/create_admin_user.sh
```

## 5) Generate TLS material

Pick a hostname (recommended): `kafka.yourdomain.com` (Ensure your domain's DNS A Record points to the AWS Public IP).

```bash
sudo bash scripts/generate_tls.sh kafka.yourdomain.com
```

This writes certs into `secrets/` on the VM.

## 6) Switch to secure public mode

```bash
sudo bash scripts/enable_secure_public.sh kafka.yourdomain.com
```

At this point you can open `9092` publicly.

## 7) Create admin.properties for CLI tools

Kafka CLI tooling (topic/user/ACL commands) needs a config file with your admin login.

```bash
sudo bash scripts/write_admin_properties.sh
```

## 8) Onboard an app

Example: create user `app1`, topic prefix `app1.` and a topic `app1.events`.

```bash
sudo bash scripts/provision_app.sh \
  --app app1 \
  --topic app1.events \
  --partitions 3 \
  --replication 1
```

## Notes

- Remember that AWS Free Tier lasts for **12 months**.
- Keep `auto.create.topics.enable=false` (prevents abuse).
- Use topic prefixes per app (`app1.*`) and ACLs.


