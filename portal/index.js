const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const { exec } = require('child_process');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(cors());
app.use(bodyParser.json());
app.use(express.static('public'));
app.set('trust proxy', 1); // For accurate IP rate limiting behind proxies/AWS

const PORT = process.env.PORT || 8080;
const INVITE_CODE = process.env.INVITE_CODE || ''; // empty means no invite code required
const MAX_GLOBAL_USERS = 50;
const DB_FILE = path.join(__dirname, 'db.json');

// Initialize database
if (!fs.existsSync(DB_FILE)) {
    fs.writeFileSync(DB_FILE, JSON.stringify({ total_users: 0, users: [], ips: [] }));
}

// Helper to run shell commands
const runCommand = (command) => {
    return new Promise((resolve, reject) => {
        exec(command, (error, stdout, stderr) => {
            if (error) {
                console.error(`Command Error: ${stderr}`);
                return reject(error);
            }
            resolve(stdout);
        });
    });
};

app.post('/api/register', async (req, res) => {
    const { username, inviteCode } = req.body;
    const clientIp = req.ip;

    // 1. Validation Logic
    if (!username || !/^[a-z0-9_]{3,15}$/.test(username)) {
        return res.status(400).json({ error: 'Invalid username. Must be 3-15 lowercase letters, numbers, or underscores only.' });
    }

    if (INVITE_CODE && inviteCode !== INVITE_CODE) {
        return res.status(403).json({ error: 'Invalid invite code.' });
    }

    // 2. Global Abuse Protection Logic
    const db = JSON.parse(fs.readFileSync(DB_FILE));
    
    // Check if IP has already registered FOREVER
    if (db.ips && db.ips.includes(clientIp)) {
        return res.status(403).json({ error: 'This IP address has already registered an account. Only one account per person is allowed.' });
    }

    if (db.total_users >= MAX_GLOBAL_USERS) {
        return res.status(403).json({ error: 'Global capacity reached. This free-tier server cannot host any more users.' });
    }

    if (db.users.includes(username)) {
        return res.status(400).json({ error: 'Username is already taken. Please choose another.' });
    }

    // Generate a secure random password
    const password = crypto.randomBytes(12).toString('base64').replace(/[^a-zA-Z0-9]/g, '').substring(0, 16) + "!";
    const topicName = `${username}.events`;
    
    try {
        // We assume this is running on the EC2 instance where /opt/kafka/bin and /etc/kafka/admin.properties exist.
        const adminProps = '/etc/kafka/admin.properties';
        
        // Ensure the CLI tools exist before trying to run them (prevents crashing if tested locally on Windows)
        if (fs.existsSync('/opt/kafka/bin/kafka-configs.sh')) {
            // Create SCRAM user
            await runCommand(`/opt/kafka/bin/kafka-configs.sh --bootstrap-server 127.0.0.1:9092 --command-config ${adminProps} --alter --add-config "SCRAM-SHA-512=[iterations=4096,password=${password}]" --entity-type users --entity-name ${username}`);
            
            // Create Default Topic
            await runCommand(`/opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --command-config ${adminProps} --create --if-not-exists --topic ${topicName} --partitions 3 --replication-factor 1`);
            
            // Set Topic ACLs
            await runCommand(`/opt/kafka/bin/kafka-acls.sh --bootstrap-server 127.0.0.1:9092 --command-config ${adminProps} --add --allow-principal "User:${username}" --operation Read --operation Write --operation Describe --operation Create --topic "${username}." --resource-pattern-type prefixed`);
            
            // Set Group ACLs
            await runCommand(`/opt/kafka/bin/kafka-acls.sh --bootstrap-server 127.0.0.1:9092 --command-config ${adminProps} --add --allow-principal "User:${username}" --operation Read --operation Describe --group "${username}" --resource-pattern-type prefixed`);
        } else {
            console.log(`[DRY RUN - Windows local] Created user ${username} with topic ${topicName}`);
        }

        // Save to DB to track global limits and permanent IPs
        db.total_users += 1;
        db.users.push(username);
        if (!db.ips) db.ips = [];
        db.ips.push(clientIp);
        fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2));

        res.json({
            success: true,
            username,
            password,
            topic: topicName,
            bootstrap: 'kafka.subartaghosh.co.in:9092'
        });

    } catch (err) {
        console.error("Failed to provision user:", err);
        res.status(500).json({ error: 'Internal server error while provisioning Kafka resources.' });
    }
});

app.listen(PORT, () => {
    console.log(`Kafka Registration Portal running on port ${PORT}`);
    console.log(`Abuse Protection Enabled: Max ${MAX_GLOBAL_USERS} users total. Rate limit: 1 per IP forever.`);
});
