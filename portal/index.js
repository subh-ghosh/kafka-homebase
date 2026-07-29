const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const { exec } = require('child_process');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const axios = require('axios');

const app = express();
app.use(cors());
app.use(bodyParser.json());
app.use(express.static('public'));

const PORT = process.env.PORT || 8080;
const MAX_GLOBAL_USERS = 50;
const DB_FILE = path.join(__dirname, 'db.json');

// THESE MUST BE SET BEFORE RUNNING
const GITHUB_CLIENT_ID = process.env.GITHUB_CLIENT_ID || '';
const GITHUB_CLIENT_SECRET = process.env.GITHUB_CLIENT_SECRET || '';

// Initialize database
if (!fs.existsSync(DB_FILE)) {
    // we use githubIds instead of ips now
    fs.writeFileSync(DB_FILE, JSON.stringify({ total_users: 0, users: [], githubIds: [] }));
}

// Endpoint for frontend to get the Client ID to redirect users to GitHub
app.get('/api/config', (req, res) => {
    res.json({ clientId: GITHUB_CLIENT_ID });
});

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
    const { username, code } = req.body;

    if (!GITHUB_CLIENT_ID || !GITHUB_CLIENT_SECRET) {
        return res.status(500).json({ error: 'GitHub OAuth is not configured on the server.' });
    }

    if (!username || !/^[a-z0-9_]{3,15}$/.test(username)) {
        return res.status(400).json({ error: 'Invalid username. Must be 3-15 lowercase letters, numbers, or underscores only.' });
    }

    if (!code) {
        return res.status(400).json({ error: 'Missing GitHub authorization code.' });
    }

    // 1. Exchange code for GitHub Access Token
    let accessToken = null;
    try {
        const tokenRes = await axios.post('https://github.com/login/oauth/access_token', {
            client_id: GITHUB_CLIENT_ID,
            client_secret: GITHUB_CLIENT_SECRET,
            code: code
        }, {
            headers: { Accept: 'application/json' }
        });
        
        if (tokenRes.data.error) {
            return res.status(401).json({ error: 'GitHub authorization failed or code expired.' });
        }
        accessToken = tokenRes.data.access_token;
    } catch (err) {
        return res.status(500).json({ error: 'Failed to communicate with GitHub.' });
    }

    // 2. Fetch GitHub User Profile
    let githubId = null;
    try {
        const userRes = await axios.get('https://api.github.com/user', {
            headers: { Authorization: `Bearer ${accessToken}` }
        });
        githubId = userRes.data.id.toString();
    } catch (err) {
        return res.status(500).json({ error: 'Failed to fetch GitHub profile.' });
    }

    // 3. Global Abuse Protection Logic
    const db = JSON.parse(fs.readFileSync(DB_FILE));
    
    // Check if this GitHub human has already registered FOREVER
    if (db.githubIds && db.githubIds.includes(githubId)) {
        return res.status(403).json({ error: 'Your GitHub account has already registered a Kafka account. Only one account per person is allowed.' });
    }

    if (db.total_users >= MAX_GLOBAL_USERS) {
        return res.status(403).json({ error: 'Global capacity reached. This free-tier server cannot host any more users.' });
    }

    if (db.users.includes(username)) {
        return res.status(400).json({ error: 'Username is already taken. Please choose another.' });
    }

    // 4. Provision Kafka Resources
    const password = crypto.randomBytes(12).toString('base64').replace(/[^a-zA-Z0-9]/g, '').substring(0, 16) + "!";
    const topicName = `${username}.events`;
    
    try {
        const adminProps = '/etc/kafka/admin.properties';
        
        if (fs.existsSync('/opt/kafka/bin/kafka-configs.sh')) {
            await runCommand(`/opt/kafka/bin/kafka-configs.sh --bootstrap-server 127.0.0.1:9092 --command-config ${adminProps} --alter --add-config "SCRAM-SHA-512=[iterations=4096,password=${password}]" --entity-type users --entity-name ${username}`);
            await runCommand(`/opt/kafka/bin/kafka-topics.sh --bootstrap-server 127.0.0.1:9092 --command-config ${adminProps} --create --if-not-exists --topic ${topicName} --partitions 3 --replication-factor 1`);
            await runCommand(`/opt/kafka/bin/kafka-acls.sh --bootstrap-server 127.0.0.1:9092 --command-config ${adminProps} --add --allow-principal "User:${username}" --operation Read --operation Write --operation Describe --operation Create --topic "${username}." --resource-pattern-type prefixed`);
            await runCommand(`/opt/kafka/bin/kafka-acls.sh --bootstrap-server 127.0.0.1:9092 --command-config ${adminProps} --add --allow-principal "User:${username}" --operation Read --operation Describe --group "${username}" --resource-pattern-type prefixed`);
        } else {
            console.log(`[DRY RUN - Windows local] Created user ${username} with topic ${topicName} for GitHub ID ${githubId}`);
        }

        // Save to DB to track global limits and permanent GitHub IDs
        db.total_users += 1;
        db.users.push(username);
        if (!db.githubIds) db.githubIds = [];
        db.githubIds.push(githubId);
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
    console.log(`Abuse Protection Enabled: Max ${MAX_GLOBAL_USERS} users total. Strict GitHub OAuth verification active.`);
});
