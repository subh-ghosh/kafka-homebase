const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const { exec } = require('child_process');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const os = require('os');
const { default: PQueue } = require('p-queue');
const { Kafka } = require('kafkajs');
const db = require('./db');

const app = express();
app.use(cors());
app.use(bodyParser.json());
app.use(express.static('public'));

const PORT = process.env.PORT || 8080;
const MAX_GLOBAL_USERS = 200;

// THESE MUST BE SET BEFORE RUNNING
const GITHUB_CLIENT_ID = process.env.GITHUB_CLIENT_ID || '';
const GITHUB_CLIENT_SECRET = process.env.GITHUB_CLIENT_SECRET || '';
const ADMIN_USER = process.env.ADMIN_USER || 'admin';
const ADMIN_PASS = process.env.ADMIN_PASS || 'Zxcasq481062@';
// Kafka broker SASL credentials (separate from portal admin login)
const KAFKA_ADMIN_USER = process.env.KAFKA_ADMIN_USER || 'admin';
const KAFKA_ADMIN_PASS = process.env.KAFKA_ADMIN_PASS || 'admin';

// Queue to handle heavy shell operations concurrently
const shellQueue = new PQueue({ concurrency: 2 });

// Native Kafka Client
const kafkaClient = new Kafka({
    clientId: 'portal-admin',
    brokers: ['broker.subartaghosh.co.in:9092'],
    ssl: { rejectUnauthorized: false },
    sasl: {
        mechanism: 'scram-sha-512',
        username: KAFKA_ADMIN_USER,
        password: KAFKA_ADMIN_PASS
    }
});

const generatePassword = (length) => {
    return crypto.randomBytes(length).toString('base64').replace(/[^a-zA-Z0-9]/g, '').substring(0, length) + "!";
};

const generateRandomUsername = () => {
    return 'user_' + crypto.randomBytes(4).toString('hex');
};

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

const safeRunCommand = (command) => {
    return runCommand(command).catch(e => {
        console.log(`[Ignored Error] Command failed: ${command}`);
    });
};

// -----------------------------------------------------
// PUBLIC ROUTES
// -----------------------------------------------------

// Helper for verifying access token
const verifyUserHelper = async (req, res) => {
    let accessToken = req.headers['authorization']?.split(' ')[1];
    if (!accessToken && req.body && req.body.accessToken) {
        accessToken = req.body.accessToken;
    }
    if (!accessToken) {
        res.status(401).json({ error: 'Missing token' });
        return null;
    }

    let githubId = null;
    try {
        const tokenRes = await axios.get('https://api.github.com/user', {
            headers: { Authorization: `Bearer ${accessToken}` }
        });
        githubId = tokenRes.data.id.toString();
    } catch (err) {
        res.status(401).json({ error: 'Invalid or expired GitHub token' });
        return null;
    }

    const foundUser = await db.getUserByGithubId(githubId);
    if (!foundUser) {
        res.status(404).json({ error: 'User not found in portal.' });
        return null;
    }
    return foundUser.username;
};

// Get current user credentials
// Accepts either a one-time OAuth code OR an existing gho_ access token
app.get('/api/user', async (req, res) => {
    const authHeader = req.headers['authorization'];
    if (!authHeader) return res.status(401).json({ error: 'Missing token' });
    const token = authHeader.split(' ')[1];

    let accessToken = null;

    // If already a GitHub access token (starts with gho_), use directly
    if (token.startsWith('gho_')) {
        accessToken = token;
    } else {
        // Treat as a one-time OAuth code and exchange it
        try {
            const tokenRes = await axios.post('https://github.com/login/oauth/access_token', {
                client_id: GITHUB_CLIENT_ID,
                client_secret: GITHUB_CLIENT_SECRET,
                code: token
            }, { headers: { Accept: 'application/json' } });
            if (tokenRes.data.error) return res.status(401).json({ error: 'Token exchange failed' });
            accessToken = tokenRes.data.access_token;
        } catch (err) {
            return res.status(500).json({ error: 'GitHub auth failed' });
        }
    }

    let githubId = null;
    try {
        const userRes = await axios.get('https://api.github.com/user', { headers: { Authorization: `Bearer ${accessToken}` } });
        githubId = userRes.data.id.toString();
    } catch (err) {
        return res.status(401).json({ error: 'Invalid or expired GitHub token' });
    }

    const foundData = await db.getUserByGithubId(githubId);
    if (foundData) {
        res.json({
            exists: true,
            username: foundData.username,
            password: '****************',
            topic: foundData.topicName,
            storageMB: foundData.storageMB || 0,
            accessToken // Return access token for frontend reuse
        });
    } else {
        res.json({ exists: false, accessToken });
    }
});

// Register new user
app.post('/api/register', async (req, res) => {
    const { accessToken } = req.body;
    if (!accessToken) return res.status(400).json({ error: 'Missing access token.' });

    let githubId = null;
    let githubHandle = null;
    try {
        const userRes = await axios.get('https://api.github.com/user', {
            headers: { Authorization: `Bearer ${accessToken}` }
        });
        githubId = userRes.data.id.toString();
        githubHandle = userRes.data.login;
    } catch (err) {
        return res.status(500).json({ error: 'Failed to fetch GitHub profile.' });
    }

    const existingUser = await db.getUserByGithubId(githubId);
    if (existingUser) {
        return res.status(403).json({ error: 'Account already exists.' });
    }

    const totalUsers = await db.getTotalUsers();
    if (totalUsers >= MAX_GLOBAL_USERS) {
        return res.status(403).json({ error: 'Global capacity reached.' });
    }

    const username = generateRandomUsername();
    const password = generatePassword(16);
    const topicName = `${username}.events`;

    try {
        await shellQueue.add(async () => {
            const adminProps = '/etc/kafka/admin.properties';
            if (fs.existsSync('/opt/kafka/bin/kafka-configs.sh')) {
                await runCommand(`/opt/kafka/bin/kafka-configs.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --alter --add-config "SCRAM-SHA-512=[iterations=4096,password=${password}]" --entity-type users --entity-name ${username}`);
                await runCommand(`/opt/kafka/bin/kafka-topics.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --create --if-not-exists --topic ${topicName} --partitions 3 --replication-factor 1`);
                await runCommand(`/opt/kafka/bin/kafka-acls.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --add --allow-principal "User:${username}" --operation Read --operation Write --operation Describe --operation Create --topic "${username}." --resource-pattern-type prefixed`);
                await runCommand(`/opt/kafka/bin/kafka-acls.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --add --allow-principal "User:${username}" --operation Read --operation Describe --group "${username}" --resource-pattern-type prefixed`);
                await runCommand(`/opt/kafka/bin/kafka-configs.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --alter --add-config 'producer_byte_rate=1048576,consumer_byte_rate=1048576' --entity-type users --entity-name ${username}`);
            } else {
                console.log(`[DRY RUN - Windows local] Created user ${username} with topic ${topicName}`);
            }
        });

        await db.createUser({ username, githubId, githubHandle, topicName });

        res.json({
            success: true,
            username,
            password,
            topic: topicName
        });
    } catch (err) {
        console.error("Failed to provision user:", err);
        res.status(500).json({ error: 'Internal server error while provisioning Kafka resources.' });
    }
});

// Regenerate Credentials
app.post('/api/regenerate', async (req, res) => {
    const { accessToken } = req.body;
    if (!accessToken) return res.status(400).json({ error: 'Missing access token.' });

    let githubId = null;
    try {
        const userRes = await axios.get('https://api.github.com/user', { headers: { Authorization: `Bearer ${accessToken}` } });
        githubId = userRes.data.id.toString();
    } catch (err) {
        return res.status(500).json({ error: 'GitHub profile failed' });
    }

    const foundData = await db.getUserByGithubId(githubId);
    if (!foundData) return res.status(404).json({ error: 'User not found.' });

    const newPassword = generatePassword(16);
    const foundUser = foundData.username;

    try {
        await shellQueue.add(async () => {
            const adminProps = '/etc/kafka/admin.properties';
            if (fs.existsSync('/opt/kafka/bin/kafka-configs.sh')) {
                await runCommand(`/opt/kafka/bin/kafka-configs.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --alter --add-config "SCRAM-SHA-512=[iterations=4096,password=${newPassword}]" --entity-type users --entity-name ${foundUser}`);
            }
        });

        await db.updateUpdatedAt(foundUser);
        res.json({ success: true, password: newPassword });
    } catch (err) {
        console.error("Failed to regenerate password:", err);
        res.status(500).json({ error: 'Failed to update Kafka credentials' });
    }
});

// Delete Account
app.post('/api/delete_account', async (req, res) => {
    const { accessToken } = req.body;
    if (!accessToken) return res.status(400).json({ error: 'Missing access token.' });

    let githubId = null;
    try {
        const userRes = await axios.get('https://api.github.com/user', { headers: { Authorization: `Bearer ${accessToken}` } });
        githubId = userRes.data.id.toString();
    } catch (err) {
        return res.status(500).json({ error: 'GitHub profile failed' });
    }

    const foundData = await db.getUserByGithubId(githubId);
    if (!foundData) return res.status(404).json({ error: 'User not found.' });
    const foundUser = foundData.username;

    try {
        await shellQueue.add(async () => {
            const adminProps = '/etc/kafka/admin.properties';
            if (fs.existsSync('/opt/kafka/bin/kafka-configs.sh')) {
                await safeRunCommand(`/opt/kafka/bin/kafka-configs.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --alter --delete-config "SCRAM-SHA-512" --entity-type users --entity-name ${foundUser}`);
                await safeRunCommand(`/opt/kafka/bin/kafka-topics.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --delete --topic "${foundUser}.events"`);
                await safeRunCommand(`/opt/kafka/bin/kafka-acls.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --remove --allow-principal "User:${foundUser}" --operation Read --operation Write --operation Describe --operation Create --topic "${foundUser}." --resource-pattern-type prefixed --force`);
                await safeRunCommand(`/opt/kafka/bin/kafka-acls.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --remove --allow-principal "User:${foundUser}" --operation Read --operation Describe --group "${foundUser}" --resource-pattern-type prefixed --force`);
            }
        });

        await db.deleteUser(foundUser);
        res.json({ success: true });
    } catch (err) {
        console.error("Failed to delete account:", err);
        res.status(500).json({ error: 'Failed to delete account from Kafka.' });
    }
});

// -----------------------------------------------------
// TOPIC MANAGEMENT ROUTES
// -----------------------------------------------------

app.get('/api/topics', async (req, res) => {
    const username = await verifyUserHelper(req, res);
    if (!username) return;

    try {
        const admin = kafkaClient.admin();
        await admin.connect();
        const allTopics = await admin.listTopics();
        await admin.disconnect();
        
        const topics = allTopics.filter(t => t.startsWith(`${username}.`));
        res.json({ success: true, topics });
    } catch (err) {
        console.error("Topics fetch error:", err);
        res.status(500).json({ error: 'Failed to list topics via Kafka API' });
    }
});

app.post('/api/topics', async (req, res) => {
    const username = await verifyUserHelper(req, res);
    if (!username) return;
    const { suffix } = req.body;
    if (!suffix || !/^[a-zA-Z0-9_-]+$/.test(suffix)) {
        return res.status(400).json({ error: 'Invalid topic suffix. Use alphanumeric characters, hyphens, and underscores.' });
    }
    const fullTopic = `${username}.${suffix}`;

    try {
        const admin = kafkaClient.admin();
        await admin.connect();
        await admin.createTopics({
            waitForLeaders: true,
            topics: [{ topic: fullTopic, numPartitions: 3, replicationFactor: 1 }]
        });
        await admin.disconnect();
        res.json({ success: true, topic: fullTopic });
    } catch (err) {
        console.error("Topic create error:", err);
        res.status(500).json({ error: 'Failed to create topic' });
    }
});

app.delete('/api/topics/:topic', async (req, res) => {
    const username = await verifyUserHelper(req, res);
    if (!username) return;
    const topic = req.params.topic;
    if (!topic.startsWith(`${username}.`)) {
        return res.status(403).json({ error: 'You do not own this topic.' });
    }

    try {
        const admin = kafkaClient.admin();
        await admin.connect();
        await admin.deleteTopics({ topics: [topic] });
        await admin.disconnect();
        res.json({ success: true });
    } catch (err) {
        console.error("Topic delete error:", err);
        res.status(500).json({ error: 'Failed to delete topic' });
    }
});

app.get('/api/topics/:topic/data', async (req, res) => {
    const username = await verifyUserHelper(req, res);
    if (!username) return;
    const topic = req.params.topic;
    if (!topic.startsWith(`${username}.`)) {
        return res.status(403).json({ error: 'You do not own this topic.' });
    }

    try {
        const admin = kafkaClient.admin();
        await admin.connect();
        const offsets = await admin.fetchTopicOffsets(topic);
        await admin.disconnect();

        const consumer = kafkaClient.consumer({ groupId: `portal-viewer-${Date.now()}` });
        await consumer.connect();
        await consumer.subscribe({ topic, fromBeginning: false });
        
        // Seek to end - 20 messages per partition roughly
        for (const p of offsets) {
            let start = parseInt(p.high) - 20;
            if (start < parseInt(p.low)) start = parseInt(p.low);
            if (start < 0) start = 0;
            consumer.seek({ topic, partition: p.partition, offset: start.toString() });
        }

        let data = [];
        let fetched = 0;

        await consumer.run({
            eachMessage: async ({ message }) => {
                data.push(message.value.toString());
                fetched++;
            },
        });

        // Wait a short time to collect messages then disconnect
        await new Promise(r => setTimeout(r, 800));
        await consumer.disconnect();
        
        res.json({ success: true, data: data.slice(-20) });
    } catch (err) {
        console.error("Data fetch error:", err);
        res.status(500).json({ error: 'Failed to fetch topic data' });
    }
});

app.post('/api/topics/:topic/produce', async (req, res) => {
    const username = await verifyUserHelper(req, res);
    if (!username) return;
    const topic = req.params.topic;
    if (!topic.startsWith(`${username}.`)) {
        return res.status(403).json({ error: 'You do not own this topic.' });
    }
    const { payload } = req.body;
    if (!payload) return res.status(400).json({ error: 'Missing payload' });

    try {
        const producer = kafkaClient.producer();
        await producer.connect();
        await producer.send({
            topic: topic,
            messages: [{ value: typeof payload === "string" ? payload : JSON.stringify(payload) }]
        });
        await producer.disconnect();
        res.json({ success: true });
    } catch (err) {
        console.error("Produce error:", err);
        res.status(500).json({ error: 'Failed to produce message' });
    }
});

app.get('/api/consumer-groups', async (req, res) => {
    const username = await verifyUserHelper(req, res);
    if (!username) return;

    try {
        const adminProps = '/etc/kafka/admin.properties';
        let groups = [];
        if (fs.existsSync('/opt/kafka/bin/kafka-consumer-groups.sh')) {
            const listOut = await runCommand(`/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --list`);
            const userGroups = listOut.split('\n').map(g => g.trim()).filter(g => g.startsWith(username));

            for (const group of userGroups) {
                try {
                    const descOut = await runCommand(`/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --describe --group ${group}`);
                    groups.push({ group, details: descOut });
                } catch (e) {
                    console.error("Describe failed for group", group, e);
                }
            }
        }
        res.json({ success: true, groups });
    } catch (err) {
        console.error("Consumer groups error:", err);
        res.status(500).json({ error: 'Failed to fetch consumer groups' });
    }
});

// -----------------------------------------------------
// ADMIN ROUTES
// -----------------------------------------------------
const adminAuth = (req, res, next) => {
    const authHeader = req.headers['authorization'];
    if (!authHeader) return res.status(401).json({ error: 'Unauthorized' });

    const [user, pass] = Buffer.from(authHeader.split(' ')[1], 'base64').toString().split(':');
    if (user === ADMIN_USER && pass === ADMIN_PASS) {
        return next();
    }
    return res.status(401).json({ error: 'Invalid admin credentials' });
};

app.post('/api/admin/login', adminAuth, (req, res) => {
    res.json({ success: true });
});

app.get('/api/admin/health', adminAuth, (req, res) => {
    const loadAvg = os.loadavg();
    const totalMem = os.totalmem();
    const freeMem = os.freemem();
    res.json({
        success: true,
        cpu_load: loadAvg,
        total_mem_mb: Math.round(totalMem / (1024 * 1024)),
        free_mem_mb: Math.round(freeMem / (1024 * 1024))
    });
});

const getFolderSize = (dirPath) => {
    let size = 0;
    if (fs.existsSync(dirPath)) {
        const files = fs.readdirSync(dirPath);
        for (let file of files) {
            const filePath = path.join(dirPath, file);
            const stats = fs.statSync(filePath);
            if (stats.isDirectory()) {
                size += getFolderSize(filePath);
            } else {
                size += stats.size;
            }
        }
    }
    return size;
};

const getUserStorageMB = (username) => {
    let totalSize = 0;
    const logDirBase = '/tmp/kafka-logs';
    if (fs.existsSync(logDirBase)) {
        const logDirs = fs.readdirSync(logDirBase).filter(d => d.startsWith(`${username}.`));
        for (const dir of logDirs) {
            totalSize += getFolderSize(path.join(logDirBase, dir));
        }
    }
    return (totalSize / (1024 * 1024)).toFixed(2);
};

app.get('/api/admin/users', adminAuth, async (req, res) => {
    const users = await db.getAllUsers();
    const total_users = users.length;
    
    const usersWithStats = {};
    for (const info of users) {
        usersWithStats[info.username] = { 
            ...info, 
            storageMB: getUserStorageMB(info.username) 
        };
    }

    res.json({
        total_users: total_users,
        max_users: MAX_GLOBAL_USERS,
        users: usersWithStats
    });
});

app.delete('/api/admin/users/:username', adminAuth, async (req, res) => {
    const username = req.params.username;
    const foundData = await db.getUserByName(username);

    if (!foundData) {
        return res.status(404).json({ error: 'User not found in DB.' });
    }

    try {
        await shellQueue.add(async () => {
            const adminProps = '/etc/kafka/admin.properties';
            if (fs.existsSync('/opt/kafka/bin/kafka-configs.sh')) {
                await safeRunCommand(`/opt/kafka/bin/kafka-configs.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --alter --delete-config "SCRAM-SHA-512" --entity-type users --entity-name ${username}`);
                await safeRunCommand(`/opt/kafka/bin/kafka-topics.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --delete --topic "${username}.events"`);
                await safeRunCommand(`/opt/kafka/bin/kafka-acls.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --remove --allow-principal "User:${username}" --operation Read --operation Write --operation Describe --operation Create --topic "${username}." --resource-pattern-type prefixed --force`);
                await safeRunCommand(`/opt/kafka/bin/kafka-acls.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --remove --allow-principal "User:${username}" --operation Read --operation Describe --group "${username}" --resource-pattern-type prefixed --force`);
            }
        });

        await db.deleteUser(username);
        res.json({ success: true, message: `Successfully deleted user ${username}` });
    } catch (err) {
        console.error("Failed to delete user:", err);
        res.status(500).json({ error: 'Failed to delete user from Kafka.' });
    }
});

// START
db.initDb().then(() => {
    app.listen(PORT, () => {
        console.log(`Kafka Registration Portal running on port ${PORT}`);
        console.log(`Abuse Protection Enabled: Max ${MAX_GLOBAL_USERS} users total. Strict GitHub OAuth verification active.`);
    });
});

// -----------------------------------------------------
// BACKGROUND ENFORCER (125MB STORAGE CAP)
// -----------------------------------------------------
const checkDiskUsage = async () => {
    const users = await db.getAllUsers();
    const MAX_MB = 125.00;

    for (const info of users) {
        const username = info.username;
        const storageMB = parseFloat(getUserStorageMB(username));
        
        if (storageMB > MAX_MB && info.quotaExceeded === 0) {
            // Revoke write ACL
            await shellQueue.add(async () => {
                const adminProps = '/etc/kafka/admin.properties';
                if (fs.existsSync('/opt/kafka/bin/kafka-acls.sh')) {
                    await safeRunCommand(`/opt/kafka/bin/kafka-acls.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --remove --allow-principal "User:${username}" --operation Write --topic "${username}." --resource-pattern-type prefixed --force`);
                }
            });
            await db.setQuotaExceeded(username, true);
            console.log(`User ${username} exceeded 125MB disk quota (${storageMB} MB). Write ACL revoked.`);
        } else if (storageMB <= MAX_MB && info.quotaExceeded === 1) {
            // Restore write ACL
            await shellQueue.add(async () => {
                const adminProps = '/etc/kafka/admin.properties';
                if (fs.existsSync('/opt/kafka/bin/kafka-acls.sh')) {
                    await safeRunCommand(`/opt/kafka/bin/kafka-acls.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --add --allow-principal "User:${username}" --operation Write --topic "${username}." --resource-pattern-type prefixed`);
                }
            });
            await db.setQuotaExceeded(username, false);
            console.log(`User ${username} dropped below 125MB disk quota (${storageMB} MB). Write ACL restored.`);
        }
    }
};

setInterval(checkDiskUsage, 60000); // Check every minute
