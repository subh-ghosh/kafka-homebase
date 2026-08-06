const express = require('express');
const compression = require('compression');
const cors = require('cors');
const { exec } = require('child_process');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const os = require('os');
const { default: PQueue } = require('p-queue');
const { Kafka } = require('kafkajs');
const rateLimit = require('express-rate-limit');
const db = require('./db');

const app = express();
app.set('trust proxy', true); // Trust Cloudflare and proxy headers

// Gzip compress all responses — reduces transfer size by 60-80% for JSON/HTML
app.use(compression({ level: 6, threshold: 512 }));

app.use(cors({ origin: process.env.ALLOWED_ORIGIN || '*' }));
app.use(express.json({ limit: '16kb' }));  // Express 5 built-in JSON parser

// Static files with ETags and 1-hour browser caching
app.use(express.static('public', {
    etag: true,
    lastModified: true,
    maxAge: '1h',
    setHeaders: (res) => {
        res.setHeader('Vary', 'Accept-Encoding');
    }
}));

// ── Rate Limiters ────────────────────────────────────────
const registerLimiter = rateLimit({
    windowMs: 60 * 60 * 1000,  // 1 hour
    max: 10,
    standardHeaders: true,
    legacyHeaders: false,
    validate: { xForwardedForHeader: false },
    message: { error: 'Too many registration attempts. Try again in an hour.' }
});
const mutationLimiter = rateLimit({
    windowMs: 15 * 60 * 1000,  // 15 minutes
    max: 50,
    standardHeaders: true,
    legacyHeaders: false,
    validate: { xForwardedForHeader: false },
    message: { error: 'Too many requests. Try again in a few minutes.' }
});
const apiLimiter = rateLimit({
    windowMs: 15 * 60 * 1000,  // 15 minutes
    max: 200,
    standardHeaders: true,
    legacyHeaders: false,
    validate: { xForwardedForHeader: false },
    message: { error: 'Too many requests. Please slow down.' }
});
const adminLimiter = rateLimit({
    windowMs: 60 * 1000,       // 1 minute
    max: 30,
    standardHeaders: true,
    legacyHeaders: false,
    validate: { xForwardedForHeader: false },
    message: { error: 'Admin rate limit exceeded.' }
});
// Apply general limiter to all /api routes
app.use('/api/', apiLimiter);

const PORT = process.env.PORT || 8080;
const MAX_GLOBAL_USERS = 200;

// THESE MUST BE SET BEFORE RUNNING
const GITHUB_CLIENT_ID = process.env.GITHUB_CLIENT_ID || '';
const GITHUB_CLIENT_SECRET = process.env.GITHUB_CLIENT_SECRET || '';
const ADMIN_USER = process.env.ADMIN_USER || 'admin';
const ADMIN_PASS = process.env.ADMIN_PASS;
if (!ADMIN_PASS) {
    console.error('FATAL: ADMIN_PASS environment variable is not set. Refusing to start.');
    process.exit(1);
}

// Queue to handle heavy shell operations concurrently
const shellQueue = new PQueue({ concurrency: 2 });

// Native Kafka Client — connects via INTERNAL PLAINTEXT listener (127.0.0.1:9094)
const kafkaClient = new Kafka({
    clientId: 'portal-admin',
    brokers: ['127.0.0.1:9094'],
});

// Singleton Kafka admin — avoids per-request connect/disconnect overhead
let _adminInstance = null;
const getAdmin = async () => {
    if (!_adminInstance) {
        _adminInstance = kafkaClient.admin();
        try { await _adminInstance.connect(); }
        catch (e) { _adminInstance = null; throw e; }
    }
    return _adminInstance;
};

// Singleton Kafka producer
let _producerInstance = null;
const getProducer = async () => {
    if (!_producerInstance) {
        _producerInstance = kafkaClient.producer();
        try { await _producerInstance.connect(); }
        catch (e) { _producerInstance = null; throw e; }
    }
    return _producerInstance;
};

// GitHub token → username cache (5-min TTL) — prevents GitHub API rate-limit exhaustion
const tokenCache = new Map();
const TOKEN_TTL = 5 * 60 * 1000;

// Username validator — must match internal pattern to block shell injection
const VALID_USERNAME_RE = /^user_[a-f0-9]{8}$/;
const isValidUsername = (u) => VALID_USERNAME_RE.test(u);

const generatePassword = (length = 16) => {
    const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
    const bytes = crypto.randomBytes(length);
    let res = '';
    for (let i = 0; i < length; i++) {
        res += chars[bytes[i] % chars.length];
    }
    return res + '!';
};

const sanitizeGithubUsername = async (handle) => {
    let clean = (handle || '').toLowerCase().replace(/[^a-z0-9_-]/g, '_').substring(0, 35);
    if (!clean) clean = 'user_' + crypto.randomBytes(4).toString('hex');
    const existing = await db.getUserByName(clean);
    if (!existing) return clean;
    return `${clean}_${crypto.randomBytes(2).toString('hex')}`;
};

// /api/config is static — client ID never changes at runtime
// Cache at Cloudflare edge for 5 minutes, browser for 2 minutes
app.get('/api/config', (req, res) => {
    res.setHeader('Cache-Control', 'public, max-age=120, s-maxage=300');
    res.setHeader('Vary', 'Accept-Encoding');
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

// Prune expired token cache entries every 10 minutes
setInterval(() => {
    const now = Date.now();
    for (const [token, data] of tokenCache.entries()) {
        if (data.expiresAt <= now) tokenCache.delete(token);
    }
}, 10 * 60 * 1000);

// ── PUBLIC ROUTES ────────────────────────────────────────

// Helper for verifying access token — 5-min cache prevents hammering GitHub API
const verifyUserHelper = async (req, res) => {
    let accessToken = req.headers['authorization']?.split(' ')[1];
    if (!accessToken && req.body && req.body.accessToken) {
        accessToken = req.body.accessToken;
    }
    if (!accessToken) {
        res.status(401).json({ error: 'Missing token' });
        return null;
    }

    // Serve from cache if fresh
    const cached = tokenCache.get(accessToken);
    if (cached && cached.expiresAt > Date.now()) {
        return cached.username;
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

    // Cache the resolved username for 5 minutes
    tokenCache.set(accessToken, { username: foundUser.username, expiresAt: Date.now() + TOKEN_TTL });
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
            storageMB: parseFloat(getUserStorageMB(foundData.username)),
            accessToken // Return access token for frontend reuse
        });
    } else {
        res.json({ exists: false, accessToken });
    }
});

// Register new user
app.post('/api/register', registerLimiter, async (req, res) => {
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

    const username = await sanitizeGithubUsername(githubHandle);
    const password = generatePassword(16);
    const topicName = `${username}.events`;

    try {
        await shellQueue.add(async () => {
            const adminProps = '/etc/kafka/admin.properties';
            if (fs.existsSync('/opt/kafka/bin/kafka-configs.sh')) {
                await runCommand(`/opt/kafka/bin/kafka-configs.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --alter --add-config "SCRAM-SHA-512=[iterations=4096,password=${password}]" --entity-type users --entity-name ${username}`);
                await runCommand(`/opt/kafka/bin/kafka-topics.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --create --if-not-exists --topic ${topicName} --partitions 3 --replication-factor 1`);
                const altUsername = username.includes('-') ? username.replace(/-/g, '_') : username.replace(/_/g, '-');
                await runCommand(`/opt/kafka/bin/kafka-acls.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --add --allow-principal "User:${username}" --operation Read --operation Write --operation Describe --operation Create --topic "${username}." --topic "${altUsername}." --resource-pattern-type prefixed`);
                await runCommand(`/opt/kafka/bin/kafka-acls.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --add --allow-principal "User:${username}" --operation Read --operation Describe --group "${username}" --group "${altUsername}" --resource-pattern-type prefixed`);
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
app.post('/api/regenerate', mutationLimiter, async (req, res) => {
    const username = await verifyUserHelper(req, res);
    if (!username) return;

    const newPassword = generatePassword(16);

    try {
        await shellQueue.add(async () => {
            const adminProps = '/etc/kafka/admin.properties';
            if (fs.existsSync('/opt/kafka/bin/kafka-configs.sh')) {
                await runCommand(`/opt/kafka/bin/kafka-configs.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --alter --add-config 'SCRAM-SHA-512=[iterations=4096,password=${newPassword}]' --entity-type users --entity-name ${username}`);
            }
        });

        await db.updateUpdatedAt(username);
        res.json({ success: true, password: newPassword });
    } catch (err) {
        console.error("Failed to regenerate password:", err);
        res.status(500).json({ error: 'Failed to update Kafka credentials' });
    }
});

// Delete Account
app.post('/api/delete_account', mutationLimiter, async (req, res) => {
    const username = await verifyUserHelper(req, res);
    if (!username) return;

    try {
        // Delete ALL user-owned topics (not just .events) via native Kafka API
        const admin = await getAdmin();
        const allTopics = await admin.listTopics();
        const userTopics = allTopics.filter(t => t.startsWith(`${username}.`));
        if (userTopics.length > 0) {
            await admin.deleteTopics({ topics: userTopics });
        }

        // Remove SCRAM credentials and ACLs via admin shell
        await shellQueue.add(async () => {
            const adminProps = '/etc/kafka/admin.properties';
            if (fs.existsSync('/opt/kafka/bin/kafka-configs.sh')) {
                await safeRunCommand(`/opt/kafka/bin/kafka-configs.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --alter --delete-config 'SCRAM-SHA-512' --entity-type users --entity-name ${username}`);
                await safeRunCommand(`/opt/kafka/bin/kafka-acls.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --remove --allow-principal "User:${username}" --operation Read --operation Write --operation Describe --operation Create --topic "${username}." --resource-pattern-type prefixed --force`);
                await safeRunCommand(`/opt/kafka/bin/kafka-acls.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --remove --allow-principal "User:${username}" --operation Read --operation Describe --group "${username}" --resource-pattern-type prefixed --force`);
            }
        });

        await db.deleteUser(username);
        res.json({ success: true });
    } catch (err) {
        console.error("Failed to delete account:", err);
        res.status(500).json({ error: 'Failed to delete account resources' });
    }
});

// -----------------------------------------------------
// TOPIC MANAGEMENT ROUTES
// -----------------------------------------------------

app.get('/api/topics', async (req, res) => {
    const username = await verifyUserHelper(req, res);
    if (!username) return;

    try {
        const admin = await getAdmin();
        const allTopics = await admin.listTopics();
        const topics = allTopics.filter(t => t.startsWith(`${username}.`));
        res.json({ success: true, topics });
    } catch (err) {
        console.error('Topics fetch error:', err);
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
        const admin = await getAdmin();
        const created = await admin.createTopics({
            waitForLeaders: true,
            topics: [{ topic: fullTopic, numPartitions: 3, replicationFactor: 1 }]
        });
        if (!created) {
            return res.status(409).json({ error: 'Topic already exists.' });
        }
        res.json({ success: true, topic: fullTopic });
    } catch (err) {
        if (err.type === 'TOPIC_ALREADY_EXISTS' || err.message?.includes('TOPIC_ALREADY_EXISTS')) {
            return res.status(409).json({ error: 'Topic already exists.' });
        }
        console.error('Topic create error:', err);
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
        const admin = await getAdmin();
        await admin.deleteTopics({ topics: [topic] });
        res.json({ success: true });
    } catch (err) {
        console.error('Topic delete error:', err);
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
        const groupId = `portal-view-${username}-${Date.now()}-${Math.random().toString(36).substring(7)}`;
        const consumer = kafkaClient.consumer({
            groupId,
            sessionTimeout: 6000,
            heartbeatInterval: 1000,
            maxWaitTimeInMs: 200
        });

        await consumer.connect();
        await consumer.subscribe({ topic, fromBeginning: true });

        let messages = [];

        await new Promise((resolve) => {
            const timer = setTimeout(async () => {
                try { await consumer.disconnect(); } catch (e) {}
                resolve();
            }, 4500);

            consumer.run({
                eachMessage: async ({ message }) => {
                    if (message.value) {
                        messages.push(message.value.toString());
                    }
                    if (messages.length >= 100) {
                        clearTimeout(timer);
                        try { await consumer.disconnect(); } catch (e) {}
                        resolve();
                    }
                }
            }).catch(() => resolve());
        });

        res.json({ success: true, data: messages.slice(-20) });
    } catch (err) {
        console.error('Data fetch error:', err.message || err);
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
        const producer = await getProducer();
        await producer.send({
            topic: topic,
            messages: [{ value: typeof payload === 'string' ? payload : JSON.stringify(payload) }]
        });
        res.json({ success: true });
    } catch (err) {
        console.error('Produce error:', err);
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

app.post('/api/admin/login', adminLimiter, adminAuth, (req, res) => {
    res.json({ success: true });
});

app.get('/api/admin/health', adminLimiter, adminAuth, (req, res) => {
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

const getFolderDataSize = (dirPath) => {
    let size = 0;
    if (fs.existsSync(dirPath)) {
        const files = fs.readdirSync(dirPath);
        for (let file of files) {
            const filePath = path.join(dirPath, file);
            const stats = fs.statSync(filePath);
            if (stats.isDirectory()) {
                size += getFolderDataSize(filePath);
            } else if (file.endsWith('.log')) {
                size += stats.size;
            }
        }
    }
    return size;
};

const getUserStorageMB = (username) => {
    let totalSize = 0;
    const logDirBase = '/var/lib/kafka/logs';
    if (fs.existsSync(logDirBase)) {
        const logDirs = fs.readdirSync(logDirBase).filter(d => d.startsWith(`${username}.`));
        for (const dir of logDirs) {
            totalSize += getFolderDataSize(path.join(logDirBase, dir));
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
        success: true,
        total_users: total_users,
        max_users: MAX_GLOBAL_USERS,
        users: usersWithStats
    });
});

app.delete('/api/admin/users/:username', adminAuth, async (req, res) => {
    const username = req.params.username;

    // Block shell injection — username must match internal generation pattern
    if (!isValidUsername(username)) {
        return res.status(400).json({ error: 'Invalid username format.' });
    }

    const foundData = await db.getUserByName(username);
    if (!foundData) {
        return res.status(404).json({ error: 'User not found in DB.' });
    }

    try {
        // Delete ALL user-owned topics via native Kafka API
        const admin = await getAdmin();
        const allTopics = await admin.listTopics();
        const userTopics = allTopics.filter(t => t.startsWith(`${username}.`));
        if (userTopics.length > 0) {
            await admin.deleteTopics({ topics: userTopics });
        }

        // Remove SCRAM credentials and ACLs
        await shellQueue.add(async () => {
            const adminProps = '/etc/kafka/admin.properties';
            if (fs.existsSync('/opt/kafka/bin/kafka-configs.sh')) {
                await safeRunCommand(`/opt/kafka/bin/kafka-configs.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --alter --delete-config "SCRAM-SHA-512" --entity-type users --entity-name ${username}`);
                await safeRunCommand(`/opt/kafka/bin/kafka-acls.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --remove --allow-principal "User:${username}" --operation Read --operation Write --operation Describe --operation Create --topic "${username}." --resource-pattern-type prefixed --force`);
                await safeRunCommand(`/opt/kafka/bin/kafka-acls.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --remove --allow-principal "User:${username}" --operation Read --operation Describe --group "${username}" --resource-pattern-type prefixed --force`);
            }
        });

        await db.deleteUser(username);
        res.json({ success: true, message: `Successfully deleted user ${username}` });
    } catch (err) {
        console.error('Failed to delete user:', err);
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
    try {
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
                console.log(`[QuotaEnforcer] User ${username} exceeded 125MB (${storageMB} MB). Write ACL revoked.`);
            } else if (storageMB <= MAX_MB && info.quotaExceeded === 1) {
                // Restore write ACL
                await shellQueue.add(async () => {
                    const adminProps = '/etc/kafka/admin.properties';
                    if (fs.existsSync('/opt/kafka/bin/kafka-acls.sh')) {
                        await safeRunCommand(`/opt/kafka/bin/kafka-acls.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --add --allow-principal "User:${username}" --operation Write --topic "${username}." --resource-pattern-type prefixed`);
                    }
                });
                await db.setQuotaExceeded(username, false);
                console.log(`[QuotaEnforcer] User ${username} dropped below 125MB (${storageMB} MB). Write ACL restored.`);
            }
        }
    } catch (err) {
        console.error('[QuotaEnforcer] Error during disk usage check:', err.message || err);
    }
};

setInterval(checkDiskUsage, 60000); // Check every minute

// ── Graceful Shutdown ────────────────────────────────────
const shutdown = async (signal) => {
    console.log(`[Portal] ${signal} received — shutting down gracefully...`);
    try {
        if (_adminInstance) {
            await _adminInstance.disconnect();
            console.log('[Portal] Kafka admin disconnected.');
        }
        if (_producerInstance) {
            await _producerInstance.disconnect();
            console.log('[Portal] Kafka producer disconnected.');
        }
    } catch (err) {
        console.error('[Portal] Error during shutdown cleanup:', err.message);
    }
    process.exit(0);
};

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT',  () => shutdown('SIGINT'));
