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
const MAX_GLOBAL_USERS = 200;
const DB_FILE = path.join(__dirname, 'db.json');

// THESE MUST BE SET BEFORE RUNNING
const GITHUB_CLIENT_ID = process.env.GITHUB_CLIENT_ID || '';
const GITHUB_CLIENT_SECRET = process.env.GITHUB_CLIENT_SECRET || '';
const ADMIN_USER = process.env.ADMIN_USER || 'admin';
const ADMIN_PASS = process.env.ADMIN_PASS || 'Zxcasq481062@';

// Initialize database
if (!fs.existsSync(DB_FILE)) {
    fs.writeFileSync(DB_FILE, JSON.stringify({ total_users: 0, users: [], githubIds: [], user_mappings: {} }));
}

// Ensure user_mappings exists in older db versions
const ensureDbSchema = (db) => {
    if (!db.user_mappings) db.user_mappings = {};
    return db;
};

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

    let db = JSON.parse(fs.readFileSync(DB_FILE));
    db = ensureDbSchema(db);

    let foundUser = null;
    for (const [username, data] of Object.entries(db.user_mappings)) {
        if (data.githubId === githubId) {
            foundUser = username;
            break;
        }
    }

    if (!foundUser) {
        res.status(404).json({ error: 'User not found in portal.' });
        return null;
    }

    return foundUser;
};

// Get current user credentials
app.get('/api/user', async (req, res) => {
    const authHeader = req.headers['authorization'];
    if (!authHeader) return res.status(401).json({ error: 'Missing token' });
    const code = authHeader.split(' ')[1];

    let accessToken = null;
    try {
        const tokenRes = await axios.post('https://github.com/login/oauth/access_token', {
            client_id: GITHUB_CLIENT_ID,
            client_secret: GITHUB_CLIENT_SECRET,
            code: code
        }, { headers: { Accept: 'application/json' } });
        if (tokenRes.data.error) return res.status(401).json({ error: 'Token exchange failed' });
        accessToken = tokenRes.data.access_token;
    } catch (err) {
        return res.status(500).json({ error: 'GitHub auth failed' });
    }

    let githubId = null;
    try {
        const userRes = await axios.get('https://api.github.com/user', { headers: { Authorization: `Bearer ${accessToken}` } });
        githubId = userRes.data.id.toString();
    } catch (err) {
        return res.status(500).json({ error: 'GitHub profile failed' });
    }

    let db = JSON.parse(fs.readFileSync(DB_FILE));
    db = ensureDbSchema(db);

    let foundUser = null;
    let foundData = null;
    for (const [username, data] of Object.entries(db.user_mappings)) {
        if (data.githubId === githubId) {
            foundUser = username;
            foundData = data;
            break;
        }
    }

    if (foundUser && foundData) {
        res.json({
            exists: true,
            username: foundUser,
            password: '****************',
            topic: foundData.topicName || `${foundUser}.events`,
            accessToken // Give access token back to frontend to reuse
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

    let db = JSON.parse(fs.readFileSync(DB_FILE));
    db = ensureDbSchema(db);

    if (db.githubIds && db.githubIds.includes(githubId)) {
        return res.status(403).json({ error: 'Account already exists.' });
    }

    if (db.total_users >= MAX_GLOBAL_USERS) {
        return res.status(403).json({ error: 'Global capacity reached.' });
    }

    const username = generateRandomUsername();
    const password = generatePassword(16);
    const topicName = `${username}.events`;

    try {
        const adminProps = '/etc/kafka/admin.properties';
        if (fs.existsSync('/opt/kafka/bin/kafka-configs.sh')) {
            await runCommand(`/opt/kafka/bin/kafka-configs.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --alter --add-config "SCRAM-SHA-512=[iterations=4096,password=${password}]" --entity-type users --entity-name ${username}`);
            await runCommand(`/opt/kafka/bin/kafka-topics.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --create --if-not-exists --topic ${topicName} --partitions 3 --replication-factor 1`);
            await runCommand(`/opt/kafka/bin/kafka-acls.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --add --allow-principal "User:${username}" --operation Read --operation Write --operation Describe --operation Create --topic "${username}." --resource-pattern-type prefixed`);
            await runCommand(`/opt/kafka/bin/kafka-acls.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --add --allow-principal "User:${username}" --operation Read --operation Describe --group "${username}" --resource-pattern-type prefixed`);
            await runCommand(`/opt/kafka/bin/kafka-configs.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --alter --add-config 'producer_byte_rate=1048576,consumer_byte_rate=1048576' --entity-type users --entity-name ${username}`);
        } else {
            console.log(`[DRY RUN - Windows local] Created user ${username} with topic ${topicName} for GitHub ID ${githubId}`);
        }

        db.total_users += 1;
        db.users.push(username);
        if (!db.githubIds) db.githubIds = [];
        db.githubIds.push(githubId);

        db.user_mappings[username] = {
            githubId: githubId,
            githubHandle: githubHandle,
            topicName: topicName,
            created_at: new Date().toISOString()
        };

        fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2));

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

    let db = JSON.parse(fs.readFileSync(DB_FILE));
    db = ensureDbSchema(db);

    let foundUser = null;
    for (const [username, data] of Object.entries(db.user_mappings)) {
        if (data.githubId === githubId) {
            foundUser = username;
            break;
        }
    }

    if (!foundUser) return res.status(404).json({ error: 'User not found.' });

    const newPassword = generatePassword(16);

    try {
        const adminProps = '/etc/kafka/admin.properties';
        if (fs.existsSync('/opt/kafka/bin/kafka-configs.sh')) {
            await runCommand(`/opt/kafka/bin/kafka-configs.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --alter --add-config "SCRAM-SHA-512=[iterations=4096,password=${newPassword}]" --entity-type users --entity-name ${foundUser}`);
        }

        db.user_mappings[foundUser].updated_at = new Date().toISOString();
        fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2));

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

    let db = JSON.parse(fs.readFileSync(DB_FILE));
    db = ensureDbSchema(db);

    let foundUser = null;
    for (const [username, data] of Object.entries(db.user_mappings)) {
        if (data.githubId === githubId) {
            foundUser = username;
            break;
        }
    }

    if (!foundUser) return res.status(404).json({ error: 'User not found.' });

    try {
        const adminProps = '/etc/kafka/admin.properties';
        if (fs.existsSync('/opt/kafka/bin/kafka-configs.sh')) {
            await runCommand(`/opt/kafka/bin/kafka-configs.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --alter --delete-config "SCRAM-SHA-512" --entity-type users --entity-name ${foundUser}`);
            await runCommand(`/opt/kafka/bin/kafka-topics.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --delete --topic "${foundUser}.events"`);
            await runCommand(`/opt/kafka/bin/kafka-acls.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --remove --allow-principal "User:${foundUser}" --operation Read --operation Write --operation Describe --operation Create --topic "${foundUser}." --resource-pattern-type prefixed --force`);
            await runCommand(`/opt/kafka/bin/kafka-acls.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --remove --allow-principal "User:${foundUser}" --operation Read --operation Describe --group "${foundUser}" --resource-pattern-type prefixed --force`);
        }

        db.users = db.users.filter(u => u !== foundUser);
        db.githubIds = db.githubIds.filter(id => id !== githubId);
        delete db.user_mappings[foundUser];
        db.total_users -= 1;
        fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2));

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
        const adminProps = '/etc/kafka/admin.properties';
        if (fs.existsSync('/opt/kafka/bin/kafka-topics.sh')) {
            const out = await runCommand(`/opt/kafka/bin/kafka-topics.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --list`);
            const topics = out.split('\n').map(t => t.trim()).filter(t => t.startsWith(`${username}.`));
            res.json({ success: true, topics });
        } else {
            // Local fallback
            res.json({ success: true, topics: [`${username}.events`] });
        }
    } catch (err) {
        console.error(err);
        res.status(500).json({ error: 'Failed to list topics' });
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
        const adminProps = '/etc/kafka/admin.properties';
        if (fs.existsSync('/opt/kafka/bin/kafka-topics.sh')) {
            await runCommand(`/opt/kafka/bin/kafka-topics.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --create --if-not-exists --topic ${fullTopic} --partitions 3 --replication-factor 1`);
        }
        res.json({ success: true, topic: fullTopic });
    } catch (err) {
        console.error(err);
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
        const adminProps = '/etc/kafka/admin.properties';
        if (fs.existsSync('/opt/kafka/bin/kafka-topics.sh')) {
            await runCommand(`/opt/kafka/bin/kafka-topics.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --delete --topic ${topic}`);
        }
        res.json({ success: true });
    } catch (err) {
        console.error(err);
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
        const adminProps = '/etc/kafka/admin.properties';
        let data = [];
        if (fs.existsSync('/opt/kafka/bin/kafka-console-consumer.sh')) {
            const out = await runCommand(`/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server broker.subartaghosh.co.in:9092 --consumer.config ${adminProps} --topic ${topic} --max-messages 20 --timeout-ms 2000`);
            data = out.split('\n').filter(l => l.trim() !== '');
        } else {
            data = ['{"example": "test data locally"}'];
        }
        // Since timeout causes an exit code 1 sometimes if it doesn't read 20 messages, we should wrap the exec properly, but runCommand rejects on exit code 1.
        // Wait, if it rejects on exit code 1, it will fall into catch block! We need to handle this.
        res.json({ success: true, data });
    } catch (err) {
        // If it was just a timeout because less than 20 messages exist, it will throw an error containing the output.
        // Node's child_process.exec returns stdout in the error object sometimes, or we can just swallow the error if stdout is present.
        if (err.stdout) {
            const data = err.stdout.split('\n').filter(l => l.trim() !== '');
            res.json({ success: true, data });
        } else {
            console.error("Data fetch error:", err);
            res.status(500).json({ error: 'Failed to fetch topic data' });
        }
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
        const adminProps = '/etc/kafka/admin.properties';
        if (fs.existsSync('/opt/kafka/bin/kafka-console-producer.sh')) {
            const tmpFile = `/tmp/${username}_${Date.now()}.json`;
            fs.writeFileSync(tmpFile, typeof payload === "string" ? payload : JSON.stringify(payload));
            await runCommand(`/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server broker.subartaghosh.co.in:9092 --producer.config ${adminProps} --topic ${topic} < ${tmpFile}`);
            fs.unlinkSync(tmpFile);
        }
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

app.get('/api/admin/users', adminAuth, (req, res) => {
    let db = JSON.parse(fs.readFileSync(DB_FILE));
    db = ensureDbSchema(db);
    
    const usersWithStats = {};
    for (const [username, info] of Object.entries(db.user_mappings)) {
        usersWithStats[username] = { 
            ...info, 
            storageMB: getUserStorageMB(username) 
        };
    }

    res.json({
        total_users: db.total_users,
        max_users: MAX_GLOBAL_USERS,
        users: usersWithStats
    });
});

app.delete('/api/admin/users/:username', adminAuth, async (req, res) => {
    const username = req.params.username;

    let db = JSON.parse(fs.readFileSync(DB_FILE));
    db = ensureDbSchema(db);

    if (!db.users.includes(username)) {
        return res.status(404).json({ error: 'User not found in DB.' });
    }

    try {
        const adminProps = '/etc/kafka/admin.properties';
        if (fs.existsSync('/opt/kafka/bin/kafka-configs.sh')) {
            await runCommand(`/opt/kafka/bin/kafka-configs.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --alter --delete-config "SCRAM-SHA-512" --entity-type users --entity-name ${username}`);
            await runCommand(`/opt/kafka/bin/kafka-topics.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --delete --topic "${username}.events"`);
            await runCommand(`/opt/kafka/bin/kafka-acls.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --remove --allow-principal "User:${username}" --operation Read --operation Write --operation Describe --operation Create --topic "${username}." --resource-pattern-type prefixed --force`);
            await runCommand(`/opt/kafka/bin/kafka-acls.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --remove --allow-principal "User:${username}" --operation Read --operation Describe --group "${username}" --resource-pattern-type prefixed --force`);
        }

        db.users = db.users.filter(u => u !== username);
        if (db.user_mappings[username]) {
            const githubId = db.user_mappings[username].githubId;
            db.githubIds = db.githubIds.filter(id => id !== githubId);
            delete db.user_mappings[username];
        }
        db.total_users -= 1;
        fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2));

        res.json({ success: true, message: `Successfully deleted user ${username}` });
    } catch (err) {
        console.error("Failed to delete user:", err);
        res.status(500).json({ error: 'Failed to delete user from Kafka.' });
    }
});

app.listen(PORT, () => {
    console.log(`Kafka Registration Portal running on port ${PORT}`);
    console.log(`Abuse Protection Enabled: Max ${MAX_GLOBAL_USERS} users total. Strict GitHub OAuth verification active.`);
});

// -----------------------------------------------------
// BACKGROUND ENFORCER (125MB STORAGE CAP)
// -----------------------------------------------------
const checkDiskUsage = async () => {
    if (!fs.existsSync(DB_FILE)) return;
    let db = JSON.parse(fs.readFileSync(DB_FILE));
    db = ensureDbSchema(db);
    
    const MAX_MB = 125.00;

    for (const [username, info] of Object.entries(db.user_mappings)) {
        const storageMB = parseFloat(getUserStorageMB(username));
        
        if (storageMB > MAX_MB && !info.quotaExceeded) {
            // Revoke write ACL
            const adminProps = '/etc/kafka/admin.properties';
            if (fs.existsSync('/opt/kafka/bin/kafka-acls.sh')) {
                await runCommand(`/opt/kafka/bin/kafka-acls.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --remove --allow-principal "User:${username}" --operation Write --topic "${username}." --resource-pattern-type prefixed --force`);
            }
            db.user_mappings[username].quotaExceeded = true;
            fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2));
            console.log(`User ${username} exceeded 125MB disk quota (${storageMB} MB). Write ACL revoked.`);
        } else if (storageMB <= MAX_MB && info.quotaExceeded) {
            // Restore write ACL
            const adminProps = '/etc/kafka/admin.properties';
            if (fs.existsSync('/opt/kafka/bin/kafka-acls.sh')) {
                await runCommand(`/opt/kafka/bin/kafka-acls.sh --bootstrap-server broker.subartaghosh.co.in:9092 --command-config ${adminProps} --add --allow-principal "User:${username}" --operation Write --topic "${username}." --resource-pattern-type prefixed`);
            }
            db.user_mappings[username].quotaExceeded = false;
            fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2));
            console.log(`User ${username} dropped below 125MB disk quota (${storageMB} MB). Write ACL restored.`);
        }
    }
};

setInterval(checkDiskUsage, 60000); // Check every minute
