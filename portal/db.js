const sqlite3 = require('sqlite3');
const { open } = require('sqlite');
const fs = require('fs');
const path = require('path');

const DB_JSON_FILE = path.join(__dirname, 'db.json');
const DB_SQLITE_FILE = path.join(__dirname, 'db.sqlite');

let dbPromise = null;

const initDb = async () => {
    if (dbPromise) return dbPromise;

    dbPromise = open({
        filename: DB_SQLITE_FILE,
        driver: sqlite3.Database
    }).then(async (db) => {
        await db.exec(`
            CREATE TABLE IF NOT EXISTS users (
                username TEXT PRIMARY KEY,
                githubId TEXT UNIQUE,
                githubHandle TEXT,
                topicName TEXT,
                quotaExceeded INTEGER DEFAULT 0,
                created_at TEXT,
                updated_at TEXT
            );
        `);
        
        // Migrate data if db.json exists and table is empty
        if (fs.existsSync(DB_JSON_FILE)) {
            const row = await db.get(`SELECT COUNT(*) as count FROM users`);
            if (row.count === 0) {
                console.log("Migrating db.json to db.sqlite...");
                const oldDb = JSON.parse(fs.readFileSync(DB_JSON_FILE));
                if (oldDb.user_mappings) {
                    for (const [username, data] of Object.entries(oldDb.user_mappings)) {
                        await db.run(
                            `INSERT INTO users (username, githubId, githubHandle, topicName, quotaExceeded, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
                            [username, data.githubId, data.githubHandle || '', data.topicName, data.quotaExceeded ? 1 : 0, data.created_at || new Date().toISOString(), data.updated_at || null]
                        );
                    }
                }
                console.log("Migration complete. Renaming db.json to db.json.bak");
                fs.renameSync(DB_JSON_FILE, DB_JSON_FILE + '.bak');
            }
        }
        return db;
    });
    
    return dbPromise;
};

const getDb = async () => {
    return await initDb();
};

const getUserByGithubId = async (githubId) => {
    const db = await getDb();
    return await db.get(`SELECT * FROM users WHERE githubId = ?`, [githubId]);
};

const getUserByName = async (username) => {
    const db = await getDb();
    return await db.get(`SELECT * FROM users WHERE username = ?`, [username]);
};

const getTotalUsers = async () => {
    const db = await getDb();
    const row = await db.get(`SELECT COUNT(*) as count FROM users`);
    return row.count;
};

const createUser = async (user) => {
    const db = await getDb();
    await db.run(
        `INSERT INTO users (username, githubId, githubHandle, topicName, created_at) VALUES (?, ?, ?, ?, ?)`,
        [user.username, user.githubId, user.githubHandle, user.topicName, new Date().toISOString()]
    );
};

const updateUpdatedAt = async (username) => {
    const db = await getDb();
    await db.run(`UPDATE users SET updated_at = ? WHERE username = ?`, [new Date().toISOString(), username]);
};

const deleteUser = async (username) => {
    const db = await getDb();
    await db.run(`DELETE FROM users WHERE username = ?`, [username]);
};

const setQuotaExceeded = async (username, exceeded) => {
    const db = await getDb();
    await db.run(`UPDATE users SET quotaExceeded = ? WHERE username = ?`, [exceeded ? 1 : 0, username]);
};

const getAllUsers = async () => {
    const db = await getDb();
    return await db.all(`SELECT * FROM users`);
};

module.exports = {
    initDb,
    getDb,
    getUserByGithubId,
    getUserByName,
    getTotalUsers,
    createUser,
    updateUpdatedAt,
    deleteUser,
    setQuotaExceeded,
    getAllUsers
};
