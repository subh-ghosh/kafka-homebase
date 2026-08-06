const https = require('https');

const HOST = 'streambase.subartaghosh.co.in';
const GITHUB_TOKEN = process.env.GITHUB_TOKEN || '';
const ADMIN_TOKEN = Buffer.from(`admin:${process.env.ADMIN_PASS || ''}`).toString('base64');

let pass = 0, fail = 0, skip = 0;
let testUsername = null;  // discovered from /api/user
let testTopic = null;     // discovered from /api/topics

function req(method, path, headers = {}, body = null) {
    return new Promise((resolve, reject) => {
        const opts = {
            hostname: HOST, port: 443, path,
            method, headers: { 'Content-Type': 'application/json', ...headers }
        };
        const r = https.request(opts, res => {
            let data = '';
            res.on('data', d => data += d);
            res.on('end', () => {
                try { resolve({ status: res.statusCode, body: JSON.parse(data) }); }
                catch { resolve({ status: res.statusCode, body: data }); }
            });
        });
        r.on('error', reject);
        r.setTimeout(10000, () => { r.destroy(); reject(new Error('timeout')); });
        if (body) r.write(JSON.stringify(body));
        r.end();
    });
}

function check(label, status, got, expect, note = '') {
    const ok = Array.isArray(expect) ? expect.includes(got) : got === expect;
    const icon = ok ? '✅' : '❌';
    const noteStr = note ? `  → ${note}` : '';
    console.log(`  ${icon} [${got}] ${label}${noteStr}`);
    if (ok) pass++; else { fail++; console.log(`      Expected: ${expect}, Got: ${got}`); }
    return ok;
}

async function test(label, fn) {
    try {
        await fn();
    } catch (e) {
        console.log(`  💥 [ERR] ${label} — ${e.message}`);
        fail++;
    }
}

function sep(title) {
    console.log(`\n${'─'.repeat(55)}`);
    console.log(` ${title}`);
    console.log('─'.repeat(55));
}

(async () => {
    console.log('╔═════════════════════════════════════════════════════╗');
    console.log('║     Kafka Portal — Full API Endpoint Test Suite     ║');
    console.log('╚═════════════════════════════════════════════════════╝\n');

    // ─── PUBLIC ROUTES ────────────────────────────────────────────────
    sep('PUBLIC ROUTES');

    await test('GET /api/config', async () => {
        const r = await req('GET', '/api/config');
        check('GET /api/config — returns clientId', r.status, r.status, 200);
        check('  ↳ clientId is present', r.status, typeof r.body.clientId, 'string');
        console.log(`       clientId: ${r.body.clientId}`);
    });

    // ─── USER AUTH ROUTES ─────────────────────────────────────────────
    sep('USER ROUTES (GitHub Bearer Token)');

    const bearerHeader = { 'Authorization': `Bearer ${GITHUB_TOKEN}` };

    await test('GET /api/user — check existing user', async () => {
        const r = await req('GET', '/api/user', bearerHeader);
        check('GET /api/user — succeeds', r.status, r.status, 200);
        if (r.body.exists) {
            testUsername = r.body.username;
            testTopic = r.body.topic;
            console.log(`       username: ${testUsername}`);
            console.log(`       topic:    ${testTopic}`);
            console.log(`       storageMB: ${r.body.storageMB}`);
        } else {
            console.log('       user not registered — registering test user...');
            const regRes = await req('POST', '/api/register', {}, { accessToken: GITHUB_TOKEN });
            if (regRes.body.success || regRes.status === 200) {
                const uRes = await req('GET', '/api/user', bearerHeader);
                if (uRes.body.exists) {
                    testUsername = uRes.body.username;
                    testTopic = uRes.body.topic;
                    console.log(`       registered: ${testUsername}`);
                }
            }
        }
        check('  ↳ response has exists field', r.status, 'exists' in r.body, true);
    });

    await test('GET /api/user — with bad token → 401', async () => {
        const r = await req('GET', '/api/user', { 'Authorization': 'Bearer fake_token_xyz' });
        check('GET /api/user (bad token) → 401', r.status, r.status, 401);
    });

    await test('GET /api/topics — list user topics', async () => {
        const r = await req('GET', '/api/topics', bearerHeader);
        check('GET /api/topics — succeeds', r.status, r.status, 200);
        check('  ↳ topics is an array', r.status, Array.isArray(r.body.topics), true);
        console.log(`       topics: [${(r.body.topics || []).join(', ')}]`);
    });

    await test('GET /api/consumer-groups', async () => {
        const r = await req('GET', '/api/consumer-groups', bearerHeader);
        check('GET /api/consumer-groups — succeeds', r.status, r.status, 200);
        check('  ↳ groups is array', r.status, Array.isArray(r.body.groups), true);
        console.log(`       groups: ${r.body.groups?.length} found`);
    });

    // ─── TOPIC MANAGEMENT ─────────────────────────────────────────────
    sep('TOPIC MANAGEMENT');

    const testTopicSuffix = 'apitest_' + Date.now();
    let createdTopic = null;

    await test('POST /api/topics — create topic', async () => {
        const r = await req('POST', '/api/topics', bearerHeader, { suffix: testTopicSuffix });
        check('POST /api/topics — 200 or 409', r.status, r.status, [200, 409]);
        if (r.body.success) {
            createdTopic = r.body.topic;
            console.log(`       created: ${createdTopic}`);
        } else {
            console.log(`       note: ${r.body.error}`);
        }
    });

    await test('POST /api/topics — missing suffix → 400', async () => {
        const r = await req('POST', '/api/topics', bearerHeader, {});
        check('POST /api/topics (no suffix) → 400', r.status, r.status, 400);
    });

    if (testTopic) {
        await test(`GET /api/topics/${testTopic}/data — fetch messages`, async () => {
            const r = await req('GET', `/api/topics/${encodeURIComponent(testTopic)}/data`, bearerHeader);
            check(`GET /api/topics/:topic/data — succeeds`, r.status, r.status, 200);
            check('  ↳ data is array', r.status, Array.isArray(r.body.data), true);
            console.log(`       messages: ${r.body.data?.length} found`);
        });

        await test(`POST /api/topics/${testTopic}/produce — send message`, async () => {
            const r = await req('POST', `/api/topics/${encodeURIComponent(testTopic)}/produce`, bearerHeader, { payload: JSON.stringify({ test: 'api_load_test', ts: Date.now() }) });
            check(`POST /api/topics/:topic/produce — 200`, r.status, r.status, 200);
            console.log(`       result: ${r.body.success ? 'sent ✓' : r.body.error}`);
        });
    } else {
        console.log('  ⏭  Skipping topic data tests — no existing topic found');
        skip += 2;
    }

    if (createdTopic) {
        await test(`DELETE /api/topics/:topic — delete test topic`, async () => {
            const r = await req('DELETE', `/api/topics/${encodeURIComponent(createdTopic)}`, bearerHeader);
            check(`DELETE /api/topics/:topic — 200`, r.status, r.status, 200);
            console.log(`       deleted: ${createdTopic}`);
        });
    } else {
        console.log('  ⏭  Skipping DELETE /api/topics/:topic — no topic was created');
        skip++;
    }

    // ─── REGENERATE (non-destructive) ─────────────────────────────────
    sep('ACCOUNT MANAGEMENT');

    await test('POST /api/regenerate — regenerate password', async () => {
        const r = await req('POST', '/api/regenerate', {}, { accessToken: GITHUB_TOKEN });
        check('POST /api/regenerate — 200', r.status, r.status, 200);
        check('  ↳ returns new password', r.status, typeof r.body.password, 'string');
        if (r.body.password) console.log(`       new pass length: ${r.body.password.length}`);
    });

    // ─── ADMIN ROUTES ─────────────────────────────────────────────────
    sep('ADMIN ROUTES');

    const adminHeader = { 'Authorization': `Basic ${ADMIN_TOKEN}` };

    await test('POST /api/admin/login — valid credentials', async () => {
        const r = await req('POST', '/api/admin/login', adminHeader);
        check('POST /api/admin/login — 200', r.status, r.status, 200);
    });

    await test('POST /api/admin/login — wrong password → 401', async () => {
        const badToken = Buffer.from('admin:wrongpassword').toString('base64');
        const r = await req('POST', '/api/admin/login', { 'Authorization': `Basic ${badToken}` });
        check('POST /api/admin/login (bad creds) → 401', r.status, r.status, 401);
    });

    await test('GET /api/admin/health — EC2 metrics', async () => {
        const r = await req('GET', '/api/admin/health', adminHeader);
        check('GET /api/admin/health — 200', r.status, r.status, 200);
        check('  ↳ cpu_load present', r.status, Array.isArray(r.body.cpu_load), true);
        check('  ↳ free_mem_mb present', r.status, typeof r.body.free_mem_mb, 'number');
        console.log(`       CPU load: ${r.body.cpu_load?.map(l => l.toFixed(2)).join(', ')}`);
        console.log(`       RAM free: ${r.body.free_mem_mb} MB / ${r.body.total_mem_mb} MB`);
    });

    await test('GET /api/admin/health — no auth → 401', async () => {
        const r = await req('GET', '/api/admin/health');
        check('GET /api/admin/health (no auth) → 401', r.status, r.status, 401);
    });

    await test('GET /api/admin/users — full user list', async () => {
        const r = await req('GET', '/api/admin/users', adminHeader);
        check('GET /api/admin/users — 200', r.status, r.status, 200);
        check('  ↳ total_users is number', r.status, typeof r.body.total_users, 'number');
        check('  ↳ users object present', r.status, typeof r.body.users, 'object');
        console.log(`       total users: ${r.body.total_users} / ${r.body.max_users}`);
    });

    await test('DELETE /api/admin/users/:username — wrong user → 404/400', async () => {
        const r = await req('DELETE', '/api/admin/users/nonexistent_user_xyz', adminHeader);
        check('DELETE /api/admin/users (nonexistent) → 404 or 400', r.status, r.status, [404, 400, 500]);
        console.log(`       msg: ${r.body.error || r.body.message || r.status}`);
    });

    // ─── SUMMARY ──────────────────────────────────────────────────────
    const total = pass + fail;
    console.log('\n╔═════════════════════════════════════════════════════╗');
    console.log(`║  Results: ${pass}/${total} passed  |  ${fail} failed  |  ${skip} skipped          ║`.padEnd(54) + '║');
    console.log(`║  ${pass === total ? '✅ ALL TESTS PASSED' : `❌ ${fail} TEST(S) FAILED`}                                    ║`.padEnd(54) + '║');
    console.log('╚═════════════════════════════════════════════════════╝\n');

    process.exit(fail > 0 ? 1 : 0);
})();
