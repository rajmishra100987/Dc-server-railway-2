// ============================================================
// 🚀 MULTI-COOKIE ROUND ROBIN MESSENGER BOT — ZERO BLOCKING
// ------------------------------------------------------------
// Features:
//  ✅ Multi-cookie support (1 se N tak)
//  ✅ Har cookie se independent login (alag session)
//  ✅ Round robin sending (har message next healthy cookie se)
//  ✅ Cookie fail → background retry (loop NEVER rukta)
//  ✅ Timeout protection (connect 30s, send 15s)
//  ✅ Exponential backoff (10s → 5m cap)
//  ✅ Anti-crash + graceful shutdown
//  ✅ Zero terminal log spam (DEBUG=1 to enable)
//  ✅ /health endpoint pe live logs + cookie status
// ============================================================

// 🔇 Terminal silencer
if (process.env.DEBUG !== '1') {
    const noop = () => {};
    console.log = noop;
    console.info = noop;
    console.warn = noop;
    console.error = noop;
    console.debug = noop;
}

global.WebSocket = require('ws');

const express = require('express');
const { MessengerClient, Platform, CookieManager } = require('messagix-js');
const { setTimeout: sleep } = require('timers/promises');
const fs = require('fs');

const app = express();
const PORT = process.env.PORT || 3000;

// ==================== CONSTANTS ====================
const CONNECT_TIMEOUT_MS = 30000;                                 // 30s
const SEND_TIMEOUT_MS = 15000;                                    // 15s
const BACKOFF_STEPS_MS = [10000, 30000, 60000, 120000, 300000];   // 10s → 5m
const CONFIG_ERROR_WAIT_MS = 10000;                               // 10s
const MAX_LOG_LINES = 200;

// ==================== STATE ====================
let isShuttingDown = false;
let globalStates = null;
const logs = [];

// ==================== ANTI-CRASH ====================
process.on('uncaughtException', () => {});
process.on('unhandledRejection', () => {});

// ==================== HELPERS ====================
function pushLog(msg) {
    const line = `[${new Date().toLocaleTimeString()}] ${msg}`;
    logs.push(line);
    if (logs.length > MAX_LOG_LINES) logs.splice(0, logs.length - MAX_LOG_LINES);
}

function readTextFile(filename, defaultValue = '') {
    try {
        if (fs.existsSync(filename)) {
            return fs.readFileSync(filename, 'utf8').trim();
        }
    } catch (_) {}
    return defaultValue;
}

// Har non-empty line = ek cookie set
function parseCookies(raw) {
    return raw
        .split('\n')
        .map((l) => l.trim())
        .filter((l) => l && l.includes('c_user=') && l.includes('xs='));
}

function withTimeout(promise, ms, label) {
    let timer;
    const tp = new Promise((_, rej) => {
        timer = setTimeout(() => rej(new Error(`${label} timeout ${ms}ms`)), ms);
    });
    return Promise.race([promise, tp]).finally(() => clearTimeout(timer));
}

async function safeDisconnect(client) {
    if (!client) return;
    try {
        await client.disconnect();
    } catch (_) {}
}

async function interruptibleSleep(ms) {
    const step = 1000;
    for (let elapsed = 0; elapsed < ms; elapsed += step) {
        if (isShuttingDown) return false;
        await sleep(Math.min(step, ms - elapsed));
    }
    return true;
}

function backoffMs(failCount) {
    const idx = Math.min(Math.max(failCount - 1, 0), BACKOFF_STEPS_MS.length - 1);
    return BACKOFF_STEPS_MS[idx];
}

// ==================== ROUTES ====================
app.head('/', (req, res) => res.status(200).end());
app.get('/', (req, res) => res.send('raj mishra dc server running ✅️'));
app.get('/ping', (req, res) => res.send('Pong - Server is Active!'));

app.get('/health', (req, res) => {
    const cookieStatus = globalStates
        ? globalStates.map((s) => ({
              cookie: s.idx,
              healthy: s.healthy,
              failCount: s.failCount,
              retrying: s.retryInProgress,
          }))
        : [];

    res.status(200).json({
        status: 'ONLINE',
        uptime: Math.round(process.uptime()),
        isShuttingDown,
        totalCookies: globalStates ? globalStates.length : 0,
        healthyCount: globalStates ? globalStates.filter((s) => s.healthy).length : 0,
        cookieStatus,
        logs: logs.slice(-40),
    });
});

app.listen(PORT, '0.0.0.0', () => {
    pushLog(`[SYSTEM] Live on port ${PORT}`);
    startMultiCookieBot();
});

// ==================== MAIN BOT ====================
async function startMultiCookieBot() {
    pushLog('[BOT] Starting multi-cookie round-robin bot...');

    while (!isShuttingDown) {
        try {
            // ---- Read config files ----
            const cookiesRaw = readTextFile('cookies.txt');
            const threadId = readTextFile('convo.txt');
            const delaySec = parseInt(readTextFile('time.txt', '120'), 10) || 120;
            const hatersName = readTextFile('hatersname.txt');
            const messagesRaw = readTextFile('messages.txt', 'Hello');
            const messages = messagesRaw.split('\n').map((m) => m.trim()).filter(Boolean);
            const cookieList = parseCookies(cookiesRaw);

            if (cookieList.length === 0 || !threadId || messages.length === 0) {
                pushLog('[CONFIG ERROR] cookies.txt / convo.txt / messages.txt check karo. 10s retry...');
                await interruptibleSleep(CONFIG_ERROR_WAIT_MS);
                continue;
            }

            pushLog(`[INIT] ${cookieList.length} cookie(s) mile. Initial connect shuru...`);

            // ---- State per cookie ----
            const states = cookieList.map((str, i) => ({
                idx: i + 1,
                cookieStr: str,
                client: null,
                healthy: false,
                failCount: 0,
                nextRetryAt: 0,
                retryInProgress: false,
            }));

            globalStates = states;

            // ---- Initial connect (sequential — FB safe) ----
            for (const state of states) {
                if (isShuttingDown) break;
                await tryConnectState(state);
                await interruptibleSleep(2000); // chhota gap
            }

            const healthyCount = states.filter((s) => s.healthy).length;
            pushLog(`[INIT] ${healthyCount}/${cookieList.length} cookies connected.`);

            if (healthyCount === 0) {
                pushLog('[WARN] 0 cookies healthy. Background retry chalega.');
            }

            let msgIndex = 0;
            let rrIndex = 0;
            let loopCount = 1;

            // ============ SEND LOOP (ZERO BLOCK) ============
            while (!isShuttingDown) {
                const loopStart = Date.now();

                // ---- Live file re-read ----
                const curDelay = parseInt(readTextFile('time.txt', String(delaySec)), 10) || 120;
                const curHaters = readTextFile('hatersname.txt');
                const curMsgsRaw = readTextFile('messages.txt');
                const curMsgs = curMsgsRaw
                    ? curMsgsRaw.split('\n').map((m) => m.trim()).filter(Boolean)
                    : messages;

                if (curMsgs.length === 0) {
                    await interruptibleSleep(CONFIG_ERROR_WAIT_MS);
                    continue;
                }

                if (msgIndex >= curMsgs.length) {
                    msgIndex = 0;
                    loopCount++;
                    pushLog(`[LOOP] Round ${loopCount} starting`);
                }

                const rawMsg = curMsgs[msgIndex] || 'Hello';
                const finalMsg = curHaters ? `${curHaters} ${rawMsg}` : rawMsg;

                // ---- Try send via round robin ----
                let attempts = 0;
                let sent = false;

                while (attempts < states.length && !isShuttingDown) {
                    const st = states[rrIndex % states.length];
                    rrIndex++;

                    // Unhealthy? → background retry + skip (no block)
                    if (!st.healthy) {
                        if (!st.retryInProgress && Date.now() >= st.nextRetryAt) {
                            st.retryInProgress = true;
                            // Fire and forget
                            tryConnectState(st)
                                .catch(() => {})
                                .finally(() => {
                                    st.retryInProgress = false;
                                });
                        }
                        attempts++;
                        continue;
                    }

                    // Healthy → send
                    try {
                        await withTimeout(
                            st.client.sendMessage(threadId, finalMsg),
                            SEND_TIMEOUT_MS,
                            `send[c${st.idx}]`
                        );
                        pushLog(`🚀 [Cookie ${st.idx}] Sent: ${finalMsg}`);
                        st.failCount = 0;
                        sent = true;
                        break;
                    } catch (err) {
                        st.failCount++;
                        pushLog(`⚠️ [Cookie ${st.idx}] fail (${st.failCount}): ${err.message}`);

                        const oldClient = st.client;
                        st.client = null;
                        st.healthy = false;
                        st.nextRetryAt = Date.now() + backoffMs(st.failCount);
                        st.retryInProgress = false;

                        // Background disconnect
                        safeDisconnect(oldClient).catch(() => {});
                        attempts++;
                    }
                }

                if (!sent) {
                    pushLog('⚠️ Koi cookie ready nahi. Round skip (delay continue).');
                }

                msgIndex++;

                // Adjust delay for time already spent
                const elapsed = Date.now() - loopStart;
                const remaining = curDelay * 1000 - elapsed;
                if (remaining > 0) {
                    const ok = await interruptibleSleep(remaining);
                    if (!ok) break;
                }
            }
        } catch (err) {
            pushLog(`[FATAL] ${err.message}. 30s baad retry...`);
            await interruptibleSleep(30000);
        }
    }

    pushLog('[BOT] Shutdown complete.');
}

// ==================== CONNECT HELPER ====================
async function tryConnectState(state) {
    if (state.client) {
        await safeDisconnect(state.client);
        state.client = null;
    }

    try {
        const cm = CookieManager.fromString(Platform.Messenger, state.cookieStr);
        const client = new MessengerClient({
            platform: Platform.Messenger,
            cookies: cm.getAll(),
            enableE2EE: false,
        });

        await withTimeout(client.loadMessagesPage(), CONNECT_TIMEOUT_MS, `load[c${state.idx}]`);
        await withTimeout(client.connect(), CONNECT_TIMEOUT_MS, `conn[c${state.idx}]`);

        state.client = client;
        state.healthy = true;
        state.failCount = 0;
        state.nextRetryAt = 0;
        pushLog(`✅ Cookie ${state.idx} connected`);
    } catch (err) {
        await safeDisconnect(state.client);
        state.client = null;
        state.healthy = false;
        state.failCount++;
        state.nextRetryAt = Date.now() + backoffMs(state.failCount);
        pushLog(`❌ Cookie ${state.idx} fail (${state.failCount}): ${err.message}`);
    }
}

// ==================== GRACEFUL SHUTDOWN ====================
async function gracefulShutdown(signal) {
    if (isShuttingDown) return;
    isShuttingDown = true;
    pushLog(`[SHUTDOWN] ${signal}`);

    if (globalStates) {
        for (const st of globalStates) {
            st.retryInProgress = false;
            if (st.client) {
                try { await st.client.disconnect(); } catch (_) {}
                st.client = null;
            }
        }
    }

    await sleep(500);
    process.exit(0);
}

process.on('SIGINT', () => gracefulShutdown('SIGINT'));
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
