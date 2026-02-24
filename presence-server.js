const http = require('http');
const fs = require('fs');
const path = require('path');
const { URL } = require('url');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT || 4317);
const WS_PATH = '/ws';
const ONLINE_WINDOW_MS = Number(process.env.PRESENCE_ONLINE_WINDOW_MS || 35000);
const CLEANUP_INTERVAL_MS = Number(process.env.PRESENCE_CLEANUP_INTERVAL_MS || 10000);
const STATE_FILE = process.env.SHARED_STATE_FILE || path.join(__dirname, 'shared-state.json');
const DEBUG_WS = process.env.WS_DEBUG !== '0';

const ROOT_DIR = __dirname;
const CONTENT_TYPES = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon',
  '.webp': 'image/webp'
};

const presenceByUser = new Map();
const userSockets = new Map();
const socketUser = new Map();

function wsLog(...args) {
  if (!DEBUG_WS) return;
  console.log('[ws]', ...args);
}

function safeText(value, fallback = '') {
  const text = String(value || '').trim();
  return text || fallback;
}

function safeRole(value) {
  const role = safeText(value, 'member');
  return ['member', 'admin', 'superadmin', 'inteligencia', 'financeiro'].includes(role) ? role : 'member';
}

function safePage(value) {
  return safeText(value, 'dashboard').slice(0, 80);
}

function normalizeAppState(rawState) {
  const source = rawState && typeof rawState === 'object' && !Array.isArray(rawState) ? rawState : {};
  return {
    users: Array.isArray(source.users) ? source.users : [],
    tickets: Array.isArray(source.tickets) ? source.tickets : [],
    logs: Array.isArray(source.logs) ? source.logs : [],
    tasks: Array.isArray(source.tasks) ? source.tasks : [],
    notes: Array.isArray(source.notes) ? source.notes : [],
    announcements: Array.isArray(source.announcements) ? source.announcements : [],
    settings: source.settings && typeof source.settings === 'object' && !Array.isArray(source.settings) ? source.settings : {}
  };
}

function normalizeStealthBus(rawBus) {
  const source = rawBus && typeof rawBus === 'object' && !Array.isArray(rawBus) ? rawBus : {};
  const sessions = Array.isArray(source.sessions) ? source.sessions : [];
  return {
    sessions: sessions.slice(0, 50).map((session) => ({
      id: Number(session.id) || Date.now(),
      createdBy: safeText(session.createdBy, ''),
      participants: Array.isArray(session.participants)
        ? session.participants.map((item) => safeText(item, '')).filter(Boolean).slice(0, 2)
        : [],
      createdAt: Number(session.createdAt) || Date.now(),
      updatedAt: Number(session.updatedAt) || Date.now(),
      messages: Array.isArray(session.messages)
        ? session.messages.slice(-500).map((message) => ({
            id: Number(message.id) || Date.now(),
            author: safeText(message.author, 'sistema'),
            content: safeText(message.content, '').slice(0, 1600),
            createdAt: Number(message.createdAt) || Date.now(),
            editedAt: message.editedAt ? Number(message.editedAt) : null
          }))
        : []
    }))
  };
}

function normalizeLiveBroadcast(rawPayload) {
  if (!rawPayload || typeof rawPayload !== 'object' || Array.isArray(rawPayload)) return null;
  const message = safeText(rawPayload.message, '').slice(0, 220);
  if (!message) return null;
  return {
    id: Number(rawPayload.id) || Date.now(),
    sender: safeText(rawPayload.sender, 'sistema'),
    anonymous: Boolean(rawPayload.anonymous),
    level: rawPayload.level === 'critical' ? 'critical' : 'normal',
    audience: rawPayload.audience && typeof rawPayload.audience === 'object' && !Array.isArray(rawPayload.audience)
      ? {
          type: ['all', 'role', 'user'].includes(rawPayload.audience.type) ? rawPayload.audience.type : 'all',
          role: safeRole(rawPayload.audience.role),
          user: safeText(rawPayload.audience.user, '')
        }
      : { type: 'all', role: 'member', user: '' },
    recipients: Array.isArray(rawPayload.recipients)
      ? rawPayload.recipients.map((item) => safeText(item, '')).filter(Boolean).slice(0, 500)
      : [],
    message,
    createdAt: Number(rawPayload.createdAt) || Date.now()
  };
}

function loadPersistedState() {
  try {
    if (!fs.existsSync(STATE_FILE)) {
      return {
        appState: normalizeAppState({}),
        stealthBus: normalizeStealthBus({ sessions: [] }),
        liveBroadcast: null
      };
    }
    const raw = fs.readFileSync(STATE_FILE, 'utf-8');
    const parsed = JSON.parse(raw);
    return {
      appState: normalizeAppState(parsed.appState),
      stealthBus: normalizeStealthBus(parsed.stealthBus),
      liveBroadcast: normalizeLiveBroadcast(parsed.liveBroadcast)
    };
  } catch {
    return {
      appState: normalizeAppState({}),
      stealthBus: normalizeStealthBus({ sessions: [] }),
      liveBroadcast: null
    };
  }
}

let persisted = loadPersistedState();
let appState = persisted.appState;
let stealthBusState = persisted.stealthBus;
let liveBroadcastState = persisted.liveBroadcast;

let persistTimer = null;
function schedulePersistState() {
  if (persistTimer) return;
  persistTimer = setTimeout(() => {
    persistTimer = null;
    const payload = {
      appState,
      stealthBus: stealthBusState,
      liveBroadcast: liveBroadcastState
    };
    try {
      fs.writeFileSync(STATE_FILE, JSON.stringify(payload, null, 2), 'utf-8');
    } catch {
      // ignore file persistence errors
    }
  }, 200);
}

function sendJson(ws, payload) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  try {
    ws.send(JSON.stringify(payload));
  } catch {
    // ignore send failures
  }
}

function broadcastJson(payload) {
  wsLog('broadcast', payload && payload.type ? payload.type : 'unknown', 'clients=', wss.clients.size);
  for (const client of wss.clients) {
    sendJson(client, payload);
  }
}

function appStateStats(state) {
  const safe = state && typeof state === 'object' && !Array.isArray(state) ? state : {};
  return {
    users: Array.isArray(safe.users) ? safe.users.length : 0,
    tickets: Array.isArray(safe.tickets) ? safe.tickets.length : 0,
    logs: Array.isArray(safe.logs) ? safe.logs.length : 0,
    tasks: Array.isArray(safe.tasks) ? safe.tasks.length : 0,
    notes: Array.isArray(safe.notes) ? safe.notes.length : 0,
    announcements: Array.isArray(safe.announcements) ? safe.announcements.length : 0
  };
}

function getSnapshotPayload() {
  const now = Date.now();
  const snapshot = {};
  for (const [username, entry] of presenceByUser.entries()) {
    if (!entry || !entry.lastSeen || now - entry.lastSeen > ONLINE_WINDOW_MS) continue;
    snapshot[username] = {
      username,
      role: safeRole(entry.role),
      page: safePage(entry.page),
      loginAt: Number(entry.loginAt) || now,
      lastSeen: Number(entry.lastSeen) || now
    };
  }
  return { type: 'snapshot', presence: snapshot };
}

function broadcastSnapshot() {
  broadcastJson(getSnapshotPayload());
}

function attachSocketToUser(ws, username) {
  const prevUsername = socketUser.get(ws);
  if (prevUsername && prevUsername !== username) {
    const prevSet = userSockets.get(prevUsername);
    if (prevSet) {
      prevSet.delete(ws);
      if (prevSet.size === 0) {
        userSockets.delete(prevUsername);
      }
    }
  }

  socketUser.set(ws, username);
  let set = userSockets.get(username);
  if (!set) {
    set = new Set();
    userSockets.set(username, set);
  }
  set.add(ws);
}

function detachSocketFromUser(ws) {
  const username = socketUser.get(ws);
  if (!username) return null;

  socketUser.delete(ws);
  const set = userSockets.get(username);
  if (!set) return username;

  set.delete(ws);
  if (set.size === 0) {
    userSockets.delete(username);
    presenceByUser.delete(username);
  }

  return username;
}

function upsertPresenceFromPayload(ws, payload) {
  const username = safeText(payload.username).toLowerCase();
  if (!username) return false;

  attachSocketToUser(ws, username);
  const now = Date.now();
  presenceByUser.set(username, {
    username,
    role: safeRole(payload.role),
    page: safePage(payload.page),
    loginAt: Number(payload.loginAt) || now,
    lastSeen: Number(payload.lastSeen) || now
  });
  return true;
}

function applyLeaveFromPayload(ws, payload) {
  const requestedUsername = safeText(payload.username).toLowerCase();
  const boundUsername = socketUser.get(ws);
  const username = requestedUsername || boundUsername;
  if (!username) return false;

  const set = userSockets.get(username);
  if (set) {
    set.delete(ws);
    if (set.size === 0) {
      userSockets.delete(username);
      presenceByUser.delete(username);
    }
  } else {
    presenceByUser.delete(username);
  }

  if (boundUsername === username) {
    socketUser.delete(ws);
  }
  return true;
}

function cleanupStalePresence() {
  const now = Date.now();
  let changed = false;

  for (const [username, entry] of presenceByUser.entries()) {
    const lastSeen = Number(entry.lastSeen) || 0;
    if (!lastSeen || now - lastSeen > ONLINE_WINDOW_MS) {
      presenceByUser.delete(username);
      changed = true;
    }
  }

  if (changed) {
    broadcastSnapshot();
  }
}

function serveStatic(req, res) {
  let reqUrl;
  try {
    reqUrl = new URL(req.url, `http://${req.headers.host || 'localhost'}`);
  } catch {
    res.writeHead(400, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Bad request');
    return;
  }

  let pathname = decodeURIComponent(reqUrl.pathname);
  if (pathname === '/') pathname = '/index.html';

  const safePath = path.normalize(path.join(ROOT_DIR, pathname));
  if (!safePath.startsWith(ROOT_DIR)) {
    res.writeHead(403, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Forbidden');
    return;
  }

  fs.stat(safePath, (statErr, stat) => {
    if (statErr || !stat) {
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('Not found');
      return;
    }

    const filePath = stat.isDirectory() ? path.join(safePath, 'index.html') : safePath;
    fs.readFile(filePath, (readErr, data) => {
      if (readErr) {
        res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
        res.end('Not found');
        return;
      }
      const ext = path.extname(filePath).toLowerCase();
      const type = CONTENT_TYPES[ext] || 'application/octet-stream';
      res.writeHead(200, { 'Content-Type': type, 'Cache-Control': 'no-cache' });
      res.end(data);
    });
  });
}

const server = http.createServer(serveStatic);
const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  let reqUrl;
  try {
    reqUrl = new URL(req.url || '/', `http://${req.headers.host || 'localhost'}`);
  } catch {
    socket.destroy();
    return;
  }

  const pathname = reqUrl.pathname || '/';
  const pathOk = pathname === WS_PATH || pathname === `${WS_PATH}/`;
  if (!pathOk) {
    socket.destroy();
    return;
  }

  wss.handleUpgrade(req, socket, head, (ws) => {
    wss.emit('connection', ws, req);
  });
});

wss.on('connection', (ws) => {
  wsLog('client connected', 'clients=', wss.clients.size);
  sendJson(ws, getSnapshotPayload());
  sendJson(ws, { type: 'app_state_sync', state: appState, origin: 'server_init' });
  sendJson(ws, { type: 'stealth_sync', bus: stealthBusState });
  if (liveBroadcastState) {
    sendJson(ws, { type: 'live_broadcast_sync', payload: liveBroadcastState });
  }

  ws.on('message', (raw) => {
    let payload;
    try {
      payload = JSON.parse(raw.toString());
    } catch {
      return;
    }
    if (!payload || typeof payload !== 'object') return;
    wsLog('recv', payload.type || 'unknown', 'clients=', wss.clients.size);

    if (payload.type === 'presence') {
      const changed = upsertPresenceFromPayload(ws, payload);
      if (changed) broadcastSnapshot();
      return;
    }

    if (payload.type === 'leave') {
      const changed = applyLeaveFromPayload(ws, payload);
      if (changed) broadcastSnapshot();
      return;
    }

    if (payload.type === 'app_state_sync') {
      appState = normalizeAppState(payload.state);
      schedulePersistState();
      wsLog('apply app_state_sync', appStateStats(appState), 'origin=', safeText(payload.origin, '') || 'remote');
      broadcastJson({
        type: 'app_state_sync',
        state: appState,
        origin: safeText(payload.origin, '') || 'remote'
      });
      return;
    }

    if (payload.type === 'stealth_sync') {
      stealthBusState = normalizeStealthBus(payload.bus);
      schedulePersistState();
      wsLog('apply stealth_sync', 'sessions=', Array.isArray(stealthBusState.sessions) ? stealthBusState.sessions.length : 0);
      broadcastJson({ type: 'stealth_sync', bus: stealthBusState });
      return;
    }

    if (payload.type === 'live_broadcast_sync') {
      liveBroadcastState = normalizeLiveBroadcast(payload.payload);
      if (liveBroadcastState) {
        schedulePersistState();
        wsLog('apply live_broadcast_sync', 'sender=', liveBroadcastState.sender, 'audience=', liveBroadcastState.audience.type);
        broadcastJson({ type: 'live_broadcast_sync', payload: liveBroadcastState });
      }
      return;
    }

    if (payload.type === 'ping') {
      sendJson(ws, { type: 'pong', ts: Date.now() });
      return;
    }

    if (payload.type === 'snapshot_request') {
      sendJson(ws, getSnapshotPayload());
    }
  });

  ws.on('close', () => {
    const username = detachSocketFromUser(ws);
    wsLog('client disconnected', username || '-', 'clients=', wss.clients.size);
    if (username) {
      broadcastSnapshot();
    }
  });
});

setInterval(cleanupStalePresence, CLEANUP_INTERVAL_MS);

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[portal] HTTP + WS online em http://0.0.0.0:${PORT}`);
  console.log(`[portal] WebSocket path: ${WS_PATH}`);
  console.log(`[portal] Estado persistido em: ${STATE_FILE}`);
});

process.on('SIGINT', () => {
  server.close(() => process.exit(0));
});
process.on('SIGTERM', () => {
  server.close(() => process.exit(0));
});
