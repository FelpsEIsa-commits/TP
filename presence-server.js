const { WebSocketServer } = require('ws');

const port = Number(process.env.PRESENCE_PORT || 4317);
const ONLINE_WINDOW_MS = Number(process.env.PRESENCE_ONLINE_WINDOW_MS || 35000);
const CLEANUP_INTERVAL_MS = Number(process.env.PRESENCE_CLEANUP_INTERVAL_MS || 10000);

const presenceByUser = new Map();
const userSockets = new Map();
const socketUser = new Map();
let stealthBusState = { sessions: [] };
let liveBroadcastState = null;

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

function sendJson(ws, payload) {
  if (!ws || ws.readyState !== ws.OPEN) return;
  try {
    ws.send(JSON.stringify(payload));
  } catch {
    // ignore send failure
  }
}

function broadcastJson(payload) {
  for (const client of wss.clients) {
    sendJson(client, payload);
  }
}

function broadcastSnapshot() {
  broadcastJson(getSnapshotPayload());
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
        ? session.messages
            .slice(-400)
            .map((message) => ({
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

const wss = new WebSocketServer({ port });

wss.on('connection', (ws) => {
  sendJson(ws, getSnapshotPayload());
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

    if (payload.type === 'ping') {
      sendJson(ws, { type: 'pong', ts: Date.now() });
      return;
    }

    if (payload.type === 'stealth_sync') {
      stealthBusState = normalizeStealthBus(payload.bus);
      broadcastJson({ type: 'stealth_sync', bus: stealthBusState });
      return;
    }

    if (payload.type === 'live_broadcast_sync') {
      liveBroadcastState = normalizeLiveBroadcast(payload.payload);
      if (liveBroadcastState) {
        broadcastJson({ type: 'live_broadcast_sync', payload: liveBroadcastState });
      }
    }
  });

  ws.on('close', () => {
    const username = detachSocketFromUser(ws);
    if (username) {
      broadcastSnapshot();
    }
  });
});

setInterval(cleanupStalePresence, CLEANUP_INTERVAL_MS);

wss.on('listening', () => {
  console.log(`[presence] websocket server running on port ${port}`);
});

process.on('SIGINT', () => {
  wss.close(() => process.exit(0));
});
process.on('SIGTERM', () => {
  wss.close(() => process.exit(0));
});
