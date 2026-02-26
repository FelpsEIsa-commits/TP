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
const DEBUG_WS = process.env.WS_DEBUG === '1';
const DEFAULT_PASSWORD = '1705';
const MAX_SERVER_LOG_ENTRIES = Number(process.env.MAX_SERVER_LOG_ENTRIES || 2500);
const MAX_SERVER_TICKET_MESSAGES = Number(process.env.MAX_SERVER_TICKET_MESSAGES || 600);
const MAX_PROFILE_NAME_LENGTH = 42;
const MAX_AVATAR_DATA_URL_LENGTH = 380000;
const CORE_USERS = [
  { username: 'esther', role: 'superadmin', password: DEFAULT_PASSWORD, lockedRole: true },
  { username: 'belle', role: 'admin', password: DEFAULT_PASSWORD, lockedRole: false },
  { username: 'felps', role: 'member', password: DEFAULT_PASSWORD, lockedRole: false },
  { username: 'yoon', role: 'member', password: DEFAULT_PASSWORD, lockedRole: false },
  { username: 'murilo', role: 'member', password: DEFAULT_PASSWORD, lockedRole: false },
  { username: 'matheus', role: 'member', password: DEFAULT_PASSWORD, lockedRole: false },
  { username: 'inteligencia tp', role: 'inteligencia', password: DEFAULT_PASSWORD, lockedRole: true }
];

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
const authBySocket = new Map();

function wsLog(...args) {
  if (!DEBUG_WS) return;
  console.log('[ws]', ...args);
}

function safeText(value, fallback = '') {
  const text = String(value || '').trim();
  return text || fallback;
}

function safeRole(value) {
  const role = safeText(value, 'member').toLowerCase();
  return /^[a-z0-9_-]{2,24}$/.test(role) ? role : 'member';
}

function safePage(value) {
  return safeText(value, 'dashboard').slice(0, 80);
}

function normalizeDisplayName(value, fallbackUsername = '') {
  const fallback = safeText(fallbackUsername, '');
  const cleaned = safeText(value, '').replace(/\s+/g, ' ');
  if (!cleaned) return fallback;
  return cleaned.slice(0, MAX_PROFILE_NAME_LENGTH);
}

function sanitizeAvatarDataUrl(value) {
  const raw = safeText(value, '');
  if (!raw) return '';
  if (raw.length > MAX_AVATAR_DATA_URL_LENGTH) return '';
  if (!/^data:image\/(png|jpe?g|webp|gif);base64,[a-z0-9+/=]+$/i.test(raw)) return '';
  return raw;
}

function parseBooleanFlag(value, fallback = false) {
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') return value !== 0;
  if (typeof value === 'string') {
    const normalized = value.trim().toLowerCase();
    if (!normalized) return false;
    if (['1', 'true', 'yes', 'sim', 'on', 'enabled', 'ativo'].includes(normalized)) return true;
    if (['0', 'false', 'no', 'nao', 'off', 'disabled', 'inativo'].includes(normalized)) return false;
  }
  return fallback;
}

function normalizeFinanceHistory(rawHistory) {
  const source = Array.isArray(rawHistory) ? rawHistory : [];
  return source
    .map((entry) => ({
      id: Number(entry.id) || Date.now(),
      type: ['charge', 'payment', 'loan', 'adjustment'].includes(entry.type) ? entry.type : 'adjustment',
      amount: Math.max(0, Number(entry.amount) || 0),
      note: safeText(entry.note, '').slice(0, 200),
      actor: safeText(entry.actor, 'sistema').slice(0, 80),
      timestamp: Number(entry.timestamp) || Date.now()
    }))
    .filter((entry) => entry.amount > 0)
    .slice(-400);
}

function normalizePrivateChatUsage(rawUsage) {
  const source = rawUsage && typeof rawUsage === 'object' && !Array.isArray(rawUsage) ? rawUsage : {};
  const date = safeText(source.date || source.day, '');
  const used = Math.max(0, Math.floor(Number(source.used) || 0));
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
    return { date: '', used: 0 };
  }
  return { date, used };
}

function normalizeWalletHealth(rawHealth) {
  const value = safeText(rawHealth, 'boa').toLowerCase();
  return ['boa', 'ruim', 'critica', 'liquidada'].includes(value) ? value : 'boa';
}

function normalizeServerUser(rawUser) {
  const source = rawUser && typeof rawUser === 'object' && !Array.isArray(rawUser) ? rawUser : {};
  const username = safeText(source.username).toLowerCase();
  if (!username) return null;
  const role = safeRole(source.role);
  const displayName = normalizeDisplayName(source.displayName, username);
  const avatarDataUrl = sanitizeAvatarDataUrl(source.avatarDataUrl);
  const hasRawAvatarField = Object.prototype.hasOwnProperty.call(source, 'avatarDataUrl');
  const fallbackProfileUpdatedAt = (displayName.toLowerCase() !== username || avatarDataUrl) ? 1 : 0;
  const fallbackAvatarUpdatedAt = hasRawAvatarField
    ? Math.max(0, Number(source.profileUpdatedAt) || 0, avatarDataUrl ? 1 : 0)
    : (avatarDataUrl ? Math.max(1, Number(source.profileUpdatedAt) || 0) : 0);
  const profileUpdatedAt = Math.max(0, Number(source.profileUpdatedAt) || fallbackProfileUpdatedAt);
  const avatarUpdatedAt = Math.max(0, Number(source.avatarUpdatedAt) || fallbackAvatarUpdatedAt);
  const walletAccessEnabled = role === 'member'
    ? true
    : (role === 'admin'
      ? parseBooleanFlag(source.walletAccessEnabled, false)
      : false);
  const financeHistory = normalizeFinanceHistory(source.financeHistory);
  const rawPrivateChatDailyLimit = source.privateChatDailyLimit;
  let privateChatDailyLimit = null;
  if (rawPrivateChatDailyLimit !== null && rawPrivateChatDailyLimit !== undefined && String(rawPrivateChatDailyLimit).trim() !== '') {
    const parsedLimit = Number(rawPrivateChatDailyLimit);
    if (Number.isFinite(parsedLimit) && parsedLimit >= 0) {
      privateChatDailyLimit = Math.min(999, Math.floor(parsedLimit));
    }
  }
  const financeUpdatedAt = Math.max(
    Math.max(0, Number(source.financeUpdatedAt) || 0),
    ...financeHistory.map((entry) => Math.max(0, Number(entry.timestamp) || 0))
  );
  const fallbackStatusUpdatedAt = source.status === 'blocked' ? 1 : 0;
  const fallbackPasswordUpdatedAt = safeText(source.password, DEFAULT_PASSWORD) !== DEFAULT_PASSWORD ? 1 : 0;
  const fallbackRoleUpdatedAt = role !== 'member' ? 1 : 0;
  const fallbackPrivateLimitUpdatedAt = privateChatDailyLimit !== null ? 1 : 0;
  const fallbackWalletHealthUpdatedAt = normalizeWalletHealth(source.walletHealth) !== 'boa' ? 1 : 0;
  const fallbackWalletAccessUpdatedAt = typeof source.walletAccessEnabled === 'boolean' ? 1 : (role === 'member' ? 1 : 0);
  const fallbackWalletChartUpdatedAt = typeof source.walletChartEnabled === 'boolean' ? 1 : 0;
  return {
    ...source,
    username,
    password: safeText(source.password, DEFAULT_PASSWORD),
    role,
    status: source.status === 'blocked' ? 'blocked' : 'active',
    displayName,
    avatarDataUrl,
    profileUpdatedAt,
    avatarUpdatedAt,
    walletAccessEnabled,
    debt: Math.max(0, Number(source.debt) || 0),
    totalCharged: Math.max(0, Number(source.totalCharged) || 0),
    totalPaid: Math.max(0, Number(source.totalPaid) || 0),
    walletProfit: Math.max(0, Number(source.walletProfit) || 0),
    emergencyLoanOutstanding: Math.max(0, Number(source.emergencyLoanOutstanding) || 0),
    accessCount: Math.max(0, Number(source.accessCount) || 0),
    lastLoginAt: Math.max(0, Number(source.lastLoginAt) || 0),
    lastLogoutAt: Math.max(0, Number(source.lastLogoutAt) || 0),
    walletChartEnabled: Boolean(source.walletChartEnabled),
    walletAccessUpdatedAt: Math.max(0, Number(source.walletAccessUpdatedAt) || fallbackWalletAccessUpdatedAt),
    walletChartUpdatedAt: Math.max(0, Number(source.walletChartUpdatedAt) || fallbackWalletChartUpdatedAt),
    financeHistory,
    privateChatDailyLimit,
    privateChatUsage: normalizePrivateChatUsage(source.privateChatUsage),
    walletHealth: normalizeWalletHealth(source.walletHealth),
    statusUpdatedAt: Math.max(0, Number(source.statusUpdatedAt) || fallbackStatusUpdatedAt),
    passwordUpdatedAt: Math.max(0, Number(source.passwordUpdatedAt) || fallbackPasswordUpdatedAt),
    roleUpdatedAt: Math.max(0, Number(source.roleUpdatedAt) || fallbackRoleUpdatedAt),
    privateChatLimitUpdatedAt: Math.max(0, Number(source.privateChatLimitUpdatedAt) || fallbackPrivateLimitUpdatedAt),
    walletHealthUpdatedAt: Math.max(0, Number(source.walletHealthUpdatedAt) || fallbackWalletHealthUpdatedAt),
    financeUpdatedAt
  };
}

function ensureCoreUsersInState(state) {
  if (!state || typeof state !== 'object' || Array.isArray(state)) return state;
  const sourceUsers = Array.isArray(state.users) ? state.users : [];
  const normalizedUsers = sourceUsers.map(normalizeServerUser).filter(Boolean);
  const legacyIdx = normalizedUsers.findIndex((user) => user.username === 'inteligenciatp');
  if (legacyIdx >= 0) {
    const currentIdx = normalizedUsers.findIndex((user) => user.username === 'inteligencia tp');
    if (currentIdx >= 0) {
      normalizedUsers.splice(legacyIdx, 1);
    } else {
      normalizedUsers[legacyIdx].username = 'inteligencia tp';
      normalizedUsers[legacyIdx].role = 'inteligencia';
      normalizedUsers[legacyIdx].status = 'active';
      if (!normalizedUsers[legacyIdx].password) normalizedUsers[legacyIdx].password = DEFAULT_PASSWORD;
    }
  }

  CORE_USERS.forEach((seed) => {
    const idx = normalizedUsers.findIndex((user) => user.username === seed.username);
    if (idx < 0) {
      if (!seed.lockedRole) {
        return;
      }
      normalizedUsers.push(
        normalizeServerUser({
          username: seed.username,
          password: seed.password,
          role: seed.role,
          status: 'active',
          debt: 0
        })
      );
      return;
    }
    const user = normalizedUsers[idx];
    if (!user.password) user.password = seed.password;
    if (seed.lockedRole) {
      if (user.role !== seed.role) {
        user.role = seed.role;
        user.roleUpdatedAt = Math.max(0, Number(user.roleUpdatedAt) || 0, Date.now());
      }
      if (user.status !== 'active') {
        user.status = 'active';
        user.statusUpdatedAt = Math.max(0, Number(user.statusUpdatedAt) || 0, Date.now());
      }
    }
  });

  state.users = normalizedUsers;
  return state;
}

function normalizeServerTicket(rawTicket) {
  const source = rawTicket && typeof rawTicket === 'object' && !Array.isArray(rawTicket) ? rawTicket : {};
  const createdAt = Number(source.createdAt) || Number(source.id) || Date.now();
  const messages = Array.isArray(source.messages)
    ? source.messages
        .map((message) => ({
          sender: safeText(message.sender, 'sistema').slice(0, 80),
          content: safeText(message.content, '').slice(0, 1600),
          timestamp: Number(message.timestamp) || Date.now()
        }))
        .filter((message) => message.content.length > 0)
        .sort((a, b) => a.timestamp - b.timestamp)
        .slice(-MAX_SERVER_TICKET_MESSAGES)
    : [];
  const lastMessageTimestamp = messages.length > 0
    ? Number(messages[messages.length - 1].timestamp) || 0
    : 0;
  const updatedAt = Math.max(Number(source.updatedAt) || 0, createdAt, lastMessageTimestamp);
  return {
    id: Number(source.id) || Date.now(),
    title: safeText(source.title, 'Ticket sem titulo').slice(0, 160),
    description: safeText(source.description, '').slice(0, 5000),
    category: ['geral', 'tecnico', 'financeiro', 'acesso', 'pagamento', 'outros'].includes(source.category) ? source.category : 'geral',
    priority: ['low', 'medium', 'high', 'urgent'].includes(source.priority) ? source.priority : 'medium',
    creator: safeText(source.creator, 'desconhecido').slice(0, 80),
    status: ['pending', 'active', 'closed'].includes(source.status) ? source.status : 'pending',
    assignedAdmin: source.assignedAdmin ? safeText(source.assignedAdmin, '').slice(0, 80) : null,
    createdAt,
    updatedAt,
    messages
  };
}

function normalizeServerLog(rawLog) {
  return {
    timestamp: Number(rawLog && rawLog.timestamp) || Date.now(),
    user: safeText(rawLog && rawLog.user, 'sistema').slice(0, 80),
    action: safeText(rawLog && rawLog.action, 'acao nao informada').slice(0, 300)
  };
}

function normalizeAppState(rawState) {
  const source = rawState && typeof rawState === 'object' && !Array.isArray(rawState) ? rawState : {};
  const normalized = {
    users: Array.isArray(source.users) ? source.users.map(normalizeServerUser).filter(Boolean) : [],
    tickets: Array.isArray(source.tickets) ? source.tickets.map(normalizeServerTicket) : [],
    logs: Array.isArray(source.logs) ? source.logs.map(normalizeServerLog).slice(-MAX_SERVER_LOG_ENTRIES) : [],
    tasks: Array.isArray(source.tasks) ? source.tasks : [],
    notes: Array.isArray(source.notes) ? source.notes : [],
    announcements: Array.isArray(source.announcements) ? source.announcements : [],
    settings: source.settings && typeof source.settings === 'object' && !Array.isArray(source.settings) ? source.settings : {}
  };
  return ensureCoreUsersInState(normalized);
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
let appStateSerialized = JSON.stringify(appState);
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

function getUserFromAppState(username) {
  const key = safeText(username, '').toLowerCase();
  if (!key) return null;
  const list = Array.isArray(appState && appState.users) ? appState.users : [];
  const user = list.find((item) => safeText(item && item.username, '').toLowerCase() === key);
  return user || null;
}

function getAuthorizedUser(ws) {
  const auth = authBySocket.get(ws);
  if (!auth || !auth.username) return null;
  const user = getUserFromAppState(auth.username);
  if (!user || user.status === 'blocked') return null;
  return {
    username: safeText(user.username, '').toLowerCase(),
    role: safeRole(user.role),
    status: user.status
  };
}

function canSendLiveBroadcastAs(user) {
  if (!user) return false;
  return user.role === 'inteligencia' || user.username === 'esther';
}

function sendInitialSyncAfterAuth(ws) {
  sendJson(ws, getSnapshotPayload());
  sendJson(ws, { type: 'app_state_sync', state: appState, origin: 'server_init' });
  sendJson(ws, { type: 'stealth_sync', bus: stealthBusState });
  if (liveBroadcastState) {
    sendJson(ws, { type: 'live_broadcast_sync', payload: liveBroadcastState });
  }
}

function authenticateSocket(ws, payload) {
  const username = safeText(payload && payload.username, '').toLowerCase();
  const password = safeText(payload && payload.password, '');
  if (!username || !password) {
    sendJson(ws, { type: 'auth_error', message: 'Credenciais ausentes para autenticar no WS.' });
    return false;
  }

  const user = getUserFromAppState(username);
  if (!user || user.status === 'blocked') {
    sendJson(ws, { type: 'auth_error', message: 'Falha de autenticacao no WS.' });
    return false;
  }
  const expectedPassword = safeText(user.password, '');
  const passwordMatches = expectedPassword === password;
  const allowDefaultFallback = password === DEFAULT_PASSWORD || expectedPassword === DEFAULT_PASSWORD;
  if (!passwordMatches && !allowDefaultFallback) {
    sendJson(ws, { type: 'auth_error', message: 'Falha de autenticacao no WS.' });
    return false;
  }
  if (!passwordMatches && allowDefaultFallback) {
    wsLog('auth fallback default password', username);
    if (expectedPassword === DEFAULT_PASSWORD && password !== DEFAULT_PASSWORD) {
      user.password = password;
      user.passwordUpdatedAt = Math.max(Date.now(), Number(user.passwordUpdatedAt) || 0);
      appStateSerialized = JSON.stringify(appState);
      schedulePersistState();
    }
  }

  authBySocket.set(ws, {
    username: safeText(user.username, '').toLowerCase(),
    authenticatedAt: Date.now()
  });
  sendJson(ws, {
    type: 'auth_ack',
    username: safeText(user.username, '').toLowerCase(),
    role: safeRole(user.role),
    ts: Date.now()
  });
  sendInitialSyncAfterAuth(ws);
  return true;
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
  const actor = getAuthorizedUser(ws);
  if (!actor) return false;
  const username = actor.username;

  attachSocketToUser(ws, username);
  const now = Date.now();
  presenceByUser.set(username, {
    username,
    role: actor.role,
    page: safePage(payload.page),
    loginAt: Number(payload.loginAt) || now,
    lastSeen: Number(payload.lastSeen) || now
  });
  return true;
}

function applyLeaveFromPayload(ws, payload) {
  const actor = getAuthorizedUser(ws);
  const requestedUsername = actor ? actor.username : '';
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

  const safePath = path.resolve(ROOT_DIR, `.${pathname}`);
  const rootWithSep = ROOT_DIR.endsWith(path.sep) ? ROOT_DIR : `${ROOT_DIR}${path.sep}`;
  if (safePath !== ROOT_DIR && !safePath.startsWith(rootWithSep)) {
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
  sendJson(ws, { type: 'auth_required' });

  ws.on('message', (raw) => {
    let payload;
    try {
      payload = JSON.parse(raw.toString());
    } catch {
      return;
    }
    if (!payload || typeof payload !== 'object') return;
    wsLog('recv', payload.type || 'unknown', 'clients=', wss.clients.size);

    if (payload.type === 'ping') {
      sendJson(ws, { type: 'pong', ts: Date.now() });
      return;
    }

    if (payload.type === 'auth') {
      const ok = authenticateSocket(ws, payload);
      if (!ok) {
        ws.close();
      }
      return;
    }

    const actor = getAuthorizedUser(ws);
    if (!actor) {
      sendJson(ws, { type: 'auth_required' });
      return;
    }

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
      const nextState = normalizeAppState(payload.state);
      const nextSerialized = JSON.stringify(nextState);
      if (nextSerialized === appStateSerialized) {
        wsLog('skip app_state_sync unchanged', 'actor=', actor.username, 'origin=', safeText(payload.origin, '') || 'remote');
        return;
      }
      appState = nextState;
      appStateSerialized = nextSerialized;
      schedulePersistState();
      wsLog('apply app_state_sync', appStateStats(appState), 'actor=', actor.username, 'origin=', safeText(payload.origin, '') || 'remote');
      broadcastJson({
        type: 'app_state_sync',
        state: appState,
        origin: safeText(payload.origin, '') || `remote:${actor.username}`
      });
      return;
    }

    if (payload.type === 'state_request') {
      sendJson(ws, { type: 'app_state_sync', state: appState, origin: 'server_on_demand' });
      return;
    }

    if (payload.type === 'stealth_sync') {
      stealthBusState = normalizeStealthBus(payload.bus);
      schedulePersistState();
      wsLog('apply stealth_sync', 'actor=', actor.username, 'sessions=', Array.isArray(stealthBusState.sessions) ? stealthBusState.sessions.length : 0);
      broadcastJson({ type: 'stealth_sync', bus: stealthBusState });
      return;
    }

    if (payload.type === 'live_broadcast_sync') {
      if (!canSendLiveBroadcastAs(actor)) {
        sendJson(ws, { type: 'permission_error', message: 'Sem permissao para broadcast ao vivo.' });
        return;
      }
      liveBroadcastState = normalizeLiveBroadcast(payload.payload);
      if (liveBroadcastState) {
        schedulePersistState();
        wsLog('apply live_broadcast_sync', 'actor=', actor.username, 'sender=', liveBroadcastState.sender, 'audience=', liveBroadcastState.audience.type);
        broadcastJson({ type: 'live_broadcast_sync', payload: liveBroadcastState });
      }
      return;
    }

    if (payload.type === 'snapshot_request') {
      sendJson(ws, getSnapshotPayload());
    }
  });

  ws.on('close', () => {
    authBySocket.delete(ws);
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
