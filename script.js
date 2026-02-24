
/*
 * TP Portal Pro
 * Single-page portal with tickets, chat, admin tools and productivity modules.
 */

const STORAGE_KEYS = {
  users: 'tp_users',
  tickets: 'tp_tickets',
  logs: 'tp_logs',
  tasks: 'tp_tasks',
  notes: 'tp_notes',
  announcements: 'tp_announcements',
  settings: 'tp_settings',
  stealthBus: 'tp_stealth_bus',
  presence: 'tp_presence',
  liveBroadcast: 'tp_live_broadcast',
  ui: 'tp_ui'
};

const PAGE_TITLES = {
  dashboard: 'Painel geral',
  myTickets: 'Minhas solicitacoes',
  createTicket: 'Abrir novo ticket',
  pending: 'Fila pendente',
  allTickets: 'Gestao total de tickets',
  users: 'Gestao de usuarios',
  logs: 'Registros do sistema',
  tasks: 'Tarefas pessoais',
  notes: 'Bloco de notas',
  announcements: 'Comunicados',
  knowledge: 'Base de conhecimento',
  tools: 'Ferramentas rapidas',
  stealthChat: 'Chat irrastreavel',
  intelCenter: 'Central inteligencia',
  walletControl: 'Controle de carteiras',
  espionage: 'Espionagem',
  superTools: 'Comando superadmin',
  profile: 'Meu perfil'
};

const STATUS_LABELS = {
  pending: 'Pendente',
  active: 'Em andamento',
  closed: 'Encerrado'
};

const PRIORITY_LABELS = {
  low: 'Baixa',
  medium: 'Media',
  high: 'Alta',
  urgent: 'Urgente'
};

const CATEGORY_LABELS = {
  geral: 'Geral',
  tecnico: 'Tecnico',
  financeiro: 'Financeiro',
  acesso: 'Acesso',
  pagamento: 'Pagamento',
  outros: 'Outros'
};

const KNOWLEDGE_BASE = [
  {
    id: 'triagem',
    category: 'Processo',
    title: 'Como fazer triagem de tickets',
    content: 'Valide assunto, impacto, urgencia e dados obrigatorios. Se faltar contexto, retorne com perguntas objetivas antes de aceitar o ticket.'
  },
  {
    id: 'sla',
    category: 'Operacao',
    title: 'Politica de prioridade e SLA',
    content: 'Urgente: resposta inicial em ate 30 min. Alta: ate 2h. Media: ate 8h. Baixa: ate 24h. Sempre atualize o ticket quando houver mudanca de status.'
  },
  {
    id: 'comunicacao',
    category: 'Atendimento',
    title: 'Padrao de comunicacao com usuario',
    content: 'Escreva mensagens curtas, com linguagem simples, proximo passo claro e prazo estimado. Evite respostas vagas e tecnicas sem contexto.'
  },
  {
    id: 'seguranca',
    category: 'Seguranca',
    title: 'Boas praticas de senha',
    content: 'Use senhas com mais de 12 caracteres, misture letras, numeros e simbolos. Nunca reutilize senha entre sistemas e troque periodicamente.'
  },
  {
    id: 'auditoria',
    category: 'Governanca',
    title: 'Rastreabilidade e logs',
    content: 'Toda acao critica deve gerar registro: criacao, atribuicao, encerramento, bloqueio de usuario e ajustes financeiros.'
  },
  {
    id: 'checklist',
    category: 'Qualidade',
    title: 'Checklist antes de encerrar ticket',
    content: 'Confirmar solucao com usuario, documentar causa raiz, registrar data de encerramento e incluir orientacao para evitar recorrencia.'
  }
];

const TASK_FLOW = ['todo', 'doing', 'done'];
const FOCUS_DEFAULT_SECONDS = 25 * 60;
const DEFAULT_PASSWORD = '1705';
const PRESENCE_HEARTBEAT_MS = 12000;
const PRESENCE_ONLINE_WINDOW_MS = 28000;
const PRESENCE_SOCKET_PORT = 4317;
const PRESENCE_SOCKET_RECONNECT_MS = 2500;
const APP_STATE_SYNC_DEBOUNCE_MS = 180;
const LIVE_BROADCAST_EXPIRE_MS = 45000;
const ESPIONAGE_AUTO_REFRESH_MS = 5000;
const ESPIONAGE_STALE_PRESENCE_MS = 5 * 60 * 1000;
const DEFAULT_MEMBER_CHART_USERS = ['yoon', 'murilo', 'matheus', 'felps'];

const els = {
  loginContainer: document.getElementById('login-container'),
  loginButton: document.getElementById('login-button'),
  usernameInput: document.getElementById('username'),
  passwordInput: document.getElementById('password'),
  appContainer: document.getElementById('app-container'),
  currentUser: document.getElementById('current-user'),
  liveClock: document.getElementById('live-clock'),
  currentPageTitle: document.getElementById('current-page-title'),
  menuToggle: document.getElementById('menu-toggle'),
  sidebar: document.getElementById('sidebar'),
  content: document.getElementById('content'),
  logoutButton: document.getElementById('logout-button'),
  chatModal: document.getElementById('chat-modal'),
  chatOverlay: document.getElementById('chat-overlay'),
  chatTitle: document.getElementById('chat-title'),
  chatMessages: document.getElementById('chat-messages'),
  chatInput: document.getElementById('chat-input'),
  sendMessageButton: document.getElementById('send-message'),
  closeChatButton: document.getElementById('close-chat')
};

let session = null;
let users = [];
let tickets = [];
let logs = [];
let tasks = [];
let notes = [];
let announcements = [];
let settings = {
  maintenanceMode: false,
  maintenanceMessage: 'Portal temporariamente em manutencao.',
  walletDashboardMonth: ''
};
let activeChatTicketId = null;
let currentPage = 'dashboard';
let editingNoteId = null;
let clockInterval = null;
let focusInterval = null;
let presenceInterval = null;
let focusSecondsLeft = FOCUS_DEFAULT_SECONDS;
let focusRunning = false;
let liveBroadcastTimer = null;
let espionageAutoRefreshInterval = null;
let presenceSocket = null;
let presenceSocketReconnectTimer = null;
let sharedPresenceMap = {};
let presenceSocketConnected = false;
let presenceSocketWarned = false;
let presenceSocketManualClose = false;
let appStateSyncTimer = null;
let remoteStateInitialized = false;
let remoteStateApplying = false;
const SYNC_CLIENT_ID = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
const WS_DEBUG_ENABLED = (() => {
  try {
    const params = new URLSearchParams(window.location.search || '');
    if (params.get('wsDebug') === '1') return true;
    return localStorage.getItem('tp_ws_debug') === '1';
  } catch {
    return false;
  }
})();
let espionageViewState = {
  search: '',
  status: 'all',
  role: 'all',
  page: 'all',
  liveTarget: 'all',
  liveRole: 'member',
  liveUser: ''
};

function wsDebugLog(...args) {
  if (!WS_DEBUG_ENABLED) return;
  console.log('[tp-ws]', ...args);
}

function safeParse(raw, fallback) {
  if (!raw) return fallback;
  try {
    const parsed = JSON.parse(raw);
    return parsed ?? fallback;
  } catch {
    return fallback;
  }
}

function createId() {
  return Date.now() + Math.floor(Math.random() * 1000);
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

function formatDateTime(value) {
  if (!value) return '-';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '-';
  return date.toLocaleString('pt-BR');
}

function formatDuration(ms) {
  const safeMs = Math.max(0, Number(ms) || 0);
  const totalSeconds = Math.floor(safeMs / 1000);
  const days = Math.floor(totalSeconds / 86400);
  const hours = Math.floor((totalSeconds % 86400) / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  const parts = [];
  if (days > 0) parts.push(`${days}d`);
  if (hours > 0 || days > 0) parts.push(`${hours}h`);
  if (minutes > 0 || hours > 0 || days > 0) parts.push(`${minutes}m`);
  parts.push(`${seconds}s`);
  return parts.join(' ');
}

function formatCurrency(value) {
  const numeric = Number(value) || 0;
  return numeric.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
}

function roleLabel(role) {
  if (role === 'superadmin') return 'Superadmin';
  if (role === 'admin') return 'Admin';
  if (role === 'financeiro') return 'Financeiro';
  if (role === 'inteligencia') return 'Inteligencia TP';
  return 'Membro';
}

function isAdminRole(role) {
  return role === 'admin' || role === 'superadmin';
}

function canViewLogs() {
  return Boolean(session && isAdminRole(session.role));
}

function canManageWallets() {
  return Boolean(session && isAdminRole(session.role));
}

function shouldDefaultWalletChartEnabled(username) {
  const normalized = String(username || '').trim().toLowerCase();
  return DEFAULT_MEMBER_CHART_USERS.includes(normalized);
}

function canUserHaveWalletChart(user) {
  return Boolean(
    user
      && user.role === 'member'
      && user.status === 'active'
      && user.walletChartEnabled === true
  );
}

function isEsther() {
  return Boolean(session && session.username.toLowerCase() === 'esther');
}

function canAccessEspionage() {
  return Boolean(session && (session.role === 'superadmin' || session.role === 'inteligencia'));
}

function canSeeEspionageMonitoring() {
  return Boolean(session && (session.role === 'superadmin' || session.role === 'inteligencia'));
}

function canSendEspionageLiveMessage() {
  return Boolean(session && (session.role === 'inteligencia' || isEsther()));
}

function stopEspionageAutoRefresh() {
  if (!espionageAutoRefreshInterval) return;
  clearInterval(espionageAutoRefreshInterval);
  espionageAutoRefreshInterval = null;
}

function startEspionageAutoRefresh() {
  if (espionageAutoRefreshInterval) return;
  espionageAutoRefreshInterval = setInterval(() => {
    if (currentPage !== 'espionage') {
      stopEspionageAutoRefresh();
      return;
    }
    renderEspionage();
  }, ESPIONAGE_AUTO_REFRESH_MS);
}

function isEspionageAutoRefreshEnabled() {
  return Boolean(espionageAutoRefreshInterval);
}

function normalizeBroadcastAudience(rawAudience) {
  const source = rawAudience && typeof rawAudience === 'object' && !Array.isArray(rawAudience) ? rawAudience : {};
  const type = ['all', 'role', 'user'].includes(source.type) ? source.type : 'all';
  const role = ['member', 'admin', 'superadmin', 'inteligencia', 'financeiro'].includes(source.role) ? source.role : 'member';
  const user = String(source.user || '').trim();
  return { type, role, user };
}

function getBroadcastAudienceLabel(audience) {
  const safeAudience = normalizeBroadcastAudience(audience);
  if (safeAudience.type === 'all') return 'todos online';
  if (safeAudience.type === 'role') return `${roleLabel(safeAudience.role)} online`;
  return safeAudience.user ? `${safeAudience.user} (online)` : 'usuario especifico';
}

function resolveBroadcastRecipients(audience) {
  const safeAudience = normalizeBroadcastAudience(audience);
  const onlineSet = new Set(getOnlinePresenceList().map((entry) => entry.username));
  const onlineUsers = users.filter((user) => user.status === 'active' && onlineSet.has(user.username));

  if (safeAudience.type === 'role') {
    return onlineUsers.filter((user) => user.role === safeAudience.role).map((user) => user.username);
  }
  if (safeAudience.type === 'user') {
    return onlineUsers
      .filter((user) => user.username.toLowerCase() === safeAudience.user.toLowerCase())
      .map((user) => user.username);
  }
  return onlineUsers.map((user) => user.username);
}

function shouldReceiveLiveBroadcast(payload) {
  if (!session || !payload) return false;
  const recipients = Array.isArray(payload.recipients)
    ? payload.recipients.map((item) => String(item || '').trim()).filter(Boolean)
    : [];
  if (recipients.length > 0) {
    return recipients.includes(session.username);
  }

  const audience = normalizeBroadcastAudience(payload.audience);
  if (audience.type === 'all') return true;

  const me = getCurrentUser();
  if (!me) return false;
  if (audience.type === 'role') return me.role === audience.role;
  if (audience.type === 'user') return me.username.toLowerCase() === audience.user.toLowerCase();
  return false;
}

function clearStalePresenceEntries(maxAgeMs = ESPIONAGE_STALE_PRESENCE_MS) {
  const now = Date.now();
  const map = readPresenceMap();
  let removed = 0;
  Object.keys(map).forEach((username) => {
    const entry = map[username];
    const lastSeen = Number(entry && entry.lastSeen) || 0;
    if (!lastSeen || now - lastSeen > maxAgeMs) {
      delete map[username];
      removed += 1;
    }
  });
  if (removed > 0) {
    writePresenceMap(map);
  }
  return removed;
}

function createStealthSessionWithUser(targetUsername) {
  if (!session || !canCreateStealthSession()) {
    return { ok: false, message: 'Sem permissao para abrir chat irrastreavel.' };
  }

  const target = getUserByUsername(targetUsername);
  if (!target) {
    return { ok: false, message: 'Usuario alvo nao encontrado.' };
  }
  if (target.status !== 'active') {
    return { ok: false, message: 'Usuario alvo esta bloqueado.' };
  }
  if (target.username === session.username) {
    return { ok: false, message: 'Selecione outro usuario.' };
  }
  if (getActiveStealthSessionForUser(target.username)) {
    return { ok: false, message: 'Usuario alvo ja esta em um chat irrastreavel.' };
  }

  const bus = readStealthBus();
  const newSession = normalizeStealthSession({
    id: createId(),
    createdBy: session.username,
    participants: [session.username, target.username],
    createdAt: Date.now(),
    updatedAt: Date.now(),
    messages: []
  });

  bus.sessions = bus.sessions.filter((chatSession) => !chatSession.participants.includes(session.username));
  bus.sessions.push(newSession);
  writeStealthBus(bus);

  return { ok: true, target: target.username };
}

function statusLabel(status) {
  return STATUS_LABELS[status] || status;
}

function priorityLabel(priority) {
  return PRIORITY_LABELS[priority] || priority;
}

function categoryLabel(category) {
  return CATEGORY_LABELS[category] || category;
}

function statusBadge(status) {
  const label = statusLabel(status);
  return `<span class="badge status-${escapeHtml(status)}">${escapeHtml(label)}</span>`;
}

function priorityBadge(priority) {
  const label = priorityLabel(priority);
  return `<span class="badge priority-${escapeHtml(priority)}">${escapeHtml(label)}</span>`;
}

function getCurrentUser() {
  if (!session) return null;
  return users.find((user) => user.username === session.username) || null;
}

function getPendingCount() {
  return tickets.filter((ticket) => ticket.status === 'pending').length;
}

function getActiveAdmins() {
  return users.filter((user) => (user.role === 'admin' || user.role === 'superadmin') && user.status === 'active');
}

function getScopedTickets() {
  if (!session) return [];
  if (session.role === 'superadmin') return [...tickets];
  if (session.role === 'admin') {
    return tickets.filter((ticket) => ticket.assignedAdmin === session.username || ticket.creator === session.username);
  }
  return tickets.filter((ticket) => ticket.creator === session.username);
}

function getUserTasks() {
  if (!session) return [];
  return tasks.filter((task) => task.owner === session.username);
}

function getUserNotes() {
  if (!session) return [];
  return notes.filter((note) => note.owner === session.username);
}

function toShortText(text, maxLength) {
  const clean = String(text || '').trim();
  if (clean.length <= maxLength) return clean;
  return `${clean.slice(0, maxLength - 3)}...`;
}

function initData() {
  const storedUsers = safeParse(localStorage.getItem(STORAGE_KEYS.users), null);
  const storedTickets = safeParse(localStorage.getItem(STORAGE_KEYS.tickets), []);
  const storedLogs = safeParse(localStorage.getItem(STORAGE_KEYS.logs), []);
  const storedTasks = safeParse(localStorage.getItem(STORAGE_KEYS.tasks), []);
  const storedNotes = safeParse(localStorage.getItem(STORAGE_KEYS.notes), []);
  const storedAnnouncements = safeParse(localStorage.getItem(STORAGE_KEYS.announcements), []);
  const storedSettings = safeParse(localStorage.getItem(STORAGE_KEYS.settings), {});

  if (Array.isArray(storedUsers) && storedUsers.length > 0) {
    users = storedUsers.map(normalizeUser);
  } else {
    users = [
      normalizeUser({ username: 'esther', password: DEFAULT_PASSWORD, role: 'superadmin', status: 'active', debt: 0 }),
      normalizeUser({ username: 'belle', password: DEFAULT_PASSWORD, role: 'admin', status: 'active', debt: 0 }),
      normalizeUser({ username: 'felps', password: DEFAULT_PASSWORD, role: 'member', status: 'active', debt: 0 }),
      normalizeUser({ username: 'inteligencia tp', password: DEFAULT_PASSWORD, role: 'inteligencia', status: 'active', debt: 0 })
    ];
  }

  ensureCoreUsers();

  tickets = Array.isArray(storedTickets) ? storedTickets.map(normalizeTicket) : [];
  logs = Array.isArray(storedLogs)
    ? storedLogs.map((log) => ({
        timestamp: Number(log.timestamp) || Date.now(),
        user: String(log.user || 'sistema'),
        action: String(log.action || 'acao nao informada')
      }))
    : [];
  tasks = Array.isArray(storedTasks) ? storedTasks.map(normalizeTask) : [];
  notes = Array.isArray(storedNotes) ? storedNotes.map(normalizeNote) : [];

  if (Array.isArray(storedAnnouncements) && storedAnnouncements.length > 0) {
    announcements = storedAnnouncements.map(normalizeAnnouncement);
  } else {
    announcements = [
      normalizeAnnouncement({
        id: createId(),
        title: 'Bem-vindo ao TP Portal Pro',
        content: 'Use o menu para abrir tickets, acompanhar tarefas e acessar ferramentas rapidas.',
        author: 'sistema',
        audience: 'all',
        createdAt: Date.now()
      })
    ];
  }

  settings = normalizeSettings(storedSettings);
  applyDefaultPasswordMigration();

  saveData();
}

function normalizeUser(user) {
  const normalizedUsername = String(user.username || '').trim() || `usuario-${createId()}`;
  const debt = Math.max(0, Number(user.debt) || 0);
  const totalPaid = Math.max(0, Number(user.totalPaid) || 0);
  const walletProfit = Math.max(0, Number(user.walletProfit) || 0);
  const rawTotalCharged = Number(user.totalCharged);
  const settledPaid = Math.max(0, totalPaid - walletProfit);
  const minCharged = debt + settledPaid;
  const totalCharged = Number.isFinite(rawTotalCharged) ? Math.max(rawTotalCharged, minCharged) : minCharged;
  const emergencyLoanOutstanding = Math.max(0, Number(user.emergencyLoanOutstanding) || 0);
  const accessCount = Math.max(0, Number(user.accessCount) || 0);
  const lastLoginAt = Math.max(0, Number(user.lastLoginAt) || 0);
  const lastLogoutAt = Math.max(0, Number(user.lastLogoutAt) || 0);
  const normalizedRole = ['member', 'admin', 'superadmin', 'inteligencia', 'financeiro'].includes(user.role) ? user.role : 'member';
  const walletChartEnabled = normalizedRole === 'member'
    ? (typeof user.walletChartEnabled === 'boolean' ? user.walletChartEnabled : shouldDefaultWalletChartEnabled(normalizedUsername))
    : false;
  const financeHistory = Array.isArray(user.financeHistory)
    ? user.financeHistory
        .map((entry) => ({
          id: Number(entry.id) || createId(),
          type: ['charge', 'payment', 'loan', 'adjustment'].includes(entry.type) ? entry.type : 'adjustment',
          amount: Math.max(0, Number(entry.amount) || 0),
          note: String(entry.note || ''),
          actor: String(entry.actor || 'sistema'),
          timestamp: Number(entry.timestamp) || Date.now()
        }))
        .filter((entry) => entry.amount > 0)
        .slice(-400)
    : [];

  return {
    username: normalizedUsername,
    password: String(user.password || DEFAULT_PASSWORD),
    role: normalizedRole,
    status: user.status === 'blocked' ? 'blocked' : 'active',
    debt,
    totalCharged,
    totalPaid,
    walletProfit,
    emergencyLoanOutstanding,
    accessCount,
    lastLoginAt,
    lastLogoutAt,
    walletChartEnabled,
    financeHistory
  };
}

function normalizeTicket(ticket) {
  const createdAt = Number(ticket.createdAt) || Number(ticket.id) || Date.now();
  const updatedAt = Number(ticket.updatedAt) || createdAt;
  const normalizedStatus = ['pending', 'active', 'closed'].includes(ticket.status) ? ticket.status : 'pending';
  const normalizedPriority = ['low', 'medium', 'high', 'urgent'].includes(ticket.priority) ? ticket.priority : 'medium';
  const normalizedCategory = Object.keys(CATEGORY_LABELS).includes(ticket.category) ? ticket.category : 'geral';

  const normalizedMessages = Array.isArray(ticket.messages)
    ? ticket.messages
        .map((message) => ({
          sender: String(message.sender || 'sistema'),
          content: String(message.content || ''),
          timestamp: Number(message.timestamp) || Date.now()
        }))
        .filter((message) => message.content.trim().length > 0)
    : [];

  return {
    id: Number(ticket.id) || createId(),
    title: String(ticket.title || 'Ticket sem titulo'),
    description: String(ticket.description || ''),
    category: normalizedCategory,
    priority: normalizedPriority,
    creator: String(ticket.creator || 'desconhecido'),
    status: normalizedStatus,
    assignedAdmin: ticket.assignedAdmin ? String(ticket.assignedAdmin) : null,
    createdAt,
    updatedAt,
    messages: normalizedMessages
  };
}

function normalizeTask(task) {
  return {
    id: Number(task.id) || createId(),
    owner: String(task.owner || ''),
    title: String(task.title || 'Tarefa sem titulo'),
    dueDate: task.dueDate ? String(task.dueDate) : '',
    priority: ['low', 'medium', 'high'].includes(task.priority) ? task.priority : 'medium',
    status: TASK_FLOW.includes(task.status) ? task.status : 'todo',
    createdAt: Number(task.createdAt) || Date.now()
  };
}

function normalizeNote(note) {
  return {
    id: Number(note.id) || createId(),
    owner: String(note.owner || ''),
    title: String(note.title || 'Nota'),
    content: String(note.content || ''),
    updatedAt: Number(note.updatedAt) || Date.now()
  };
}

function normalizeAnnouncement(item) {
  return {
    id: Number(item.id) || createId(),
    title: String(item.title || 'Comunicado'),
    content: String(item.content || ''),
    author: String(item.author || 'sistema'),
    audience: item.audience === 'team' ? 'team' : 'all',
    createdAt: Number(item.createdAt) || Date.now()
  };
}

function normalizeSettings(rawSettings) {
  const monthCandidate = String((rawSettings && rawSettings.walletDashboardMonth) || '').trim();
  const safeMonth = parseMonthKey(monthCandidate) ? monthCandidate : getCurrentMonthKey();
  return {
    maintenanceMode: Boolean(rawSettings && rawSettings.maintenanceMode),
    maintenanceMessage:
      String((rawSettings && rawSettings.maintenanceMessage) || '').trim() || 'Portal temporariamente em manutencao.',
    passwordSeed1705Done: Boolean(rawSettings && rawSettings.passwordSeed1705Done),
    walletDashboardMonth: safeMonth
  };
}

function ensureCoreUsers() {
  const legacyIndex = users.findIndex((user) => user.username.toLowerCase() === 'inteligenciatp');
  if (legacyIndex >= 0) {
    const hasCurrentName = users.some((user) => user.username.toLowerCase() === 'inteligencia tp');
    if (hasCurrentName) {
      users.splice(legacyIndex, 1);
    } else {
      users[legacyIndex].username = 'inteligencia tp';
      users[legacyIndex].role = 'inteligencia';
      users[legacyIndex].status = 'active';
      if (!users[legacyIndex].password) users[legacyIndex].password = DEFAULT_PASSWORD;
    }
  }

  const requiredUsers = [
    { username: 'esther', password: DEFAULT_PASSWORD, role: 'superadmin' },
    { username: 'belle', password: DEFAULT_PASSWORD, role: 'admin' },
    { username: 'felps', password: DEFAULT_PASSWORD, role: 'member' },
    { username: 'yoon', password: DEFAULT_PASSWORD, role: 'member' },
    { username: 'murilo', password: DEFAULT_PASSWORD, role: 'member' },
    { username: 'matheus', password: DEFAULT_PASSWORD, role: 'member' },
    { username: 'inteligencia tp', password: DEFAULT_PASSWORD, role: 'inteligencia' }
  ];

  requiredUsers.forEach((seed) => {
    const exists = users.some((user) => user.username.toLowerCase() === seed.username.toLowerCase());
    if (!exists) {
      users.push(
        normalizeUser({
          username: seed.username,
          password: seed.password,
          role: seed.role,
          status: 'active',
          debt: 0
        })
      );
    }
  });
}

function applyDefaultPasswordMigration() {
  if (settings.passwordSeed1705Done) return;

  users.forEach((user) => {
    user.password = DEFAULT_PASSWORD;
  });

  settings.passwordSeed1705Done = true;
}

function getPresenceSocketUrl() {
  const protocol = window.location.protocol === 'https:' ? 'wss' : 'ws';
  const explicit = (window.__TP_WS_URL || '').trim();
  if (explicit) return explicit;

  const hostname = window.location.hostname || 'localhost';
  const isLocalHost = hostname === 'localhost' || hostname === '127.0.0.1';
  const currentPort = Number(window.location.port || (window.location.protocol === 'https:' ? 443 : 80));
  if (!isLocalHost || currentPort === PRESENCE_SOCKET_PORT) {
    return `${protocol}://${window.location.host}/ws`;
  }
  return `${protocol}://${hostname}:${PRESENCE_SOCKET_PORT}/ws`;
}

function refreshPresenceDependentViews() {
  if (currentPage === 'intelCenter') {
    renderIntelCenter();
    return;
  }
  if (currentPage === 'logs') {
    renderLogs();
    return;
  }
  if (currentPage === 'espionage') {
    renderEspionage();
  }
}

function collectAppStateForSync() {
  return {
    users,
    tickets,
    logs,
    tasks,
    notes,
    announcements,
    settings
  };
}

function isAppStateEffectivelyEmpty(state) {
  if (!state || typeof state !== 'object' || Array.isArray(state)) return true;
  const usersLen = Array.isArray(state.users) ? state.users.length : 0;
  const ticketsLen = Array.isArray(state.tickets) ? state.tickets.length : 0;
  const logsLen = Array.isArray(state.logs) ? state.logs.length : 0;
  const tasksLen = Array.isArray(state.tasks) ? state.tasks.length : 0;
  const notesLen = Array.isArray(state.notes) ? state.notes.length : 0;
  const annLen = Array.isArray(state.announcements) ? state.announcements.length : 0;
  return usersLen + ticketsLen + logsLen + tasksLen + notesLen + annLen === 0;
}

function schedulePresenceSocketReconnect() {
  if (!session) return;
  if (presenceSocketReconnectTimer) return;
  presenceSocketReconnectTimer = setTimeout(() => {
    presenceSocketReconnectTimer = null;
    connectPresenceSocket();
  }, PRESENCE_SOCKET_RECONNECT_MS);
}

function scheduleAppStateSync() {
  if (remoteStateApplying) return;
  if (!presenceSocketConnected || !remoteStateInitialized) return;
  if (appStateSyncTimer) return;

  appStateSyncTimer = setTimeout(() => {
    appStateSyncTimer = null;
    const state = collectAppStateForSync();
    wsDebugLog('send app_state_sync', {
      users: Array.isArray(state.users) ? state.users.length : 0,
      tickets: Array.isArray(state.tickets) ? state.tickets.length : 0,
      logs: Array.isArray(state.logs) ? state.logs.length : 0,
      tasks: Array.isArray(state.tasks) ? state.tasks.length : 0
    });
    sendPresenceSocketMessage({
      type: 'app_state_sync',
      origin: SYNC_CLIENT_ID,
      state
    });
  }, APP_STATE_SYNC_DEBOUNCE_MS);
}

function persistLocalDataOnly() {
  localStorage.setItem(STORAGE_KEYS.users, JSON.stringify(users));
  localStorage.setItem(STORAGE_KEYS.tickets, JSON.stringify(tickets));
  localStorage.setItem(STORAGE_KEYS.logs, JSON.stringify(logs));
  localStorage.setItem(STORAGE_KEYS.tasks, JSON.stringify(tasks));
  localStorage.setItem(STORAGE_KEYS.notes, JSON.stringify(notes));
  localStorage.setItem(STORAGE_KEYS.announcements, JSON.stringify(announcements));
  localStorage.setItem(STORAGE_KEYS.settings, JSON.stringify(settings));
}

function applySyncedAppState(rawState, options = {}) {
  if (!rawState || typeof rawState !== 'object' || Array.isArray(rawState)) return false;
  remoteStateApplying = true;
  users = Array.isArray(rawState.users) ? rawState.users.map(normalizeUser) : users;
  tickets = Array.isArray(rawState.tickets) ? rawState.tickets.map(normalizeTicket) : tickets;
  logs = Array.isArray(rawState.logs)
    ? rawState.logs.map((log) => ({
        timestamp: Number(log.timestamp) || Date.now(),
        user: String(log.user || 'sistema'),
        action: String(log.action || 'acao nao informada')
      }))
    : logs;
  tasks = Array.isArray(rawState.tasks) ? rawState.tasks.map(normalizeTask) : tasks;
  notes = Array.isArray(rawState.notes) ? rawState.notes.map(normalizeNote) : notes;
  announcements = Array.isArray(rawState.announcements) ? rawState.announcements.map(normalizeAnnouncement) : announcements;
  settings = normalizeSettings(rawState.settings || settings);
  persistLocalDataOnly();
  remoteStateApplying = false;

  if (options.render && session) {
    renderSidebar();
    renderContent(currentPage);
  }
  return true;
}

function sendPresenceSocketMessage(payload) {
  if (!presenceSocket || presenceSocket.readyState !== WebSocket.OPEN) return;
  try {
    wsDebugLog('ws.send', payload.type || 'unknown');
    presenceSocket.send(JSON.stringify(payload));
  } catch {
    // ignore transport failures; reconnect flow handles it
  }
}

function connectPresenceSocket() {
  if (!session) return;
  if (presenceSocket && (presenceSocket.readyState === WebSocket.OPEN || presenceSocket.readyState === WebSocket.CONNECTING)) return;

  presenceSocketManualClose = false;
  remoteStateInitialized = false;
  const socketUrl = getPresenceSocketUrl();
  let socket;
  try {
    socket = new WebSocket(socketUrl);
  } catch {
    if (!presenceSocketWarned) {
      showNotification('Servidor de presenca offline. Online global desativado temporariamente.', 'warning');
      presenceSocketWarned = true;
    }
    schedulePresenceSocketReconnect();
    return;
  }

  presenceSocket = socket;
  wsDebugLog('ws.connect', socketUrl);

  socket.addEventListener('open', () => {
    if (presenceSocket !== socket || !session) return;
    presenceSocketConnected = true;
    presenceSocketWarned = false;
    wsDebugLog('ws.open');
    sendPresenceSocketMessage({
      type: 'presence',
      username: session.username,
      role: session.role,
      page: currentPage || 'dashboard',
      loginAt: Number(session.loginAt) || Date.now(),
      lastSeen: Date.now()
    });
  });

  socket.addEventListener('message', (event) => {
    let payload = null;
    try {
      payload = JSON.parse(event.data);
    } catch {
      return;
    }
    if (!payload || typeof payload !== 'object') return;
    wsDebugLog('ws.message', payload.type || 'unknown', payload.origin || '');

    if (payload.type === 'snapshot' && payload.presence && typeof payload.presence === 'object' && !Array.isArray(payload.presence)) {
      sharedPresenceMap = payload.presence;
      writePresenceMap(sharedPresenceMap);
      refreshPresenceDependentViews();
      return;
    }

    if (payload.type === 'app_state_sync' && payload.state && typeof payload.state === 'object' && !Array.isArray(payload.state)) {
      const localBefore = collectAppStateForSync();
      const remoteLooksEmpty = isAppStateEffectivelyEmpty(payload.state);
      const localHasData = !isAppStateEffectivelyEmpty(localBefore);

      if (payload.origin && payload.origin === SYNC_CLIENT_ID) {
        remoteStateInitialized = true;
        wsDebugLog('app_state_sync ack (self)');
        return;
      }

      applySyncedAppState(payload.state, { render: true });
      remoteStateInitialized = true;
      wsDebugLog('app_state_sync applied', payload.origin || 'remote');

      if (remoteLooksEmpty && localHasData) {
        sendPresenceSocketMessage({
          type: 'app_state_sync',
          origin: SYNC_CLIENT_ID,
          seed: true,
          state: localBefore
        });
        applySyncedAppState(localBefore, { render: true });
      }
      return;
    }

    if (payload.type === 'stealth_sync' && payload.bus && typeof payload.bus === 'object' && !Array.isArray(payload.bus)) {
      const safeBus = {
        sessions: Array.isArray(payload.bus.sessions) ? payload.bus.sessions.map(normalizeStealthSession) : []
      };
      localStorage.setItem(STORAGE_KEYS.stealthBus, JSON.stringify(safeBus));
      syncStealthFromStorage();
      return;
    }

    if (payload.type === 'live_broadcast_sync' && payload.payload && typeof payload.payload === 'object' && !Array.isArray(payload.payload)) {
      localStorage.setItem(STORAGE_KEYS.liveBroadcast, JSON.stringify(payload.payload));
      showLiveBroadcastBanner(readLiveBroadcast());
      if (currentPage === 'espionage') {
        renderEspionage();
      }
    }
  });

  socket.addEventListener('error', () => {
    wsDebugLog('ws.error');
    if (!presenceSocketWarned) {
      showNotification('Servidor de presenca offline. Online global desativado temporariamente.', 'warning');
      presenceSocketWarned = true;
    }
  });

  socket.addEventListener('close', () => {
    wsDebugLog('ws.close');
    const wasManualClose = presenceSocketManualClose;
    presenceSocketManualClose = false;
    if (presenceSocket === socket) {
      presenceSocket = null;
    }
    presenceSocketConnected = false;
    remoteStateInitialized = false;
    sharedPresenceMap = {};
    refreshPresenceDependentViews();
    if (!wasManualClose) {
      schedulePresenceSocketReconnect();
    }
  });
}

function disconnectPresenceSocket(sendLeave = true) {
  if (presenceSocketReconnectTimer) {
    clearTimeout(presenceSocketReconnectTimer);
    presenceSocketReconnectTimer = null;
  }
  if (appStateSyncTimer) {
    clearTimeout(appStateSyncTimer);
    appStateSyncTimer = null;
  }

  if (sendLeave && session && presenceSocketConnected) {
    sendPresenceSocketMessage({
      type: 'leave',
      username: session.username
    });
  }

  if (presenceSocket) {
    presenceSocketManualClose = true;
    try {
      presenceSocket.close();
    } catch {
      // ignore close failures
    }
  }
  presenceSocket = null;
  presenceSocketConnected = false;
  remoteStateInitialized = false;
  sharedPresenceMap = {};
}

function readPresenceMap() {
  const payload = safeParse(localStorage.getItem(STORAGE_KEYS.presence), {});
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return {};
  return payload;
}

function writePresenceMap(presenceMap) {
  localStorage.setItem(STORAGE_KEYS.presence, JSON.stringify(presenceMap));
}

function getPresenceMapForOnline() {
  if (presenceSocketConnected && sharedPresenceMap && typeof sharedPresenceMap === 'object' && !Array.isArray(sharedPresenceMap)) {
    return sharedPresenceMap;
  }
  return readPresenceMap();
}

function updatePresence(nextPage) {
  if (!session) return;
  const payload = {
    username: session.username,
    role: session.role,
    page: String(nextPage || currentPage || 'dashboard'),
    lastSeen: Date.now(),
    loginAt: Number(session.loginAt) || Date.now()
  };
  const map = readPresenceMap();
  map[session.username] = payload;
  writePresenceMap(map);
  sendPresenceSocketMessage({
    type: 'presence',
    ...payload
  });
}

function removePresence(usernameOverride = '') {
  const username = usernameOverride || (session ? session.username : '');
  if (!username) return;
  const map = readPresenceMap();
  delete map[username];
  writePresenceMap(map);
  sendPresenceSocketMessage({
    type: 'leave',
    username
  });
}

function startPresenceHeartbeat() {
  stopPresenceHeartbeat();
  connectPresenceSocket();
  updatePresence(currentPage);
  presenceInterval = setInterval(() => {
    updatePresence(currentPage);
  }, PRESENCE_HEARTBEAT_MS);
}

function stopPresenceHeartbeat() {
  if (presenceInterval) {
    clearInterval(presenceInterval);
    presenceInterval = null;
  }
  disconnectPresenceSocket(false);
}

function getOnlinePresenceList() {
  const now = Date.now();
  return Object.values(getPresenceMapForOnline())
    .map((entry) => ({
      username: String(entry.username || ''),
      role: String(entry.role || 'member'),
      page: String(entry.page || 'dashboard'),
      lastSeen: Number(entry.lastSeen) || 0,
      loginAt: Number(entry.loginAt) || Number(entry.lastSeen) || now
    }))
    .filter((entry) => entry.username && now - entry.lastSeen <= PRESENCE_ONLINE_WINDOW_MS)
    .sort((a, b) => b.lastSeen - a.lastSeen);
}

function getUserByUsername(username) {
  return users.find((item) => item.username === username) || null;
}

function registerUserAccess(user) {
  if (!user) return;
  user.accessCount = Math.max(0, Number(user.accessCount) || 0) + 1;
  user.lastLoginAt = Date.now();
}

function registerUserOffline(username) {
  const user = getUserByUsername(username);
  if (!user) return;
  user.lastLogoutAt = Date.now();
  saveData();
}

function readLiveBroadcast() {
  const payload = safeParse(localStorage.getItem(STORAGE_KEYS.liveBroadcast), null);
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return null;
  const message = String(payload.message || '').trim();
  if (!message) return null;
  const recipients = Array.isArray(payload.recipients)
    ? payload.recipients.map((item) => String(item || '').trim()).filter(Boolean).slice(0, 300)
    : [];
  return {
    id: Number(payload.id) || 0,
    sender: String(payload.sender || 'sistema'),
    anonymous: Boolean(payload.anonymous),
    level: payload.level === 'critical' ? 'critical' : 'normal',
    audience: normalizeBroadcastAudience(payload.audience),
    recipients,
    message: message.slice(0, 220),
    createdAt: Number(payload.createdAt) || 0
  };
}

function clearLiveBroadcastBanner() {
  if (liveBroadcastTimer) {
    clearTimeout(liveBroadcastTimer);
    liveBroadcastTimer = null;
  }
  const current = document.getElementById('live-broadcast-banner');
  if (!current) return;
  current.remove();
}

function showLiveBroadcastBanner(payload) {
  if (!session || !payload) return;
  const createdAt = Number(payload.createdAt) || Date.now();
  const age = Date.now() - createdAt;
  if (age > LIVE_BROADCAST_EXPIRE_MS) return;
  if (!shouldReceiveLiveBroadcast(payload)) {
    clearLiveBroadcastBanner();
    return;
  }

  const senderLabel = payload.anonymous
    ? 'Mensagem anonima'
    : `Mensagem de ${payload.sender || 'sistema'}`;
  const audienceLabel = getBroadcastAudienceLabel(payload.audience);
  const levelClass = payload.level === 'critical' ? 'critical' : 'normal';
  let banner = document.getElementById('live-broadcast-banner');
  if (!banner) {
    banner = document.createElement('div');
    banner.id = 'live-broadcast-banner';
    banner.className = 'live-broadcast';
    document.body.appendChild(banner);
  }

  banner.className = `live-broadcast ${levelClass}`;
  banner.classList.remove('show');
  banner.innerHTML = `
    <div class="live-broadcast-head">
      <strong>${escapeHtml(senderLabel)}</strong>
      <button type="button" class="live-broadcast-close" aria-label="Fechar">x</button>
    </div>
    <p>${escapeHtml(payload.message)}</p>
    <small>${formatDateTime(createdAt)} | Alvo: ${escapeHtml(audienceLabel)}</small>
  `;

  const closeButton = banner.querySelector('.live-broadcast-close');
  if (closeButton) {
    closeButton.addEventListener('click', () => {
      clearLiveBroadcastBanner();
    });
  }

  requestAnimationFrame(() => {
    banner.classList.add('show');
  });

  if (liveBroadcastTimer) {
    clearTimeout(liveBroadcastTimer);
  }
  const ttl = Math.max(5000, LIVE_BROADCAST_EXPIRE_MS - age);
  liveBroadcastTimer = setTimeout(() => {
    clearLiveBroadcastBanner();
  }, ttl);
}

function emitLiveBroadcast(message, options = {}) {
  if (!session) {
    return { ok: false, message: 'Sessao invalida.' };
  }
  const trimmed = String(message || '').trim().slice(0, 220);
  if (trimmed.length < 2) {
    return { ok: false, message: 'Mensagem muito curta.' };
  }

  const config = typeof options === 'boolean' ? { anonymous: options } : (options || {});
  const audience = normalizeBroadcastAudience({
    type: config.audienceType || (config.audience && config.audience.type) || 'all',
    role: config.audienceRole || (config.audience && config.audience.role) || 'member',
    user: config.audienceUser || (config.audience && config.audience.user) || ''
  });
  const recipients = resolveBroadcastRecipients(audience);
  if (recipients.length === 0) {
    return { ok: false, message: 'Nenhum usuario online corresponde ao alvo escolhido.' };
  }
  const level = config.level === 'critical' ? 'critical' : 'normal';

  const payload = {
    id: createId(),
    sender: session.username,
    anonymous: config.anonymous !== false,
    level,
    audience,
    recipients,
    message: trimmed,
    createdAt: Date.now()
  };

  localStorage.setItem(STORAGE_KEYS.liveBroadcast, JSON.stringify(payload));
  wsDebugLog('admin broadcast send', payload.level, payload.audience ? payload.audience.type : 'all');
  sendPresenceSocketMessage({
    type: 'live_broadcast_sync',
    payload
  });
  showLiveBroadcastBanner(payload);

  return {
    ok: true,
    payload,
    recipientCount: recipients.length,
    audienceLabel: getBroadcastAudienceLabel(audience)
  };
}

function toPositiveAmount(raw) {
  const amount = Number(String(raw).replace(',', '.'));
  if (!Number.isFinite(amount) || amount <= 0) return 0;
  return Number(amount.toFixed(2));
}

function toNonNegativeAmount(raw) {
  const amount = Number(String(raw).replace(',', '.'));
  if (!Number.isFinite(amount) || amount < 0) return null;
  return Number(amount.toFixed(2));
}

function registerFinanceEvent(user, type, amount, note = '', actor = null) {
  const safeAmount = toPositiveAmount(amount);
  if (!user || safeAmount <= 0) {
    return { ok: false, amount: 0, message: 'Valor invalido.' };
  }

  const entryActor = actor || (session ? session.username : 'sistema');
  const eventType = ['charge', 'payment', 'loan', 'adjustment'].includes(type) ? type : 'adjustment';
  let appliedAmount = safeAmount;

  if (eventType === 'charge') {
    user.debt += safeAmount;
    user.totalCharged += safeAmount;
  } else if (eventType === 'loan') {
    user.debt += safeAmount;
    user.totalCharged += safeAmount;
    user.emergencyLoanOutstanding = Math.min(50, user.emergencyLoanOutstanding + safeAmount);
  } else if (eventType === 'payment') {
    const appliedToDebt = Math.min(safeAmount, Math.max(0, user.debt));
    const profitAdded = Math.max(0, safeAmount - appliedToDebt);
    appliedAmount = safeAmount;
    user.debt = Math.max(0, user.debt - appliedToDebt);
    user.totalPaid += safeAmount;
    user.walletProfit = Math.max(0, (Number(user.walletProfit) || 0) + profitAdded);
    user.emergencyLoanOutstanding = Math.max(0, user.emergencyLoanOutstanding - appliedToDebt);

    const historyEntry = {
      id: createId(),
      type: eventType,
      amount: appliedAmount,
      note: String(note || ''),
      actor: entryActor,
      timestamp: Date.now()
    };

    user.financeHistory.push(historyEntry);
    if (user.financeHistory.length > 400) {
      user.financeHistory = user.financeHistory.slice(-400);
    }

    saveData();
    return {
      ok: true,
      amount: appliedAmount,
      appliedToDebt: Number(appliedToDebt.toFixed(2)),
      profitAdded: Number(profitAdded.toFixed(2)),
      message: profitAdded > 0 ? 'Pagamento com lucro registrado.' : 'Operacao registrada.'
    };
  } else {
    user.debt += safeAmount;
    user.totalCharged += safeAmount;
  }

  const historyEntry = {
    id: createId(),
    type: eventType,
    amount: appliedAmount,
    note: String(note || ''),
    actor: entryActor,
    timestamp: Date.now()
  };

  user.financeHistory.push(historyEntry);
  if (user.financeHistory.length > 400) {
    user.financeHistory = user.financeHistory.slice(-400);
  }

  saveData();
  return { ok: true, amount: appliedAmount, message: 'Operacao registrada.' };
}

function sumRecentPayments(user, days = 30) {
  if (!user || !Array.isArray(user.financeHistory)) return 0;
  const minTs = Date.now() - days * 24 * 60 * 60 * 1000;
  return user.financeHistory
    .filter((entry) => entry.type === 'payment' && entry.timestamp >= minTs)
    .reduce((sum, entry) => sum + entry.amount, 0);
}

function calculateDopamineStats(user) {
  if (!user) {
    return { xp: 0, level: 1, currentLevelXp: 0, nextLevelXp: 120, progressPercent: 0, badges: [] };
  }

  const userTasks = tasks.filter((task) => task.owner === user.username);
  const doneTasks = userTasks.filter((task) => task.status === 'done').length;
  const closedTickets = tickets.filter((ticket) => ticket.creator === user.username && ticket.status === 'closed').length;
  const paidCount = user.financeHistory.filter((entry) => entry.type === 'payment').length;
  const paidValue = user.totalPaid || 0;
  const recentMomentum = sumRecentPayments(user, 30);

  const xp = Math.round(doneTasks * 18 + closedTickets * 14 + paidCount * 10 + paidValue * 0.6 + recentMomentum * 0.8);
  const level = Math.max(1, Math.floor(xp / 120) + 1);
  const currentLevelFloor = (level - 1) * 120;
  const nextLevelXp = level * 120;
  const currentLevelXp = xp - currentLevelFloor;
  const progressPercent = Math.max(0, Math.min(100, (currentLevelXp / 120) * 100));

  const badges = [];
  if (doneTasks >= 5) badges.push('Executor');
  if (paidCount >= 3) badges.push('Compromisso em dia');
  if (recentMomentum >= 50) badges.push('Sprint financeiro');
  if (closedTickets >= 4) badges.push('Resolvedor');
  if (badges.length === 0) badges.push('Em evolucao');

  return { xp, level, currentLevelXp, nextLevelXp, progressPercent, badges };
}

function calculateTenYearPlan(totalValue, annualRatePercent = 0) {
  const principal = Math.max(0, Number(totalValue) || 0);
  const annualRate = Math.max(0, Number(annualRatePercent) || 0);
  const months = 120;
  if (principal <= 0) {
    return { monthly: 0, total: 0, interest: 0 };
  }

  const monthlyRate = annualRate / 100 / 12;
  let monthly = 0;
  if (monthlyRate === 0) {
    monthly = principal / months;
  } else {
    monthly = (principal * monthlyRate) / (1 - Math.pow(1 + monthlyRate, -months));
  }
  const total = monthly * months;
  const interest = Math.max(0, total - principal);

  return {
    monthly: Number(monthly.toFixed(2)),
    total: Number(total.toFixed(2)),
    interest: Number(interest.toFixed(2))
  };
}

function getMonthKeyFromTimestamp(timestamp) {
  const date = new Date(Number(timestamp) || Date.now());
  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`;
}

function getCurrentMonthKey() {
  return getMonthKeyFromTimestamp(Date.now());
}

function getMonthLabelFromKey(monthKey) {
  const [yearStr, monthStr] = String(monthKey).split('-');
  const year = Number(yearStr);
  const month = Number(monthStr);
  if (!Number.isFinite(year) || !Number.isFinite(month) || month < 1 || month > 12) {
    return String(monthKey || '-');
  }
  const date = new Date(year, month - 1, 1);
  return date.toLocaleDateString('pt-BR', { month: 'short', year: '2-digit' });
}

function parseMonthKey(monthKey) {
  const match = /^(\d{4})-(\d{2})$/.exec(String(monthKey || '').trim());
  if (!match) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  if (!Number.isFinite(year) || !Number.isFinite(month) || month < 1 || month > 12) return null;
  return { year, month };
}

function listRecentMonthKeys(count = 8, anchorMonthKey = getCurrentMonthKey()) {
  const safeCount = Math.max(1, Number(count) || 8);
  const parsedAnchor = parseMonthKey(anchorMonthKey) || parseMonthKey(getCurrentMonthKey());
  const anchorDate = new Date(parsedAnchor.year, parsedAnchor.month - 1, 1);
  const keys = [];
  for (let i = safeCount - 1; i >= 0; i -= 1) {
    const date = new Date(anchorDate.getFullYear(), anchorDate.getMonth() - i, 1);
    keys.push(`${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}`);
  }
  return keys;
}

function getUserFinanceMonthSummary(user, monthKey) {
  const history = Array.isArray(user && user.financeHistory) ? user.financeHistory : [];
  let charged = 0;
  let paid = 0;
  let loans = 0;
  let fines = 0;

  history.forEach((entry) => {
    if (getMonthKeyFromTimestamp(entry.timestamp) !== monthKey) return;
    const amount = Number(entry.amount) || 0;
    if (amount <= 0) return;

    if (entry.type === 'payment') {
      paid += amount;
      return;
    }

    if (entry.type === 'loan') {
      loans += amount;
    }

    if (entry.type === 'charge') {
      fines += amount;
    }

    if (entry.type === 'charge' || entry.type === 'loan' || entry.type === 'adjustment') {
      charged += amount;
    }
  });

  return {
    charged: Number(charged.toFixed(2)),
    paid: Number(paid.toFixed(2)),
    loans: Number(loans.toFixed(2)),
    fines: Number(fines.toFixed(2)),
    debt: Math.max(0, Number(user && user.debt) || 0),
    walletValue: Math.max(0, Number(user && user.totalPaid) || 0),
    profit: Math.max(0, Number(user && user.walletProfit) || 0)
  };
}

function getAggregateMonthFinanceSummary(monthKey, scopedUsers = users) {
  const total = {
    charged: 0,
    paid: 0,
    loans: 0,
    fines: 0,
    debt: 0,
    walletValue: 0,
    profit: 0
  };

  scopedUsers.forEach((user) => {
    const summary = getUserFinanceMonthSummary(user, monthKey);
    total.charged += summary.charged;
    total.paid += summary.paid;
    total.loans += summary.loans;
    total.fines += summary.fines;
    total.debt += summary.debt;
    total.walletValue += summary.walletValue;
    total.profit += summary.profit;
  });

  return {
    charged: Number(total.charged.toFixed(2)),
    paid: Number(total.paid.toFixed(2)),
    loans: Number(total.loans.toFixed(2)),
    fines: Number(total.fines.toFixed(2)),
    debt: Number(total.debt.toFixed(2)),
    walletValue: Number(total.walletValue.toFixed(2)),
    profit: Number(total.profit.toFixed(2))
  };
}

function buildAggregateFinanceSeries(monthsCount = 8, anchorMonthKey = getCurrentMonthKey(), scopedUsers = users) {
  const monthKeys = listRecentMonthKeys(monthsCount, anchorMonthKey);
  const labels = monthKeys.map(getMonthLabelFromKey);
  const debtSeries = new Array(monthKeys.length).fill(0);
  const paidSeries = new Array(monthKeys.length).fill(0);
  const chargeSeries = new Array(monthKeys.length).fill(0);

  scopedUsers.forEach((user) => {
    const userSeries = buildFinanceSeries(user, monthKeys.length, anchorMonthKey);
    userSeries.debtSeries.forEach((value, index) => {
      debtSeries[index] += Number(value) || 0;
    });
    userSeries.paidSeries.forEach((value, index) => {
      paidSeries[index] += Number(value) || 0;
    });
    userSeries.chargeSeries.forEach((value, index) => {
      chargeSeries[index] += Number(value) || 0;
    });
  });

  return {
    monthKeys,
    labels,
    debtSeries: debtSeries.map((value) => Number(value.toFixed(2))),
    paidSeries: paidSeries.map((value) => Number(value.toFixed(2))),
    chargeSeries: chargeSeries.map((value) => Number(value.toFixed(2)))
  };
}

function buildFinanceSeries(user, monthsCount = 8, anchorMonthKey = getCurrentMonthKey()) {
  const monthKeys = listRecentMonthKeys(monthsCount, anchorMonthKey);
  const months = monthKeys.map((key) => ({
    key,
    label: getMonthLabelFromKey(key),
    charges: 0,
    payments: 0
  }));

  const monthIndex = new Map(months.map((month, index) => [month.key, index]));
  const history = Array.isArray(user && user.financeHistory) ? user.financeHistory.slice().sort((a, b) => a.timestamp - b.timestamp) : [];
  const firstKey = months[0].key;

  let chargesBeforeWindow = 0;
  let paymentsBeforeWindow = 0;
  let chargesAll = 0;
  let paymentsAll = 0;

  history.forEach((entry) => {
    const key = getMonthKeyFromTimestamp(entry.timestamp);
    const amount = Number(entry.amount) || 0;
    if (entry.type === 'payment') {
      paymentsAll += amount;
    } else if (entry.type === 'charge' || entry.type === 'loan' || entry.type === 'adjustment') {
      chargesAll += amount;
    }

    if (monthIndex.has(key)) {
      const idx = monthIndex.get(key);
      if (entry.type === 'payment') {
        months[idx].payments += amount;
      } else if (entry.type === 'charge' || entry.type === 'loan' || entry.type === 'adjustment') {
        months[idx].charges += amount;
      }
      return;
    }

    if (key < firstKey) {
      if (entry.type === 'payment') paymentsBeforeWindow += amount;
      if (entry.type === 'charge' || entry.type === 'loan' || entry.type === 'adjustment') chargesBeforeWindow += amount;
    }
  });

  const debtNow = Math.max(0, Number(user && user.debt) || 0);
  const baselineBeforeHistory = Math.max(0, debtNow - (chargesAll - paymentsAll));
  let debtLevel = Math.max(0, baselineBeforeHistory + (chargesBeforeWindow - paymentsBeforeWindow));
  let paidCumulative = Math.max(0, paymentsBeforeWindow);
  let chargedCumulative = Math.max(0, chargesBeforeWindow);

  const debtSeries = [];
  const paidSeries = [];
  const chargeSeries = [];
  const labels = [];

  months.forEach((month) => {
    debtLevel = Math.max(0, debtLevel + month.charges - month.payments);
    paidCumulative += month.payments;
    chargedCumulative += month.charges;
    debtSeries.push(Number(debtLevel.toFixed(2)));
    paidSeries.push(Number(paidCumulative.toFixed(2)));
    chargeSeries.push(Number(chargedCumulative.toFixed(2)));
    labels.push(month.label);
  });

  return { labels, debtSeries, paidSeries, chargeSeries, monthKeys };
}

function drawFinanceSeriesCanvas(canvas, series, options = {}) {
  if (!canvas || !series) return;
  const ctx = canvas.getContext('2d');
  if (!ctx) return;

  const rect = canvas.getBoundingClientRect();
  const pixelRatio = window.devicePixelRatio || 1;
  const width = Math.max(240, Math.floor(rect.width || canvas.clientWidth || 320));
  const height = Math.max(180, Math.floor(rect.height || canvas.clientHeight || 220));
  canvas.width = Math.floor(width * pixelRatio);
  canvas.height = Math.floor(height * pixelRatio);
  ctx.setTransform(pixelRatio, 0, 0, pixelRatio, 0, 0);

  ctx.clearRect(0, 0, width, height);
  const bgGradient = ctx.createLinearGradient(0, 0, 0, height);
  bgGradient.addColorStop(0, 'rgba(10, 33, 46, 0.92)');
  bgGradient.addColorStop(1, 'rgba(7, 21, 30, 0.92)');
  ctx.fillStyle = bgGradient;
  ctx.fillRect(0, 0, width, height);

  const padding = { left: 44, right: 18, top: 18, bottom: 34 };
  const plotW = width - padding.left - padding.right;
  const plotH = height - padding.top - padding.bottom;
  if (plotW <= 40 || plotH <= 40) return;

  const labels = Array.isArray(series.labels) ? series.labels.slice() : [];
  const paidRaw = Array.isArray(series.paidSeries) ? series.paidSeries.slice() : [];
  const debtRaw = Array.isArray(series.debtSeries) ? series.debtSeries.slice() : [];
  const chargeRaw = Array.isArray(series.chargeSeries) ? series.chargeSeries.slice() : [];
  const secondaryKey = options.secondaryKey === 'charge' ? 'charge' : 'debt';
  const secondaryRaw = secondaryKey === 'charge' ? chargeRaw : debtRaw;
  const pointsCount = Math.max(1, labels.length, paidRaw.length, secondaryRaw.length);

  while (labels.length < pointsCount) labels.push(String(labels.length + 1));
  while (paidRaw.length < pointsCount) paidRaw.push(0);
  while (secondaryRaw.length < pointsCount) secondaryRaw.push(0);

  const progress = Math.max(0, Math.min(1, Number(options.progress ?? 1)));
  const paidSeries = paidRaw.map((value) => Number((Math.max(0, value) * progress).toFixed(2)));
  const secondarySeries = secondaryRaw.map((value) => Number((Math.max(0, value) * progress).toFixed(2)));

  const paidColor = options.paidColor || '#14d9a2';
  const paidFill = options.paidFill || 'rgba(20, 217, 162, 0.12)';
  const secondaryColor = options.debtColor || '#ff7b8b';
  const secondaryFill = options.debtFill || 'rgba(255, 123, 139, 0.12)';
  const paidLabel = options.paidLabel || 'Carteira';
  const secondaryLabel = options.debtLabel || (secondaryKey === 'charge' ? 'Cobrado' : 'Divida');
  const maxValue = Math.max(50, ...secondarySeries, ...paidSeries);

  ctx.strokeStyle = 'rgba(130, 188, 220, 0.18)';
  ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i += 1) {
    const y = padding.top + (plotH / 4) * i;
    ctx.beginPath();
    ctx.moveTo(padding.left, y);
    ctx.lineTo(width - padding.right, y);
    ctx.stroke();
  }

  function pointX(index) {
    if (pointsCount === 1) return padding.left + plotW / 2;
    return padding.left + (plotW / (pointsCount - 1)) * index;
  }

  function pointY(value) {
    return padding.top + plotH - (value / maxValue) * plotH;
  }

  function traceSmoothLine(values) {
    if (values.length === 0) return;
    if (values.length === 1) {
      ctx.moveTo(pointX(0), pointY(values[0]));
      ctx.lineTo(pointX(0), pointY(values[0]));
      return;
    }
    ctx.moveTo(pointX(0), pointY(values[0]));
    for (let i = 1; i < values.length; i += 1) {
      const prevX = pointX(i - 1);
      const prevY = pointY(values[i - 1]);
      const currX = pointX(i);
      const currY = pointY(values[i]);
      const midX = (prevX + currX) / 2;
      const midY = (prevY + currY) / 2;
      ctx.quadraticCurveTo(prevX, prevY, midX, midY);
      if (i === values.length - 1) {
        ctx.quadraticCurveTo(currX, currY, currX, currY);
      }
    }
  }

  function drawSmoothSeries(values, color, fillColor) {
    const bottomY = padding.top + plotH;

    ctx.beginPath();
    traceSmoothLine(values);
    ctx.strokeStyle = color;
    ctx.lineWidth = 2.5;
    ctx.stroke();

    ctx.beginPath();
    traceSmoothLine(values);
    ctx.lineTo(pointX(values.length - 1), bottomY);
    ctx.lineTo(pointX(0), bottomY);
    ctx.closePath();
    const fillGradient = ctx.createLinearGradient(0, padding.top, 0, bottomY);
    fillGradient.addColorStop(0, fillColor);
    fillGradient.addColorStop(1, 'rgba(6, 15, 23, 0)');
    ctx.fillStyle = fillGradient;
    ctx.fill();

    values.forEach((value, index) => {
      const x = pointX(index);
      const y = pointY(value);
      ctx.beginPath();
      ctx.arc(x, y, 2.8, 0, Math.PI * 2);
      ctx.fillStyle = color;
      ctx.fill();
      ctx.beginPath();
      ctx.arc(x, y, 5.5, 0, Math.PI * 2);
      ctx.fillStyle = `${color}22`;
      ctx.fill();
    });
  }

  drawSmoothSeries(secondarySeries, secondaryColor, secondaryFill);
  drawSmoothSeries(paidSeries, paidColor, paidFill);

  ctx.fillStyle = '#8cb2c8';
  ctx.font = '11px IBM Plex Sans';
  const labelStep = Math.max(1, Math.ceil(pointsCount / 6));
  labels.forEach((label, index) => {
    if (index % labelStep !== 0 && index !== labels.length - 1) return;
    const x = pointX(index) - 10;
    const y = height - 12;
    ctx.fillText(String(label), x, y);
  });

  ctx.fillStyle = paidColor;
  ctx.fillText(paidLabel, padding.left, 12);
  ctx.fillStyle = secondaryColor;
  ctx.fillText(secondaryLabel, padding.left + 88, 12);

  ctx.fillStyle = '#7ea5bb';
  ctx.font = '10px IBM Plex Sans';
  const topValue = formatCurrency(maxValue);
  ctx.fillText(topValue, 6, padding.top + 2);
}

function drawFinanceSeriesCanvasAnimated(canvas, series, options = {}) {
  if (!canvas || !series) return;
  const duration = Math.max(240, Number(options.durationMs) || 780);
  const start = performance.now();

  function frame(now) {
    const progress = Math.min(1, (now - start) / duration);
    drawFinanceSeriesCanvas(canvas, series, { ...options, progress });
    if (progress < 1) {
      requestAnimationFrame(frame);
    }
  }

  requestAnimationFrame(frame);
}

function drawWalletCanvas(canvas, user, optionsOrMonth = getCurrentMonthKey()) {
  if (!canvas || !user) return;
  const options = typeof optionsOrMonth === 'object'
    ? optionsOrMonth
    : { anchorMonthKey: optionsOrMonth };
  const anchorMonthKey = options.anchorMonthKey || getCurrentMonthKey();
  const series = buildFinanceSeries(user, 8, anchorMonthKey);
  const chartOptions = {
    secondaryKey: 'charge',
    paidLabel: options.paidLabel || 'Carteira',
    debtLabel: options.debtLabel || 'Cobrado',
    paidColor: options.paidColor || '#26e2b1',
    debtColor: options.debtColor || '#6cb4ff',
    paidFill: options.paidFill || 'rgba(38, 226, 177, 0.18)',
    debtFill: options.debtFill || 'rgba(108, 180, 255, 0.16)'
  };
  if (options.animate === false) {
    drawFinanceSeriesCanvas(canvas, series, chartOptions);
    return;
  }
  drawFinanceSeriesCanvasAnimated(canvas, series, chartOptions);
}

function renderSeriesDetails(outputEl, series, selectedIndex = -1, options = {}) {
  if (!outputEl || !series) return;
  const labels = Array.isArray(series.labels) ? series.labels : [];
  const paidValues = Array.isArray(series.paidSeries) ? series.paidSeries : [];
  const chargeValues = Array.isArray(series.chargeSeries) ? series.chargeSeries : [];
  const debtValues = Array.isArray(series.debtSeries) ? series.debtSeries : [];
  const monthKeys = Array.isArray(series.monthKeys) ? series.monthKeys : [];
  const secondaryKey = options.secondaryKey === 'debt' ? 'debt' : 'charge';
  const secondaryLabel = secondaryKey === 'debt' ? 'Divida' : 'Cobrado';
  const secondaryValues = secondaryKey === 'debt' ? debtValues : chargeValues;
  const safeIndex = Math.max(0, Math.min(labels.length - 1, selectedIndex >= 0 ? selectedIndex : labels.length - 1));

  if (labels.length === 0) {
    outputEl.innerHTML = '<div class="empty-state">Sem dados para detalhar.</div>';
    return;
  }

  const rows = labels
    .map((label, index) => {
      const paid = Number(paidValues[index] || 0);
      const secondary = Number(secondaryValues[index] || 0);
      const debt = Number(debtValues[index] || 0);
      const prevPaid = index > 0 ? Number(paidValues[index - 1] || 0) : 0;
      const prevSecondary = index > 0 ? Number(secondaryValues[index - 1] || 0) : 0;
      const paidDelta = paid - prevPaid;
      const secondaryDelta = secondary - prevSecondary;
      const deltaLabelA = `${paidDelta >= 0 ? '+' : ''}${formatCurrency(paidDelta)}`;
      const deltaLabelB = `${secondaryDelta >= 0 ? '+' : ''}${formatCurrency(secondaryDelta)}`;
      const monthFull = monthKeys[index] ? getMonthLabelFromKey(monthKeys[index]) : label;
      return `
        <tr class="${index === safeIndex ? 'active' : ''}">
          <td>${escapeHtml(monthFull)}</td>
          <td>${formatCurrency(paid)} <small>${escapeHtml(deltaLabelA)}</small></td>
          <td>${formatCurrency(secondary)} <small>${escapeHtml(deltaLabelB)}</small></td>
          <td>${formatCurrency(debt)}</td>
        </tr>
      `;
    })
    .join('');

  const selectedLabel = monthKeys[safeIndex] ? getMonthLabelFromKey(monthKeys[safeIndex]) : labels[safeIndex];
  outputEl.innerHTML = `
    <div class="chart-details-head">
      <strong>Detalhe selecionado: ${escapeHtml(selectedLabel)}</strong>
      <small>Clique no grafico para trocar o mes destacado</small>
    </div>
    <div class="table-wrap chart-details-table">
      <table>
        <thead>
          <tr>
            <th>Mes</th>
            <th>Carteira</th>
            <th>${escapeHtml(secondaryLabel)}</th>
            <th>Divida</th>
          </tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    </div>
  `;
}

function bindChartDetails(canvas, series, outputEl, options = {}) {
  if (!canvas || !series || !outputEl) return;
  renderSeriesDetails(outputEl, series, -1, options);

  canvas.addEventListener('click', (event) => {
    const labels = Array.isArray(series.labels) ? series.labels : [];
    if (labels.length === 0) return;
    const rect = canvas.getBoundingClientRect();
    const x = event.clientX - rect.left;
    const paddingLeft = 44;
    const paddingRight = 18;
    const plotW = Math.max(1, rect.width - paddingLeft - paddingRight);
    const relative = Math.max(0, Math.min(plotW, x - paddingLeft));
    const idx = labels.length === 1
      ? 0
      : Math.round(relative / (plotW / (labels.length - 1)));
    renderSeriesDetails(outputEl, series, idx, options);
  });
}

function saveData() {
  persistLocalDataOnly();
  scheduleAppStateSync();
}

function addLog(action) {
  logs.push({
    timestamp: Date.now(),
    user: session ? session.username : 'sistema',
    action: String(action || 'acao sem descricao')
  });
  saveData();
}

function showNotification(message, type = 'info') {
  const notification = document.createElement('div');
  notification.className = `notification ${escapeHtml(type)}`;
  notification.textContent = message;
  document.body.appendChild(notification);

  requestAnimationFrame(() => notification.classList.add('show'));

  setTimeout(() => {
    notification.classList.remove('show');
    setTimeout(() => notification.remove(), 240);
  }, 3400);
}

function handleLogin() {
  const username = els.usernameInput.value.trim().toLowerCase();
  const password = els.passwordInput.value;

  if (!username || !password) {
    showNotification('Preencha usuario e senha.', 'warning');
    return;
  }

  const user = users.find((item) => item.username.toLowerCase() === username && item.password === password);

  if (!user) {
    showNotification('Credenciais invalidas.', 'error');
    return;
  }

  if (user.status === 'blocked') {
    showNotification('Este usuario esta bloqueado.', 'error');
    return;
  }

  if (settings.maintenanceMode && user.role !== 'superadmin' && user.role !== 'inteligencia') {
    showNotification(settings.maintenanceMessage || 'Portal em manutencao.', 'warning');
    return;
  }

  registerUserAccess(user);
  saveData();

  session = { username: user.username, role: user.role, loginAt: Date.now() };
  localStorage.setItem(STORAGE_KEYS.ui, JSON.stringify({ lastUser: user.username }));

  els.currentUser.textContent = `${user.username} (${roleLabel(user.role)})`;
  els.loginContainer.classList.add('hidden');
  els.appContainer.classList.remove('hidden');

  addLog('Fez login no portal');
  renderSidebar();
  navigate('dashboard');
  startClock();
  startPresenceHeartbeat();
  syncStealthFromStorage();
  showLiveBroadcastBanner(readLiveBroadcast());

  showNotification(`Bem-vindo, ${user.username}.`, 'success');
}

function handleLogout() {
  const logoutUsername = session ? session.username : '';
  if (session) {
    addLog('Fez logout do portal');
    registerUserOffline(session.username);
  }

  session = null;
  currentPage = 'dashboard';
  activeChatTicketId = null;
  destroyStealthChat(false);
  closeChat();

  stopClock();
  removePresence(logoutUsername);
  stopPresenceHeartbeat();
  stopEspionageAutoRefresh();
  clearLiveBroadcastBanner();

  els.loginContainer.classList.remove('hidden');
  els.appContainer.classList.add('hidden');
  els.appContainer.classList.remove('sidebar-open');
  els.passwordInput.value = '';
  els.content.innerHTML = '';
  els.sidebar.innerHTML = '';
}

function startClock() {
  stopClock();
  updateClock();
  clockInterval = setInterval(updateClock, 1000);
}

function stopClock() {
  if (clockInterval) {
    clearInterval(clockInterval);
    clockInterval = null;
  }
}

function updateClock() {
  if (!els.liveClock) return;
  const now = new Date();
  els.liveClock.textContent = now.toLocaleString('pt-BR');
}
function getNavigationLinks() {
  const links = [
    { id: 'dashboard', label: 'Painel' },
    { id: 'myTickets', label: 'Solicitacoes' },
    { id: 'createTicket', label: 'Abrir ticket' }
  ];

  if (session && isAdminRole(session.role)) {
    links.push({ id: 'pending', label: 'Pendentes', badge: getPendingCount() });
  }

  if (session && session.role === 'superadmin') {
    links.push({ id: 'allTickets', label: 'Todos os tickets' });
    links.push({ id: 'users', label: 'Usuarios' });
    links.push({ id: 'superTools', label: 'Comando master' });
  }

  if (session && canAccessEspionage()) {
    links.push({ id: 'espionage', label: 'Espionagem' });
  }

  if (canViewLogs()) {
    links.push({ id: 'logs', label: 'Registros' });
  }

  if (session && (session.role === 'superadmin' || session.role === 'inteligencia' || Boolean(getActiveStealthSessionForUser(session.username)))) {
    links.push({ id: 'stealthChat', label: 'Chat irrastreavel' });
  }

  if (session && (session.role === 'inteligencia' || session.role === 'superadmin')) {
    links.push({ id: 'intelCenter', label: 'Central intel' });
  }

  if (canManageWallets()) {
    links.push({ id: 'walletControl', label: 'Carteiras' });
  }

  links.push({ id: 'tasks', label: 'Tarefas' });
  links.push({ id: 'notes', label: 'Notas' });
  links.push({ id: 'announcements', label: 'Comunicados' });
  links.push({ id: 'knowledge', label: 'Conhecimento' });
  links.push({ id: 'tools', label: 'Ferramentas' });
  links.push({ id: 'profile', label: 'Perfil' });

  return links;
}

function renderSidebar() {
  els.sidebar.innerHTML = '';

  getNavigationLinks().forEach((link) => {
    const anchor = document.createElement('a');
    anchor.href = '#';
    anchor.dataset.page = link.id;
    const badgeHtml = typeof link.badge === 'number' && link.badge > 0
      ? `<span class="nav-pill">${link.badge}</span>`
      : '';
    anchor.innerHTML = `<span>${escapeHtml(link.label)}</span>${badgeHtml}`;

    anchor.addEventListener('click', (event) => {
      event.preventDefault();
      navigate(link.id);
      if (window.innerWidth <= 920) {
        els.appContainer.classList.remove('sidebar-open');
      }
    });

    els.sidebar.appendChild(anchor);
  });

  updateSidebarSelection();
}

function updateSidebarSelection() {
  const links = els.sidebar.querySelectorAll('a');
  links.forEach((link) => {
    if (link.dataset.page === currentPage) {
      link.classList.add('active');
    } else {
      link.classList.remove('active');
    }
  });
}

function navigate(page) {
  if (currentPage === 'stealthChat' && page !== 'stealthChat') {
    destroyStealthChat(false);
  }
  if (currentPage === 'espionage' && page !== 'espionage') {
    stopEspionageAutoRefresh();
  }

  const availablePages = new Set(getNavigationLinks().map((link) => link.id));
  if (!availablePages.has(page)) {
    page = 'dashboard';
  }

  currentPage = page;
  updatePresence(currentPage);
  els.currentPageTitle.textContent = PAGE_TITLES[page] || 'Portal';
  renderContent(page);
  renderSidebar();
}

function renderContent(page) {
  switch (page) {
    case 'dashboard':
      renderDashboard();
      return;
    case 'myTickets':
      renderMyTickets();
      return;
    case 'createTicket':
      renderCreateTicket();
      return;
    case 'pending':
      renderPendingTickets();
      return;
    case 'allTickets':
      renderAllTickets();
      return;
    case 'users':
      renderUsersManagement();
      return;
    case 'logs':
      renderLogs();
      return;
    case 'superTools':
      renderSuperTools();
      return;
    case 'tasks':
      renderTasks();
      return;
    case 'notes':
      renderNotes();
      return;
    case 'announcements':
      renderAnnouncements();
      return;
    case 'knowledge':
      renderKnowledge();
      return;
    case 'tools':
      renderTools();
      return;
    case 'stealthChat':
      renderStealthChat();
      return;
    case 'intelCenter':
      renderIntelCenter();
      return;
    case 'walletControl':
      renderWalletControl();
      return;
    case 'espionage':
      renderEspionage();
      return;
    case 'profile':
      renderProfile();
      return;
    default:
      renderDashboard();
  }
}

function renderDashboard() {
  const user = getCurrentUser();
  if (!user) {
    renderAccessDenied('Sessao invalida. Faca login novamente.');
    return;
  }

  const selectedMonthKey = getCurrentMonthKey();

  const scopedTickets = getScopedTickets();
  const pendingCount = scopedTickets.filter((ticket) => ticket.status === 'pending').length;
  const activeCount = scopedTickets.filter((ticket) => ticket.status === 'active').length;
  const closedCount = scopedTickets.filter((ticket) => ticket.status === 'closed').length;
  const userTasks = getUserTasks();
  const openTasks = userTasks.filter((task) => task.status !== 'done');
  const dueSoon = openTasks
    .filter((task) => task.dueDate)
    .sort((a, b) => a.dueDate.localeCompare(b.dueDate))
    .slice(0, 4);
  const recentAnnouncements = announcements
    .slice()
    .sort((a, b) => b.createdAt - a.createdAt)
    .slice(0, 3);

  const financeHistory = user && Array.isArray(user.financeHistory)
    ? user.financeHistory.slice().sort((a, b) => b.timestamp - a.timestamp).slice(0, 6)
    : [];
  const memberUsers = users.filter((item) => item.role === 'member');
  const aggregateSeries = buildAggregateFinanceSeries(8, selectedMonthKey, memberUsers);
  const aggregateMonth = getAggregateMonthFinanceSummary(selectedMonthKey, memberUsers);
  const userMonthSummary = getUserFinanceMonthSummary(user, selectedMonthKey);
  const recentPaid = sumRecentPayments(user, 30);
  const dopamine = calculateDopamineStats(user);
  const initialPlan = calculateTenYearPlan(user ? user.debt : 0, 0);
  const initialGoal = Math.max(20, Number((userMonthSummary.charged || Math.max(1, user.debt / 12)).toFixed(2)));

  const highlightItems = [
    { label: 'Pago no mes atual', value: formatCurrency(userMonthSummary.paid) },
    { label: 'Multas/cobrancas no mes', value: formatCurrency(userMonthSummary.fines) },
    { label: 'Carteira acumulada', value: formatCurrency(user.totalPaid) },
    { label: 'Lucro acumulado', value: formatCurrency(user.walletProfit) },
    { label: 'Divida atual', value: formatCurrency(user.debt) }
  ];

  const dueSoonHtml = dueSoon.length > 0
    ? dueSoon.map((task) => `<div class="kpi-item"><strong>${escapeHtml(task.title)}</strong><span>${escapeHtml(priorityLabel(task.priority))} - entrega em ${escapeHtml(task.dueDate)}</span></div>`).join('')
    : '<div class="kpi-item"><strong>Sem tarefas com data definida</strong><span>Voce pode criar tarefas no modulo Tarefas.</span></div>';

  const announcementHtml = recentAnnouncements.length > 0
    ? recentAnnouncements.map((item) => `
        <div class="announcement">
          <div class="announcement-header">
            <strong>${escapeHtml(item.title)}</strong>
            <small>${formatDateTime(item.createdAt)}</small>
          </div>
          <p>${escapeHtml(toShortText(item.content, 180))}</p>
          <small>por ${escapeHtml(item.author)}</small>
        </div>
      `).join('')
    : '<div class="empty-state">Nenhum comunicado publicado.</div>';

  const financeHistoryHtml = financeHistory.length > 0
    ? financeHistory
        .map((entry) => {
          const typeLabel = entry.type === 'payment'
            ? 'Pagamento'
            : entry.type === 'charge'
                ? 'Cobranca'
                : 'Ajuste';
          return `
            <div class="finance-item">
              <strong>${escapeHtml(typeLabel)} ${formatCurrency(entry.amount)}</strong>
              <small>${formatDateTime(entry.timestamp)} por ${escapeHtml(entry.actor || 'sistema')}</small>
            </div>
          `;
        })
        .join('')
    : '<div class="empty-state">Sem movimentacoes financeiras ainda.</div>';

  const hasPersonalChart = canUserHaveWalletChart(user);
  const memberChartItems = users
    .filter((member) => canUserHaveWalletChart(member))
    .slice()
    .sort((a, b) => (b.totalPaid || 0) - (a.totalPaid || 0))
    .map((member, index) => {
      const summary = getUserFinanceMonthSummary(member, selectedMonthKey);
      return {
        member,
        summary,
        canvasId: `member-wallet-canvas-${index}`
      };
    });

  const memberCardsHtml = memberChartItems.length > 0
    ? memberChartItems
        .map((entry) => `
          <article class="member-wallet-card">
            <div class="member-wallet-head">
              <strong>${escapeHtml(entry.member.username)}</strong>
              <small>${escapeHtml(roleLabel(entry.member.role))}</small>
            </div>
            <canvas id="${entry.canvasId}" class="member-wallet-canvas"></canvas>
            <div class="member-wallet-meta">
              <span>Carteira: <strong>${formatCurrency(entry.summary.walletValue)}</strong></span>
              <span>Pago no mes: <strong>${formatCurrency(entry.summary.paid)}</strong></span>
              <span>Multas: <strong>${formatCurrency(entry.summary.fines)}</strong></span>
              <span>Lucro: <strong>${formatCurrency(entry.summary.profit)}</strong></span>
              <span>Divida atual: <strong>${formatCurrency(entry.summary.debt)}</strong></span>
            </div>
          </article>
        `)
        .join('')
    : '<div class="empty-state">Sem membros ativos para exibir.</div>';

  const personalChartHtml = hasPersonalChart
    ? '<canvas id="wallet-canvas" class="wallet-canvas"></canvas>'
    : '<div class="empty-state">Seu perfil nao possui grafico ativo. Somente membros autorizados pela Esther recebem grafico.</div>';
  const canShowAggregateChart = canManageWallets() || hasPersonalChart;
  const aggregateChartHtml = canShowAggregateChart
    ? '<canvas id="tp-total-canvas" class="wallet-canvas tp-total-canvas"></canvas>'
    : '<div class="empty-state">Graficos ficam disponiveis para membros liberados e equipe financeira.</div>';

  const memberSectionHtml = canManageWallets()
    ? `
      <section class="panel">
        <div class="panel-header">
          <div>
          <h3 class="panel-title">Carteira por membro</h3>
            <p class="panel-subtitle">Apenas membros com grafico liberado pela Esther aparecem aqui.</p>
          </div>
        </div>
        <div class="member-wallet-grid">
          ${memberCardsHtml}
        </div>
      </section>
    `
    : '';

  els.content.innerHTML = `
    <section class="panel wallet-panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">Carteira TP - total mensal</h2>
          <p class="panel-subtitle">Visao consolidada da TP no mes atual.</p>
        </div>
        <div class="inline-actions">
          <button id="dashboard-refresh" class="btn-ghost">Atualizar painel</button>
        </div>
      </div>

      ${aggregateChartHtml}
      <div class="wallet-summary-grid">
        <div class="kpi-item"><strong>Total pago no mes</strong><span>${formatCurrency(aggregateMonth.paid)}</span></div>
        <div class="kpi-item"><strong>Total multas/cobrancas</strong><span>${formatCurrency(aggregateMonth.fines)}</span></div>
        <div class="kpi-item"><strong>Divida aberta da TP</strong><span>${formatCurrency(aggregateMonth.debt)}</span></div>
        <div class="kpi-item"><strong>Lucro acumulado</strong><span>${formatCurrency(aggregateMonth.profit)}</span></div>
        <div class="kpi-item"><strong>Carteira acumulada TP</strong><span>${formatCurrency(aggregateMonth.walletValue)}</span></div>
      </div>
    </section>

    <section class="panel wallet-panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">Minha carteira - ${escapeHtml(session.username)}</h2>
          <p class="panel-subtitle">Painel pessoal organizado para pagar, simular e acompanhar evolucao.</p>
        </div>
      </div>

      <div class="wallet-grid">
        <div class="wallet-chart-card">
          ${personalChartHtml}
          <div class="wallet-metrics">
            <div class="kpi-item"><strong>Divida atual</strong><span>${formatCurrency(user.debt)}</span></div>
            <div class="kpi-item"><strong>Total cobrado</strong><span>${formatCurrency(user.totalCharged)}</span></div>
            <div class="kpi-item"><strong>Total pago</strong><span>${formatCurrency(user.totalPaid)}</span></div>
            <div class="kpi-item"><strong>Lucro acumulado</strong><span>${formatCurrency(user.walletProfit)}</span></div>
            <div class="kpi-item"><strong>Pago nos ultimos 30 dias</strong><span>${formatCurrency(recentPaid)}</span></div>
            <div class="kpi-item"><strong>Pago no mes atual</strong><span>${formatCurrency(userMonthSummary.paid)}</span></div>
            <div class="kpi-item"><strong>Multa/cobranca no mes</strong><span>${formatCurrency(userMonthSummary.fines)}</span></div>
          </div>
        </div>

        <div class="wallet-actions-card">
          <h3 class="panel-title">Acoes da carteira</h3>
          <form id="dashboard-payment-form" class="form-grid">
            <div>
              <label for="dashboard-payment-amount">Registrar pagamento</label>
              <input id="dashboard-payment-amount" type="number" min="1" step="0.01" placeholder="Ex: 25.00" />
            </div>
            <div>
              <label for="dashboard-payment-note">Observacao</label>
              <input id="dashboard-payment-note" type="text" maxlength="120" placeholder="PIX, dinheiro, transferencia..." />
            </div>
            <div class="full inline-actions">
              <button class="btn-primary" type="submit">Aplicar pagamento</button>
              <button id="dashboard-pay-all" class="btn-ghost" type="button" ${user.debt > 0 ? '' : 'disabled'}>Quitar tudo</button>
            </div>
          </form>

          <form id="dashboard-plan-form" class="form-grid" style="margin-top: 8px;">
            <div>
              <label for="dashboard-plan-value">Simular por mes (10 anos)</label>
              <input id="dashboard-plan-value" type="number" min="1" step="0.01" value="${Math.max(1, Number(user ? user.debt : 0).toFixed(2))}" />
            </div>
            <div>
              <label for="dashboard-plan-rate">Juros anual (%)</label>
              <input id="dashboard-plan-rate" type="number" min="0" max="40" step="0.01" value="0" />
            </div>
            <div class="full inline-actions">
              <button class="btn-ghost" type="submit">Calcular plano</button>
            </div>
          </form>

          <div id="dashboard-plan-output" class="tool-output">
            Em 10 anos: <strong>${formatCurrency(initialPlan.monthly)}/mes</strong> | Total ${formatCurrency(initialPlan.total)}
          </div>

          <form id="dashboard-goal-form" class="form-grid" style="margin-top: 8px;">
            <div>
              <label for="dashboard-goal-target">Meta pessoal no mes</label>
              <input id="dashboard-goal-target" type="number" min="1" step="0.01" value="${initialGoal.toFixed(2)}" />
            </div>
            <div class="inline-actions" style="align-items:end;">
              <button class="btn-secondary" type="submit">Ver progresso</button>
            </div>
          </form>
          <div id="dashboard-goal-output" class="tool-output">
            Pago no mes: <strong>${formatCurrency(userMonthSummary.paid)}</strong> | Meta inicial: ${formatCurrency(initialGoal)}
          </div>
        </div>
      </div>

      <div class="dopamine-card">
        <div class="dopamine-header">
          <h3 class="panel-title">Modo dopamina do membro</h3>
          <strong>Nivel ${dopamine.level} | XP ${dopamine.xp}</strong>
        </div>
        <div class="dopamine-progress-track">
          <span class="dopamine-progress-fill" style="width:${dopamine.progressPercent.toFixed(1)}%;"></span>
        </div>
        <div class="dopamine-row">
          <small>${dopamine.currentLevelXp.toFixed(0)} / 120 XP para o proximo nivel (${dopamine.nextLevelXp} XP)</small>
          <small>Badges: ${dopamine.badges.map((badge) => escapeHtml(badge)).join(', ')}</small>
        </div>
      </div>

      <div class="finance-history">
        <h3 class="panel-title">Ultimas movimentacoes financeiras</h3>
        ${financeHistoryHtml}
      </div>
    </section>

    ${memberSectionHtml}

    <section class="panel">
      <h3 class="panel-title">Sinais da minha carteira</h3>
      <p class="panel-subtitle">Resumo rapido para agir com prioridade financeira.</p>
      <div class="kpi-list">
        ${highlightItems.map((item) => `<div class="kpi-item"><strong>${escapeHtml(item.value)}</strong><span>${escapeHtml(item.label)}</span></div>`).join('')}
      </div>
    </section>

    <section class="panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">Resumo operacional</h2>
          <p class="panel-subtitle">Tickets continuam visiveis, mas com foco secundario no painel.</p>
        </div>
      </div>
      <div class="stats-grid">
        <div class="stat-card"><div class="stat-value">${scopedTickets.length}</div><div class="stat-label">Tickets totais</div></div>
        <div class="stat-card"><div class="stat-value">${pendingCount}</div><div class="stat-label">Pendentes</div></div>
        <div class="stat-card"><div class="stat-value">${activeCount}</div><div class="stat-label">Em andamento</div></div>
        <div class="stat-card"><div class="stat-value">${closedCount}</div><div class="stat-label">Encerrados</div></div>
      </div>
    </section>

    <section class="split-grid">
      <div class="panel">
        <h3 class="panel-title">Proximas entregas</h3>
        <p class="panel-subtitle">Tarefas pessoais com data para nao perder prazo.</p>
        <div class="kpi-list">${dueSoonHtml}</div>
      </div>
    </section>

    <section class="panel">
      <h3 class="panel-title">Comunicados recentes</h3>
      <p class="panel-subtitle">Resumo das ultimas mensagens internas.</p>
      <div class="timeline">
        ${announcementHtml}
      </div>
    </section>
  `;

  const refreshButton = document.getElementById('dashboard-refresh');
  const paymentForm = document.getElementById('dashboard-payment-form');
  const payAllButton = document.getElementById('dashboard-pay-all');
  const planForm = document.getElementById('dashboard-plan-form');
  const goalForm = document.getElementById('dashboard-goal-form');
  const walletCanvas = document.getElementById('wallet-canvas');
  const totalCanvas = document.getElementById('tp-total-canvas');
  const planOutput = document.getElementById('dashboard-plan-output');
  const goalOutput = document.getElementById('dashboard-goal-output');

  if (canShowAggregateChart) {
    drawFinanceSeriesCanvasAnimated(totalCanvas, aggregateSeries, {
      secondaryKey: 'charge',
      paidLabel: 'Carteira TP',
      debtLabel: 'Cobrado TP',
      paidColor: '#26e2b1',
      debtColor: '#6cb4ff',
      paidFill: 'rgba(38, 226, 177, 0.18)',
      debtFill: 'rgba(108, 180, 255, 0.16)'
    });
  }
  if (hasPersonalChart) {
    drawWalletCanvas(walletCanvas, user, { anchorMonthKey: selectedMonthKey });
  }
  memberChartItems.forEach((entry) => {
    const canvas = document.getElementById(entry.canvasId);
    drawWalletCanvas(canvas, entry.member, { anchorMonthKey: selectedMonthKey });
  });

  refreshButton.addEventListener('click', () => {
    addLog('Atualizou o painel');
    renderDashboard();
  });

  paymentForm.addEventListener('submit', (event) => {
    event.preventDefault();
    const amountInput = document.getElementById('dashboard-payment-amount');
    const noteInput = document.getElementById('dashboard-payment-note');
    const amount = toPositiveAmount(amountInput.value);
    const note = noteInput.value.trim();

    if (!amount) {
      showNotification('Informe um valor de pagamento valido.', 'warning');
      return;
    }

    const result = registerFinanceEvent(user, 'payment', amount, note || 'Pagamento pelo painel');
    if (!result.ok) {
      showNotification(result.message, 'warning');
      return;
    }

    addLog(`Registrou pagamento proprio de ${formatCurrency(result.amount)} (lucro ${formatCurrency(result.profitAdded || 0)})`);
    const profitText = result.profitAdded > 0 ? ` | lucro ${formatCurrency(result.profitAdded)}` : '';
    showNotification(`Pagamento aplicado: ${formatCurrency(result.amount)}${profitText}.`, 'success');
    renderDashboard();
  });

  payAllButton.addEventListener('click', () => {
    if (user.debt <= 0) {
      showNotification('Nao ha divida para quitar.', 'warning');
      return;
    }
    const result = registerFinanceEvent(user, 'payment', user.debt, 'Quitacao total pelo painel');
    if (!result.ok) {
      showNotification(result.message, 'warning');
      return;
    }
    addLog(`Quitou divida total de ${formatCurrency(result.amount)} no painel`);
    showNotification('Divida quitada com sucesso.', 'success');
    renderDashboard();
  });

  planForm.addEventListener('submit', (event) => {
    event.preventDefault();
    const value = toPositiveAmount(document.getElementById('dashboard-plan-value').value);
    const rate = Number(document.getElementById('dashboard-plan-rate').value) || 0;
    if (!value) {
      showNotification('Informe um valor para simular.', 'warning');
      return;
    }

    const plan = calculateTenYearPlan(value, rate);
    planOutput.innerHTML = `Em 10 anos: <strong>${formatCurrency(plan.monthly)}/mes</strong> | Total ${formatCurrency(plan.total)} | Juros ${formatCurrency(plan.interest)}`;
  });

  goalForm.addEventListener('submit', (event) => {
    event.preventDefault();
    const target = toPositiveAmount(document.getElementById('dashboard-goal-target').value);
    if (!target) {
      showNotification('Informe uma meta valida.', 'warning');
      return;
    }
    const progressPercent = Math.min(999, (userMonthSummary.paid / target) * 100);
    const remaining = Math.max(0, target - userMonthSummary.paid);
    goalOutput.innerHTML = `
      Pago no mes: <strong>${formatCurrency(userMonthSummary.paid)}</strong> |
      Meta: <strong>${formatCurrency(target)}</strong> |
      Falta: <strong>${formatCurrency(remaining)}</strong> |
      Progresso: <strong>${progressPercent.toFixed(1)}%</strong>
    `;
  });
}

function renderWalletControl() {
  if (!canManageWallets()) {
    renderAccessDenied('Apenas admins podem controlar carteiras.');
    return;
  }

  const selectedMonthKey = getCurrentMonthKey();

  const members = users
    .filter((user) => user.role === 'member')
    .slice()
    .sort((a, b) => a.username.localeCompare(b.username));
  const membersWithChart = members.filter((member) => canUserHaveWalletChart(member));
  const aggregateMonth = getAggregateMonthFinanceSummary(selectedMonthKey, members);
  const aggregateSeries = buildAggregateFinanceSeries(8, selectedMonthKey, members);

  const memberRowsHtml = members.length > 0
    ? members
        .map((member) => {
          const summary = getUserFinanceMonthSummary(member, selectedMonthKey);
          const canToggleChart = isEsther() && member.role === 'member';
          const chartLabel = member.walletChartEnabled ? 'Ativo' : 'Desligado';
          return `
            <tr data-user="${escapeHtml(member.username)}">
              <td>${escapeHtml(member.username)}</td>
              <td>${escapeHtml(chartLabel)}</td>
              <td>${formatCurrency(summary.paid)}</td>
              <td>${formatCurrency(summary.fines)}</td>
              <td><input type="number" min="0" step="0.01" data-field="debt" value="${Number(member.debt).toFixed(2)}" /></td>
              <td><input type="number" min="0" step="0.01" data-field="charged" value="${Number(member.totalCharged).toFixed(2)}" /></td>
              <td><input type="number" min="0" step="0.01" data-field="paid" value="${Number(member.totalPaid).toFixed(2)}" /></td>
              <td><input type="number" min="0" step="0.01" data-field="profit" value="${Number(member.walletProfit).toFixed(2)}" /></td>
              <td>
                <div class="inline-actions">
                  <button class="btn-primary" data-action="save-wallet">Salvar</button>
                  <button class="btn-secondary" data-action="charge">Multa</button>
                  <button class="btn-secondary" data-action="payment">Pagamento</button>
                  <button class="btn-ghost" data-action="reset-chart">Zerar grafico</button>
                  <button class="btn-ghost" data-action="toggle-chart" ${canToggleChart ? '' : 'disabled'}>Grafico</button>
                </div>
              </td>
            </tr>
          `;
        })
        .join('')
    : '<tr><td colspan="9" class="empty-state">Nenhum membro cadastrado.</td></tr>';

  const chartCardsHtml = membersWithChart.length > 0
    ? membersWithChart
        .map((member, index) => {
          const summary = getUserFinanceMonthSummary(member, selectedMonthKey);
          return `
            <article class="member-wallet-card">
              <div class="member-wallet-head">
                <strong>${escapeHtml(member.username)}</strong>
                <small>Membro</small>
              </div>
              <canvas id="wallet-control-chart-${index}" class="member-wallet-canvas"></canvas>
              <div class="member-wallet-meta">
                <span>Carteira: <strong>${formatCurrency(summary.walletValue)}</strong></span>
                <span>Pago atual: <strong>${formatCurrency(summary.paid)}</strong></span>
                <span>Cobrado atual: <strong>${formatCurrency(summary.charged)}</strong></span>
                <span>Lucro: <strong>${formatCurrency(summary.profit)}</strong></span>
                <span>Divida atual: <strong>${formatCurrency(summary.debt)}</strong></span>
              </div>
            </article>
          `;
        })
        .join('')
    : '<div class="empty-state">Nenhum membro com grafico ativo.</div>';

  els.content.innerHTML = `
    <section class="panel wallet-panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">Controle de dividas e carteira dos membros</h2>
          <p class="panel-subtitle">Defina dividas, multas e valores entrando na carteira. Tudo em tempo real.</p>
        </div>
        <div class="inline-actions">
          <button id="wallet-control-refresh" class="btn-ghost">Atualizar painel</button>
        </div>
      </div>

      <canvas id="wallet-control-total-canvas" class="wallet-canvas tp-total-canvas"></canvas>
      <div class="wallet-summary-grid">
        <div class="kpi-item"><strong>Total pago</strong><span>${formatCurrency(aggregateMonth.paid)}</span></div>
        <div class="kpi-item"><strong>Total cobrado</strong><span>${formatCurrency(aggregateMonth.charged)}</span></div>
        <div class="kpi-item"><strong>Multas/cobrancas</strong><span>${formatCurrency(aggregateMonth.fines)}</span></div>
        <div class="kpi-item"><strong>Divida total membros</strong><span>${formatCurrency(aggregateMonth.debt)}</span></div>
        <div class="kpi-item"><strong>Lucro total</strong><span>${formatCurrency(aggregateMonth.profit)}</span></div>
        <div class="kpi-item"><strong>Carteira acumulada</strong><span>${formatCurrency(aggregateMonth.walletValue)}</span></div>
      </div>

      <div class="form-grid" style="margin-top:10px;">
        <div>
          <label for="wallet-global-debt">Divida geral (rateio automatico)</label>
          <input id="wallet-global-debt" type="number" min="1" step="0.01" placeholder="Ex: 500.00" />
        </div>
        <div class="inline-actions" style="align-items:end;">
          <button id="wallet-global-debt-apply" class="btn-secondary">Dividir para todos</button>
          <button id="wallet-reset-all" class="btn-danger">Resetar tudo para 0</button>
        </div>
      </div>
    </section>

    <section class="panel">
      <div class="panel-header">
        <div>
          <h3 class="panel-title">Ajuste direto (individual)</h3>
          <p class="panel-subtitle">Somente admins podem salvar ajustes. Apenas Esther define quem possui grafico.</p>
        </div>
      </div>
      <div class="filters" style="grid-template-columns: 1fr; margin-top: 4px;">
        <input id="wallet-control-search" type="text" placeholder="Buscar membro para editar carteira..." />
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Membro</th>
              <th>Grafico</th>
              <th>Pago</th>
              <th>Multa</th>
              <th>Divida</th>
              <th>Total cobrado</th>
              <th>Total pago</th>
              <th>Lucro</th>
              <th>Acoes</th>
            </tr>
          </thead>
          <tbody id="wallet-control-rows">${memberRowsHtml}</tbody>
        </table>
      </div>
    </section>

    <section class="panel">
      <div class="panel-header">
        <div>
          <h3 class="panel-title">Graficos dos membros autorizados</h3>
          <p class="panel-subtitle">Graficos em tendencia de subida (carteira e cobrado), atualizados quando houver novos pagamentos.</p>
        </div>
      </div>
      <div class="member-wallet-grid">
        ${chartCardsHtml}
      </div>
    </section>
  `;

  const refreshButton = document.getElementById('wallet-control-refresh');
  const globalDebtInput = document.getElementById('wallet-global-debt');
  const globalDebtButton = document.getElementById('wallet-global-debt-apply');
  const resetAllButton = document.getElementById('wallet-reset-all');
  const searchInput = document.getElementById('wallet-control-search');
  const rowsElement = document.getElementById('wallet-control-rows');
  const totalCanvas = document.getElementById('wallet-control-total-canvas');

  drawFinanceSeriesCanvasAnimated(totalCanvas, aggregateSeries, {
    secondaryKey: 'charge',
    paidLabel: 'Carteira membros',
    debtLabel: 'Cobrado membros',
    paidColor: '#26e2b1',
    debtColor: '#6cb4ff',
    paidFill: 'rgba(38, 226, 177, 0.18)',
    debtFill: 'rgba(108, 180, 255, 0.16)'
  });

  membersWithChart.forEach((member, index) => {
    const canvas = document.getElementById(`wallet-control-chart-${index}`);
    drawWalletCanvas(canvas, member, { anchorMonthKey: selectedMonthKey });
  });

  refreshButton.addEventListener('click', () => {
    renderWalletControl();
  });

  globalDebtButton.addEventListener('click', () => {
    const amount = toPositiveAmount(globalDebtInput.value);
    if (!amount) {
      showNotification('Informe um valor valido para divida geral.', 'warning');
      return;
    }
    const targetMembers = members.filter((member) => member.status === 'active');
    if (targetMembers.length === 0) {
      showNotification('Nao ha membros ativos para ratear.', 'warning');
      return;
    }

    const baseShare = Number((amount / targetMembers.length).toFixed(2));
    let remaining = amount;
    targetMembers.forEach((member, index) => {
      const share = index === targetMembers.length - 1
        ? Number(remaining.toFixed(2))
        : baseShare;
      remaining -= share;
      if (share > 0) {
        registerFinanceEvent(member, 'charge', share, 'Divida geral rateada');
      }
    });

    addLog(`Aplicou divida geral ${formatCurrency(amount)} para ${targetMembers.length} membros (rateio)`);
    showNotification('Divida geral distribuida com sucesso.', 'success');
    renderWalletControl();
  });

  resetAllButton.addEventListener('click', () => {
    if (!confirm('Resetar tudo para 0? Isso limpa dividas, multas, carteira e historico de todos os membros.')) return;
    members.forEach((member) => {
      member.debt = 0;
      member.totalCharged = 0;
      member.totalPaid = 0;
      member.walletProfit = 0;
      member.emergencyLoanOutstanding = 0;
      member.financeHistory = [];
    });
    saveData();
    addLog('Resetou todas as carteiras de membros para 0 (novo ciclo)');
    showNotification('Reset geral concluido. Todas as carteiras foram zeradas.', 'success');
    renderWalletControl();
  });

  searchInput.addEventListener('input', () => {
    const query = searchInput.value.trim().toLowerCase();
    const rows = rowsElement.querySelectorAll('tr[data-user]');
    rows.forEach((row) => {
      const username = String(row.dataset.user || '').toLowerCase();
      row.style.display = !query || username.includes(query) ? '' : 'none';
    });
  });

  rowsElement.addEventListener('click', (event) => {
    const button = event.target.closest('button[data-action]');
    if (!button) return;
    const row = button.closest('tr[data-user]');
    if (!row) return;
    const username = row.dataset.user;
    const target = getUserByUsername(username);
    if (!target || target.role !== 'member') return;
    const action = button.dataset.action;

    if (action === 'toggle-chart') {
      if (!isEsther()) {
        showNotification('Somente Esther pode definir quem possui grafico.', 'error');
        return;
      }
      target.walletChartEnabled = !target.walletChartEnabled;
      saveData();
      addLog(`Esther ${target.walletChartEnabled ? 'ativou' : 'desativou'} grafico de ${target.username} pelo controle de carteiras`);
      showNotification(`Grafico de ${target.username} ${target.walletChartEnabled ? 'ativado' : 'desativado'}.`, 'success');
      renderWalletControl();
      return;
    }

    if (action === 'save-wallet') {
      const debt = toNonNegativeAmount(row.querySelector('input[data-field="debt"]').value);
      const charged = toNonNegativeAmount(row.querySelector('input[data-field="charged"]').value);
      const paid = toNonNegativeAmount(row.querySelector('input[data-field="paid"]').value);
      const profit = toNonNegativeAmount(row.querySelector('input[data-field="profit"]').value);

      if (debt === null || charged === null || paid === null || profit === null) {
        showNotification('Valores invalidos. Use apenas numeros maiores ou iguais a zero.', 'warning');
        return;
      }
      if (profit > paid) {
        showNotification('Lucro nao pode ser maior que total pago.', 'warning');
        return;
      }

      target.debt = debt;
      target.totalPaid = paid;
      target.walletProfit = profit;
      target.totalCharged = Math.max(charged, debt + Math.max(0, paid - profit));
      target.emergencyLoanOutstanding = 0;

      saveData();
      addLog(`Ajustou carteira de ${target.username} (divida ${formatCurrency(debt)}, cobrado ${formatCurrency(target.totalCharged)}, pago ${formatCurrency(paid)})`);
      showNotification(`Carteira de ${target.username} atualizada.`, 'success');
      renderWalletControl();
      return;
    }

    if (action === 'charge') {
      const raw = prompt(`Valor da multa/cobranca para ${target.username}:`, '30.00');
      if (!raw) return;
      const amount = toPositiveAmount(raw);
      if (!amount) {
        showNotification('Valor invalido.', 'warning');
        return;
      }
      const result = registerFinanceEvent(target, 'charge', amount, 'Multa/cobranca via controle de carteiras');
      if (!result.ok) {
        showNotification(result.message, 'warning');
        return;
      }
      addLog(`Cobrou ${formatCurrency(result.amount)} de ${target.username} pelo controle de carteiras`);
      showNotification('Cobranca registrada com sucesso.', 'success');
      renderWalletControl();
      return;
    }

    if (action === 'payment') {
      const raw = prompt(`Valor do pagamento para ${target.username}:`, '20.00');
      if (!raw) return;
      const amount = toPositiveAmount(raw);
      if (!amount) {
        showNotification('Valor invalido.', 'warning');
        return;
      }
      const result = registerFinanceEvent(target, 'payment', amount, 'Pagamento via controle de carteiras');
      if (!result.ok) {
        showNotification(result.message, 'warning');
        return;
      }
      addLog(`Registrou pagamento de ${formatCurrency(result.amount)} para ${target.username} no controle de carteiras (lucro ${formatCurrency(result.profitAdded || 0)})`);
      const profitText = result.profitAdded > 0 ? ` | lucro ${formatCurrency(result.profitAdded)}` : '';
      showNotification(`Pagamento registrado e grafico atualizado${profitText}.`, 'success');
      renderWalletControl();
      return;
    }

    if (action === 'reset-chart') {
      if (!confirm(`Zerar os valores do grafico de ${target.username}?`)) return;
      target.financeHistory = [];
      target.totalPaid = 0;
      target.walletProfit = 0;
      target.totalCharged = Math.max(0, target.debt);
      target.emergencyLoanOutstanding = 0;
      saveData();
      addLog(`Zerou grafico da carteira de ${target.username} no controle de carteiras`);
      showNotification('Valor do grafico zerado com sucesso.', 'success');
      renderWalletControl();
    }
  });
}

function buildTicketActionButtons(ticket, allowManagement = true) {
  const buttons = [];
  const isPrivileged = session.role === 'admin' || session.role === 'superadmin';
  const canClose = session.role === 'superadmin' || ticket.assignedAdmin === session.username;
  const canOpenChat = ticket.status !== 'pending' && canAccessTicket(ticket);

  if (ticket.status === 'pending' && isPrivileged && !ticket.assignedAdmin) {
    buttons.push(`<button class="btn-primary" data-action="accept" data-ticket="${ticket.id}">Assumir</button>`);
  }

  if (canOpenChat) {
    buttons.push(`<button class="btn-secondary" data-action="chat" data-ticket="${ticket.id}">Chat</button>`);
  }

  if (allowManagement && ticket.status === 'active' && canClose) {
    buttons.push(`<button class="btn-ghost" data-action="close" data-ticket="${ticket.id}">Encerrar</button>`);
  }

  if (allowManagement && ticket.status === 'closed' && isPrivileged) {
    buttons.push(`<button class="btn-ghost" data-action="reopen" data-ticket="${ticket.id}">Reabrir</button>`);
  }

  return buttons.length > 0 ? `<div class="inline-actions">${buttons.join('')}</div>` : '-';
}

function canAccessTicket(ticket) {
  if (!session) return false;
  if (session.role === 'superadmin') return true;
  return ticket.creator === session.username || ticket.assignedAdmin === session.username;
}

function renderMyTickets() {
  const scopedTickets = getScopedTickets().sort((a, b) => b.updatedAt - a.updatedAt);

  els.content.innerHTML = `
    <section class="panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">Minhas solicitacoes</h2>
          <p class="panel-subtitle">Filtre por status, prioridade e texto para localizar rapido.</p>
        </div>
      </div>
      <div class="filters">
        <input type="text" id="my-ticket-search" placeholder="Buscar por titulo, criador ou responsavel" />
        <select id="my-ticket-status">
          <option value="all">Todos os status</option>
          <option value="pending">Pendente</option>
          <option value="active">Em andamento</option>
          <option value="closed">Encerrado</option>
        </select>
        <select id="my-ticket-priority">
          <option value="all">Todas as prioridades</option>
          <option value="low">Baixa</option>
          <option value="medium">Media</option>
          <option value="high">Alta</option>
          <option value="urgent">Urgente</option>
        </select>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Assunto</th>
              <th>Categoria</th>
              <th>Prioridade</th>
              <th>Status</th>
              <th>Responsavel</th>
              <th>Atualizado</th>
              <th>Acao</th>
            </tr>
          </thead>
          <tbody id="my-ticket-rows"></tbody>
        </table>
      </div>
    </section>
  `;

  const rowsElement = document.getElementById('my-ticket-rows');
  const searchInput = document.getElementById('my-ticket-search');
  const statusSelect = document.getElementById('my-ticket-status');
  const prioritySelect = document.getElementById('my-ticket-priority');

  function drawRows() {
    const search = searchInput.value.trim().toLowerCase();
    const statusFilter = statusSelect.value;
    const priorityFilter = prioritySelect.value;

    const filtered = scopedTickets.filter((ticket) => {
      const matchesSearch = !search
        || ticket.title.toLowerCase().includes(search)
        || ticket.creator.toLowerCase().includes(search)
        || String(ticket.assignedAdmin || '').toLowerCase().includes(search);
      const matchesStatus = statusFilter === 'all' || ticket.status === statusFilter;
      const matchesPriority = priorityFilter === 'all' || ticket.priority === priorityFilter;
      return matchesSearch && matchesStatus && matchesPriority;
    });

    if (filtered.length === 0) {
      rowsElement.innerHTML = '<tr><td colspan="8" class="empty-state">Nenhum ticket encontrado para o filtro atual.</td></tr>';
      return;
    }

    rowsElement.innerHTML = filtered.map((ticket) => `
      <tr>
        <td>#${ticket.id}</td>
        <td>
          <strong>${escapeHtml(ticket.title)}</strong><br />
          <small>${escapeHtml(toShortText(ticket.description || 'Sem descricao.', 80))}</small>
        </td>
        <td>${escapeHtml(categoryLabel(ticket.category))}</td>
        <td>${priorityBadge(ticket.priority)}</td>
        <td>${statusBadge(ticket.status)}</td>
        <td>${escapeHtml(ticket.assignedAdmin || '-')}</td>
        <td>${formatDateTime(ticket.updatedAt)}</td>
        <td>${buildTicketActionButtons(ticket, true)}</td>
      </tr>
    `).join('');
  }

  searchInput.addEventListener('input', drawRows);
  statusSelect.addEventListener('change', drawRows);
  prioritySelect.addEventListener('change', drawRows);

  rowsElement.addEventListener('click', (event) => {
    const button = event.target.closest('button[data-action]');
    if (!button) return;

    const ticketId = Number(button.dataset.ticket);
    const action = button.dataset.action;

    if (action === 'chat') {
      openChat(ticketId);
      return;
    }

    if (action === 'accept') {
      acceptTicket(ticketId, session.username);
      drawRows();
      renderSidebar();
      return;
    }

    if (action === 'close') {
      closeTicket(ticketId);
      drawRows();
      return;
    }

    if (action === 'reopen') {
      reopenTicket(ticketId);
      drawRows();
      return;
    }
  });

  drawRows();
}

function renderCreateTicket() {
  els.content.innerHTML = `
    <section class="panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">Abrir novo ticket</h2>
          <p class="panel-subtitle">Preencha os campos para direcionar sua solicitacao com mais rapidez.</p>
        </div>
      </div>
      <form id="ticket-form" class="form-grid">
        <div class="full">
          <label for="ticket-title">Assunto</label>
          <input id="ticket-title" type="text" maxlength="120" placeholder="Ex: Falha ao acessar o painel financeiro" required />
        </div>

        <div>
          <label for="ticket-category">Categoria</label>
          <select id="ticket-category">
            <option value="geral">Geral</option>
            <option value="tecnico">Tecnico</option>
            <option value="financeiro">Financeiro</option>
            <option value="acesso">Acesso</option>
            <option value="pagamento">Pagamento</option>
            <option value="outros">Outros</option>
          </select>
        </div>

        <div>
          <label for="ticket-priority">Prioridade</label>
          <select id="ticket-priority">
            <option value="low">Baixa</option>
            <option value="medium" selected>Media</option>
            <option value="high">Alta</option>
            <option value="urgent">Urgente</option>
          </select>
        </div>

        <div class="full">
          <label for="ticket-description">Descricao</label>
          <textarea id="ticket-description" maxlength="1000" placeholder="Detalhe o problema, impacto e tentativas ja realizadas."></textarea>
        </div>

        <div class="full inline-actions">
          <button type="submit" class="btn-primary">Criar ticket</button>
          <button type="button" id="ticket-clear" class="btn-ghost">Limpar formulario</button>
        </div>
      </form>
    </section>
  `;

  const form = document.getElementById('ticket-form');
  const clearButton = document.getElementById('ticket-clear');
  const titleInput = document.getElementById('ticket-title');
  const categoryInput = document.getElementById('ticket-category');
  const priorityInput = document.getElementById('ticket-priority');
  const descriptionInput = document.getElementById('ticket-description');

  form.addEventListener('submit', (event) => {
    event.preventDefault();

    const title = titleInput.value.trim();
    const category = categoryInput.value;
    const priority = priorityInput.value;
    const description = descriptionInput.value.trim();

    if (title.length < 4) {
      showNotification('Informe um assunto com pelo menos 4 caracteres.', 'warning');
      return;
    }

    createTicket({ title, category, priority, description });
    form.reset();
    navigate('myTickets');
  });

  clearButton.addEventListener('click', () => {
    form.reset();
    titleInput.focus();
  });
}

function createTicket(payload) {
  const ticket = normalizeTicket({
    id: createId(),
    title: payload.title,
    description: payload.description,
    category: payload.category,
    priority: payload.priority,
    creator: session.username,
    status: 'pending',
    assignedAdmin: null,
    createdAt: Date.now(),
    updatedAt: Date.now(),
    messages: []
  });

  tickets.push(ticket);
  saveData();
  addLog(`Criou ticket "${ticket.title}"`);
  showNotification('Ticket criado com sucesso.', 'success');
}

function renderPendingTickets() {
  if (!session || (session.role !== 'admin' && session.role !== 'superadmin')) {
    renderAccessDenied('Somente admins podem acessar a fila pendente.');
    return;
  }

  const pendingTickets = tickets
    .filter((ticket) => ticket.status === 'pending')
    .sort((a, b) => a.createdAt - b.createdAt);

  els.content.innerHTML = `
    <section class="panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">Fila pendente</h2>
          <p class="panel-subtitle">Aceite ou atribua tickets que ainda nao estao em atendimento.</p>
        </div>
      </div>
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Assunto</th>
              <th>Criador</th>
              <th>Categoria</th>
              <th>Prioridade</th>
              <th>Criado em</th>
              <th>Acao</th>
            </tr>
          </thead>
          <tbody id="pending-rows"></tbody>
        </table>
      </div>
    </section>
  `;

  const rows = document.getElementById('pending-rows');

  if (pendingTickets.length === 0) {
    rows.innerHTML = '<tr><td colspan="7" class="empty-state">Nao ha tickets pendentes.</td></tr>';
    return;
  }

  const assigneeOptions = getActiveAdmins()
    .map((admin) => `<option value="${escapeHtml(admin.username)}">${escapeHtml(admin.username)}</option>`)
    .join('');

  rows.innerHTML = pendingTickets.map((ticket) => {
    const actionHtml = session.role === 'superadmin'
      ? `
        <div class="inline-actions">
          <select data-assignee="${ticket.id}">${assigneeOptions}</select>
          <button class="btn-primary" data-action="assign" data-ticket="${ticket.id}">Atribuir</button>
        </div>
      `
      : `
        <div class="inline-actions">
          <button class="btn-primary" data-action="accept" data-ticket="${ticket.id}">Assumir</button>
        </div>
      `;

    return `
      <tr>
        <td>#${ticket.id}</td>
        <td>${escapeHtml(ticket.title)}</td>
        <td>${escapeHtml(ticket.creator)}</td>
        <td>${escapeHtml(categoryLabel(ticket.category))}</td>
        <td>${priorityBadge(ticket.priority)}</td>
        <td>${formatDateTime(ticket.createdAt)}</td>
        <td>${actionHtml}</td>
      </tr>
    `;
  }).join('');

  rows.addEventListener('click', (event) => {
    const button = event.target.closest('button[data-action]');
    if (!button) return;

    const action = button.dataset.action;
    const ticketId = Number(button.dataset.ticket);

    if (action === 'accept') {
      acceptTicket(ticketId, session.username);
      renderPendingTickets();
      renderSidebar();
      return;
    }

    if (action === 'assign') {
      const select = rows.querySelector(`select[data-assignee="${ticketId}"]`);
      const assignee = select ? select.value : '';
      if (!assignee) {
        showNotification('Escolha um responsavel para atribuir.', 'warning');
        return;
      }
      acceptTicket(ticketId, assignee);
      renderPendingTickets();
      renderSidebar();
    }
  });
}

function acceptTicket(ticketId, assignee) {
  const ticket = tickets.find((item) => item.id === ticketId);
  if (!ticket || ticket.status !== 'pending') {
    showNotification('Ticket nao esta disponivel para atribuicao.', 'warning');
    return;
  }

  ticket.status = 'active';
  ticket.assignedAdmin = assignee;
  ticket.updatedAt = Date.now();

  saveData();
  addLog(`Atribuiu ticket "${ticket.title}" para ${assignee}`);
  showNotification(`Ticket #${ticket.id} atribuido para ${assignee}.`, 'success');
}
function renderAllTickets() {
  if (!session || session.role !== 'superadmin') {
    renderAccessDenied('Apenas superadmin pode acessar todos os tickets.');
    return;
  }

  const assigneeOptions = ['<option value="">Sem responsavel</option>']
    .concat(getActiveAdmins().map((admin) => `<option value="${escapeHtml(admin.username)}">${escapeHtml(admin.username)}</option>`))
    .join('');

  els.content.innerHTML = `
    <section class="panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">Gestao total de tickets</h2>
          <p class="panel-subtitle">Atualize status, prioridade e responsavel sem sair da tabela.</p>
        </div>
      </div>

      <div class="filters">
        <input type="text" id="all-ticket-search" placeholder="Buscar por assunto, criador ou responsavel" />
        <select id="all-ticket-status">
          <option value="all">Todos os status</option>
          <option value="pending">Pendente</option>
          <option value="active">Em andamento</option>
          <option value="closed">Encerrado</option>
        </select>
        <select id="all-ticket-priority">
          <option value="all">Todas as prioridades</option>
          <option value="low">Baixa</option>
          <option value="medium">Media</option>
          <option value="high">Alta</option>
          <option value="urgent">Urgente</option>
        </select>
      </div>

      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>ID</th>
              <th>Assunto</th>
              <th>Criador</th>
              <th>Status</th>
              <th>Prioridade</th>
              <th>Responsavel</th>
              <th>Atualizado</th>
              <th>Acao</th>
            </tr>
          </thead>
          <tbody id="all-ticket-rows"></tbody>
        </table>
      </div>
    </section>
  `;

  const rowsElement = document.getElementById('all-ticket-rows');
  const searchInput = document.getElementById('all-ticket-search');
  const statusSelect = document.getElementById('all-ticket-status');
  const prioritySelect = document.getElementById('all-ticket-priority');

  function drawRows() {
    const search = searchInput.value.trim().toLowerCase();
    const statusFilter = statusSelect.value;
    const priorityFilter = prioritySelect.value;

    const filteredTickets = tickets
      .slice()
      .sort((a, b) => b.updatedAt - a.updatedAt)
      .filter((ticket) => {
        const matchesSearch = !search
          || ticket.title.toLowerCase().includes(search)
          || ticket.creator.toLowerCase().includes(search)
          || String(ticket.assignedAdmin || '').toLowerCase().includes(search);

        const matchesStatus = statusFilter === 'all' || ticket.status === statusFilter;
        const matchesPriority = priorityFilter === 'all' || ticket.priority === priorityFilter;

        return matchesSearch && matchesStatus && matchesPriority;
      });

    if (filteredTickets.length === 0) {
      rowsElement.innerHTML = '<tr><td colspan="8" class="empty-state">Nenhum ticket encontrado.</td></tr>';
      return;
    }

    rowsElement.innerHTML = filteredTickets.map((ticket) => `
      <tr>
        <td>#${ticket.id}</td>
        <td>
          <strong>${escapeHtml(ticket.title)}</strong><br />
          <small>${escapeHtml(toShortText(ticket.description || '-', 70))}</small>
        </td>
        <td>${escapeHtml(ticket.creator)}</td>
        <td>
          <select data-field="status" data-ticket="${ticket.id}">
            <option value="pending" ${ticket.status === 'pending' ? 'selected' : ''}>Pendente</option>
            <option value="active" ${ticket.status === 'active' ? 'selected' : ''}>Em andamento</option>
            <option value="closed" ${ticket.status === 'closed' ? 'selected' : ''}>Encerrado</option>
          </select>
        </td>
        <td>
          <select data-field="priority" data-ticket="${ticket.id}">
            <option value="low" ${ticket.priority === 'low' ? 'selected' : ''}>Baixa</option>
            <option value="medium" ${ticket.priority === 'medium' ? 'selected' : ''}>Media</option>
            <option value="high" ${ticket.priority === 'high' ? 'selected' : ''}>Alta</option>
            <option value="urgent" ${ticket.priority === 'urgent' ? 'selected' : ''}>Urgente</option>
          </select>
        </td>
        <td>
          <select data-field="assignee" data-ticket="${ticket.id}">
            ${assigneeOptions}
          </select>
        </td>
        <td>${formatDateTime(ticket.updatedAt)}</td>
        <td>
          <div class="inline-actions">
            <button class="btn-primary" data-action="save" data-ticket="${ticket.id}">Salvar</button>
            <button class="btn-secondary" data-action="chat" data-ticket="${ticket.id}">Chat</button>
          </div>
        </td>
      </tr>
    `).join('');

    filteredTickets.forEach((ticket) => {
      const assigneeSelect = rowsElement.querySelector(`select[data-field="assignee"][data-ticket="${ticket.id}"]`);
      if (assigneeSelect) {
        assigneeSelect.value = ticket.assignedAdmin || '';
      }
    });
  }

  rowsElement.addEventListener('click', (event) => {
    const button = event.target.closest('button[data-action]');
    if (!button) return;

    const action = button.dataset.action;
    const ticketId = Number(button.dataset.ticket);

    if (action === 'chat') {
      openChat(ticketId);
      return;
    }

    if (action === 'save') {
      const statusField = rowsElement.querySelector(`select[data-field="status"][data-ticket="${ticketId}"]`);
      const priorityField = rowsElement.querySelector(`select[data-field="priority"][data-ticket="${ticketId}"]`);
      const assigneeField = rowsElement.querySelector(`select[data-field="assignee"][data-ticket="${ticketId}"]`);

      const nextStatus = statusField ? statusField.value : 'pending';
      const nextPriority = priorityField ? priorityField.value : 'medium';
      const nextAssignee = assigneeField ? assigneeField.value : '';

      updateTicketBySuperadmin(ticketId, {
        status: nextStatus,
        priority: nextPriority,
        assignedAdmin: nextAssignee || null
      });

      drawRows();
      renderSidebar();
    }
  });

  searchInput.addEventListener('input', drawRows);
  statusSelect.addEventListener('change', drawRows);
  prioritySelect.addEventListener('change', drawRows);

  drawRows();
}

function updateTicketBySuperadmin(ticketId, updates) {
  const ticket = tickets.find((item) => item.id === ticketId);
  if (!ticket) {
    showNotification('Ticket nao encontrado.', 'error');
    return;
  }

  if (updates.status === 'active' && !updates.assignedAdmin) {
    showNotification('Ticket ativo precisa de responsavel.', 'warning');
    return;
  }

  ticket.status = ['pending', 'active', 'closed'].includes(updates.status) ? updates.status : ticket.status;
  ticket.priority = ['low', 'medium', 'high', 'urgent'].includes(updates.priority) ? updates.priority : ticket.priority;

  if (ticket.status === 'pending') {
    ticket.assignedAdmin = null;
  } else {
    ticket.assignedAdmin = updates.assignedAdmin || ticket.assignedAdmin || null;
  }

  ticket.updatedAt = Date.now();

  saveData();
  addLog(`Atualizou ticket #${ticket.id}: status ${ticket.status}, prioridade ${ticket.priority}, responsavel ${ticket.assignedAdmin || 'nenhum'}`);
  showNotification(`Ticket #${ticket.id} atualizado.`, 'success');
}

function closeTicket(ticketId) {
  const ticket = tickets.find((item) => item.id === ticketId);
  if (!ticket || ticket.status !== 'active') {
    showNotification('Apenas tickets ativos podem ser encerrados.', 'warning');
    return;
  }

  if (!(session.role === 'superadmin' || ticket.assignedAdmin === session.username)) {
    showNotification('Voce nao pode encerrar este ticket.', 'error');
    return;
  }

  ticket.status = 'closed';
  ticket.updatedAt = Date.now();

  saveData();
  addLog(`Encerrou ticket "${ticket.title}"`);
  showNotification(`Ticket #${ticket.id} encerrado.`, 'success');
}

function reopenTicket(ticketId) {
  const ticket = tickets.find((item) => item.id === ticketId);
  if (!ticket || ticket.status !== 'closed') {
    showNotification('Apenas tickets encerrados podem ser reabertos.', 'warning');
    return;
  }

  if (!(session.role === 'admin' || session.role === 'superadmin')) {
    showNotification('Somente admin pode reabrir ticket.', 'error');
    return;
  }

  ticket.status = ticket.assignedAdmin ? 'active' : 'pending';
  ticket.updatedAt = Date.now();

  saveData();
  addLog(`Reabriu ticket "${ticket.title}"`);
  showNotification(`Ticket #${ticket.id} reaberto.`, 'success');
}

function renderUsersManagement() {
  if (!session || session.role !== 'superadmin') {
    renderAccessDenied('Apenas superadmin pode gerenciar usuarios.');
    return;
  }

  const sortedUsers = users.slice().sort((a, b) => a.username.localeCompare(b.username));

  els.content.innerHTML = `
    <section class="panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">Gestao de usuarios</h2>
          <p class="panel-subtitle">Crie usuarios, ajuste papeis, controle financeiro e senha global 1705. Somente Esther altera senhas individuais.</p>
        </div>
      </div>

      <form id="user-form" class="form-grid">
        <div>
          <label for="new-username">Usuario</label>
          <input id="new-username" type="text" maxlength="30" required placeholder="novo.usuario" />
        </div>
        <div>
          <label>Senha inicial</label>
          <input type="text" value="${DEFAULT_PASSWORD}" disabled />
        </div>
        <div>
          <label for="new-role">Papel</label>
          <select id="new-role">
            <option value="member">Membro</option>
            <option value="admin">Admin</option>
            <option value="inteligencia">Inteligencia TP</option>
          </select>
        </div>
        <div class="inline-actions" style="align-items:end;">
          <button class="btn-primary" type="submit">Adicionar usuario</button>
        </div>
      </form>
    </section>

    <section class="panel">
      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Usuario</th>
              <th>Papel</th>
              <th>Status</th>
              <th>Grafico membro</th>
              <th>Divida</th>
              <th>Lucro</th>
              <th>Total pago</th>
              <th>Acoes</th>
            </tr>
          </thead>
          <tbody id="users-rows"></tbody>
        </table>
      </div>
    </section>
  `;

  const form = document.getElementById('user-form');
  const rows = document.getElementById('users-rows');

  form.addEventListener('submit', (event) => {
    event.preventDefault();

    const usernameInput = document.getElementById('new-username');
    const roleInput = document.getElementById('new-role');

    const username = usernameInput.value.trim();
    const password = DEFAULT_PASSWORD;
    const role = roleInput.value;

    if (username.length < 3) {
      showNotification('Usuario precisa ter ao menos 3 caracteres.', 'warning');
      return;
    }

    const exists = users.some((user) => user.username.toLowerCase() === username.toLowerCase());
    if (exists) {
      showNotification('Usuario ja existe.', 'error');
      return;
    }

    users.push(normalizeUser({
      username,
      password,
      role,
      status: 'active',
      debt: 0
    }));

    saveData();
    addLog(`Criou usuario ${username} (${role})`);
    showNotification('Usuario criado com sucesso.', 'success');

    form.reset();
    renderUsersManagement();
  });

  rows.innerHTML = sortedUsers.map((user) => {
    const isSelf = user.username === session.username;
    const roleSelect = `
      <select data-field="role" data-user="${escapeHtml(user.username)}" ${isSelf ? 'disabled' : ''}>
        <option value="member" ${user.role === 'member' ? 'selected' : ''}>Membro</option>
        <option value="admin" ${user.role === 'admin' ? 'selected' : ''}>Admin</option>
        <option value="inteligencia" ${user.role === 'inteligencia' ? 'selected' : ''}>Inteligencia TP</option>
        <option value="superadmin" ${user.role === 'superadmin' ? 'selected' : ''}>Superadmin</option>
      </select>
    `;
    const chartStatus = user.role === 'member'
      ? (user.walletChartEnabled ? 'Ativo' : 'Desligado')
      : '-';
    const canToggleChart = isEsther() && user.role === 'member';

    return `
      <tr>
        <td>${escapeHtml(user.username)}</td>
        <td>${roleSelect}</td>
        <td>${escapeHtml(user.status)}</td>
        <td>${escapeHtml(chartStatus)}</td>
        <td>${formatCurrency(user.debt)}</td>
        <td>${formatCurrency(user.walletProfit)}</td>
        <td>${formatCurrency(user.totalPaid)}</td>
        <td>
          <div class="inline-actions">
            <button class="btn-secondary" data-action="save-role" data-user="${escapeHtml(user.username)}" ${isSelf ? 'disabled' : ''}>Salvar papel</button>
            <button class="btn-ghost" data-action="toggle-wallet-chart" data-user="${escapeHtml(user.username)}" ${canToggleChart ? '' : 'disabled'}>Grafico</button>
            <button class="btn-ghost" data-action="toggle-status" data-user="${escapeHtml(user.username)}" ${isSelf ? 'disabled' : ''}>${user.status === 'active' ? 'Bloquear' : 'Desbloquear'}</button>
            <button class="btn-primary" data-action="charge" data-user="${escapeHtml(user.username)}">Cobrar</button>
            <button class="btn-secondary" data-action="payment" data-user="${escapeHtml(user.username)}">Registrar pgto</button>
            <button class="btn-ghost" data-action="clear-debt" data-user="${escapeHtml(user.username)}">Zerar divida</button>
            <button class="btn-ghost" data-action="reset-chart-value" data-user="${escapeHtml(user.username)}">Zerar grafico</button>
            <button class="btn-secondary" data-action="reset-pass" data-user="${escapeHtml(user.username)}" ${isEsther() ? '' : 'disabled'}>Nova senha</button>
            <button class="btn-danger" data-action="delete" data-user="${escapeHtml(user.username)}" ${isSelf ? 'disabled' : ''}>Excluir</button>
          </div>
        </td>
      </tr>
    `;
  }).join('');

  rows.addEventListener('click', (event) => {
    const button = event.target.closest('button[data-action]');
    if (!button) return;

    const action = button.dataset.action;
    const username = button.dataset.user;
    const user = users.find((item) => item.username === username);
    if (!user) return;

    if (action === 'save-role') {
      const roleSelect = rows.querySelector(`select[data-field="role"][data-user="${username}"]`);
      if (!roleSelect) return;
      const newRole = roleSelect.value;

      if (user.role === 'superadmin' && newRole !== 'superadmin') {
        const superadmins = users.filter((item) => item.role === 'superadmin').length;
        if (superadmins <= 1) {
          showNotification('Mantenha pelo menos um superadmin ativo.', 'warning');
          roleSelect.value = 'superadmin';
          return;
        }
      }

      user.role = newRole;
      if (newRole !== 'member') {
        user.walletChartEnabled = false;
      } else if (typeof user.walletChartEnabled !== 'boolean') {
        user.walletChartEnabled = shouldDefaultWalletChartEnabled(user.username);
      }
      saveData();
      addLog(`Alterou papel de ${user.username} para ${newRole}`);
      showNotification('Papel atualizado.', 'success');
      renderUsersManagement();
      return;
    }

    if (action === 'toggle-wallet-chart') {
      if (!isEsther()) {
        showNotification('Somente Esther pode escolher quem tem grafico.', 'error');
        return;
      }
      if (user.role !== 'member') {
        showNotification('Apenas membros podem ter grafico.', 'warning');
        return;
      }
      user.walletChartEnabled = !user.walletChartEnabled;
      saveData();
      addLog(`Esther ${user.walletChartEnabled ? 'ativou' : 'desativou'} grafico de carteira para ${user.username}`);
      showNotification(`Grafico de ${user.username} ${user.walletChartEnabled ? 'ativado' : 'desativado'}.`, 'success');
      renderUsersManagement();
      return;
    }

    if (action === 'toggle-status') {
      user.status = user.status === 'active' ? 'blocked' : 'active';
      saveData();
      addLog(`${user.status === 'active' ? 'Desbloqueou' : 'Bloqueou'} usuario ${user.username}`);
      showNotification(`Usuario ${user.status === 'active' ? 'desbloqueado' : 'bloqueado'}.`, 'success');
      renderUsersManagement();
      return;
    }

    if (action === 'charge') {
      const raw = prompt(`Valor da cobranca para ${user.username}:`, '50.00');
      if (!raw) return;
      const amount = toPositiveAmount(raw);
      if (!amount) {
        showNotification('Valor invalido.', 'warning');
        return;
      }
      const result = registerFinanceEvent(user, 'charge', amount, 'Cobranca manual');
      if (!result.ok) {
        showNotification(result.message, 'warning');
        return;
      }
      addLog(`Cobranca de ${formatCurrency(amount)} para ${user.username}`);
      showNotification('Cobranca registrada.', 'success');
      renderUsersManagement();
      return;
    }

    if (action === 'payment') {
      const raw = prompt(`Valor do pagamento para ${user.username}:`, '20.00');
      if (!raw) return;
      const amount = toPositiveAmount(raw);
      if (!amount) {
        showNotification('Valor invalido.', 'warning');
        return;
      }

      const result = registerFinanceEvent(user, 'payment', amount, 'Pagamento registrado pelo superadmin');
      if (!result.ok) {
        showNotification(result.message, 'warning');
        return;
      }
      addLog(`Pagamento de ${formatCurrency(result.amount)} aplicado para ${user.username} (lucro ${formatCurrency(result.profitAdded || 0)})`);
      const profitText = result.profitAdded > 0 ? ` | lucro ${formatCurrency(result.profitAdded)}` : '';
      showNotification(`Pagamento aplicado: ${formatCurrency(result.amount)}${profitText}.`, 'success');
      renderUsersManagement();
      return;
    }

    if (action === 'clear-debt') {
      if (user.debt <= 0) {
        showNotification('Este usuario ja esta sem divida.', 'warning');
        return;
      }
      const result = registerFinanceEvent(user, 'payment', user.debt, 'Quitacao total manual');
      if (!result.ok) {
        showNotification(result.message, 'warning');
        return;
      }
      addLog(`Zerou divida de ${user.username}`);
      showNotification('Divida quitada com sucesso.', 'success');
      renderUsersManagement();
      return;
    }

    if (action === 'reset-chart-value') {
      if (!confirm(`Zerar valores de grafico da carteira de ${user.username}?`)) return;
      user.financeHistory = [];
      user.totalPaid = 0;
      user.walletProfit = 0;
      user.totalCharged = Math.max(0, user.debt);
      user.emergencyLoanOutstanding = 0;
      saveData();
      addLog(`Zerou valores de grafico da carteira de ${user.username}`);
      showNotification('Valores do grafico zerados.', 'success');
      renderUsersManagement();
      return;
    }

    if (action === 'reset-pass') {
      if (!isEsther()) {
        showNotification('Somente Esther pode alterar senhas de usuarios.', 'error');
        return;
      }
      const nextPassword = prompt(`Nova senha para ${user.username}:`);
      if (!nextPassword) return;
      if (nextPassword.trim().length < 3) {
        showNotification('Senha muito curta.', 'warning');
        return;
      }
      user.password = nextPassword.trim();
      saveData();
      addLog(`Atualizou senha de ${user.username}`);
      showNotification('Senha atualizada.', 'success');
      return;
    }

    if (action === 'delete') {
      if (!confirm(`Excluir usuario ${user.username}? Esta acao nao pode ser desfeita.`)) return;

      users = users.filter((item) => item.username !== user.username);
      tickets.forEach((ticket) => {
        if (ticket.assignedAdmin === user.username) {
          ticket.assignedAdmin = null;
          if (ticket.status === 'active') ticket.status = 'pending';
          ticket.updatedAt = Date.now();
        }
      });

      saveData();
      addLog(`Excluiu usuario ${user.username}`);
      showNotification('Usuario excluido.', 'success');
      renderUsersManagement();
    }
  });
}
function renderLogs() {
  if (!canViewLogs()) {
    renderAccessDenied('Apenas admins podem acessar os registros.');
    return;
  }

  const onlineUsers = getOnlinePresenceList();
  const onlineRows = onlineUsers.length > 0
    ? onlineUsers
        .map((entry) => `
          <tr>
            <td>${escapeHtml(entry.username)}</td>
            <td>${escapeHtml(roleLabel(entry.role))}</td>
            <td>${escapeHtml(PAGE_TITLES[entry.page] || entry.page)}</td>
            <td>${formatDateTime(entry.lastSeen)}</td>
          </tr>
        `)
        .join('')
    : '<tr><td colspan="4" class="empty-state">Nenhum usuario online no momento.</td></tr>';

  els.content.innerHTML = `
    <section class="panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">Registros do sistema</h2>
          <p class="panel-subtitle">Somente admins visualizam o historico e o monitor ao vivo de atividade.</p>
        </div>
        <div class="inline-actions">
          <button id="logs-refresh" class="btn-ghost">Atualizar</button>
          <button id="logs-export" class="btn-secondary">Exportar CSV</button>
          <button id="logs-clear" class="btn-danger">Limpar logs</button>
        </div>
      </div>

      <div class="split-grid">
        <div class="panel" style="margin:0;">
          <h3 class="panel-title">Quem esta fazendo o que agora</h3>
          <p class="panel-subtitle">Usuarios ativos e tela em uso no momento.</p>
          <div class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Usuario</th>
                  <th>Papel</th>
                  <th>Tela</th>
                  <th>Ultimo sinal</th>
                </tr>
              </thead>
              <tbody id="logs-online-rows">${onlineRows}</tbody>
            </table>
          </div>
        </div>

        <div class="panel" style="margin:0;">
          <h3 class="panel-title">Filtro de auditoria</h3>
          <p class="panel-subtitle">Busque por usuario ou termo de acao.</p>
          <div class="filters" style="grid-template-columns: 1fr;">
            <input id="logs-search" type="text" placeholder="Buscar por usuario ou acao" />
          </div>
        </div>
      </div>

      <div class="filters" style="grid-template-columns: 1fr;">
        <small class="panel-subtitle">Historico completo de acoes registradas.</small>
      </div>

      <div class="table-wrap">
        <table>
          <thead>
            <tr>
              <th>Data</th>
              <th>Usuario</th>
              <th>Acao</th>
            </tr>
          </thead>
          <tbody id="logs-rows"></tbody>
        </table>
      </div>
    </section>
  `;

  const searchInput = document.getElementById('logs-search');
  const rows = document.getElementById('logs-rows');
  const refreshButton = document.getElementById('logs-refresh');
  const exportButton = document.getElementById('logs-export');
  const clearButton = document.getElementById('logs-clear');

  function getFilteredLogs() {
    const query = searchInput.value.trim().toLowerCase();
    return logs
      .slice()
      .sort((a, b) => b.timestamp - a.timestamp)
      .filter((log) => {
        if (!query) return true;
        return log.user.toLowerCase().includes(query) || log.action.toLowerCase().includes(query);
      });
  }

  function drawRows() {
    const filtered = getFilteredLogs();
    if (filtered.length === 0) {
      rows.innerHTML = '<tr><td colspan="3" class="empty-state">Nenhum registro encontrado.</td></tr>';
      return;
    }

    rows.innerHTML = filtered.map((log) => `
      <tr>
        <td>${formatDateTime(log.timestamp)}</td>
        <td>${escapeHtml(log.user)}</td>
        <td>${escapeHtml(log.action)}</td>
      </tr>
    `).join('');
  }

  function exportCsv() {
    const filtered = getFilteredLogs();
    if (filtered.length === 0) {
      showNotification('Nao ha logs para exportar.', 'warning');
      return;
    }

    const lines = ['data,usuario,acao'];
    filtered.forEach((log) => {
      const line = [
        `"${formatDateTime(log.timestamp).replace(/"/g, '""')}"`,
        `"${log.user.replace(/"/g, '""')}"`,
        `"${log.action.replace(/"/g, '""')}"`
      ].join(',');
      lines.push(line);
    });

    const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `logs_tp_portal_${Date.now()}.csv`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);

    showNotification('CSV exportado.', 'success');
  }

  searchInput.addEventListener('input', drawRows);
  refreshButton.addEventListener('click', () => {
    renderLogs();
  });
  exportButton.addEventListener('click', exportCsv);
  clearButton.addEventListener('click', () => {
    if (!confirm('Deseja realmente limpar todos os logs?')) return;
    logs = [];
    saveData();
    addLog('Limpou todos os logs');
    drawRows();
    showNotification('Logs limpos.', 'success');
  });

  drawRows();
}

function renderEspionage() {
  if (!canAccessEspionage()) {
    renderAccessDenied('Apenas superadmin ou Inteligencia TP podem acessar a aba Espionagem.');
    return;
  }

  const now = Date.now();
  const canAudit = canSeeEspionageMonitoring();
  const canSendLive = canSendEspionageLiveMessage();
  const canQuickStealth = canCreateStealthSession();
  const onlineUsers = getOnlinePresenceList();
  const latestBroadcast = readLiveBroadcast();
  const hasRecentBroadcast = Boolean(
    latestBroadcast
      && latestBroadcast.createdAt > 0
      && now - latestBroadcast.createdAt <= LIVE_BROADCAST_EXPIRE_MS
  );
  const latestBroadcastSummary = hasRecentBroadcast
    ? `${toShortText(latestBroadcast.message, 85)} (${latestBroadcast.anonymous ? 'anonima' : `por ${latestBroadcast.sender}`})`
    : 'Nenhuma mensagem ao vivo recente.';
  const liveRoleOptionsHtml = ['member', 'admin', 'superadmin', 'financeiro', 'inteligencia']
    .map((role) => `<option value="${escapeHtml(role)}">${escapeHtml(roleLabel(role))}</option>`)
    .join('');
  const liveUserOptionsHtml = onlineUsers.length > 0
    ? onlineUsers
        .slice()
        .sort((a, b) => a.username.localeCompare(b.username))
        .map((entry) => `<option value="${escapeHtml(entry.username)}">${escapeHtml(entry.username)} (${escapeHtml(roleLabel(entry.role))})</option>`)
        .join('')
    : '<option value="">Nenhum usuario online</option>';

  const livePanelHtml = canSendLive
    ? `
      <section class="panel">
        <div class="panel-header">
          <div>
            <h3 class="panel-title">Broadcast ao vivo para usuarios online</h3>
            <p class="panel-subtitle">Mensagem instantanea com alvo especifico, modo anonimo/nominal e nivel critico.</p>
          </div>
        </div>
        <form id="esp-live-form" class="form-grid">
          <div class="full">
            <label for="esp-live-message">Mensagem</label>
            <input id="esp-live-message" type="text" maxlength="220" placeholder="Digite a mensagem ao vivo..." />
          </div>
          <div>
            <label for="esp-live-mode">Modo</label>
            <select id="esp-live-mode">
              <option value="anonymous">Anonimo (sem nome)</option>
              <option value="named">Com nome aparecendo</option>
            </select>
          </div>
          <div>
            <label for="esp-live-level">Nivel</label>
            <select id="esp-live-level">
              <option value="normal">Normal</option>
              <option value="critical">Critico (destaque)</option>
            </select>
          </div>
          <div>
            <label for="esp-live-target">Alvo</label>
            <select id="esp-live-target">
              <option value="all">Todos online</option>
              <option value="role">Por papel</option>
              <option value="user">Usuario especifico</option>
            </select>
          </div>
          <div id="esp-live-role-wrap">
            <label for="esp-live-role">Papel alvo</label>
            <select id="esp-live-role">${liveRoleOptionsHtml}</select>
          </div>
          <div id="esp-live-user-wrap">
            <label for="esp-live-user">Usuario online</label>
            <select id="esp-live-user">${liveUserOptionsHtml}</select>
          </div>
          <div class="inline-actions" style="align-items:end;">
            <button id="esp-live-send" class="btn-secondary" type="submit">Enviar broadcast</button>
          </div>
        </form>
        <div id="esp-live-preview" class="tool-output"></div>
        <div class="tool-output">
          Online agora: <strong>${onlineUsers.length}</strong> | Ultima: ${escapeHtml(latestBroadcastSummary)}
        </div>
      </section>
    `
    : '';

  if (!canAudit) {
    els.content.innerHTML = `
      <section class="panel">
        <div class="panel-header">
          <div>
            <h2 class="panel-title">Espionagem</h2>
            <p class="panel-subtitle">Monitoramento completo fica restrito para superadmin. Aqui voce envia mensagens ao vivo.</p>
          </div>
          <div class="inline-actions">
            <button id="esp-refresh" class="btn-primary">Atualizar agora</button>
          </div>
        </div>

        <div class="stats-grid">
          <div class="stat-card"><div class="stat-value">${onlineUsers.length}</div><div class="stat-label">Online agora</div></div>
          <div class="stat-card"><div class="stat-value">${users.length}</div><div class="stat-label">Usuarios totais</div></div>
          <div class="stat-card"><div class="stat-value">${hasRecentBroadcast ? 'Ativa' : 'Sem envio'}</div><div class="stat-label">Mensagem ao vivo</div></div>
          <div class="stat-card"><div class="stat-value">${formatDateTime(hasRecentBroadcast ? latestBroadcast.createdAt : 0)}</div><div class="stat-label">Ultimo disparo</div></div>
        </div>
      </section>

      ${livePanelHtml}
    `;

    const refreshButton = document.getElementById('esp-refresh');
    refreshButton.addEventListener('click', () => {
      renderEspionage();
    });

    const liveForm = document.getElementById('esp-live-form');
    if (liveForm) {
      liveForm.addEventListener('submit', (event) => {
        event.preventDefault();
        if (!canSendEspionageLiveMessage()) {
          showNotification('Sem permissao para enviar mensagem ao vivo.', 'error');
          return;
        }
        const message = document.getElementById('esp-live-message').value.trim();
        const mode = document.getElementById('esp-live-mode').value;
        const sendAsAnonymous = mode !== 'named';
        const result = emitLiveBroadcast(message, sendAsAnonymous);
        if (!result.ok) {
          showNotification(result.message, 'warning');
          return;
        }
        addLog(`Enviou mensagem ao vivo (${sendAsAnonymous ? 'anonima' : 'com nome'}) na espionagem`);
        showNotification(`Mensagem ao vivo enviada para ${onlineUsers.length} usuario(s) online.`, 'success');
        renderEspionage();
      });
    }
    return;
  }

  const onlineMap = new Map(onlineUsers.map((entry) => [entry.username, entry]));
  const sortedUsers = users.slice().sort((a, b) => a.username.localeCompare(b.username));
  const totalAccessCount = sortedUsers.reduce((sum, user) => sum + Math.max(0, Number(user.accessCount) || 0), 0);
  const activeIn24h = sortedUsers.filter((user) => (Number(user.lastLoginAt) || 0) >= now - (24 * 60 * 60 * 1000)).length;
  const offlineCount = sortedUsers.filter((user) => !onlineMap.has(user.username)).length;

  const onlineRows = onlineUsers.length > 0
    ? onlineUsers
        .map((entry) => {
          const user = getUserByUsername(entry.username);
          const since = Math.max(0, now - (entry.loginAt || entry.lastSeen || now));
          const busy = Boolean(getActiveStealthSessionForUser(entry.username));
          const canStealth = canQuickStealth && session && entry.username !== session.username;
          const actionButton = canStealth
            ? `<button class="btn-ghost" data-action="esp-stealth" data-user="${escapeHtml(entry.username)}" ${busy ? 'disabled' : ''}>${busy ? 'Ocupado' : 'Chat stealth'}</button>`
            : '-';
          return `
            <tr data-user="${escapeHtml(entry.username)}" data-role="${escapeHtml(entry.role)}" data-page="${escapeHtml(entry.page)}" data-online="true">
              <td>${escapeHtml(entry.username)}</td>
              <td>${escapeHtml(roleLabel(entry.role))}</td>
              <td>${escapeHtml(PAGE_TITLES[entry.page] || entry.page)}</td>
              <td>${formatDuration(since)}</td>
              <td>${user ? user.accessCount : 0}</td>
              <td>${actionButton}</td>
            </tr>
          `;
        })
        .join('')
    : '<tr><td colspan="6" class="empty-state">Nenhum usuario online agora.</td></tr>';

  const userRows = sortedUsers
    .map((user) => {
      const onlineEntry = onlineMap.get(user.username);
      const isOnline = Boolean(onlineEntry);
      const offlineReference = Number(user.lastLogoutAt) || Number(user.lastLoginAt) || 0;
      const offlineDuration = isOnline
        ? '-'
        : (offlineReference > 0 ? formatDuration(now - offlineReference) : 'sem registro');

      return `
        <tr data-user="${escapeHtml(user.username)}" data-role="${escapeHtml(user.role)}" data-page="${escapeHtml(isOnline ? onlineEntry.page : '')}" data-online="${isOnline ? 'true' : 'false'}">
          <td>${escapeHtml(user.username)}</td>
          <td>${escapeHtml(roleLabel(user.role))}</td>
          <td>${isOnline ? '<span class="badge status-active">Online</span>' : '<span class="badge status-closed">Offline</span>'}</td>
          <td>${Math.max(0, Number(user.accessCount) || 0)}</td>
          <td>${formatDateTime(user.lastLoginAt)}</td>
          <td>${formatDateTime(user.lastLogoutAt)}</td>
          <td>${escapeHtml(offlineDuration)}</td>
        </tr>
      `;
    })
    .join('');
  const rankingRows = sortedUsers
    .slice()
    .sort((a, b) => (Number(b.accessCount) || 0) - (Number(a.accessCount) || 0))
    .slice(0, 12)
    .map((user) => `
      <tr>
        <td>${escapeHtml(user.username)}</td>
        <td>${escapeHtml(roleLabel(user.role))}</td>
        <td>${Math.max(0, Number(user.accessCount) || 0)}</td>
        <td>${onlineMap.has(user.username) ? '<span class="badge status-active">Online</span>' : '<span class="badge status-closed">Offline</span>'}</td>
      </tr>
    `)
    .join('');
  const recentEspionageLogs = logs.slice().sort((a, b) => b.timestamp - a.timestamp).slice(0, 28);
  const logRows = recentEspionageLogs.length > 0
    ? recentEspionageLogs
        .map((entry) => `
          <tr>
            <td>${formatDateTime(entry.timestamp)}</td>
            <td>${escapeHtml(entry.user)}</td>
            <td>${escapeHtml(entry.action)}</td>
          </tr>
        `)
        .join('')
    : '<tr><td colspan="3" class="empty-state">Sem logs recentes.</td></tr>';

  els.content.innerHTML = `
    <section class="panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">Espionagem</h2>
          <p class="panel-subtitle">Monitoramento de presenca e frequencia de acesso para administracao suprema.</p>
        </div>
        <div class="inline-actions">
          <button id="esp-refresh" class="btn-primary">Atualizar agora</button>
          <button id="esp-auto-toggle" class="btn-ghost">Auto 5s: OFF</button>
          <button id="esp-export-csv" class="btn-secondary">Exportar CSV</button>
          <button id="esp-clear-stale" class="btn-danger">Limpar stale</button>
        </div>
      </div>

      <div class="stats-grid">
        <div class="stat-card"><div class="stat-value">${onlineUsers.length}</div><div class="stat-label">Online agora</div></div>
        <div class="stat-card"><div class="stat-value">${offlineCount}</div><div class="stat-label">Offline agora</div></div>
        <div class="stat-card"><div class="stat-value">${activeIn24h}</div><div class="stat-label">Ativos em 24h</div></div>
        <div class="stat-card"><div class="stat-value">${totalAccessCount}</div><div class="stat-label">Acessos acumulados</div></div>
      </div>
    </section>

    ${livePanelHtml}

    <section class="panel">
      <h3 class="panel-title">Filtros taticos de espionagem</h3>
      <div class="filters" style="grid-template-columns: 2fr repeat(3, minmax(0, 1fr));">
        <input id="esp-search" type="text" placeholder="Buscar usuario..." />
        <select id="esp-filter-status">
          <option value="all">Status: todos</option>
          <option value="online">Status: online</option>
          <option value="offline">Status: offline</option>
        </select>
        <select id="esp-filter-role">
          <option value="all">Papel: todos</option>
          <option value="member">Membro</option>
          <option value="admin">Admin</option>
          <option value="superadmin">Superadmin</option>
          <option value="financeiro">Financeiro</option>
          <option value="inteligencia">Inteligencia TP</option>
        </select>
        <select id="esp-filter-page">
          <option value="all">Tela: todas</option>
          <option value="dashboard">Painel</option>
          <option value="walletControl">Carteiras</option>
          <option value="espionage">Espionagem</option>
          <option value="stealthChat">Chat irrastreavel</option>
          <option value="intelCenter">Central intel</option>
          <option value="logs">Registros</option>
        </select>
      </div>
      <div id="esp-filter-summary" class="tool-output"></div>
    </section>

    <section class="split-grid">
      <div class="panel" style="margin:0;">
        <h3 class="panel-title">Quem esta online e ha quanto tempo</h3>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Usuario</th>
                <th>Papel</th>
                <th>Tela</th>
                <th>Online ha</th>
                <th>Acessos</th>
                <th>Acoes</th>
              </tr>
            </thead>
            <tbody id="esp-online-rows">${onlineRows}</tbody>
          </table>
        </div>
      </div>

      <div class="panel" style="margin:0;">
        <h3 class="panel-title">Mapa completo de acesso</h3>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Usuario</th>
                <th>Papel</th>
                <th>Status</th>
                <th>Acessos</th>
                <th>Ultimo login</th>
                <th>Ultimo logout</th>
                <th>Offline ha</th>
              </tr>
            </thead>
            <tbody id="esp-user-rows">${userRows}</tbody>
          </table>
        </div>
      </div>
    </section>

    <section class="split-grid">
      <div class="panel" style="margin:0;">
        <h3 class="panel-title">Ranking de atividade</h3>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Usuario</th>
                <th>Papel</th>
                <th>Acessos</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody id="esp-ranking-rows">${rankingRows || '<tr><td colspan="4" class="empty-state">Sem dados.</td></tr>'}</tbody>
          </table>
        </div>
      </div>

      <div class="panel" style="margin:0;">
        <h3 class="panel-title">Log ao vivo (ultimos eventos)</h3>
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Data</th>
                <th>Usuario</th>
                <th>Acao</th>
              </tr>
            </thead>
            <tbody id="esp-log-rows">${logRows}</tbody>
          </table>
        </div>
      </div>
    </section>
  `;

  const refreshButton = document.getElementById('esp-refresh');
  const autoToggleButton = document.getElementById('esp-auto-toggle');
  const exportButton = document.getElementById('esp-export-csv');
  const clearStaleButton = document.getElementById('esp-clear-stale');
  const searchInput = document.getElementById('esp-search');
  const statusFilter = document.getElementById('esp-filter-status');
  const roleFilter = document.getElementById('esp-filter-role');
  const pageFilter = document.getElementById('esp-filter-page');
  const filterSummary = document.getElementById('esp-filter-summary');
  const userRowsElement = document.getElementById('esp-user-rows');
  const onlineRowsElement = document.getElementById('esp-online-rows');
  const rankingRowsElement = document.getElementById('esp-ranking-rows');

  searchInput.value = espionageViewState.search || '';
  statusFilter.value = ['all', 'online', 'offline'].includes(espionageViewState.status) ? espionageViewState.status : 'all';
  roleFilter.value = ['all', 'member', 'admin', 'superadmin', 'financeiro', 'inteligencia'].includes(espionageViewState.role)
    ? espionageViewState.role
    : 'all';
  pageFilter.value = espionageViewState.page || 'all';
  if (!pageFilter.value) pageFilter.value = 'all';

  function syncAutoButton() {
    const enabled = isEspionageAutoRefreshEnabled();
    autoToggleButton.textContent = enabled ? 'Auto 5s: ON' : 'Auto 5s: OFF';
    autoToggleButton.className = enabled ? 'btn-secondary' : 'btn-ghost';
  }

  function matchesUserFilters(user) {
    const query = searchInput.value.trim().toLowerCase();
    const status = statusFilter.value;
    const role = roleFilter.value;
    const page = pageFilter.value;
    const onlineEntry = onlineMap.get(user.username);
    const isOnline = Boolean(onlineEntry);
    const pageValue = isOnline ? String(onlineEntry.page || '') : '';

    if (query && !user.username.toLowerCase().includes(query)) return false;
    if (status === 'online' && !isOnline) return false;
    if (status === 'offline' && isOnline) return false;
    if (role !== 'all' && user.role !== role) return false;
    if (page !== 'all' && pageValue !== page) return false;
    return true;
  }

  function drawRankingFiltered() {
    const ranking = sortedUsers
      .filter((user) => matchesUserFilters(user))
      .sort((a, b) => (Number(b.accessCount) || 0) - (Number(a.accessCount) || 0))
      .slice(0, 12);
    rankingRowsElement.innerHTML = ranking.length > 0
      ? ranking
          .map((user) => `
            <tr>
              <td>${escapeHtml(user.username)}</td>
              <td>${escapeHtml(roleLabel(user.role))}</td>
              <td>${Math.max(0, Number(user.accessCount) || 0)}</td>
              <td>${onlineMap.has(user.username) ? '<span class="badge status-active">Online</span>' : '<span class="badge status-closed">Offline</span>'}</td>
            </tr>
          `)
          .join('')
      : '<tr><td colspan="4" class="empty-state">Sem dados para o filtro atual.</td></tr>';
  }

  function applyFiltersToTables() {
    const userRows = Array.from(userRowsElement.querySelectorAll('tr[data-user]'));
    const onlineRows = Array.from(onlineRowsElement.querySelectorAll('tr[data-user]'));
    let visibleUsers = 0;
    let visibleOnline = 0;

    userRows.forEach((row) => {
      const username = String(row.dataset.user || '');
      const target = getUserByUsername(username);
      const visible = Boolean(target && matchesUserFilters(target));
      row.style.display = visible ? '' : 'none';
      if (visible) {
        visibleUsers += 1;
        if (row.dataset.online === 'true') visibleOnline += 1;
      }
    });

    onlineRows.forEach((row) => {
      const username = String(row.dataset.user || '');
      const target = getUserByUsername(username);
      const visible = Boolean(target && matchesUserFilters(target));
      row.style.display = visible ? '' : 'none';
    });

    drawRankingFiltered();
    filterSummary.innerHTML = `Filtro ativo: <strong>${visibleUsers}</strong> usuario(s) | <strong>${visibleOnline}</strong> online`;
  }

  function updateFilterStateAndApply() {
    espionageViewState.search = searchInput.value;
    espionageViewState.status = statusFilter.value;
    espionageViewState.role = roleFilter.value;
    espionageViewState.page = pageFilter.value;
    applyFiltersToTables();
  }

  function exportFilteredEspionageCsv() {
    const filtered = sortedUsers.filter((user) => matchesUserFilters(user));
    if (filtered.length === 0) {
      showNotification('Nao ha dados no filtro para exportar.', 'warning');
      return;
    }

    const lines = ['usuario,papel,status,tela,acessos,ultimo_login,ultimo_logout'];
    filtered.forEach((user) => {
      const onlineEntry = onlineMap.get(user.username);
      const row = [
        `"${user.username.replace(/"/g, '""')}"`,
        `"${roleLabel(user.role).replace(/"/g, '""')}"`,
        `"${onlineEntry ? 'Online' : 'Offline'}"`,
        `"${(onlineEntry ? (PAGE_TITLES[onlineEntry.page] || onlineEntry.page) : '-').replace(/"/g, '""')}"`,
        `${Math.max(0, Number(user.accessCount) || 0)}`,
        `"${formatDateTime(user.lastLoginAt).replace(/"/g, '""')}"`,
        `"${formatDateTime(user.lastLogoutAt).replace(/"/g, '""')}"`
      ].join(',');
      lines.push(row);
    });

    const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `espionagem_tp_${Date.now()}.csv`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);
    showNotification('CSV de espionagem exportado.', 'success');
  }

  refreshButton.addEventListener('click', () => {
    renderEspionage();
  });
  autoToggleButton.addEventListener('click', () => {
    if (isEspionageAutoRefreshEnabled()) {
      stopEspionageAutoRefresh();
      showNotification('Auto refresh da espionagem desligado.', 'warning');
    } else {
      startEspionageAutoRefresh();
      showNotification('Auto refresh da espionagem ligado (5s).', 'success');
    }
    syncAutoButton();
  });
  exportButton.addEventListener('click', exportFilteredEspionageCsv);
  clearStaleButton.addEventListener('click', () => {
    const removed = clearStalePresenceEntries();
    if (removed > 0) {
      addLog(`Limpou ${removed} presenca(s) stale na espionagem`);
      showNotification(`${removed} presenca(s) stale removida(s).`, 'success');
      renderEspionage();
    } else {
      showNotification('Nenhuma presenca stale encontrada.', 'warning');
    }
  });

  [searchInput, statusFilter, roleFilter, pageFilter].forEach((element) => {
    const eventName = element.tagName === 'INPUT' ? 'input' : 'change';
    element.addEventListener(eventName, updateFilterStateAndApply);
  });

  onlineRowsElement.addEventListener('click', (event) => {
    const button = event.target.closest('button[data-action="esp-stealth"]');
    if (!button) return;
    const targetUsername = String(button.dataset.user || '').trim();
    if (!targetUsername) return;
    const result = createStealthSessionWithUser(targetUsername);
    if (!result.ok) {
      showNotification(result.message, 'warning');
      return;
    }
    addLog(`Abriu chat irrastreavel rapido com ${result.target} pela espionagem`);
    showNotification(`Chat irrastreavel aberto com ${result.target}.`, 'success');
    navigate('stealthChat');
  });

  const liveForm = document.getElementById('esp-live-form');
  if (liveForm) {
    const targetSelect = document.getElementById('esp-live-target');
    const roleWrap = document.getElementById('esp-live-role-wrap');
    const userWrap = document.getElementById('esp-live-user-wrap');
    const roleSelect = document.getElementById('esp-live-role');
    const userSelect = document.getElementById('esp-live-user');
    const previewOutput = document.getElementById('esp-live-preview');

    targetSelect.value = ['all', 'role', 'user'].includes(espionageViewState.liveTarget) ? espionageViewState.liveTarget : 'all';
    if (roleSelect) roleSelect.value = espionageViewState.liveRole || 'member';
    if (userSelect && espionageViewState.liveUser) userSelect.value = espionageViewState.liveUser;

    function refreshLivePreview() {
      const audience = normalizeBroadcastAudience({
        type: targetSelect.value,
        role: roleSelect ? roleSelect.value : 'member',
        user: userSelect ? userSelect.value : ''
      });
      const recipients = resolveBroadcastRecipients(audience);
      if (roleWrap) roleWrap.style.display = audience.type === 'role' ? '' : 'none';
      if (userWrap) userWrap.style.display = audience.type === 'user' ? '' : 'none';
      previewOutput.innerHTML = `Alvo: <strong>${escapeHtml(getBroadcastAudienceLabel(audience))}</strong> | Receberao agora: <strong>${recipients.length}</strong>`;
      espionageViewState.liveTarget = audience.type;
      espionageViewState.liveRole = audience.role;
      espionageViewState.liveUser = audience.user;
    }

    [targetSelect, roleSelect, userSelect].forEach((element) => {
      if (!element) return;
      element.addEventListener('change', refreshLivePreview);
    });

    liveForm.addEventListener('submit', (event) => {
      event.preventDefault();
      if (!canSendEspionageLiveMessage()) {
        showNotification('Sem permissao para enviar mensagem ao vivo.', 'error');
        return;
      }
      const message = document.getElementById('esp-live-message').value.trim();
      const sendAsAnonymous = document.getElementById('esp-live-mode').value !== 'named';
      const level = document.getElementById('esp-live-level').value === 'critical' ? 'critical' : 'normal';
      const result = emitLiveBroadcast(message, {
        anonymous: sendAsAnonymous,
        level,
        audienceType: targetSelect.value,
        audienceRole: roleSelect ? roleSelect.value : 'member',
        audienceUser: userSelect ? userSelect.value : ''
      });
      if (!result.ok) {
        showNotification(result.message, 'warning');
        return;
      }
      addLog(
        `Enviou broadcast ${level === 'critical' ? 'critico' : 'normal'} ` +
        `(${sendAsAnonymous ? 'anonimo' : 'nominal'}) para ${result.audienceLabel} (${result.recipientCount} online)`
      );
      showNotification(`Broadcast enviado para ${result.recipientCount} usuario(s) online.`, 'success');
      renderEspionage();
    });

    refreshLivePreview();
  }

  syncAutoButton();
  applyFiltersToTables();
}

function canCreateStealthSession() {
  return Boolean(session && (session.role === 'superadmin' || session.role === 'inteligencia'));
}

function normalizeStealthSession(rawSession) {
  const participants = Array.isArray(rawSession.participants)
    ? rawSession.participants.map((item) => String(item || '').trim()).filter(Boolean)
    : [];
  return {
    id: Number(rawSession.id) || createId(),
    createdBy: String(rawSession.createdBy || ''),
    participants: participants.slice(0, 2),
    createdAt: Number(rawSession.createdAt) || Date.now(),
    updatedAt: Number(rawSession.updatedAt) || Date.now(),
    messages: Array.isArray(rawSession.messages)
      ? rawSession.messages
          .map((message) => ({
            id: Number(message.id) || createId(),
            author: String(message.author || 'sistema'),
            content: String(message.content || ''),
            createdAt: Number(message.createdAt) || Date.now(),
            editedAt: message.editedAt ? Number(message.editedAt) : null
          }))
          .filter((message) => message.content.trim().length > 0)
      : []
  };
}

function readStealthBus() {
  const payload = safeParse(localStorage.getItem(STORAGE_KEYS.stealthBus), { sessions: [] });
  const sessions = Array.isArray(payload.sessions) ? payload.sessions.map(normalizeStealthSession) : [];
  return { sessions };
}

function writeStealthBus(bus) {
  const safeBus = {
    sessions: Array.isArray(bus.sessions) ? bus.sessions.map(normalizeStealthSession) : []
  };
  localStorage.setItem(STORAGE_KEYS.stealthBus, JSON.stringify(safeBus));
  wsDebugLog('stealth_sync send', Array.isArray(safeBus.sessions) ? safeBus.sessions.length : 0);
  sendPresenceSocketMessage({
    type: 'stealth_sync',
    bus: safeBus
  });
}

function getActiveStealthSessionForUser(username) {
  if (!username) return null;
  const bus = readStealthBus();
  return bus.sessions.find((chatSession) => chatSession.participants.includes(username)) || null;
}

function canUseStealthChat() {
  if (!session) return false;
  return canCreateStealthSession() || Boolean(getActiveStealthSessionForUser(session.username));
}

function getStealthPeer(chatSession, username) {
  if (!chatSession) return '-';
  return chatSession.participants.find((participant) => participant !== username) || '-';
}

function upsertStealthSession(sessionId, updater) {
  const bus = readStealthBus();
  const index = bus.sessions.findIndex((chatSession) => chatSession.id === sessionId);
  if (index === -1) return null;

  const mutable = normalizeStealthSession(bus.sessions[index]);
  updater(mutable);
  mutable.updatedAt = Date.now();
  bus.sessions[index] = mutable;
  writeStealthBus(bus);
  return mutable;
}

function destroyStealthChat(showToast = true) {
  if (!session) return;
  const activeSession = getActiveStealthSessionForUser(session.username);
  if (!activeSession) return;

  const bus = readStealthBus();
  bus.sessions = bus.sessions.filter((chatSession) => chatSession.id !== activeSession.id);
  writeStealthBus(bus);

  if (showToast) {
    showNotification('Chat irrastreavel apagado.', 'warning');
  }
}

function syncStealthFromStorage() {
  if (!session) return;

  const activeSession = getActiveStealthSessionForUser(session.username);
  const inStealthPage = currentPage === 'stealthChat';

  if (activeSession && !inStealthPage) {
    showNotification(`Chat irrastreavel aberto com ${getStealthPeer(activeSession, session.username)}.`, 'warning');
    navigate('stealthChat');
    return;
  }

  if (!activeSession && inStealthPage && !canCreateStealthSession()) {
    showNotification('Chat irrastreavel encerrado.', 'warning');
    navigate('dashboard');
    return;
  }

  if (inStealthPage) {
    renderStealthChat();
  } else {
    renderSidebar();
  }
}

function renderStealthChat() {
  if (!canUseStealthChat()) {
    renderAccessDenied('Apenas Superadmin, Inteligencia TP ou usuarios convidados podem usar este chat.');
    return;
  }

  const activeSession = session ? getActiveStealthSessionForUser(session.username) : null;

  if (!activeSession) {
    if (!canCreateStealthSession()) {
      renderAccessDenied('Aguardando convite para chat irrastreavel.');
      return;
    }

    const availableTargets = users
      .filter((user) => user.status === 'active' && user.username !== session.username)
      .sort((a, b) => a.username.localeCompare(b.username))
      .map((user) => {
        const busy = Boolean(getActiveStealthSessionForUser(user.username));
        return {
          username: user.username,
          label: `${user.username} (${roleLabel(user.role)})${busy ? ' - ocupado' : ''}`,
          busy
        };
      });

    els.content.innerHTML = `
      <section class="panel">
        <h2 class="panel-title">Chat irrastreavel</h2>
        <p class="panel-subtitle">
          Escolha com qual usuario o chat sera criado. Se o usuario estiver logado, a tela abre na hora para ele.
        </p>
        <p class="panel-subtitle">Sem log de mensagens. Ao sair da sala, o chat e apagado para todos.</p>

        <form id="stealth-create-form" class="form-grid" style="margin-top: 12px;">
          <div class="full">
            <label for="stealth-target">Usuario alvo</label>
            <select id="stealth-target" ${availableTargets.length ? '' : 'disabled'}>
              ${availableTargets
                .map(
                  (target) =>
                    `<option value="${escapeHtml(target.username)}" ${target.busy ? 'disabled' : ''}>${escapeHtml(target.label)}</option>`
                )
                .join('')}
            </select>
          </div>
          <div class="full inline-actions">
            <button class="btn-primary" type="submit" ${availableTargets.some((target) => !target.busy) ? '' : 'disabled'}>Criar chat agora</button>
          </div>
        </form>
      </section>
    `;

    const createForm = document.getElementById('stealth-create-form');
    createForm.addEventListener('submit', (event) => {
      event.preventDefault();
      const targetSelect = document.getElementById('stealth-target');
      const targetUser = targetSelect.value;

      if (!targetUser) {
        showNotification('Selecione um usuario para abrir o chat.', 'warning');
        return;
      }

      if (getActiveStealthSessionForUser(targetUser)) {
        showNotification('Este usuario ja esta em um chat irrastreavel.', 'warning');
        return;
      }

      const bus = readStealthBus();
      const newSession = normalizeStealthSession({
        id: createId(),
        createdBy: session.username,
        participants: [session.username, targetUser],
        createdAt: Date.now(),
        updatedAt: Date.now(),
        messages: []
      });

      bus.sessions = bus.sessions.filter((chatSession) => !chatSession.participants.includes(session.username));
      bus.sessions.push(newSession);
      writeStealthBus(bus);
      showNotification(`Chat irrastreavel criado com ${targetUser}.`, 'success');

      renderSidebar();
      renderStealthChat();
    });
    return;
  }

  const peer = getStealthPeer(activeSession, session.username);

  els.content.innerHTML = `
    <section class="panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">Sessao irrastreavel ativa</h2>
          <p class="panel-subtitle">Conectado com ${escapeHtml(peer)} desde ${formatDateTime(activeSession.createdAt)}</p>
        </div>
        <div class="inline-actions">
          <button id="stealth-close" class="btn-secondary">Sair e apagar</button>
          <button id="stealth-delete-all" class="btn-danger">Apagar mensagens</button>
        </div>
      </div>

      <div id="stealth-messages" class="stealth-messages"></div>

      <form id="stealth-form" class="chat-input-area" style="margin-top: 12px;">
        <input id="stealth-input" type="text" maxlength="1200" placeholder="Digite uma mensagem sem rastreio..." />
        <button class="btn-primary" type="submit">Enviar</button>
      </form>
    </section>
  `;

  const messagesBox = document.getElementById('stealth-messages');
  const form = document.getElementById('stealth-form');
  const input = document.getElementById('stealth-input');
  const closeButton = document.getElementById('stealth-close');
  const deleteAllButton = document.getElementById('stealth-delete-all');

  function drawStealthMessages() {
    const liveSession = getActiveStealthSessionForUser(session.username);
    if (!liveSession) {
      messagesBox.innerHTML = '<div class="empty-state">Chat encerrado.</div>';
      return;
    }

    if (!liveSession.messages.length) {
      messagesBox.innerHTML = '<div class="empty-state">Sem mensagens ainda.</div>';
      return;
    }

    messagesBox.innerHTML = liveSession.messages
      .map((message) => {
        const editedTag = message.editedAt ? ` (editada ${formatDateTime(message.editedAt)})` : '';
        return `
          <div class="stealth-message">
            <div class="stealth-message-header">
              <strong>${escapeHtml(message.author)}</strong>
              <small>${formatDateTime(message.createdAt)}${escapeHtml(editedTag)}</small>
            </div>
            <p>${escapeHtml(message.content)}</p>
            <div class="inline-actions">
              <button class="btn-ghost" data-action="edit-stealth" data-msg="${message.id}">Editar</button>
              <button class="btn-danger" data-action="delete-stealth" data-msg="${message.id}">Apagar</button>
            </div>
          </div>
        `;
      })
      .join('');

    messagesBox.scrollTop = messagesBox.scrollHeight;
  }

  form.addEventListener('submit', (event) => {
    event.preventDefault();
    const text = input.value.trim();
    if (!text) return;

    upsertStealthSession(activeSession.id, (chatSession) => {
      chatSession.messages.push({
        id: createId(),
        author: session.username,
        content: text,
        createdAt: Date.now(),
        editedAt: null
      });
    });

    input.value = '';
    drawStealthMessages();
  });

  messagesBox.addEventListener('click', (event) => {
    const button = event.target.closest('button[data-action][data-msg]');
    if (!button) return;

    const messageId = Number(button.dataset.msg);
    const action = button.dataset.action;
    const liveSession = getActiveStealthSessionForUser(session.username);
    if (!liveSession) return;

    const message = liveSession.messages.find((item) => item.id === messageId);
    if (!message) return;

    if (action === 'edit-stealth') {
      const updated = prompt('Editar mensagem:', message.content);
      if (updated === null) return;
      const nextContent = updated.trim();
      if (!nextContent) {
        showNotification('Mensagem vazia nao e permitida.', 'warning');
        return;
      }
      upsertStealthSession(liveSession.id, (chatSession) => {
        const mutableMessage = chatSession.messages.find((item) => item.id === messageId);
        if (!mutableMessage) return;
        mutableMessage.content = nextContent;
        mutableMessage.editedAt = Date.now();
      });
      drawStealthMessages();
      return;
    }

    if (action === 'delete-stealth') {
      upsertStealthSession(liveSession.id, (chatSession) => {
        chatSession.messages = chatSession.messages.filter((item) => item.id !== messageId);
      });
      drawStealthMessages();
    }
  });

  closeButton.addEventListener('click', () => {
    destroyStealthChat(true);
    renderSidebar();
    if (canCreateStealthSession()) {
      renderStealthChat();
    } else {
      navigate('dashboard');
    }
  });

  deleteAllButton.addEventListener('click', () => {
    if (!confirm('Apagar todas as mensagens desta sessao?')) return;
    upsertStealthSession(activeSession.id, (chatSession) => {
      chatSession.messages = [];
    });
    drawStealthMessages();
  });

  drawStealthMessages();
}

function renderSuperTools() {
  if (!session || session.role !== 'superadmin') {
    renderAccessDenied('Apenas superadmin pode usar o comando master.');
    return;
  }

  const totals = {
    users: users.length,
    pending: tickets.filter((ticket) => ticket.status === 'pending').length,
    active: tickets.filter((ticket) => ticket.status === 'active').length,
    closed: tickets.filter((ticket) => ticket.status === 'closed').length,
    debt: users.reduce((sum, user) => sum + (Number(user.debt) || 0), 0)
  };

  els.content.innerHTML = `
    <section class="panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">Comando Superadmin</h2>
          <p class="panel-subtitle">Ferramentas administrativas de alto impacto.</p>
        </div>
      </div>
      <div class="stats-grid">
        <div class="stat-card"><div class="stat-value">${totals.users}</div><div class="stat-label">Usuarios</div></div>
        <div class="stat-card"><div class="stat-value">${totals.pending}</div><div class="stat-label">Pendentes</div></div>
        <div class="stat-card"><div class="stat-value">${totals.active}</div><div class="stat-label">Em andamento</div></div>
        <div class="stat-card"><div class="stat-value">${formatCurrency(totals.debt)}</div><div class="stat-label">Divida total</div></div>
      </div>
    </section>

    <section class="panel super-grid">
      <div>
        <h3 class="panel-title">Operacoes em massa</h3>
        <p class="panel-subtitle">Acoes rapidas para organizar o ambiente.</p>
        <div class="inline-actions" style="margin-top: 10px;">
          <button id="st-reassign-pending" class="btn-primary">Distribuir pendentes</button>
          <button id="st-clear-closed" class="btn-secondary">Excluir encerrados</button>
          <button id="st-zero-debts" class="btn-ghost">Zerar dividas</button>
          <button id="st-force-1705" class="btn-secondary">Forcar senhas 1705</button>
        </div>
      </div>

      <div>
        <h3 class="panel-title">Backup e restauracao</h3>
        <p class="panel-subtitle">Exporte tudo para JSON ou importe um backup.</p>
        <div class="inline-actions" style="margin-top: 10px;">
          <button id="st-export-backup" class="btn-primary">Exportar backup</button>
          <input id="st-import-backup" type="file" accept=".json,application/json" />
        </div>
      </div>

      <div>
        <h3 class="panel-title">Modo manutencao</h3>
        <p class="panel-subtitle">Bloqueia login de membros e admins.</p>
        <label><input id="st-maintenance-toggle" type="checkbox" ${settings.maintenanceMode ? 'checked' : ''} /> Ativar manutencao</label>
        <label for="st-maintenance-message">Mensagem</label>
        <input id="st-maintenance-message" type="text" maxlength="180" value="${escapeHtml(settings.maintenanceMessage)}" />
        <div class="inline-actions" style="margin-top: 10px;">
          <button id="st-save-maintenance" class="btn-secondary">Salvar manutencao</button>
        </div>
      </div>

      <div>
        <h3 class="panel-title">Conta Inteligencia TP</h3>
        <p class="panel-subtitle">Reativa e restaura credenciais padrao.</p>
        <div class="inline-actions" style="margin-top: 10px;">
          <button id="st-reset-intel" class="btn-primary">Resetar Inteligencia TP</button>
        </div>
      </div>
    </section>
  `;

  const reassignPendingButton = document.getElementById('st-reassign-pending');
  const clearClosedButton = document.getElementById('st-clear-closed');
  const zeroDebtsButton = document.getElementById('st-zero-debts');
  const exportBackupButton = document.getElementById('st-export-backup');
  const importBackupInput = document.getElementById('st-import-backup');
  const maintenanceToggle = document.getElementById('st-maintenance-toggle');
  const maintenanceMessage = document.getElementById('st-maintenance-message');
  const saveMaintenanceButton = document.getElementById('st-save-maintenance');
  const resetIntelButton = document.getElementById('st-reset-intel');
  const force1705Button = document.getElementById('st-force-1705');

  reassignPendingButton.addEventListener('click', () => {
    const adminPool = users.filter((user) => (user.role === 'admin' || user.role === 'superadmin') && user.status === 'active');
    const pendingTickets = tickets.filter((ticket) => ticket.status === 'pending');

    if (!adminPool.length) {
      showNotification('Nao ha admins ativos para distribuicao.', 'warning');
      return;
    }
    if (!pendingTickets.length) {
      showNotification('Nao ha tickets pendentes para distribuir.', 'warning');
      return;
    }

    pendingTickets.forEach((ticket, index) => {
      const assignee = adminPool[index % adminPool.length];
      ticket.status = 'active';
      ticket.assignedAdmin = assignee.username;
      ticket.updatedAt = Date.now();
    });

    saveData();
    addLog(`Distribuiu ${pendingTickets.length} tickets pendentes entre admins`);
    showNotification('Pendentes distribuidos com sucesso.', 'success');
    renderSuperTools();
    renderSidebar();
  });

  clearClosedButton.addEventListener('click', () => {
    const closedCount = tickets.filter((ticket) => ticket.status === 'closed').length;
    if (!closedCount) {
      showNotification('Nao existem tickets encerrados.', 'warning');
      return;
    }

    if (!confirm(`Excluir ${closedCount} tickets encerrados?`)) return;
    tickets = tickets.filter((ticket) => ticket.status !== 'closed');
    saveData();
    addLog(`Excluiu ${closedCount} tickets encerrados`);
    showNotification('Tickets encerrados removidos.', 'success');
    renderSuperTools();
  });

  zeroDebtsButton.addEventListener('click', () => {
    let affected = 0;
    users.forEach((user) => {
      if (user.debt > 0) {
        const result = registerFinanceEvent(user, 'payment', user.debt, 'Quitacao global superadmin');
        if (result.ok) affected += 1;
      }
    });
    addLog(`Quitou dividas em massa de ${affected} usuarios`);
    showNotification(`Dividas quitadas para ${affected} usuarios.`, 'success');
    renderSuperTools();
  });

  force1705Button.addEventListener('click', () => {
    users.forEach((user) => {
      user.password = DEFAULT_PASSWORD;
    });
    settings.passwordSeed1705Done = true;
    saveData();
    addLog('Forcou senha padrao 1705 para todos os usuarios');
    showNotification('Todas as senhas foram definidas para 1705.', 'success');
  });

  exportBackupButton.addEventListener('click', () => {
    const snapshot = {
      exportedAt: Date.now(),
      users,
      tickets,
      logs,
      tasks,
      notes,
      announcements,
      settings
    };

    const blob = new Blob([JSON.stringify(snapshot, null, 2)], { type: 'application/json;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `tp_portal_backup_${Date.now()}.json`;
    document.body.appendChild(link);
    link.click();
    link.remove();
    URL.revokeObjectURL(url);

    addLog('Exportou backup completo do sistema');
    showNotification('Backup exportado em JSON.', 'success');
  });

  importBackupInput.addEventListener('change', () => {
    const file = importBackupInput.files && importBackupInput.files[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = () => {
      try {
        const payload = JSON.parse(String(reader.result || '{}'));
        if (!confirm('Importar backup e sobrescrever dados atuais?')) return;

        users = Array.isArray(payload.users) ? payload.users.map(normalizeUser) : users;
        tickets = Array.isArray(payload.tickets) ? payload.tickets.map(normalizeTicket) : tickets;
        logs = Array.isArray(payload.logs)
          ? payload.logs.map((log) => ({
              timestamp: Number(log.timestamp) || Date.now(),
              user: String(log.user || 'sistema'),
              action: String(log.action || 'acao nao informada')
            }))
          : logs;
        tasks = Array.isArray(payload.tasks) ? payload.tasks.map(normalizeTask) : tasks;
        notes = Array.isArray(payload.notes) ? payload.notes.map(normalizeNote) : notes;
        announcements = Array.isArray(payload.announcements) ? payload.announcements.map(normalizeAnnouncement) : announcements;
        settings = normalizeSettings(payload.settings || settings);

        ensureCoreUsers();
        applyDefaultPasswordMigration();
        saveData();
        addLog('Importou backup completo do sistema');
        showNotification('Backup importado com sucesso.', 'success');
        renderSidebar();
        renderSuperTools();
      } catch {
        showNotification('Falha ao importar backup. JSON invalido.', 'error');
      }
    };
    reader.readAsText(file);
    importBackupInput.value = '';
  });

  saveMaintenanceButton.addEventListener('click', () => {
    settings.maintenanceMode = maintenanceToggle.checked;
    settings.maintenanceMessage = maintenanceMessage.value.trim() || 'Portal temporariamente em manutencao.';
    saveData();
    addLog(`Atualizou manutencao: ${settings.maintenanceMode ? 'ativada' : 'desativada'}`);
    showNotification('Configuracao de manutencao salva.', 'success');
  });

  resetIntelButton.addEventListener('click', () => {
    const username = 'inteligencia tp';
    let intelUser = users.find((user) => user.username.toLowerCase() === username);
    if (!intelUser) {
      intelUser = normalizeUser({
        username,
        password: DEFAULT_PASSWORD,
        role: 'inteligencia',
        status: 'active',
        debt: 0
      });
      users.push(intelUser);
    } else {
      intelUser.role = 'inteligencia';
      intelUser.status = 'active';
      intelUser.password = DEFAULT_PASSWORD;
    }

    saveData();
    addLog('Resetou conta Inteligencia TP');
    showNotification('Conta Inteligencia TP pronta (login: inteligencia tp / 1705).', 'success');
  });
}

function renderIntelCenter() {
  if (!session || (session.role !== 'inteligencia' && session.role !== 'superadmin')) {
    renderAccessDenied('Apenas Inteligencia TP ou superadmin podem acessar a central.');
    return;
  }

  const canAudit = canViewLogs();
  const onlineUsers = canAudit ? getOnlinePresenceList() : [];
  const recentLogs = canAudit ? logs.slice().sort((a, b) => b.timestamp - a.timestamp).slice(0, 18) : [];
  const debtRanking = users
    .slice()
    .sort((a, b) => b.debt - a.debt)
    .slice(0, 8);
  const blockedUsers = users.filter((user) => user.status === 'blocked');

  const onlineRows = onlineUsers.length
    ? onlineUsers
        .map(
          (entry) => `
      <tr>
        <td>${escapeHtml(entry.username)}</td>
        <td>${escapeHtml(roleLabel(entry.role))}</td>
        <td>${escapeHtml(PAGE_TITLES[entry.page] || entry.page)}</td>
        <td>${formatDateTime(entry.lastSeen)}</td>
      </tr>
    `
        )
        .join('')
    : '<tr><td colspan="4" class="empty-state">Nenhum usuario online no momento.</td></tr>';

  const logsRows = recentLogs.length
    ? recentLogs
        .map(
          (log) => `
      <tr>
        <td>${formatDateTime(log.timestamp)}</td>
        <td>${escapeHtml(log.user)}</td>
        <td>${escapeHtml(log.action)}</td>
      </tr>
    `
        )
        .join('')
    : '<tr><td colspan="3" class="empty-state">Sem logs recentes.</td></tr>';

  const debtRows = debtRanking
    .map(
      (user) => `
    <tr>
      <td>${escapeHtml(user.username)}</td>
      <td>${escapeHtml(roleLabel(user.role))}</td>
      <td>${formatCurrency(user.debt)}</td>
      <td>${formatCurrency(user.totalPaid)}</td>
    </tr>
  `
    )
    .join('');

  const monitorHtml = canAudit
    ? `
      <div class="split-grid">
        <div class="panel" style="margin:0;">
          <h3 class="panel-title">Usuarios online</h3>
          <div class="table-wrap">
            <table>
              <thead>
                <tr><th>Usuario</th><th>Papel</th><th>Tela atual</th><th>Ultimo sinal</th></tr>
              </thead>
              <tbody>${onlineRows}</tbody>
            </table>
          </div>
        </div>

        <div class="panel" style="margin:0;">
          <h3 class="panel-title">Radar financeiro</h3>
          <div class="table-wrap">
            <table>
              <thead>
                <tr><th>Usuario</th><th>Papel</th><th>Divida</th><th>Total pago</th></tr>
              </thead>
              <tbody>${debtRows}</tbody>
            </table>
          </div>
        </div>
      </div>
    `
    : `
      <div class="panel" style="margin:0;">
        <h3 class="panel-title">Radar financeiro</h3>
        <p class="panel-subtitle">Visao focada em carteira por usuario. Logs e presenca ficam apenas para admins.</p>
        <div class="table-wrap">
          <table>
            <thead>
              <tr><th>Usuario</th><th>Papel</th><th>Divida</th><th>Total pago</th></tr>
            </thead>
            <tbody>${debtRows}</tbody>
          </table>
        </div>
      </div>
    `;

  const logsSectionHtml = canAudit
    ? `
      <section class="panel">
        <h3 class="panel-title">Log ao vivo (ultimos eventos)</h3>
        <div class="table-wrap">
          <table>
            <thead>
              <tr><th>Data</th><th>Usuario</th><th>Acao</th></tr>
            </thead>
            <tbody>${logsRows}</tbody>
          </table>
        </div>
      </section>
    `
    : '';

  els.content.innerHTML = `
    <section class="panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">Central Inteligencia</h2>
          <p class="panel-subtitle">Operacao financeira, alertas globais e desbloqueio rapido.</p>
        </div>
        <div class="inline-actions">
          <button id="intel-refresh" class="btn-primary">Atualizar agora</button>
        </div>
      </div>

      ${monitorHtml}
    </section>

    <section class="panel">
      <div class="panel-header">
        <div>
          <h3 class="panel-title">Acoes Inteligencia</h3>
          <p class="panel-subtitle">Comandos rapidos para operacao e motivacao da equipe.</p>
        </div>
      </div>

      <div class="form-grid">
        <div class="full">
          <label for="intel-alert-title">Alerta relampago</label>
          <input id="intel-alert-title" type="text" maxlength="80" placeholder="Ex: Revisao geral em 10 minutos" />
        </div>
        <div class="full">
          <label for="intel-alert-content">Mensagem</label>
          <input id="intel-alert-content" type="text" maxlength="180" placeholder="Mensagem curta para todos os usuarios." />
        </div>
        <div class="full inline-actions">
          <button id="intel-send-alert" class="btn-secondary">Enviar alerta global</button>
        </div>
      </div>

      <div class="form-grid" style="margin-top: 10px;">
        <div>
          <label for="intel-unblock-user">Desbloqueio rapido</label>
          <select id="intel-unblock-user">
            ${
              blockedUsers.length
                ? blockedUsers.map((user) => `<option value="${escapeHtml(user.username)}">${escapeHtml(user.username)}</option>`).join('')
                : '<option value="">Sem usuarios bloqueados</option>'
            }
          </select>
        </div>
        <div class="inline-actions" style="align-items: end;">
          <button id="intel-unblock-btn" class="btn-primary" ${blockedUsers.length ? '' : 'disabled'}>Desbloquear usuario</button>
        </div>
      </div>
    </section>
    ${logsSectionHtml}
  `;

  const refreshButton = document.getElementById('intel-refresh');
  const sendAlertButton = document.getElementById('intel-send-alert');
  const unblockButton = document.getElementById('intel-unblock-btn');

  refreshButton.addEventListener('click', () => {
    renderIntelCenter();
  });

  sendAlertButton.addEventListener('click', () => {
    const title = document.getElementById('intel-alert-title').value.trim();
    const content = document.getElementById('intel-alert-content').value.trim();
    if (title.length < 3 || content.length < 3) {
      showNotification('Preencha titulo e mensagem do alerta.', 'warning');
      return;
    }

    announcements.push(
      normalizeAnnouncement({
        id: createId(),
        title,
        content,
        author: session.username,
        audience: 'all',
        createdAt: Date.now()
      })
    );
    saveData();
    addLog(`Enviou alerta global: ${title}`);
    showNotification('Alerta enviado para todos.', 'success');
    renderIntelCenter();
  });

  if (unblockButton) {
    unblockButton.addEventListener('click', () => {
      const select = document.getElementById('intel-unblock-user');
      const username = select.value;
      if (!username) {
        showNotification('Nenhum usuario selecionado.', 'warning');
        return;
      }
      const target = getUserByUsername(username);
      if (!target) {
        showNotification('Usuario nao encontrado.', 'error');
        return;
      }
      target.status = 'active';
      saveData();
      addLog(`${session.username} desbloqueou usuario ${target.username} pela central inteligencia`);
      showNotification(`Usuario ${target.username} desbloqueado.`, 'success');
      renderIntelCenter();
    });
  }
}

function renderTasks() {
  const userTasks = getUserTasks();

  els.content.innerHTML = `
    <section class="panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">Tarefas pessoais</h2>
          <p class="panel-subtitle">Organize seu fluxo entre A fazer, Em execucao e Concluido.</p>
        </div>
      </div>

      <form id="task-form" class="form-grid">
        <div>
          <label for="task-title">Tarefa</label>
          <input id="task-title" type="text" placeholder="Ex: Revisar chamados de acesso" maxlength="120" required />
        </div>
        <div>
          <label for="task-due">Data limite</label>
          <input id="task-due" type="date" />
        </div>
        <div>
          <label for="task-priority">Prioridade</label>
          <select id="task-priority">
            <option value="low">Baixa</option>
            <option value="medium" selected>Media</option>
            <option value="high">Alta</option>
          </select>
        </div>
        <div class="inline-actions" style="align-items:end;">
          <button class="btn-primary" type="submit">Adicionar tarefa</button>
        </div>
      </form>

      <div class="task-board">
        <div class="task-column">
          <h3>A fazer <span class="column-count" id="count-todo">0</span></h3>
          <div id="tasks-todo" class="task-list"></div>
        </div>
        <div class="task-column">
          <h3>Em execucao <span class="column-count" id="count-doing">0</span></h3>
          <div id="tasks-doing" class="task-list"></div>
        </div>
        <div class="task-column">
          <h3>Concluido <span class="column-count" id="count-done">0</span></h3>
          <div id="tasks-done" class="task-list"></div>
        </div>
      </div>
    </section>
  `;

  const form = document.getElementById('task-form');
  const titleInput = document.getElementById('task-title');
  const dueInput = document.getElementById('task-due');
  const priorityInput = document.getElementById('task-priority');

  function sortedTasksByStatus(status) {
    return userTasks
      .filter((task) => task.status === status)
      .sort((a, b) => {
        if (a.dueDate && b.dueDate) return a.dueDate.localeCompare(b.dueDate);
        if (a.dueDate) return -1;
        if (b.dueDate) return 1;
        return b.createdAt - a.createdAt;
      });
  }

  function cardHtml(task) {
    const stepIndex = TASK_FLOW.indexOf(task.status);
    const canMoveBack = stepIndex > 0;
    const canMoveForward = stepIndex < TASK_FLOW.length - 1;
    const dueText = task.dueDate ? `Entrega: ${escapeHtml(task.dueDate)}` : 'Sem prazo';

    return `
      <div class="task-card" data-task="${task.id}">
        <strong>${escapeHtml(task.title)}</strong>
        <div class="task-meta">${dueText} | ${escapeHtml(priorityLabel(task.priority))}</div>
        <div class="task-actions">
          <button class="btn-ghost" data-action="back" data-task="${task.id}" ${canMoveBack ? '' : 'disabled'}>Voltar</button>
          <button class="btn-primary" data-action="next" data-task="${task.id}" ${canMoveForward ? '' : 'disabled'}>Avancar</button>
          <button class="btn-danger" data-action="delete" data-task="${task.id}">Excluir</button>
        </div>
      </div>
    `;
  }

  function drawBoard() {
    ['todo', 'doing', 'done'].forEach((status) => {
      const listElement = document.getElementById(`tasks-${status}`);
      const countElement = document.getElementById(`count-${status}`);
      const statusTasks = sortedTasksByStatus(status);
      countElement.textContent = String(statusTasks.length);

      if (statusTasks.length === 0) {
        listElement.innerHTML = '<div class="empty-state">Nenhuma tarefa nesta coluna.</div>';
      } else {
        listElement.innerHTML = statusTasks.map(cardHtml).join('');
      }
    });
  }

  function findTask(taskId) {
    return tasks.find((task) => task.id === taskId && task.owner === session.username);
  }

  form.addEventListener('submit', (event) => {
    event.preventDefault();

    const title = titleInput.value.trim();
    const dueDate = dueInput.value;
    const priority = priorityInput.value;

    if (title.length < 3) {
      showNotification('Informe uma tarefa com ao menos 3 caracteres.', 'warning');
      return;
    }

    const newTask = normalizeTask({
      id: createId(),
      owner: session.username,
      title,
      dueDate,
      priority,
      status: 'todo',
      createdAt: Date.now()
    });

    tasks.push(newTask);
    userTasks.push(newTask);
    saveData();
    addLog(`Criou tarefa "${title}"`);

    form.reset();
    drawBoard();
    showNotification('Tarefa adicionada.', 'success');
  });

  const taskBoard = document.querySelector('.task-board');
  taskBoard.addEventListener('click', (event) => {
    const button = event.target.closest('button[data-task][data-action]');
    if (!button) return;

    const taskId = Number(button.dataset.task);
    const action = button.dataset.action;
    const task = findTask(taskId);
    if (!task) return;

    if (action === 'delete') {
      tasks = tasks.filter((item) => item.id !== taskId);
      const index = userTasks.findIndex((item) => item.id === taskId);
      if (index >= 0) userTasks.splice(index, 1);
      saveData();
      addLog(`Excluiu tarefa "${task.title}"`);
      drawBoard();
      return;
    }

    const step = TASK_FLOW.indexOf(task.status);
    if (action === 'back' && step > 0) {
      task.status = TASK_FLOW[step - 1];
    }

    if (action === 'next' && step < TASK_FLOW.length - 1) {
      task.status = TASK_FLOW[step + 1];
    }

    saveData();
    addLog(`Moveu tarefa "${task.title}" para ${task.status}`);
    drawBoard();
  });

  drawBoard();
}

function renderNotes() {
  editingNoteId = null;

  els.content.innerHTML = `
    <section class="panel note-layout">
      <div>
        <h2 class="panel-title">Bloco de notas</h2>
        <p class="panel-subtitle">Guarde informacoes rapidas para seu dia a dia.</p>

        <form id="note-form" class="form-grid">
          <div class="full">
            <label for="note-title">Titulo</label>
            <input id="note-title" type="text" maxlength="100" placeholder="Titulo da nota" required />
          </div>
          <div class="full">
            <label for="note-content">Conteudo</label>
            <textarea id="note-content" maxlength="3000" placeholder="Escreva aqui..."></textarea>
          </div>
          <div class="full inline-actions">
            <button class="btn-primary" type="submit" id="note-save">Salvar nota</button>
            <button class="btn-ghost" type="button" id="note-clear">Limpar</button>
          </div>
        </form>
      </div>

      <div>
        <h3 class="panel-title">Minhas notas</h3>
        <div id="notes-list" class="note-list"></div>
      </div>
    </section>
  `;

  const noteForm = document.getElementById('note-form');
  const noteTitle = document.getElementById('note-title');
  const noteContent = document.getElementById('note-content');
  const clearButton = document.getElementById('note-clear');
  const notesList = document.getElementById('notes-list');

  function ownNotes() {
    return getUserNotes().slice().sort((a, b) => b.updatedAt - a.updatedAt);
  }

  function drawList() {
    const list = ownNotes();
    if (list.length === 0) {
      notesList.innerHTML = '<div class="empty-state">Nenhuma nota cadastrada.</div>';
      return;
    }

    notesList.innerHTML = list.map((note) => `
      <article class="note-card">
        <h4>${escapeHtml(note.title)}</h4>
        <div class="note-preview">${escapeHtml(toShortText(note.content || 'Sem conteudo.', 120))}</div>
        <small>${formatDateTime(note.updatedAt)}</small>
        <div class="inline-actions" style="margin-top:8px;">
          <button class="btn-secondary" data-action="edit-note" data-note="${note.id}">Editar</button>
          <button class="btn-danger" data-action="delete-note" data-note="${note.id}">Excluir</button>
        </div>
      </article>
    `).join('');
  }

  function resetForm() {
    editingNoteId = null;
    noteForm.reset();
    noteTitle.focus();
    document.getElementById('note-save').textContent = 'Salvar nota';
  }

  noteForm.addEventListener('submit', (event) => {
    event.preventDefault();

    const title = noteTitle.value.trim();
    const content = noteContent.value.trim();

    if (title.length < 2) {
      showNotification('Titulo deve ter ao menos 2 caracteres.', 'warning');
      return;
    }

    if (editingNoteId) {
      const existing = notes.find((note) => note.id === editingNoteId && note.owner === session.username);
      if (!existing) return;

      existing.title = title;
      existing.content = content;
      existing.updatedAt = Date.now();

      saveData();
      addLog(`Atualizou nota "${title}"`);
      showNotification('Nota atualizada.', 'success');
    } else {
      notes.push(normalizeNote({
        id: createId(),
        owner: session.username,
        title,
        content,
        updatedAt: Date.now()
      }));
      saveData();
      addLog(`Criou nota "${title}"`);
      showNotification('Nota criada.', 'success');
    }

    resetForm();
    drawList();
  });

  clearButton.addEventListener('click', resetForm);

  notesList.addEventListener('click', (event) => {
    const button = event.target.closest('button[data-action][data-note]');
    if (!button) return;

    const noteId = Number(button.dataset.note);
    const action = button.dataset.action;
    const note = notes.find((item) => item.id === noteId && item.owner === session.username);
    if (!note) return;

    if (action === 'edit-note') {
      editingNoteId = note.id;
      noteTitle.value = note.title;
      noteContent.value = note.content;
      document.getElementById('note-save').textContent = 'Atualizar nota';
      noteTitle.focus();
      return;
    }

    if (action === 'delete-note') {
      if (!confirm('Excluir esta nota?')) return;
      notes = notes.filter((item) => item.id !== note.id);
      saveData();
      addLog(`Excluiu nota "${note.title}"`);
      drawList();
      if (editingNoteId === note.id) {
        resetForm();
      }
      showNotification('Nota excluida.', 'success');
    }
  });

  drawList();
}
function renderAnnouncements() {
  const canPublish = session.role === 'admin' || session.role === 'superadmin';

  const formHtml = canPublish
    ? `
      <section class="panel">
        <h2 class="panel-title">Publicar comunicado</h2>
        <form id="announcement-form" class="form-grid">
          <div>
            <label for="announcement-title">Titulo</label>
            <input id="announcement-title" type="text" maxlength="120" placeholder="Titulo do aviso" required />
          </div>
          <div>
            <label for="announcement-audience">Publico</label>
            <select id="announcement-audience">
              <option value="all">Todos</option>
              <option value="team">Equipe administrativa</option>
            </select>
          </div>
          <div class="full">
            <label for="announcement-content">Conteudo</label>
            <textarea id="announcement-content" maxlength="2000" placeholder="Mensagem do comunicado"></textarea>
          </div>
          <div class="full inline-actions">
            <button class="btn-primary" type="submit">Publicar</button>
          </div>
        </form>
      </section>
    `
    : '';

  els.content.innerHTML = `
    ${formHtml}
    <section class="panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">Comunicados internos</h2>
          <p class="panel-subtitle">Linha do tempo com avisos relevantes da equipe.</p>
        </div>
      </div>
      <div id="announcement-list" class="timeline"></div>
    </section>
  `;

  const list = document.getElementById('announcement-list');

  function visibleAnnouncements() {
    return announcements
      .slice()
      .sort((a, b) => b.createdAt - a.createdAt)
      .filter((item) => {
        if (item.audience === 'all') return true;
        return session.role === 'admin' || session.role === 'superadmin';
      });
  }

  function drawAnnouncements() {
    const items = visibleAnnouncements();
    if (items.length === 0) {
      list.innerHTML = '<div class="empty-state">Nenhum comunicado disponivel.</div>';
      return;
    }

    list.innerHTML = items.map((item) => {
      const canDelete = session.role === 'superadmin' || item.author === session.username;
      return `
        <article class="announcement">
          <div class="announcement-header">
            <strong>${escapeHtml(item.title)}</strong>
            <small>${formatDateTime(item.createdAt)}</small>
          </div>
          <p>${escapeHtml(item.content)}</p>
          <small>Autor: ${escapeHtml(item.author)} | Publico: ${item.audience === 'all' ? 'Todos' : 'Equipe administrativa'}</small>
          ${canDelete ? `<div class="inline-actions" style="margin-top:8px;"><button class="btn-danger" data-action="delete-announcement" data-item="${item.id}">Excluir</button></div>` : ''}
        </article>
      `;
    }).join('');
  }

  if (canPublish) {
    const form = document.getElementById('announcement-form');
    form.addEventListener('submit', (event) => {
      event.preventDefault();

      const title = document.getElementById('announcement-title').value.trim();
      const content = document.getElementById('announcement-content').value.trim();
      const audience = document.getElementById('announcement-audience').value;

      if (title.length < 3 || content.length < 3) {
        showNotification('Titulo e conteudo precisam ter ao menos 3 caracteres.', 'warning');
        return;
      }

      announcements.push(normalizeAnnouncement({
        id: createId(),
        title,
        content,
        author: session.username,
        audience,
        createdAt: Date.now()
      }));

      saveData();
      addLog(`Publicou comunicado "${title}"`);
      form.reset();
      drawAnnouncements();
      showNotification('Comunicado publicado.', 'success');
    });
  }

  list.addEventListener('click', (event) => {
    const button = event.target.closest('button[data-action="delete-announcement"]');
    if (!button) return;

    const itemId = Number(button.dataset.item);
    const item = announcements.find((announcement) => announcement.id === itemId);
    if (!item) return;

    const canDelete = session.role === 'superadmin' || item.author === session.username;
    if (!canDelete) {
      showNotification('Voce nao pode excluir este comunicado.', 'error');
      return;
    }

    if (!confirm('Excluir este comunicado?')) return;

    announcements = announcements.filter((announcement) => announcement.id !== item.id);
    saveData();
    addLog(`Excluiu comunicado "${item.title}"`);
    drawAnnouncements();
    showNotification('Comunicado excluido.', 'success');
  });

  drawAnnouncements();
}

function renderKnowledge() {
  els.content.innerHTML = `
    <section class="panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">Base de conhecimento</h2>
          <p class="panel-subtitle">Artigos curtos para acelerar resolucao e padronizar atendimento.</p>
        </div>
      </div>

      <div class="filters" style="grid-template-columns: 1fr;">
        <input id="knowledge-search" type="text" placeholder="Buscar por titulo, categoria ou conteudo" />
      </div>

      <div id="knowledge-list" class="knowledge-grid"></div>
    </section>
  `;

  const searchInput = document.getElementById('knowledge-search');
  const list = document.getElementById('knowledge-list');

  function drawArticles() {
    const query = searchInput.value.trim().toLowerCase();
    const filtered = KNOWLEDGE_BASE.filter((article) => {
      if (!query) return true;
      return article.title.toLowerCase().includes(query)
        || article.category.toLowerCase().includes(query)
        || article.content.toLowerCase().includes(query);
    });

    if (filtered.length === 0) {
      list.innerHTML = '<div class="empty-state">Nenhum artigo encontrado para o filtro atual.</div>';
      return;
    }

    list.innerHTML = filtered.map((article) => `
      <details class="knowledge-item">
        <summary>${escapeHtml(article.title)} <small style="color: var(--muted);">(${escapeHtml(article.category)})</small></summary>
        <p>${escapeHtml(article.content)}</p>
      </details>
    `).join('');
  }

  searchInput.addEventListener('input', drawArticles);
  drawArticles();
}

function generatePassword(length, options) {
  const pools = [];
  if (options.lower) pools.push('abcdefghijklmnopqrstuvwxyz');
  if (options.upper) pools.push('ABCDEFGHIJKLMNOPQRSTUVWXYZ');
  if (options.numbers) pools.push('0123456789');
  if (options.symbols) pools.push('!@#$%&*()-_=+[]{}?');

  if (pools.length === 0) return '';

  let password = '';
  for (let i = 0; i < length; i += 1) {
    const pool = pools[Math.floor(Math.random() * pools.length)];
    const char = pool[Math.floor(Math.random() * pool.length)];
    password += char;
  }

  return password;
}

function updateFocusDisplay() {
  const display = document.getElementById('focus-time');
  const status = document.getElementById('focus-status');
  if (!display || !status) return;

  const minutes = Math.floor(focusSecondsLeft / 60);
  const seconds = focusSecondsLeft % 60;
  display.textContent = `${String(minutes).padStart(2, '0')}:${String(seconds).padStart(2, '0')}`;
  status.textContent = focusRunning ? 'Rodando' : 'Pausado';
}

function startFocusTimer() {
  if (focusRunning) return;
  focusRunning = true;
  updateFocusDisplay();

  focusInterval = setInterval(() => {
    focusSecondsLeft -= 1;

    if (focusSecondsLeft <= 0) {
      focusSecondsLeft = 0;
      clearInterval(focusInterval);
      focusInterval = null;
      focusRunning = false;
      updateFocusDisplay();
      showNotification('Ciclo de foco finalizado.', 'success');
      return;
    }

    updateFocusDisplay();
  }, 1000);
}

function pauseFocusTimer() {
  if (focusInterval) {
    clearInterval(focusInterval);
    focusInterval = null;
  }
  focusRunning = false;
  updateFocusDisplay();
}

function resetFocusTimer() {
  pauseFocusTimer();
  focusSecondsLeft = FOCUS_DEFAULT_SECONDS;
  updateFocusDisplay();
}

function renderTools() {
  els.content.innerHTML = `
    <section class="panel">
      <div class="panel-header">
        <div>
          <h2 class="panel-title">Ferramentas rapidas</h2>
          <p class="panel-subtitle">Recursos simples para produtividade no dia a dia.</p>
        </div>
      </div>

      <div class="tool-grid">
        <article class="tool-card">
          <h3 class="panel-title">Gerador de senha</h3>
          <label for="pwd-length">Tamanho</label>
          <input id="pwd-length" type="number" min="6" max="64" value="14" />

          <label><input id="pwd-lower" type="checkbox" checked /> minusculas</label>
          <label><input id="pwd-upper" type="checkbox" checked /> maiusculas</label>
          <label><input id="pwd-number" type="checkbox" checked /> numeros</label>
          <label><input id="pwd-symbol" type="checkbox" /> simbolos</label>

          <div class="inline-actions">
            <button id="pwd-generate" class="btn-primary">Gerar</button>
            <button id="pwd-copy" class="btn-secondary">Copiar</button>
          </div>

          <div id="pwd-output" class="tool-output">Clique em gerar para criar uma senha.</div>
        </article>

        <article class="tool-card">
          <h3 class="panel-title">Contador de texto</h3>
          <textarea id="text-counter-input" placeholder="Cole um texto para contar palavras, caracteres e linhas."></textarea>
          <div class="tool-output" id="text-counter-output">0 caracteres | 0 palavras | 0 linhas</div>
        </article>

        <article class="tool-card">
          <h3 class="panel-title">Timer de foco (25:00)</h3>
          <div id="focus-time" class="focus-display">25:00</div>
          <div id="focus-status" class="tool-output" style="text-align:center;">Pausado</div>
          <div class="inline-actions">
            <button id="focus-start" class="btn-primary">Iniciar</button>
            <button id="focus-pause" class="btn-secondary">Pausar</button>
            <button id="focus-reset" class="btn-ghost">Resetar</button>
          </div>
        </article>

        <article class="tool-card">
          <h3 class="panel-title">Plano de 10 anos</h3>
          <label for="plan-value-tool">Valor total</label>
          <input id="plan-value-tool" type="number" min="1" step="0.01" placeholder="Ex: 1200" />

          <label for="plan-rate-tool">Juros anual (%)</label>
          <input id="plan-rate-tool" type="number" min="0" max="40" step="0.01" value="0" />

          <div class="inline-actions">
            <button id="plan-calc-tool" class="btn-primary">Calcular</button>
          </div>
          <div id="plan-output-tool" class="tool-output">Descubra quanto ficaria por mes durante 10 anos.</div>

          <label for="dopamine-target-tool">Meta dopamina (R$)</label>
          <input id="dopamine-target-tool" type="number" min="1" step="0.01" value="100" />
          <button id="dopamine-rush-tool" class="btn-secondary">Gerar desafio</button>
          <div id="dopamine-rush-output" class="tool-output">Clique para gerar um desafio rapido de motivacao.</div>
        </article>
      </div>
    </section>
  `;

  const pwdLength = document.getElementById('pwd-length');
  const pwdLower = document.getElementById('pwd-lower');
  const pwdUpper = document.getElementById('pwd-upper');
  const pwdNumber = document.getElementById('pwd-number');
  const pwdSymbol = document.getElementById('pwd-symbol');
  const pwdOutput = document.getElementById('pwd-output');
  const pwdGenerate = document.getElementById('pwd-generate');
  const pwdCopy = document.getElementById('pwd-copy');

  function generateAndShowPassword() {
    const length = Math.max(6, Math.min(64, Number(pwdLength.value) || 14));
    const password = generatePassword(length, {
      lower: pwdLower.checked,
      upper: pwdUpper.checked,
      numbers: pwdNumber.checked,
      symbols: pwdSymbol.checked
    });

    if (!password) {
      showNotification('Selecione pelo menos um tipo de caractere.', 'warning');
      return;
    }

    pwdOutput.textContent = password;
  }

  pwdGenerate.addEventListener('click', generateAndShowPassword);

  pwdCopy.addEventListener('click', async () => {
    const text = pwdOutput.textContent || '';
    if (!text || text.includes('Clique em gerar')) {
      showNotification('Gere uma senha antes de copiar.', 'warning');
      return;
    }

    try {
      await navigator.clipboard.writeText(text);
      showNotification('Senha copiada para a area de transferencia.', 'success');
    } catch {
      showNotification('Nao foi possivel copiar automaticamente.', 'error');
    }
  });

  const textInput = document.getElementById('text-counter-input');
  const textOutput = document.getElementById('text-counter-output');

  textInput.addEventListener('input', () => {
    const text = textInput.value;
    const chars = text.length;
    const words = text.trim() ? text.trim().split(/\s+/).length : 0;
    const lines = text ? text.split(/\n/).length : 0;
    textOutput.textContent = `${chars} caracteres | ${words} palavras | ${lines} linhas`;
  });

  const focusStart = document.getElementById('focus-start');
  const focusPause = document.getElementById('focus-pause');
  const focusReset = document.getElementById('focus-reset');
  const planCalcTool = document.getElementById('plan-calc-tool');
  const planValueTool = document.getElementById('plan-value-tool');
  const planRateTool = document.getElementById('plan-rate-tool');
  const planOutputTool = document.getElementById('plan-output-tool');
  const dopamineTargetTool = document.getElementById('dopamine-target-tool');
  const dopamineRushTool = document.getElementById('dopamine-rush-tool');
  const dopamineRushOutput = document.getElementById('dopamine-rush-output');

  focusStart.addEventListener('click', startFocusTimer);
  focusPause.addEventListener('click', pauseFocusTimer);
  focusReset.addEventListener('click', resetFocusTimer);

  planCalcTool.addEventListener('click', () => {
    const value = toPositiveAmount(planValueTool.value);
    const rate = Number(planRateTool.value) || 0;
    if (!value) {
      showNotification('Informe um valor para calcular.', 'warning');
      return;
    }
    const result = calculateTenYearPlan(value, rate);
    planOutputTool.innerHTML = `<strong>${formatCurrency(result.monthly)}/mes</strong> por 10 anos | Total ${formatCurrency(result.total)} | Juros ${formatCurrency(result.interest)}`;
  });

  dopamineRushTool.addEventListener('click', () => {
    const target = Math.max(1, toPositiveAmount(dopamineTargetTool.value));
    const challengeA = (target * 0.15).toFixed(2);
    const challengeB = (target / 12).toFixed(2);
    const challengeC = (target / 120).toFixed(2);
    dopamineRushOutput.innerHTML = `
      Desafio: pagar <strong>${formatCurrency(challengeA)}</strong> em 7 dias.<br />
      Rota anual: <strong>${formatCurrency(challengeB)}</strong>/mes por 12 meses.<br />
      Rota 10 anos: <strong>${formatCurrency(challengeC)}</strong>/mes por 120 meses.
    `;
  });

  updateFocusDisplay();
}
function renderProfile() {
  const user = getCurrentUser();
  if (!user) {
    renderAccessDenied('Sessao invalida. Faca login novamente.');
    return;
  }
  const canChangePassword = isEsther();

  const createdTickets = tickets.filter((ticket) => ticket.creator === user.username).length;
  const assignedTickets = tickets.filter((ticket) => ticket.assignedAdmin === user.username).length;
  const pendingTasks = getUserTasks().filter((task) => task.status !== 'done').length;

  els.content.innerHTML = `
    <section class="panel grid-2">
      <div>
        <h2 class="panel-title">Meu perfil</h2>
        <p class="panel-subtitle">Dados da conta e indicadores rapidos.</p>

        <div class="kpi-list">
          <div class="kpi-item"><strong>Usuario</strong><span>${escapeHtml(user.username)}</span></div>
          <div class="kpi-item"><strong>Papel</strong><span>${escapeHtml(roleLabel(user.role))}</span></div>
          <div class="kpi-item"><strong>Status</strong><span>${escapeHtml(user.status)}</span></div>
          <div class="kpi-item"><strong>Divida atual</strong><span>${formatCurrency(user.debt)}</span></div>
          <div class="kpi-item"><strong>Total cobrado</strong><span>${formatCurrency(user.totalCharged)}</span></div>
          <div class="kpi-item"><strong>Total pago</strong><span>${formatCurrency(user.totalPaid)}</span></div>
          <div class="kpi-item"><strong>Lucro acumulado</strong><span>${formatCurrency(user.walletProfit)}</span></div>
          <div class="kpi-item"><strong>Tickets criados</strong><span>${createdTickets}</span></div>
          <div class="kpi-item"><strong>Tickets atribuidos</strong><span>${assignedTickets}</span></div>
          <div class="kpi-item"><strong>Tarefas em aberto</strong><span>${pendingTasks}</span></div>
        </div>

        <form id="profile-finance-form" class="form-grid" style="margin-top: 12px;">
          <div>
            <label for="profile-payment-amount">Pagar agora</label>
            <input id="profile-payment-amount" type="number" min="1" step="0.01" placeholder="Ex: 30.00" />
          </div>
          <div class="inline-actions" style="align-items:end;">
            <button class="btn-primary" type="submit">Registrar pagamento</button>
          </div>
        </form>
      </div>

      <div>
        <h3 class="panel-title">Alterar senha</h3>
        <p class="panel-subtitle">${canChangePassword ? 'Como Esther, voce pode alterar sua senha.' : 'Senha padrao controlada pela Esther (1705).'}</p>
        <form id="password-form" class="form-grid">
          <div class="full">
            <label for="current-password">Senha atual</label>
            <input id="current-password" type="password" ${canChangePassword ? 'required' : 'disabled'} />
          </div>
          <div class="full">
            <label for="new-password-profile">Nova senha</label>
            <input id="new-password-profile" type="password" ${canChangePassword ? 'required' : 'disabled'} />
          </div>
          <div class="full">
            <label for="confirm-password-profile">Confirmar nova senha</label>
            <input id="confirm-password-profile" type="password" ${canChangePassword ? 'required' : 'disabled'} />
          </div>
          <div class="full inline-actions">
            <button class="btn-primary" type="submit" ${canChangePassword ? '' : 'disabled'}>Atualizar senha</button>
          </div>
        </form>
      </div>
    </section>
  `;

  const form = document.getElementById('password-form');
  const financeForm = document.getElementById('profile-finance-form');

  form.addEventListener('submit', (event) => {
    event.preventDefault();

    if (!canChangePassword) {
      showNotification('Somente Esther pode alterar senhas.', 'warning');
      return;
    }

    const current = document.getElementById('current-password').value;
    const next = document.getElementById('new-password-profile').value.trim();
    const confirmNext = document.getElementById('confirm-password-profile').value.trim();

    if (current !== user.password) {
      showNotification('Senha atual incorreta.', 'error');
      return;
    }

    if (next.length < 4) {
      showNotification('Nova senha deve ter pelo menos 4 caracteres.', 'warning');
      return;
    }

    if (next !== confirmNext) {
      showNotification('Confirmacao de senha nao confere.', 'warning');
      return;
    }

    user.password = next;
    saveData();
    addLog('Atualizou a propria senha');
    form.reset();
    showNotification('Senha atualizada com sucesso.', 'success');
  });

  financeForm.addEventListener('submit', (event) => {
    event.preventDefault();
    const amount = toPositiveAmount(document.getElementById('profile-payment-amount').value);
    if (!amount) {
      showNotification('Informe um valor valido para pagamento.', 'warning');
      return;
    }
    const result = registerFinanceEvent(user, 'payment', amount, 'Pagamento pelo perfil');
    if (!result.ok) {
      showNotification(result.message, 'warning');
      return;
    }
    addLog(`Pagamento proprio no perfil: ${formatCurrency(result.amount)} (lucro ${formatCurrency(result.profitAdded || 0)})`);
    const profitText = result.profitAdded > 0 ? ` | lucro ${formatCurrency(result.profitAdded)}` : '';
    showNotification(`Pagamento aplicado: ${formatCurrency(result.amount)}${profitText}.`, 'success');
    renderProfile();
  });

}

function renderAccessDenied(message) {
  els.content.innerHTML = `
    <section class="panel">
      <h2 class="panel-title">Acesso restrito</h2>
      <p class="panel-subtitle">${escapeHtml(message)}</p>
    </section>
  `;
}

function openChat(ticketId) {
  const ticket = tickets.find((item) => item.id === ticketId);
  if (!ticket) {
    showNotification('Ticket nao encontrado.', 'error');
    return;
  }

  if (!canAccessTicket(ticket)) {
    showNotification('Voce nao tem permissao para este chat.', 'error');
    return;
  }

  activeChatTicketId = ticketId;
  els.chatTitle.textContent = `Conversa #${ticket.id} - ${ticket.title}`;
  renderChatMessages(ticket);

  els.chatModal.classList.remove('hidden');
  els.chatInput.value = '';
  els.chatInput.focus();
}

function renderChatMessages(ticket) {
  if (!ticket.messages || ticket.messages.length === 0) {
    els.chatMessages.innerHTML = '<div class="empty-state">Nenhuma mensagem ainda. Inicie a conversa.</div>';
    return;
  }

  els.chatMessages.innerHTML = ticket.messages.map((message) => {
    const mine = message.sender === session.username;
    return `
      <div class="chat-message ${mine ? 'mine' : ''}">
        <strong>${escapeHtml(message.sender)}</strong><br />
        <span>${escapeHtml(message.content)}</span>
        <small>${formatDateTime(message.timestamp)}</small>
      </div>
    `;
  }).join('');

  els.chatMessages.scrollTop = els.chatMessages.scrollHeight;
}

function sendMessage() {
  const text = els.chatInput.value.trim();
  if (!text) return;

  const ticket = tickets.find((item) => item.id === activeChatTicketId);
  if (!ticket) {
    showNotification('Ticket nao encontrado.', 'error');
    return;
  }

  if (!canAccessTicket(ticket)) {
    showNotification('Voce nao pode enviar mensagem neste ticket.', 'error');
    return;
  }

  ticket.messages.push({
    sender: session.username,
    content: text,
    timestamp: Date.now()
  });
  ticket.updatedAt = Date.now();

  saveData();
  addLog(`Enviou mensagem no ticket #${ticket.id}`);

  els.chatInput.value = '';
  renderChatMessages(ticket);
}

function closeChat() {
  els.chatModal.classList.add('hidden');
  activeChatTicketId = null;
}

function bindStaticEvents() {
  els.loginButton.addEventListener('click', handleLogin);

  els.passwordInput.addEventListener('keydown', (event) => {
    if (event.key === 'Enter') {
      event.preventDefault();
      handleLogin();
    }
  });

  els.usernameInput.addEventListener('keydown', (event) => {
    if (event.key === 'Enter') {
      event.preventDefault();
      handleLogin();
    }
  });

  els.logoutButton.addEventListener('click', handleLogout);

  els.sendMessageButton.addEventListener('click', sendMessage);
  els.chatInput.addEventListener('keydown', (event) => {
    if (event.key === 'Enter') {
      event.preventDefault();
      sendMessage();
    }
  });

  els.closeChatButton.addEventListener('click', closeChat);
  els.chatOverlay.addEventListener('click', closeChat);

  els.menuToggle.addEventListener('click', () => {
    els.appContainer.classList.toggle('sidebar-open');
  });

  window.addEventListener('resize', () => {
    if (window.innerWidth > 920) {
      els.appContainer.classList.remove('sidebar-open');
    }
  });

  window.addEventListener('beforeunload', () => {
    destroyStealthChat(false);
    stopEspionageAutoRefresh();
    if (session && session.username) {
      registerUserOffline(session.username);
    }
    removePresence();
    disconnectPresenceSocket(false);
  });

  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && !els.chatModal.classList.contains('hidden')) {
      closeChat();
    }
  });

  window.addEventListener('storage', (event) => {
    if (event.key === STORAGE_KEYS.stealthBus) {
      syncStealthFromStorage();
    }
    if (event.key === STORAGE_KEYS.liveBroadcast) {
      showLiveBroadcastBanner(readLiveBroadcast());
      if (currentPage === 'espionage') {
        renderEspionage();
      }
    }
    if (event.key === STORAGE_KEYS.presence && currentPage === 'intelCenter') {
      renderIntelCenter();
    }
    if ((event.key === STORAGE_KEYS.logs || event.key === STORAGE_KEYS.users) && currentPage === 'intelCenter') {
      const refreshedLogs = safeParse(localStorage.getItem(STORAGE_KEYS.logs), logs);
      const refreshedUsers = safeParse(localStorage.getItem(STORAGE_KEYS.users), users);
      logs = Array.isArray(refreshedLogs) ? refreshedLogs : logs;
      users = Array.isArray(refreshedUsers) ? refreshedUsers.map(normalizeUser) : users;
      renderIntelCenter();
    }
    if ((event.key === STORAGE_KEYS.logs || event.key === STORAGE_KEYS.presence) && currentPage === 'logs') {
      const refreshedLogs = safeParse(localStorage.getItem(STORAGE_KEYS.logs), logs);
      logs = Array.isArray(refreshedLogs) ? refreshedLogs : logs;
      renderLogs();
    }
    if ((event.key === STORAGE_KEYS.users || event.key === STORAGE_KEYS.settings) && currentPage === 'dashboard') {
      const refreshedUsers = safeParse(localStorage.getItem(STORAGE_KEYS.users), users);
      const refreshedSettings = safeParse(localStorage.getItem(STORAGE_KEYS.settings), settings);
      users = Array.isArray(refreshedUsers) ? refreshedUsers.map(normalizeUser) : users;
      settings = normalizeSettings(refreshedSettings);
      renderDashboard();
    }
    if ((event.key === STORAGE_KEYS.users || event.key === STORAGE_KEYS.settings) && currentPage === 'walletControl') {
      const refreshedUsers = safeParse(localStorage.getItem(STORAGE_KEYS.users), users);
      const refreshedSettings = safeParse(localStorage.getItem(STORAGE_KEYS.settings), settings);
      users = Array.isArray(refreshedUsers) ? refreshedUsers.map(normalizeUser) : users;
      settings = normalizeSettings(refreshedSettings);
      renderWalletControl();
    }
    if ((event.key === STORAGE_KEYS.users || event.key === STORAGE_KEYS.presence || event.key === STORAGE_KEYS.logs) && currentPage === 'espionage') {
      const refreshedUsers = safeParse(localStorage.getItem(STORAGE_KEYS.users), users);
      const refreshedLogs = safeParse(localStorage.getItem(STORAGE_KEYS.logs), logs);
      users = Array.isArray(refreshedUsers) ? refreshedUsers.map(normalizeUser) : users;
      logs = Array.isArray(refreshedLogs) ? refreshedLogs : logs;
      renderEspionage();
    }
  });
}

function restoreLastUserHint() {
  const uiState = safeParse(localStorage.getItem(STORAGE_KEYS.ui), {});
  if (uiState && typeof uiState.lastUser === 'string') {
    els.usernameInput.value = uiState.lastUser;
  }
}

function bootstrap() {
  initData();
  bindStaticEvents();
  restoreLastUserHint();
}

bootstrap();
