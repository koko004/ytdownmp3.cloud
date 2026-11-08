const express = require('express');
const http = require('http');
const { Queue } = require('bullmq');
const Redis = require('ioredis');
const os = require('os');
const fs = require('fs');
const path = require('path');
const { execSync, exec } = require('child_process');
const crypto = require('crypto');
const session = require('express-session');
const geoip = require('geoip-lite');
const { WebSocketServer } = require('ws');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public'), { index: false }));

const PORT = process.env.PORT || 4000;
const REDIS_HOST = process.env.REDIS_HOST || 'redis';
const REDIS_PORT = process.env.REDIS_PORT || 6379;
const ADMIN_USER = process.env.ADMIN_USER || 'admin';
const ADMIN_PASS = process.env.ADMIN_PASS || 'changeme123';
const SESSION_SECRET = process.env.SESSION_SECRET || crypto.randomBytes(32).toString('hex');

const conversionQueue = new Queue('conversions', {
  connection: { host: REDIS_HOST, port: REDIS_PORT },
});

const redisClient = new Redis({ host: REDIS_HOST, port: REDIS_PORT, maxRetriesPerRequest: 1 });

const visitorLog = [];
const history = {
  network: [],
  visitors: [],
  queue: [],
  cpu: [],
  memory: [],
  disk: [],
};
const HISTORY_MAX = 288;

// In Docker the host's /proc, /sys and /etc/os-release are bind-mounted
// read-only under /host. Native installs have no /host, so use the local paths.
const HOST_PREFIX = fs.existsSync('/host/proc/cpuinfo') ? '/host' : '';

function readHostFile(p) {
  return fs.readFileSync(HOST_PREFIX + p, 'utf8');
}

function getLinuxDistro() {
  for (const p of [`${HOST_PREFIX}/etc/os-release`, '/etc/os-release']) {
    try {
      const osRelease = fs.readFileSync(p, 'utf8');
      const name = osRelease.match(/^PRETTY_NAME="?([^"\n]+)"?/m);
      if (name) return name[1];
    } catch (e) {}
  }
  return 'Linux';
}

function getSystemStats() {
  let cpus = os.cpus();
  let totalMem = os.totalmem();
  let freeMem = os.freemem();
  let usedMem = totalMem - freeMem;

  try {
    const cpuInfo = readHostFile('/proc/cpuinfo');
    const processorCount = (cpuInfo.match(/^processor\s*:/gm) || []).length;
    const modelMatch = cpuInfo.match(/^model name\s*:\s*(.+)/m);
    const speedMatch = cpuInfo.match(/^cpu MHz\s*:\s*(.+)/m);
    if (processorCount > 0) {
      cpus = Array(processorCount).fill({
        model: modelMatch ? modelMatch[1].trim() : cpus[0]?.model || 'unknown',
        speed: speedMatch ? parseFloat(speedMatch[1]) : cpus[0]?.speed || 0,
        times: { user: 0, nice: 0, sys: 0, idle: 0, irq: 0 }
      });
    }
  } catch (e) {}

  try {
    const meminfo = readHostFile('/proc/meminfo');
    const getVal = (key) => {
      const m = meminfo.match(new RegExp(`${key}:\\s+(\\d+)`));
      return m ? parseInt(m[1]) * 1024 : 0;
    };
    totalMem = getVal('MemTotal');
    freeMem = getVal('MemAvailable') || getVal('MemFree');
    usedMem = totalMem - freeMem;
  } catch (e) {}

  let loadAvg = [0, 0, 0];
  try {
    const load = os.loadavg();
    loadAvg = [load[0].toFixed(2), load[1].toFixed(2), load[2].toFixed(2)];
  } catch (e) {}

  let diskUsage = { total: 0, used: 0, free: 0 };
  try {
    const df = execSync("df -B1 / | tail -1").toString().trim().split(/\s+/);
    diskUsage = { total: parseInt(df[1]), used: parseInt(df[2]), free: parseInt(df[3]) };
  } catch (e) {}

  let swapInfo = { total: 0, used: 0, free: 0 };
  try {
    const meminfo = readHostFile('/proc/meminfo');
    const getValue = (key) => {
      const match = meminfo.match(new RegExp(`${key}:\\s+(\\d+)`));
      return match ? parseInt(match[1]) * 1024 : 0;
    };
    swapInfo = { total: getValue('SwapTotal'), used: getValue('SwapTotal') - getValue('SwapFree'), free: getValue('SwapFree') };
  } catch (e) {}

  let cpuTemp = null;
  try {
    const thermal = execSync(`cat ${HOST_PREFIX}/sys/class/thermal/thermal_zone0/temp 2>/dev/null || echo 0`).toString().trim();
    const temp = parseInt(thermal);
    if (temp > 0) cpuTemp = (temp / 1000).toFixed(1) + '°C';
  } catch (e) {}

  let cpuSpeed = cpus[0]?.speed || 0;

  let networkInfo = { interfaces: [], totalRx: 0, totalTx: 0 };
  try {
    const netDev = readHostFile('/proc/net/dev');
    const lines = netDev.split('\n').slice(2);
    for (const line of lines) {
      if (!line.trim()) continue;
      const parts = line.trim().split(/[\s:]+/);
      if (parts[0] === 'lo') continue;
      const rx = parseInt(parts[1]);
      const tx = parseInt(parts[9]);
      networkInfo.interfaces.push({
        name: parts[0],
        rxBytes: rx,
        txBytes: tx,
        rxPackets: parseInt(parts[2]),
        txPackets: parseInt(parts[10]),
      });
      networkInfo.totalRx += rx;
      networkInfo.totalTx += tx;
    }
  } catch (e) {}

  let cpuUsage = { avgUsage: '0', usage: [] };
  try {
    const stat = readHostFile('/proc/stat');
    const lines = stat.split('\n');
    const cpuLines = lines.filter(l => /^cpu\d/.test(l));
    cpuUsage.usage = cpuLines.map((line, i) => {
      const parts = line.trim().split(/\s+/);
      const user = parseInt(parts[1]) || 0;
      const nice = parseInt(parts[2]) || 0;
      const sys = parseInt(parts[3]) || 0;
      const idle = parseInt(parts[4]) || 0;
      const irq = parseInt(parts[5]) || 0;
      const total = user + nice + sys + idle + irq;
      const usagePercent = total > 0 ? ((total - idle) / total * 100).toFixed(1) : '0';
      return { user, nice, sys, idle, irq, usagePercent };
    });
    const totalAll = cpuUsage.usage.reduce((acc, c) => acc + parseFloat(c.usagePercent), 0);
    cpuUsage.avgUsage = cpuLines.length > 0 ? (totalAll / cpuLines.length).toFixed(1) : '0';
  } catch (e) {}

  return {
    hostname: os.hostname(),
    platform: os.platform(),
    arch: os.arch(),
    distro: getLinuxDistro(),
    uptime: os.uptime(),
    cpu: {
      model: cpus[0]?.model || 'unknown',
      speed: cpuSpeed,
      cores: cpus.length,
      threads: cpus.length,
      temperature: cpuTemp,
      usage: cpuUsage.usage,
      avgUsage: cpuUsage.avgUsage,
    },
    memory: { total: totalMem, used: usedMem, free: freeMem, usagePercent: (usedMem / totalMem * 100).toFixed(1) },
    swap: swapInfo,
    loadAvg,
    disk: diskUsage,
    network: networkInfo,
  };
}

async function getQueueStats() {
  try {
    const [waiting, active, completed, failed, delayed] = await Promise.all([
      conversionQueue.getWaitingCount(),
      conversionQueue.getActiveCount(),
      conversionQueue.getCompletedCount(),
      conversionQueue.getFailedCount(),
      conversionQueue.getDelayedCount(),
    ]);
    const recentJobs = await conversionQueue.getJobs(['completed', 'failed', 'active', 'waiting', 'delayed'], 0, 99);
    const jobHistory = recentJobs.map(j => ({
      id: j.id, jobId: j.data.jobId, url: j.data.url, format: j.data.format,
      status: j.state, timestamp: j.timestamp,
      duration: j.finishedOn && j.processedOn ? j.finishedOn - j.processedOn : null,
    })).sort((a, b) => (b.timestamp || 0) - (a.timestamp || 0));
    return { counts: { waiting, active, completed, failed, delayed, total: waiting + active + completed + failed + delayed }, recentJobs, jobHistory };
  } catch (err) {
    console.error('[Admin] Queue stats error:', err);
    return { counts: { waiting: 0, active: 0, completed: 0, failed: 0, delayed: 0, total: 0 }, recentJobs: [], jobHistory: [] };
  }
}

function getMachineIPs() {
  const ips = [];
  try {
    const output = readHostFile('/proc/net/fib_trie');
    const sections = output.split('Main');
    for (const section of sections) {
      const lines = section.split('\n');
      let currentIface = '';
      for (const line of lines) {
        const ifaceMatch = line.match(/^(\s+)(\S+)$/);
        if (ifaceMatch && !ifaceMatch[2].match(/^\d/)) { currentIface = ifaceMatch[2]; continue; }
        const ipMatch = line.match(/\/32 host LOCAL\s+(\d+\.\d+\.\d+\.\d+)/);
        if (ipMatch && !ipMatch[1].startsWith('127.') && ipMatch[1] !== '0.0.0.0') {
          const exists = ips.find(i => i.ip === ipMatch[1]);
          if (!exists) ips.push({ interface: currentIface || 'unknown', ip: ipMatch[1], netmask: '255.255.255.0', mac: '' });
        }
      }
    }
  } catch (e) {}
  try {
    const nets = os.networkInterfaces();
    for (const name of Object.keys(nets)) {
      for (const net of nets[name]) {
        if (net.family === 'IPv4' && !net.internal) {
          const exists = ips.find(i => i.ip === net.address);
          if (!exists) ips.push({ interface: name, ip: net.address, netmask: net.netmask, mac: net.mac });
        }
      }
    }
  } catch (e) {}
  return ips;
}

async function fetchWithTimeout(url, ms = 3000) {
    const controller = new AbortController();
    const id = setTimeout(() => controller.abort(), ms);
    try {
      return await fetch(url, { signal: controller.signal });
    } finally {
      clearTimeout(id);
    }
  }

  async function checkServices() {
    const services = [];
    
    try {
      await redisClient.ping();
      services.push({ name: 'Redis', status: 'running', detail: 'Connected' });
    } catch (e) {
      services.push({ name: 'Redis', status: 'stopped', detail: e.message });
    }

    try {
      const apiRes = await fetchWithTimeout(process.env.API_HEALTH_URL || 'http://172.17.0.1:8081/health');
      if (apiRes.ok) {
        const data = await apiRes.json();
        services.push({ name: 'API', status: 'running', detail: data.status });
      } else {
        services.push({ name: 'API', status: 'stopped', detail: `HTTP ${apiRes.status}` });
      }
    } catch (e) {
      services.push({ name: 'API', status: 'stopped', detail: e.message });
    }

    try {
      const activeCount = await conversionQueue.getActiveCount();
      const waitingCount = await conversionQueue.getWaitingCount();
      if (activeCount > 0 || waitingCount >= 0) {
        services.push({ name: 'Worker', status: 'running', detail: `Active: ${activeCount}, Waiting: ${waitingCount}` });
      } else {
        services.push({ name: 'Worker', status: 'unknown', detail: 'No queue activity' });
      }
    } catch (e) {
      services.push({ name: 'Worker', status: 'error', detail: e.message });
    }

    try {
      const frontendRes = await fetchWithTimeout(process.env.FRONTEND_HEALTH_URL || 'http://frontend:80/health').catch(() => null);
      if (frontendRes?.ok) {
        services.push({ name: 'Frontend', status: 'running', detail: 'Healthy' });
      } else {
        services.push({ name: 'Frontend', status: 'unknown', detail: 'No health endpoint' });
      }
    } catch (e) {
      services.push({ name: 'Frontend', status: 'unknown', detail: e.message });
    }

    try {
      const df = execSync("df -B1 / | tail -1").toString().trim().split(/\s+/);
      const usedPct = (parseInt(df[2]) / parseInt(df[1]) * 100).toFixed(1);
      services.push({ name: 'Disk', status: usedPct > 90 ? 'error' : usedPct > 80 ? 'warning' : 'running', detail: `${usedPct}% used` });
    } catch (e) {
      services.push({ name: 'Disk', status: 'error', detail: e.message });
    }

    const load = os.loadavg();
    const loadStatus = load[0] > os.cpus().length * 2 ? 'error' : load[0] > os.cpus().length ? 'warning' : 'running';
    services.push({ name: 'Load', status: loadStatus, detail: `1m: ${load[0].toFixed(2)}, 5m: ${load[1].toFixed(2)}, 15m: ${load[2].toFixed(2)}` });

    return services;
  }

function trackVisitor(req) {
  const ip = req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.socket.remoteAddress?.replace('::ffff:', '');
  const now = Date.now();
  const ua = req.headers['user-agent'] || 'Unknown';
  const geo = geoip.lookup(ip) || { country: 'Unknown', city: '', region: '', ll: [0, 0] };
  const existing = visitorLog.find(v => v.ip === ip);

  if (existing) {
    existing.lastSeen = now;
    existing.visits++;
    existing.userAgent = ua;
    if (!existing.pageViews) existing.pageViews = [];
    existing.pageViews.push({ path: req.path, time: now });
    if (existing.pageViews.length > 100) existing.pageViews = existing.pageViews.slice(-100);
  } else {
    visitorLog.push({
      ip, firstSeen: now, lastSeen: now, visits: 1, userAgent: ua,
      country: geo.country, city: geo.city, region: geo.region, ll: geo.ll,
      pageViews: [{ path: req.path, time: now }],
    });
    if (visitorLog.length > 2000) visitorLog.shift();
  }
}

function recordHistory() {
  const sys = getSystemStats();
  const now = Date.now();
  history.cpu.push({ time: now, value: parseFloat(sys.cpu.avgUsage) });
  history.memory.push({ time: now, value: parseFloat(sys.memory.usagePercent) });
  history.disk.push({ time: now, value: (sys.disk.used / sys.disk.total * 100).toFixed(1) });
  history.network.push({ time: now, rx: sys.network.totalRx, tx: sys.network.totalTx });
  history.visitors.push({ time: now, count: visitorLog.length });
  getQueueStats().then(q => {
    history.queue.push({ time: now, value: q.counts.active });
    if (history.queue.length > HISTORY_MAX) history.queue = history.queue.slice(-HISTORY_MAX);
  }).catch(() => {});
  Object.keys(history).forEach(k => { if (k !== 'queue' && history[k].length > HISTORY_MAX) history[k] = history[k].slice(-HISTORY_MAX); });
}

app.set('trust proxy', 1);
app.use(session({
  secret: SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: { secure: 'auto', httpOnly: true, maxAge: 24 * 60 * 60 * 1000, sameSite: 'lax', path: '/' },
}));

app.use((req, res, next) => {
  if (req.path.startsWith('/api/') && !req.path.startsWith('/api/public/')) return next();
  trackVisitor(req);
  next();
});

const requireAuth = (req, res, next) => {
  if (req.session.authenticated) return next();
  if (req.path === '/login' || req.path === '/api/login') return next();
  if (req.originalUrl.startsWith('/api/')) return res.status(401).json({ error: 'Unauthorized' });
  res.redirect('/login');
};

app.get('/login', (req, res) => {
  if (req.session.authenticated) return res.redirect('/');
  res.sendFile(path.join(__dirname, 'public', 'login.html'));
});

app.post('/api/login', (req, res) => {
  const { username, password } = req.body;
  if (username === ADMIN_USER && password === ADMIN_PASS) {
    req.session.authenticated = true;
    req.session.user = username;
    return res.json({ success: true });
  }
  res.status(401).json({ error: 'Invalid credentials' });
});

app.post('/api/logout', (req, res) => {
  req.session.destroy(() => res.json({ success: true }));
});

app.get('/api/auth/status', (req, res) => {
  res.json({ authenticated: !!req.session.authenticated, user: req.session.user });
});

app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', service: 'admin', uptime: process.uptime() });
});

app.get('/api/version', (req, res) => {
  try {
    const version = fs.readFileSync(path.join(__dirname, '..', 'VERSION'), 'utf8').trim();
    res.json({ version });
  } catch (e) {
    res.json({ version: '1.10' });
  }
});

// Logo upload endpoint
app.post('/api/admin/logo', requireAuth, async (req, res) => {
  try {
    const { logoBase64 } = req.body;
    if (!logoBase64 || !logoBase64.startsWith('data:image/png;base64,')) {
      return res.status(400).json({ error: 'Invalid PNG image' });
    }
    
    const base64Data = logoBase64.replace('data:image/png;base64,', '');
    const pngBuffer = Buffer.from(base64Data, 'base64');
    
    // Save original PNG
    const pngPath = path.join(__dirname, 'public', 'admin-logo.png');
    fs.writeFileSync(pngPath, pngBuffer);
    
    // Convert to WebP (two sizes)
    const { execSync } = require('child_process');
    execSync(`cwebp -q 80 -resize 560 560 ${pngPath} -o ${path.join(__dirname, 'public', 'logo.webp')}`);
    execSync(`cwebp -q 80 -resize 280 280 ${pngPath} -o ${path.join(__dirname, 'public', 'logo-280.webp')}`);
    
    res.json({ success: true, message: 'Logo updated successfully' });
  } catch (e) {
    res.status(500).json({ error: 'Failed to process logo', detail: e.message });
  }
});

// Public API endpoints (no auth required)
app.get('/api/public/progress-bar', async (req, res) => {
  try {
    const type = await redisClient.get('config:progressBarType') || 'bubble';
    res.json({ type });
  } catch (e) {
    res.json({ type: 'bubble' });
  }
});

// Auth middleware for all other /api routes
app.use('/api', requireAuth);

app.get('/api/stats', async (req, res) => {
  try {
    const sys = getSystemStats();
    const queue = await getQueueStats();
    const ips = getMachineIPs();
    const services = await checkServices();

    const visitors = visitorLog.map(v => ({
      ip: v.ip, country: v.country, city: v.city, region: v.region, ll: v.ll,
      visits: v.visits, firstSeen: v.firstSeen, lastSeen: v.lastSeen,
      sessionDuration: ((v.lastSeen - v.firstSeen) / 1000 / 60).toFixed(1),
      userAgent: v.userAgent, recentPages: v.pageViews?.slice(-5) || [],
    }));

    const now = Date.now();
    const last24h = visitorLog.filter(v => now - v.lastSeen < 86400000);
    const last1h = visitorLog.filter(v => now - v.lastSeen < 3600000);

    res.json({
      system: sys, queue, visitors: { total: visitorLog.length, last24h: last24h.length, last1h: last1h.length, list: visitors },
      machineIPs: ips, services, history, timestamp: now,
    });
  } catch (err) {
    console.error('[Admin] Stats error:', err);
    res.status(500).json({ error: 'Failed to get stats' });
  }
});

app.get('/api/config/progress-bar', async (req, res) => {
  try {
    const type = await redisClient.get('config:progressBarType') || 'bubble';
    res.json({ type });
  } catch (e) {
    res.json({ type: 'bubble' });
  }
});

app.post('/api/config/progress-bar', async (req, res) => {
  try {
    const { type } = req.body;
    if (!['bubble', 'striped'].includes(type)) {
      return res.status(400).json({ error: 'Invalid type' });
    }
    await redisClient.set('config:progressBarType', type);
    res.json({ success: true, type });
  } catch (e) {
    res.status(500).json({ error: 'Failed to save' });
  }
});

// ========== PROXY MANAGEMENT API ==========

const PROXY_KEY = 'config:proxies';
const PROXY_STATS_KEY = 'config:proxy_stats';
const ROTATION_KEY = 'config:proxy_rotation';

async function getProxies() {
  const data = await redisClient.get(PROXY_KEY);
  return data ? JSON.parse(data) : [];
}

async function saveProxies(proxies) {
  await redisClient.set(PROXY_KEY, JSON.stringify(proxies));
}

async function getProxyStats() {
  const data = await redisClient.get(PROXY_STATS_KEY);
  return data ? JSON.parse(data) : {};
}

async function saveProxyStats(stats) {
  await redisClient.set(PROXY_STATS_KEY, JSON.stringify(stats));
}

async function getRotationConfig() {
  const data = await redisClient.get(ROTATION_KEY);
  return data ? JSON.parse(data) : { strategy: 'round_robin', currentIndex: 0 };
}

async function saveRotationConfig(config) {
  await redisClient.set(ROTATION_KEY, JSON.stringify(config));
}

// Get all proxies
app.get('/api/proxies', async (req, res) => {
  try {
    const proxies = await getProxies();
    const stats = await getProxyStats();
    const rotation = await getRotationConfig();
    res.json({ proxies, stats, rotation });
  } catch (e) {
    res.status(500).json({ error: 'Failed to get proxies' });
  }
});

// Add new proxy
app.post('/api/proxies', async (req, res) => {
  try {
    const { url, name, country, enabled = true } = req.body;
    if (!url || !url.startsWith('socks5://')) {
      return res.status(400).json({ error: 'URL must start with socks5://' });
    }
    const proxies = await getProxies();
    const newProxy = {
      id: crypto.randomUUID(),
      url,
      name: name || url,
      country: country || 'Unknown',
      enabled,
      createdAt: Date.now(),
      lastTested: null,
      latency: null
    };
    proxies.push(newProxy);
    await saveProxies(proxies);
    res.json({ success: true, proxy: newProxy });
  } catch (e) {
    res.status(500).json({ error: 'Failed to add proxy' });
  }
});

// Update proxy
app.put('/api/proxies/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const updates = req.body;
    const proxies = await getProxies();
    const idx = proxies.findIndex(p => p.id === id);
    if (idx === -1) return res.status(404).json({ error: 'Proxy not found' });
    proxies[idx] = { ...proxies[idx], ...updates };
    await saveProxies(proxies);
    res.json({ success: true, proxy: proxies[idx] });
  } catch (e) {
    res.status(500).json({ error: 'Failed to update proxy' });
  }
});

// Delete proxy
app.delete('/api/proxies/:id', async (req, res) => {
  try {
    const { id } = req.params;
    let proxies = await getProxies();
    proxies = proxies.filter(p => p.id !== id);
    await saveProxies(proxies);
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: 'Failed to delete proxy' });
  }
});

// Test single proxy
app.post('/api/proxies/:id/test', async (req, res) => {
  try {
    const { id } = req.params;
    const proxies = await getProxies();
    const proxy = proxies.find(p => p.id === id);
    if (!proxy) return res.status(404).json({ error: 'Proxy not found' });

    const start = Date.now();
    let success = false;
    let error = null;

    try {
      const { default: fetch } = await import('node-fetch');
      const agent = new (await import('socks-proxy-agent')).SocksProxyAgent(proxy.url);
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 10000);
      const response = await fetch('https://www.youtube.com', {
        agent,
        signal: controller.signal,
        method: 'HEAD'
      });
      clearTimeout(timeout);
      success = response.ok;
      if (!success) error = `HTTP ${response.status}`;
    } catch (e) {
      error = e.message;
    }

    const latency = Date.now() - start;
    const stats = await getProxyStats();
    stats[proxy.id] = stats[proxy.id] || { total: 0, success: 0, failed: 0, totalLatency: 0 };
    stats[proxy.id].total++;
    stats[proxy.id].totalLatency += latency;
    if (success) stats[proxy.id].success++; else stats[proxy.id].failed++;
    stats[proxy.id].avgLatency = Math.round(stats[proxy.id].totalLatency / stats[proxy.id].total);
    stats[proxy.id].successRate = Math.round((stats[proxy.id].success / stats[proxy.id].total) * 100);
    await saveProxyStats(stats);

    const updatedProxies = await getProxies();
    const pIdx = updatedProxies.findIndex(p => p.id === id);
    if (pIdx !== -1) {
      updatedProxies[pIdx].lastTested = Date.now();
      updatedProxies[pIdx].latency = latency;
      await saveProxies(updatedProxies);
    }

    res.json({ success, latency, error, stats: stats[proxy.id] });
  } catch (e) {
    res.status(500).json({ error: 'Test failed', detail: e.message });
  }
});

// Test all proxies (parallel with concurrency limit)
app.post('/api/proxies/test-all', async (req, res) => {
  try {
    const proxies = await getProxies();
    const enabledProxies = proxies.filter(p => p.enabled);
    const results = [];
    const CONCURRENCY = 5;

    async function testOne(proxy) {
      try {
        const { default: fetch } = await import('node-fetch');
        const agent = new (await import('socks-proxy-agent')).SocksProxyAgent(proxy.url);
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 10000);
        const start = Date.now();
        const response = await fetch('https://www.youtube.com', {
          agent, signal: controller.signal, method: 'HEAD'
        });
        clearTimeout(timeout);
        const latency = Date.now() - start;
        const success = response.ok;
        results.push({ id: proxy.id, name: proxy.name, success, latency, error: success ? null : `HTTP ${response.status}` });
      } catch (e) {
        results.push({ id: proxy.id, name: proxy.name, success: false, latency: null, error: e.message });
      }
    }

    for (let i = 0; i < enabledProxies.length; i += CONCURRENCY) {
      const batch = enabledProxies.slice(i, i + CONCURRENCY);
      await Promise.all(batch.map(p => testOne(p)));
    }

    const stats = await getProxyStats();
    for (const r of results) {
      if (!stats[r.id]) stats[r.id] = { total: 0, success: 0, failed: 0, totalLatency: 0 };
      stats[r.id].total++;
      if (r.success) stats[r.id].success++; else stats[r.id].failed++;
      if (r.latency) stats[r.id].totalLatency += r.latency;
      stats[r.id].avgLatency = Math.round(stats[r.id].totalLatency / stats[r.id].total);
      stats[r.id].successRate = Math.round((stats[r.id].success / stats[r.id].total) * 100);
    }
    await saveProxyStats(stats);

    res.json({ results });
  } catch (e) {
    res.status(500).json({ error: 'Batch test failed', detail: e.message });
  }
});

// Update rotation config
app.post('/api/proxies/rotation', async (req, res) => {
  try {
    const { strategy } = req.body;
    if (!['round_robin', 'random', 'least_used', 'sticky'].includes(strategy)) {
      return res.status(400).json({ error: 'Invalid strategy' });
    }
    await saveRotationConfig({ strategy, currentIndex: 0 });
    res.json({ success: true, strategy });
  } catch (e) {
    res.status(500).json({ error: 'Failed to update rotation config' });
  }
});

// Get next proxy for rotation
app.get('/api/proxies/next', async (req, res) => {
  try {
    const proxies = await getProxies();
    const enabled = proxies.filter(p => p.enabled);
    if (enabled.length === 0) return res.status(404).json({ error: 'No enabled proxies' });

    const rotation = await getRotationConfig();
    let selected;

    switch (rotation.strategy) {
      case 'random':
        selected = enabled[Math.floor(Math.random() * enabled.length)];
        break;
      case 'least_used':
        const stats = await getProxyStats();
        selected = enabled.reduce((min, p) => {
          const s = stats[p.id] || { total: 0 };
          const minS = stats[min.id] || { total: 0 };
          return s.total < minS.total ? p : min;
        });
        break;
      case 'sticky':
        // Use current index but don't increment
        selected = enabled[rotation.currentIndex % enabled.length];
        break;
      case 'round_robin':
      default:
        selected = enabled[rotation.currentIndex % enabled.length];
        rotation.currentIndex = (rotation.currentIndex + 1) % enabled.length;
        await saveRotationConfig(rotation);
        break;
    }

    res.json({ proxy: selected, rotation: rotation.strategy });
  } catch (e) {
    res.status(500).json({ error: 'Failed to get next proxy' });
  }
});

// Disable proxy when YouTube blocks it (called by worker)
app.post('/api/proxies/:id/disable', async (req, res) => {
  try {
    const { id } = req.params;
    const { reason } = req.body || {};
    const proxies = await getProxies();
    const idx = proxies.findIndex(p => p.id === id);
    if (idx === -1) return res.status(404).json({ error: 'Proxy not found' });
    proxies[idx].enabled = false;
    proxies[idx].disabledReason = reason || 'blocked';
    proxies[idx].disabledAt = Date.now();
    await saveProxies(proxies);
    console.log(`[Admin] Proxy ${proxies[idx].name} auto-disabled: ${reason}`);
    res.json({ success: true, proxy: proxies[idx] });
  } catch (e) {
    res.status(500).json({ error: 'Failed to disable proxy' });
  }
});

// Bulk import proxies
app.post('/api/proxies/import', async (req, res) => {
  try {
    const { proxies } = req.body;
    if (!Array.isArray(proxies)) return res.status(400).json({ error: 'Expected array' });

    const existing = await getProxies();
    const existingUrls = new Set(existing.map(p => p.url));
    let added = 0;

    for (const p of proxies) {
      if (!p.url || !p.url.startsWith('socks5://')) continue;
      if (existingUrls.has(p.url)) continue;
      existing.push({
        id: crypto.randomUUID(),
        url: p.url,
        name: p.name || p.url,
        country: p.country || 'Unknown',
        enabled: p.enabled !== false,
        createdAt: Date.now(),
        lastTested: null,
        latency: null
      });
      added++;
    }

    await saveProxies(existing);
    res.json({ success: true, added, total: existing.length });
  } catch (e) {
    res.status(500).json({ error: 'Import failed' });
  }
});

app.get('*', (req, res) => {
  if (req.path.startsWith('/api/')) {
    return res.status(404).json({ error: 'Not found' });
  }
  if (req.session.authenticated) {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
  } else {
    res.redirect('/login');
  }
});

// ========== HTTP SERVER + WEBSOCKET ==========
const server = http.createServer(app);
const wss = new WebSocketServer({ server });

// ========== LOG STREAMING ==========
const LOG_BUFFER_KEY = 'logs:buffer';
const LOG_CHANNEL = 'logs:stream';
const LOG_BUFFER_MAX = 2000;

const logSubscriber = new Redis({ host: REDIS_HOST, port: REDIS_PORT, maxRetriesPerRequest: 1 });
logSubscriber.subscribe(LOG_CHANNEL).catch(() => {});
logSubscriber.on('message', (channel, message) => {
  if (channel === LOG_CHANNEL) {
    wss.clients.forEach(client => {
      if (client.readyState === 1) {
        try { client.send(message); } catch (e) {}
      }
    });
  }
});

wss.on('connection', async (ws) => {
  console.log('[Admin] WebSocket client connected');
  try {
    const buffer = await redisClient.lrange(LOG_BUFFER_KEY, 0, -1);
    for (const line of buffer) {
      try { ws.send(line); } catch (e) { break; }
    }
  } catch (e) {}
  ws.on('close', () => console.log('[Admin] WebSocket client disconnected'));
});

async function publishLog(level, source, ...args) {
  const msg = JSON.stringify({ t: Date.now(), l: level, s: source, m: args.join(' ') });
  try {
    await redisClient.rpush(LOG_BUFFER_KEY, msg);
    await redisClient.ltrim(LOG_BUFFER_KEY, -LOG_BUFFER_MAX, -1);
    await redisClient.publish(LOG_CHANNEL, msg);
  } catch (e) {}
}

// Override console for admin logs
const _origLog = console.log;
const _origError = console.error;
const _origWarn = console.warn;
console.log = (...a) => { _origLog(...a); publishLog('info', 'admin', ...a); };
console.error = (...a) => { _origError(...a); publishLog('error', 'admin', ...a); };
console.warn = (...a) => { _origWarn(...a); publishLog('warn', 'admin', ...a); };

// ========== PROXY HEALTH CHECK CRON ==========
const HEALTH_CHECK_BATCH = 20;
const HEALTH_CHECK_INTERVAL = 15 * 60 * 1000;

async function testSingleProxy(proxy) {
  const start = Date.now();
  try {
    const { default: fetch } = await import('node-fetch');
    const agent = new (await import('socks-proxy-agent')).SocksProxyAgent(proxy.url);
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 10000);
    const response = await fetch('https://www.youtube.com', { agent, signal: controller.signal, method: 'HEAD' });
    clearTimeout(timeout);
    const latency = Date.now() - start;
    return { id: proxy.id, name: proxy.name, success: response.ok, latency, error: response.ok ? null : `HTTP ${response.status}` };
  } catch (e) {
    return { id: proxy.id, name: proxy.name, success: false, latency: null, error: e.message };
  }
}

async function runProxyHealthCheck() {
  try {
    const proxies = await getProxies();
    if (proxies.length === 0) return;

    let lastIndex = parseInt(await redisClient.get('config:proxyHealthIndex') || '0');
    const batch = [];
    for (let i = 0; i < HEALTH_CHECK_BATCH; i++) {
      batch.push(proxies[(lastIndex + i) % proxies.length]);
    }

    console.log(`[Admin] Health check: testing ${batch.length} proxies (starting from index ${lastIndex})...`);

    const results = [];
    const CONCURRENCY = 5;
    for (let i = 0; i < batch.length; i += CONCURRENCY) {
      const chunk = batch.slice(i, i + CONCURRENCY);
      const chunkResults = await Promise.allSettled(chunk.map(p => testSingleProxy(p)));
      for (const r of chunkResults) {
        results.push(r.value || (r.reason ? { id: '?', success: false, error: r.reason.message } : null));
      }
    }

    const stats = await getProxyStats();
    const allProxies = await getProxies();
    let reEnabled = 0, disabled = 0, passed = 0;

    for (const r of results) {
      if (!r) continue;
      if (!stats[r.id]) stats[r.id] = { total: 0, success: 0, failed: 0, totalLatency: 0, consecutiveFailures: 0 };
      stats[r.id].total++;
      if (r.success) {
        stats[r.id].success++;
        stats[r.id].consecutiveFailures = 0;
        passed++;
      } else {
        stats[r.id].failed++;
        stats[r.id].consecutiveFailures = (stats[r.id].consecutiveFailures || 0) + 1;
      }
      if (r.latency) stats[r.id].totalLatency += r.latency;
      stats[r.id].avgLatency = Math.round(stats[r.id].totalLatency / stats[r.id].total);
      stats[r.id].successRate = Math.round((stats[r.id].success / stats[r.id].total) * 100);

      const pIdx = allProxies.findIndex(p => p.id === r.id);
      if (pIdx === -1) continue;

      allProxies[pIdx].lastTested = Date.now();
      allProxies[pIdx].latency = r.latency;

      // Re-enable disabled proxies that pass test
      if (!allProxies[pIdx].enabled && r.success) {
        allProxies[pIdx].enabled = true;
        allProxies[pIdx].disabledReason = null;
        allProxies[pIdx].disabledAt = null;
        console.log(`[Admin] Proxy "${allProxies[pIdx].name}" RE-ENABLED (health check passed, latency: ${r.latency}ms)`);
        reEnabled++;
      }

      // Auto-disable proxies with consistently low success rate
      if (stats[r.id].total >= 5 && stats[r.id].successRate < 30 && allProxies[pIdx].enabled) {
        allProxies[pIdx].enabled = false;
        allProxies[pIdx].disabledReason = `Low success rate: ${stats[r.id].successRate}% (${stats[r.id].consecutiveFailures} consecutive failures)`;
        allProxies[pIdx].disabledAt = Date.now();
        console.log(`[Admin] Proxy "${allProxies[pIdx].name}" auto-disabled: success rate ${stats[r.id].successRate}%`);
        disabled++;
      }
    }

    await saveProxyStats(stats);
    await saveProxies(allProxies);

    const nextIndex = (lastIndex + HEALTH_CHECK_BATCH) % proxies.length;
    await redisClient.set('config:proxyHealthIndex', nextIndex.toString());

    console.log(`[Admin] Health check done: ${passed}/${results.length} passed, ${reEnabled} re-enabled, ${disabled} disabled`);
  } catch (e) {
    console.error('[Admin] Health check error:', e.message);
  }
}

setInterval(runProxyHealthCheck, HEALTH_CHECK_INTERVAL);
setTimeout(runProxyHealthCheck, 5000);

// ========== COOKIES MANAGEMENT ==========
app.get('/api/cookies', requireAuth, async (req, res) => {
  try {
    const content = await redisClient.get('config:cookies');
    let validation = null;
    if (content) {
      const hasSID = content.includes('SID') && !content.includes('__Secure-3PSID');
      const hasSAPISID = content.includes('SAPISID') || content.includes('__Secure-1PAPISID');
      const hasLoginCookie = content.includes('SSID') || content.includes('HSID') || content.includes('__Secure-1PSID');
      const hasSOCS = content.includes('SOCS');
      const isValid = hasLoginCookie || hasSAPISID;
      validation = {
        hasAuth: isValid,
        hasSID: content.includes('\tSID\t') || content.includes('\t__Secure-3PSID\t'),
        hasSAPISID,
        hasLoginCookie,
        hasSOCS,
        lineCount: content.split('\n').filter(l => l.trim() && !l.startsWith('#')).length,
        detail: isValid
          ? 'Cookies contain authentication tokens'
          : hasSOCS
            ? 'Only consent cookies found (SOCS). You need cookies from a LOGGED IN YouTube session.'
            : 'No authentication cookies found. Export cookies while logged in to YouTube.',
      };
    }
    res.json({
      exists: !!content,
      size: content ? content.length : 0,
      preview: content ? content.split('\n').slice(0, 5).join('\n') : null,
      lines: content ? content.split('\n').length : 0,
      validation,
    });
  } catch (e) {
    res.status(500).json({ error: 'Failed to check cookies' });
  }
});

app.post('/api/cookies', requireAuth, async (req, res) => {
  try {
    const { content } = req.body;
    if (!content) return res.status(400).json({ error: 'Content is required' });
    if (!content.includes('Netscape') && !content.includes('# HTTP Cookie File')) {
      return res.status(400).json({ error: 'Invalid cookies file. Export cookies in Netscape format from your browser.' });
    }
    await redisClient.set('config:cookies', content);
    console.log('[Admin] Cookies uploaded (' + content.length + ' bytes, ' + content.split('\n').length + ' lines)');
    res.json({ success: true, size: content.length, lines: content.split('\n').length });
  } catch (e) {
    res.status(500).json({ error: 'Failed to save cookies' });
  }
});

app.delete('/api/cookies', requireAuth, async (req, res) => {
  try {
    await redisClient.del('config:cookies');
    console.log('[Admin] Cookies deleted');
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: 'Failed to delete cookies' });
  }
});

// ========== LOG BUFFER ENDPOINT ==========
app.get('/api/logs', requireAuth, async (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 200;
    const source = req.query.source || '';
    const level = req.query.level || '';
    let buffer = await redisClient.lrange(LOG_BUFFER_KEY, -limit, -1);

    if (source || level) {
      buffer = buffer.filter(line => {
        try {
          const parsed = JSON.parse(line);
          if (source && parsed.s !== source) return false;
          if (level && parsed.l !== level) return false;
          return true;
        } catch (e) { return true; }
      });
    }

    res.json({ logs: buffer });
  } catch (e) {
    res.status(500).json({ error: 'Failed to get logs' });
  }
});

// ========== START SERVER ==========
setInterval(recordHistory, 30000);
recordHistory();

server.listen(PORT, () => {
  console.log(`[Admin] Server running on port ${PORT}`);
  console.log(`[Admin] WebSocket available for log streaming`);
  console.log(`[Admin] Redis: ${REDIS_HOST}:${REDIS_PORT}`);
  console.log(`[Admin] Default login: ${ADMIN_USER} / ${ADMIN_PASS}`);
});