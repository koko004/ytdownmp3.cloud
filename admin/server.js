const express = require('express');
const { Queue } = require('bullmq');
const Redis = require('ioredis');
const os = require('os');
const fs = require('fs');
const path = require('path');
const { execSync, exec } = require('child_process');
const crypto = require('crypto');
const session = require('express-session');
const geoip = require('geoip-lite');

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

function getSystemStats() {
  const cpus = os.cpus();
  const totalMem = os.totalmem();
  const freeMem = os.freemem();
  const usedMem = totalMem - freeMem;

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

  let networkInfo = { interfaces: [], totalRx: 0, totalTx: 0 };
  try {
    const netDev = fs.readFileSync('/proc/net/dev', 'utf8');
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

  return {
    hostname: os.hostname(),
    platform: os.platform(),
    arch: os.arch(),
    uptime: os.uptime(),
    cpu: {
      model: cpus[0]?.model || 'unknown',
      cores: cpus.length,
      usage: cpus.map(c => {
        const total = Object.values(c.times).reduce((a, b) => a + b, 0);
        return { user: c.times.user, nice: c.times.nice, sys: c.times.sys, idle: c.times.idle, irq: c.times.irq, usagePercent: ((total - c.times.idle) / total * 100).toFixed(1) };
      }),
      avgUsage: (cpus.reduce((acc, c) => {
        const total = Object.values(c.times).reduce((a, b) => a + b, 0);
        return acc + ((total - c.times.idle) / total * 100);
      }, 0) / cpus.length).toFixed(1),
    },
    memory: { total: totalMem, used: usedMem, free: freeMem, usagePercent: (usedMem / totalMem * 100).toFixed(1) },
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
    const recentJobs = await conversionQueue.getJobs(['completed', 'failed'], 0, 49);
    const jobHistory = recentJobs.map(j => ({
      id: j.id, jobId: j.data.jobId, url: j.data.url, format: j.data.format,
      status: j.state, timestamp: j.timestamp,
      duration: j.finishedOn && j.processedOn ? j.finishedOn - j.processedOn : null,
    }));
    return { counts: { waiting, active, completed, failed, delayed, total: waiting + active + completed + failed + delayed }, recentJobs, jobHistory };
  } catch (err) {
    console.error('[Admin] Queue stats error:', err);
    return { counts: { waiting: 0, active: 0, completed: 0, failed: 0, delayed: 0, total: 0 }, recentJobs: [], jobHistory: [] };
  }
}

function getMachineIPs() {
  const ips = [];
  const nets = os.networkInterfaces();
  for (const name of Object.keys(nets)) {
    for (const net of nets[name]) {
      if (net.family === 'IPv4' && !net.internal) {
        ips.push({ interface: name, ip: net.address, netmask: net.netmask, mac: net.mac });
      }
    }
  }
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
      const apiRes = await fetchWithTimeout('http://api:3000/health');
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
      const frontendRes = await fetchWithTimeout('http://frontend/health').catch(() => null);
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
  Object.keys(history).forEach(k => { if (history[k].length > HISTORY_MAX) history[k] = history[k].slice(-HISTORY_MAX); });
}

app.set('trust proxy', 1);
app.use(session({
  secret: SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: { secure: 'auto', httpOnly: true, maxAge: 24 * 60 * 60 * 1000, sameSite: 'lax' },
}));

const requireAuth = (req, res, next) => {
  if (req.session.authenticated) return next();
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

// Test all proxies
app.post('/api/proxies/test-all', async (req, res) => {
  try {
    const proxies = await getProxies();
    const enabledProxies = proxies.filter(p => p.enabled);
    const results = [];

    for (const proxy of enabledProxies) {
      try {
        const { default: fetch } = await import('node-fetch');
        const agent = new (await import('socks-proxy-agent')).SocksProxyAgent(proxy.url);
        const controller = new AbortController();
        const timeout = setTimeout(() => controller.abort(), 10000);
        const start = Date.now();
        const response = await fetch('https://www.youtube.com', {
          agent,
          signal: controller.signal,
          method: 'HEAD'
        });
        clearTimeout(timeout);
        const latency = Date.now() - start;
        const success = response.ok;
        results.push({ id: proxy.id, name: proxy.name, success, latency, error: success ? null : `HTTP ${response.status}` });
      } catch (e) {
        results.push({ id: proxy.id, name: proxy.name, success: false, latency: null, error: e.message });
      }
    }

    // Update stats
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

setInterval(recordHistory, 30000);
recordHistory();

app.listen(PORT, () => {
  console.log(`[Admin] Server running on port ${PORT}`);
  console.log(`[Admin] Redis: ${REDIS_HOST}:${REDIS_PORT}`);
  console.log(`[Admin] Default login: ${ADMIN_USER} / ${ADMIN_PASS}`);
});