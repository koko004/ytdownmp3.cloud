const express = require('express');
const { Queue } = require('bullmq');
const os = require('os');
const fs = require('fs');
const path = require('path');
const { execSync } = require('child_process');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const PORT = process.env.PORT || 4000;
const REDIS_HOST = process.env.REDIS_HOST || 'redis';
const REDIS_PORT = process.env.REDIS_PORT || 6379;

const conversionQueue = new Queue('conversions', {
  connection: { host: REDIS_HOST, port: REDIS_PORT },
});

const visitorLog = [];
const networkStats = { totalBytesIn: 0, totalBytesOut: 0, lastUpdate: Date.now() };

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
    diskUsage = {
      total: parseInt(df[1]),
      used: parseInt(df[2]),
      free: parseInt(df[3]),
    };
  } catch (e) {}

  let networkInfo = { interfaces: [] };
  try {
    const netDev = fs.readFileSync('/proc/net/dev', 'utf8');
    const lines = netDev.split('\n').slice(2);
    for (const line of lines) {
      if (!line.trim()) continue;
      const parts = line.trim().split(/[\s:]+/);
      if (parts[0] === 'lo') continue;
      networkInfo.interfaces.push({
        name: parts[0],
        rxBytes: parseInt(parts[1]),
        txBytes: parseInt(parts[9]),
        rxPackets: parseInt(parts[2]),
        txPackets: parseInt(parts[10]),
      });
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
    memory: {
      total: totalMem,
      used: usedMem,
      free: freeMem,
      usagePercent: (usedMem / totalMem * 100).toFixed(1),
    },
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
      id: j.id,
      jobId: j.data.jobId,
      url: j.data.url,
      format: j.data.format,
      status: j.state,
      timestamp: j.timestamp,
      duration: j.finishedOn && j.processedOn ? j.finishedOn - j.processedOn : null,
    }));

    return {
      counts: { waiting, active, completed, failed, delayed, total: waiting + active + completed + failed + delayed },
      recentJobs,
      jobHistory,
    };
  } catch (err) {
    console.error('[Admin] Queue stats error:', err);
    return { counts: { waiting: 0, active: 0, completed: 0, failed: 0, delayed: 0, total: 0 }, recentJobs: [], jobHistory: [] };
  }
}

function trackVisitor(req) {
  const ip = req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.socket.remoteAddress;
  const now = Date.now();
  const ua = req.headers['user-agent'] || 'Unknown';
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
      ip,
      firstSeen: now,
      lastSeen: now,
      visits: 1,
      userAgent: ua,
      country: 'Unknown',
      pageViews: [{ path: req.path, time: now }],
    });
    if (visitorLog.length > 1000) visitorLog.shift();
  }
}

app.use((req, res, next) => {
  if (req.path === '/api/stats' || req.path === '/api/health') {
    return next();
  }
  trackVisitor(req);
  next();
});

app.get('/api/stats', async (req, res) => {
  try {
    const system = getSystemStats();
    const queue = await getQueueStats();

    const visitors = visitorLog.map(v => ({
      ip: v.ip,
      country: v.country,
      visits: v.visits,
      firstSeen: v.firstSeen,
      lastSeen: v.lastSeen,
      sessionDuration: ((v.lastSeen - v.firstSeen) / 1000 / 60).toFixed(1),
      userAgent: v.userAgent,
      recentPages: v.pageViews?.slice(-5) || [],
    }));

    const now = Date.now();
    const last24h = visitorLog.filter(v => now - v.lastSeen < 86400000);
    const last1h = visitorLog.filter(v => now - v.lastSeen < 3600000);

    res.json({
      system,
      queue,
      visitors: {
        total: visitorLog.length,
        last24h: last24h.length,
        last1h: last1h.length,
        list: visitors,
      },
      network: {
        interfaces: system.network.interfaces,
        uptime: system.uptime,
      },
      timestamp: now,
    });
  } catch (err) {
    console.error('[Admin] Stats error:', err);
    res.status(500).json({ error: 'Failed to get stats' });
  }
});

app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', service: 'admin', uptime: process.uptime() });
});

app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, () => {
  console.log(`[Admin] Server running on port ${PORT}`);
  console.log(`[Admin] Redis: ${REDIS_HOST}:${REDIS_PORT}`);
});
