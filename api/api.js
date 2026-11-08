const express = require('express');
const { Queue } = require('bullmq');
const { v4: uuidv4 } = require('uuid');
const path = require('path');
const fs = require('fs');
const Redis = require('ioredis');
const { execFile, spawn } = require('child_process');
const http = require('http');
const https = require('https');
const { SocksProxyAgent } = require('socks-proxy-agent');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const REDIS_HOST = process.env.REDIS_HOST || 'redis';
const REDIS_PORT = process.env.REDIS_PORT || 6379;
const DOWNLOADS_DIR = process.env.DOWNLOADS_DIR || '/app/downloads';

const conversionQueue = new Queue('conversions', {
  connection: { host: REDIS_HOST, port: REDIS_PORT },
  defaultJobOptions: {
    removeOnComplete: { age: 604800 },
    removeOnFail: { age: 604800 },
  },
});

const redisClient = new Redis({ host: REDIS_HOST, port: REDIS_PORT, maxRetriesPerRequest: 1 });

// ========== LOG STREAMING ==========
const LOG_CHANNEL = 'logs:stream';
const LOG_BUFFER_KEY = 'logs:buffer';
const logPublisher = new Redis({ host: REDIS_HOST, port: REDIS_PORT, maxRetriesPerRequest: 1 });

function publishLog(level, ...args) {
  const msg = JSON.stringify({ t: Date.now(), l: level, s: 'api', m: args.join(' ') });
  logPublisher.publish(LOG_CHANNEL, msg).catch(() => {});
  logPublisher.rpush(LOG_BUFFER_KEY, msg).then(() => {
    logPublisher.ltrim(LOG_BUFFER_KEY, -2000, -1).catch(() => {});
  }).catch(() => {});
}

const _origLog = console.log;
const _origError = console.error;
const _origWarn = console.warn;
console.log = (...a) => { _origLog(...a); publishLog('info', ...a); };
console.error = (...a) => { _origError(...a); publishLog('error', ...a); };
console.warn = (...a) => { _origWarn(...a); publishLog('warn', ...a); };

// ========== COOKIES SYNC FROM REDIS ==========
async function syncCookiesFile() {
  try {
    const content = await redisClient.get('config:cookies');
    if (content) {
      fs.writeFileSync('/app/cookies.txt', content);
      return true;
    }
  } catch (e) {}
  return false;
}

// Sync cookies on startup
syncCookiesFile().then(ok => {
  if (ok) console.log('[API] Cookies synced from Redis');
  else console.log('[API] No cookies in Redis, using file if exists');
});

app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  next();
});

app.post('/convert', async (req, res) => {
  try {
    const { url, format } = req.body;

    if (!url) {
      return res.status(400).json({ error: 'URL is required' });
    }

    if (!/youtu(\.be|be\.com)/.test(url)) {
      return res.status(400).json({ error: 'Invalid YouTube URL' });
    }

    const validFormats = ['mp3', 'mp4'];
    const selectedFormat = validFormats.includes(format) ? format : 'mp3';

    const jobId = uuidv4();

    await conversionQueue.add('convert', {
      jobId,
      url,
      format: selectedFormat,
    }, {
      jobId,
      priority: selectedFormat === 'mp3' ? 1 : 2,
    });

    console.log(`[API] Job ${jobId} enqueued for ${url} (${selectedFormat})`);

    res.status(202).json({ jobId, status: 'queued' });
  } catch (err) {
    console.error('[API] Error enqueueing job:', err);
    res.status(500).json({ error: 'Failed to enqueue conversion' });
  }
});

app.get('/status/:jobId', async (req, res) => {
  try {
    const { jobId } = req.params;
    const job = await conversionQueue.getJob(jobId);

    if (!job) {
      return res.status(404).json({ error: 'Job not found' });
    }

    const state = await job.getState();
    const progress = job.progress || 0;
    const data = job.data || {};

    let status;
    switch (state) {
      case 'waiting':
      case 'delayed':
      case 'active':
        status = 'processing';
        break;
      case 'completed':
        status = 'completed';
        break;
      case 'failed':
        status = 'failed';
        break;
      default:
        status = state;
    }

    const response = {
      jobId,
      status,
      progress: typeof progress === 'number' ? progress : 0,
      format: data.format || 'mp3',
    };

    if (status === 'completed') {
      response.downloadUrl = `/download/${jobId}`;
    }

    if (status === 'failed') {
      response.error = job.failedReason || 'Conversion failed';
    }

    res.json(response);
  } catch (err) {
    console.error('[API] Error checking status:', err);
    res.status(500).json({ error: 'Failed to check status' });
  }
});

app.get('/download/:jobId', async (req, res) => {
  try {
    const { jobId } = req.params;
    const job = await conversionQueue.getJob(jobId);

    if (!job) {
      return res.status(404).json({ error: 'Job not found' });
    }

    const state = await job.getState();
    if (state !== 'completed') {
      return res.status(404).json({ error: 'File not ready yet' });
    }

    const { format, displayName } = job.data;
    const ext = format === 'mp3' ? '.mp3' : '.mp4';
    const safeName = (displayName || 'audio').replace(/[^\w\s\-./()]/g, '').replace(/\s+/g, ' ').trim().slice(0, 100) || 'audio';
    const filename = `${safeName}${ext}`;
    const filePath = path.join(DOWNLOADS_DIR, `${jobId}${ext}`);

    if (!fs.existsSync(filePath)) {
      return res.status(404).json({ error: 'File not found on disk' });
    }

    const mimeTypes = { '.mp3': 'audio/mpeg', '.mp4': 'video/mp4' };
    res.setHeader('Content-Type', mimeTypes[ext] || 'application/octet-stream');
    res.setHeader('Content-Disposition', `attachment; filename="${filename}"`);
    res.setHeader('Cache-Control', 'no-store');

    const stream = fs.createReadStream(filePath);
    stream.pipe(res);

    stream.on('end', () => {
      setTimeout(() => {
        try { fs.unlinkSync(filePath); } catch (e) {}
      }, 60000);
    });

    stream.on('error', (err) => {
      console.error('[API] Stream error:', err);
      if (!res.headersSent) {
        res.status(500).json({ error: 'Error reading file' });
      }
    });
  } catch (err) {
    console.error('[API] Error serving download:', err);
    res.status(500).json({ error: 'Failed to serve file' });
  }
});

app.get('/health', (req, res) => {
  res.json({ status: 'ok', service: 'api' });
});

app.get('/public/progress-bar', async (req, res) => {
  try {
    const type = await redisClient.get('config:progressBarType') || 'bubble';
    res.json({ type });
  } catch (e) {
    res.json({ type: 'bubble' });
  }
});

const WARP_PROXY = process.env.WARP_PROXY || '';
const COOKIES_FILE = process.env.COOKIES_FILE || '/app/cookies.txt';

function hasCookies() {
  return fs.existsSync(COOKIES_FILE) && fs.statSync(COOKIES_FILE).size > 10;
}

async function getAdminProxies() {
  try {
    const data = await redisClient.get('config:proxies');
    if (!data) return [];
    return JSON.parse(data).filter(p => p.enabled);
  } catch (e) {
    return [];
  }
}

function buildYtDlpArgs(baseArgs, proxyUrl) {
  const args = [...baseArgs];
  if (proxyUrl) args.push('--proxy', proxyUrl);
  if (hasCookies()) {
    args.push('--cookies', COOKIES_FILE);
  }
  args.push('--no-check-certificate');
  args.push('--sleep-requests', '1');
  args.push('--sleep-interval', '3');
  args.push('--max-sleep-interval', '7');
  return args;
}

function runYtDlp(args, timeout = 30000) {
  return new Promise((resolve, reject) => {
    const proc = spawn('yt-dlp', args);
    let stdout = '';
    let stderr = '';
    const timer = setTimeout(() => { proc.kill('SIGTERM'); reject(new Error('yt-dlp timeout')); }, timeout);
    proc.stdout.on('data', d => { stdout += d.toString(); });
    proc.stderr.on('data', d => { stderr += d.toString(); });
    proc.on('close', code => {
      clearTimeout(timer);
      if (code === 0) resolve(stdout.trim());
      else reject(new Error(stderr.slice(0, 500)));
    });
    proc.on('error', err => { clearTimeout(timer); reject(err); });
  });
}

async function getStreamUrl(url, proxyUrl, format = 'm4a') {
  const baseArgs = ['-g', '--no-playlist', '--no-warnings'];
  if (format === 'mp4') {
    baseArgs.push('-f', 'bestvideo[ext=mp4]+bestaudio/bestvideo+bestaudio/best');
  } else {
    // Use bestaudio with fallback to best (video+audio) for compatibility
    baseArgs.push('-f', 'bestaudio/best');
  }
  const args = buildYtDlpArgs(baseArgs, proxyUrl);
  args.push(url);
  return runYtDlp(args, 30000);
}

async function getVideoTitle(url, proxyUrl) {
  try {
    const baseArgs = ['--get-title', '--no-playlist', '--no-warnings'];
    const args = buildYtDlpArgs(baseArgs, proxyUrl);
    args.push(url);
    return await runYtDlp(args, 15000);
  } catch (e) {
    return null;
  }
}

async function getVideoInfo(url, proxyUrl) {
  try {
    const baseArgs = ['--dump-json', '--no-playlist', '--no-warnings'];
    const args = buildYtDlpArgs(baseArgs, proxyUrl);
    args.push(url);
    const stdout = await runYtDlp(args, 30000);
    return JSON.parse(stdout);
  } catch (e) {
    return null;
  }
}

async function getStreamUrlFromInfo(info, format) {
  if (!info || !info.formats) return null;
  if (format === 'mp4') {
    const f = info.formats.find(f => f.vcodec !== 'none' && f.acodec !== 'none' && (f.ext === 'mp4' || f.ext === 'webm'));
    return f?.url || null;
  }
  const audioFormats = info.formats.filter(f => f.acodec !== 'none' && f.vcodec === 'none');
  if (format === 'opus') {
    const f = audioFormats.find(f => f.ext === 'webm' || f.ext === 'opus') || audioFormats[0];
    return f?.url || null;
  }
  // m4a or mp3
  const f = audioFormats.find(f => f.ext === 'm4a' || f.mimetype?.includes('mp4')) || audioFormats[0];
  return f?.url || null;
}

app.get('/formats', async (req, res) => {
  try {
    const { url } = req.query;
    if (!url || !/youtu(\.be|be\.com)/.test(url)) {
      return res.status(400).json({ error: 'Valid YouTube URL required' });
    }
    res.json({
      formats: [
        { id: 'mp3', label: 'MP3', type: 'audio', mime: 'audio/mpeg', clientSide: true, description: 'Universal audio (client-side conversion)' },
        { id: 'm4a', label: 'M4A', type: 'audio', mime: 'audio/mp4', clientSide: false, description: 'AAC audio, instant download' },
        { id: 'opus', label: 'Opus', type: 'audio', mime: 'audio/webm', clientSide: false, description: 'High quality, small size' },
        { id: 'mp4', label: 'MP4', type: 'video', mime: 'video/mp4', clientSide: false, description: 'Video with audio' },
      ]
    });
  } catch (err) {
    res.status(500).json({ error: 'Failed to get formats' });
  }
});

app.get('/stream', async (req, res) => {
  try {
    const { url, format } = req.query;
    if (!url || !/youtu(\.be|be\.com)/.test(url)) {
      return res.status(400).json({ error: 'Valid YouTube URL required' });
    }

    const validFormats = ['mp3', 'm4a', 'opus', 'mp4'];
    const fmt = validFormats.includes(format) ? format : 'm4a';

    // Sync cookies from Redis before attempting
    await syncCookiesFile();

    let streamUrl = null;
    let videoTitle = null;
    let thumbnailUrl = null;
    const adminProxies = await getAdminProxies();

    const enabledProxies = adminProxies.filter(p => p.enabled);
    const shuffledProxies = [...enabledProxies].sort(() => Math.random() - 0.5);

    const strategies = [
      ...(WARP_PROXY ? [{ name: 'WARP', proxy: WARP_PROXY }] : []),
      { name: 'Direct', proxy: null },
      ...shuffledProxies.slice(0, 8).map(p => ({ name: p.name, proxy: p.url })),
    ];

    // Strategy 1: Get full video info (title + stream URL) via --dump-json
    for (const strategy of strategies) {
      try {
        const info = await getVideoInfo(url, strategy.proxy);
        if (info && info.formats) {
          const urlFromInfo = await getStreamUrlFromInfo(info, fmt);
          if (urlFromInfo) {
            streamUrl = urlFromInfo;
            videoTitle = info.title || null;
            thumbnailUrl = info.thumbnail || null;
            console.log(`[API] Video info + stream obtained via ${strategy.name}: "${videoTitle}"`);
            break;
          }
        }
      } catch (err) {
        console.warn(`[API] ${strategy.name} info failed: ${err.message.slice(0, 100)}`);
      }
    }

    // Strategy 2: Fallback to -g (get URL) + --get-title separately
    if (!streamUrl) {
      for (const strategy of strategies) {
        try {
          const [urlResult, titleResult] = await Promise.all([
            getStreamUrl(url, strategy.proxy, fmt),
            getVideoTitle(url, strategy.proxy)
          ]);
          streamUrl = urlResult;
          if (titleResult) videoTitle = titleResult;
          console.log(`[API] Stream URL obtained via ${strategy.name} (-g fallback)`);
          break;
        } catch (err) {
          console.warn(`[API] ${strategy.name} stream failed: ${err.message.slice(0, 100)}`);
        }
      }
    }

    // Strategy 3: Cookies fallback
    if (!streamUrl && hasCookies()) {
      console.log('[API] Fallback: trying with cookies...');
      try {
        const info = await getVideoInfo(url, null);
        if (info && info.formats) {
          const urlFromInfo = await getStreamUrlFromInfo(info, fmt);
          if (urlFromInfo) {
            streamUrl = urlFromInfo;
            videoTitle = info.title || null;
            thumbnailUrl = info.thumbnail || null;
            console.log('[API] Video info obtained via cookies fallback');
          }
        }
      } catch (err) {
        console.warn(`[API] Cookies fallback failed: ${err.message.slice(0, 100)}`);
      }
    }

    if (!streamUrl) {
      return res.status(502).json({ error: 'Failed to extract audio stream URL' });
    }

    const isHttps = streamUrl.startsWith('https');
    const client = isHttps ? https : http;

    // Determine proxy for streaming the actual content
    let proxyUrl = null;
    for (const s of strategies) {
      if (s.proxy && streamUrl.includes('googlevideo')) {
        proxyUrl = s.proxy;
        break;
      }
    }
    let agent = undefined;
    if (proxyUrl && proxyUrl.startsWith('socks')) {
      agent = new SocksProxyAgent(proxyUrl);
    }

    const makeRequest = (targetUrl, redirectCount = 0) => {
      if (redirectCount > 5) {
        return res.status(502).json({ error: 'Too many redirects' });
      }

      const parsedUrl = new URL(targetUrl);
      const reqOptions = {
        hostname: parsedUrl.hostname,
        port: parsedUrl.port || (isHttps ? 443 : 80),
        path: parsedUrl.pathname + parsedUrl.search,
        method: 'GET',
        headers: {
          'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
          'Accept': '*/*',
          'Accept-Language': 'en-US,en;q=0.9',
          'Referer': 'https://www.youtube.com/',
          'Origin': 'https://www.youtube.com',
        },
      };
      if (agent) reqOptions.agent = agent;

      const proxyReq = client.request(reqOptions, (proxyRes) => {
        if (proxyRes.statusCode >= 300 && proxyRes.statusCode < 400 && proxyRes.headers.location) {
          return makeRequest(proxyRes.headers.location, redirectCount + 1);
        }

        const contentLength = parseInt(proxyRes.headers['content-length'] || '0', 10);
        const mimeTypes = {
          'm4a': 'audio/mp4',
          'opus': 'audio/webm',
          'mp3': 'audio/mpeg',
          'mp4': 'video/mp4',
        };

        res.setHeader('Access-Control-Allow-Origin', '*');
        res.setHeader('Content-Type', mimeTypes[fmt] || proxyRes.headers['content-type'] || 'application/octet-stream');
        if (contentLength) res.setHeader('Content-Length', contentLength);
        res.setHeader('Cache-Control', 'no-store');
        if (videoTitle) {
          const safeTitle = videoTitle.replace(/[^\w\s\-./()]/g, '').replace(/\s+/g, ' ').trim().slice(0, 100) || 'audio';
          res.setHeader('Content-Disposition', `attachment; filename="${safeTitle}.${fmt === 'mp4' ? 'mp4' : fmt}"`);
          res.setHeader('X-Video-Title', videoTitle);
        } else {
          res.setHeader('Content-Disposition', `attachment; filename="audio.${fmt === 'mp4' ? 'mp4' : fmt}"`);
        }
        if (thumbnailUrl) {
          res.setHeader('X-Thumbnail-Url', thumbnailUrl);
        }

        proxyRes.pipe(res);
      });

      proxyReq.on('error', (err) => {
        console.error('[API] Stream proxy error:', err.message);
        if (!res.headersSent) {
          res.status(502).json({ error: 'Stream proxy error' });
        }
      });

      proxyReq.end();
    };

    makeRequest(streamUrl);
  } catch (err) {
    console.error('[API] Stream error:', err);
    if (!res.headersSent) {
      res.status(500).json({ error: 'Stream extraction failed' });
    }
  }
});

app.listen(PORT, () => {
  console.log(`[API] Server running on port ${PORT}`);
  console.log(`[API] Redis: ${REDIS_HOST}:${REDIS_PORT}`);
});
