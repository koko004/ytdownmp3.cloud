const { Worker } = require('bullmq');
const { spawn, execFile } = require('child_process');
const path = require('path');
const fs = require('fs');
const http = require('http');
const Redis = require('ioredis');

const REDIS_HOST = process.env.REDIS_HOST || 'redis';
const REDIS_PORT = process.env.REDIS_PORT || 6379;
const DOWNLOADS_DIR = process.env.DOWNLOADS_DIR || '/app/downloads';
const CONCURRENCY = parseInt(process.env.WORKER_CONCURRENCY || '2', 10);
const JOB_TIMEOUT = parseInt(process.env.JOB_TIMEOUT || '120000', 10);
const HEALTH_PORT = parseInt(process.env.HEALTH_PORT || '3000', 10);
const COOKIES_FILE = process.env.COOKIES_FILE || '/app/cookies.txt';

if (!fs.existsSync(DOWNLOADS_DIR)) {
  fs.mkdirSync(DOWNLOADS_DIR, { recursive: true });
}

const redisClient = new Redis({ host: REDIS_HOST, port: REDIS_PORT, maxRetriesPerRequest: 1 });

const hasCookiesFile = fs.existsSync(COOKIES_FILE);
console.log(`[Worker] Cookies file: ${hasCookiesFile ? COOKIES_FILE : 'NOT FOUND'}`);
console.log(`[Worker] Concurrency: ${CONCURRENCY}, Timeout: ${JOB_TIMEOUT}ms`);

async function getAdminProxies() {
  try {
    const data = await redisClient.get('config:proxies');
    if (!data) return [];
    return JSON.parse(data).filter(p => p.enabled);
  } catch (e) {
    console.error('[Worker] Failed to get admin proxies:', e.message);
    return [];
  }
}

function buildYtDlpArgs(baseArgs, proxyUrl) {
  const args = [...baseArgs];

  if (proxyUrl) {
    args.push('--proxy', proxyUrl);
  }

  if (hasCookiesFile) {
    args.push('--cookies', COOKIES_FILE);
  }

  args.push('--extractor-args', 'youtube:player_client=web_safari');
  args.push('--no-check-certificate');
  args.push('--sleep-requests', '1');
  args.push('--sleep-interval', '3');
  args.push('--max-sleep-interval', '7');

  return args;
}

async function tryYtDlp(args, timeout = 30000) {
  return new Promise((resolve, reject) => {
    execFile('yt-dlp', args, { timeout }, (err, stdout, stderr) => {
      if (err) reject(new Error(stderr?.slice(0, 300) || err.message));
      else resolve(stdout);
    });
  });
}

async function getVideoInfoWithFallback(url) {
  const baseArgs = ['--dump-json', '--no-playlist', '--no-warnings'];

  const adminProxies = await getAdminProxies();
  const strategies = [
    ...adminProxies.map(p => ({ name: p.name, proxy: p.url })),
    { name: 'Direct', proxy: null },
  ];

  for (const strategy of strategies) {
    try {
      const args = buildYtDlpArgs(baseArgs, strategy.proxy);
      args.push(url);
      console.log(`[Worker] Trying info via ${strategy.name}${strategy.proxy ? ' (' + strategy.proxy.substring(0, 30) + '...)' : ''}`);
      const stdout = await tryYtDlp(args, 30000);
      console.log(`[Worker] Info fetched via ${strategy.name}`);
      return JSON.parse(stdout);
    } catch (err) {
      console.warn(`[Worker] ${strategy.name} failed for info: ${err.message.slice(0, 200)}`);
    }
  }

  throw new Error('All strategies failed to fetch video info');
}

async function downloadAndConvertWithFallback(jobId, url, format, job) {
  const outputFile = path.join(DOWNLOADS_DIR, `${jobId}.${format}`);

  let baseArgs;
  if (format === 'mp3') {
    baseArgs = [
      '--no-playlist', '--no-warnings', '--newline',
      '-x', '--audio-format', 'mp3', '--audio-quality', '0',
      '-o', outputFile
    ];
  } else {
    baseArgs = [
      '--no-playlist', '--no-warnings', '--newline',
      '-f', 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
      '--merge-output-format', 'mp4',
      '-o', outputFile
    ];
  }

  const adminProxies = await getAdminProxies();
  const strategies = [
    ...adminProxies.map(p => ({ name: p.name, proxy: p.url })),
    { name: 'Direct', proxy: null },
  ];

  for (const strategy of strategies) {
    try {
      const args = buildYtDlpArgs(baseArgs, strategy.proxy);
      args.push(url);
      console.log(`[Worker] Downloading via ${strategy.name}${strategy.proxy ? ' (' + strategy.proxy.substring(0, 30) + '...)' : ''}`);

      const result = await new Promise((resolve, reject) => {
        const proc = spawn('yt-dlp', args);
        let lastProgress = 10;
        let stderrData = '';
        const progressRegex = /(\d+\.?\d*)%/;

        proc.stdout.on('data', (data) => {
          const text = data.toString();
          stderrData += text;
          const match = text.match(progressRegex);
          if (match) {
            const progress = Math.min(parseFloat(match[1]), 99);
            if (progress > lastProgress) {
              lastProgress = progress;
              job.updateProgress(progress);
            }
          }
        });

        proc.stderr.on('data', (data) => {
          const text = data.toString();
          stderrData += text;
          const match = text.match(progressRegex);
          if (match) {
            const progress = Math.min(parseFloat(match[1]), 99);
            if (progress > lastProgress) {
              lastProgress = progress;
              job.updateProgress(progress);
            }
          }
        });

        proc.on('close', (code) => {
          if (code === 0) resolve(outputFile);
          else reject(new Error(`exit ${code}: ${stderrData.slice(0, 500)}`));
        });
        proc.on('error', reject);
      });

      console.log(`[Worker] Download completed via ${strategy.name}`);
      return result;
    } catch (err) {
      console.warn(`[Worker] ${strategy.name} failed for download: ${err.message.slice(0, 200)}`);
    }
  }

  throw new Error('All strategies failed to download video');
}

const healthServer = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', service: 'worker', uptime: process.uptime() }));
  } else {
    res.writeHead(404);
    res.end();
  }
});

healthServer.listen(HEALTH_PORT, '0.0.0.0', () => {
  console.log(`[Worker] Health server listening on port ${HEALTH_PORT}`);
});

function runCmd(cmd, args, timeout = JOB_TIMEOUT) {
  return new Promise((resolve, reject) => {
    execFile(cmd, args, { timeout }, (err, stdout, stderr) => {
      if (err) reject(err);
      else resolve(stdout);
    });
  });
}

function embedCoverArt(coverPath, mediaPath, format) {
  if (format !== 'mp3') return Promise.resolve();

  return new Promise((resolve) => {
    const tmpOut = mediaPath + '.tmp';
    execFile('ffmpeg', [
      '-y', '-i', mediaPath, '-i', coverPath,
      '-map', '0:a', '-map', '1:v',
      '-c', 'copy',
      '-id3v2_version', '3',
      '-metadata:s:v', 'title=Album cover',
      '-metadata:s:v', 'comment=Cover (front)',
      tmpOut
    ], { timeout: 30000 }, (err) => {
      if (err) return resolve();
      if (fs.existsSync(tmpOut)) {
        fs.renameSync(tmpOut, mediaPath);
      }
      resolve();
    });
  });
}

async function processJob(job) {
  const { jobId, url, format } = job.data;
  const workerId = process.env.HOSTNAME || 'unknown';

  console.log(`[Worker:${workerId}] Processing job ${jobId} (${format})`);

  try {
    await job.updateProgress(5);

    let info;
    try {
      info = await getVideoInfoWithFallback(url);
    } catch (err) {
      throw new Error(`Cannot access video: ${err.message}`);
    }

    const channel = (info.channel || info.uploader || 'Unknown').replace(/[/\\?%*:|"<>]/g, '-');
    const title = (info.title || 'video').replace(/[/\\?%*:|"<>]/g, '-');
    const displayName = `${channel} - ${title}`;
    const thumbnailUrl = info.thumbnail;

    job.data.displayName = displayName;
    await job.updateData(job.data);

    await job.updateProgress(10);

    const outputFile = await downloadAndConvertWithFallback(jobId, url, format, job);

    if (!fs.existsSync(outputFile)) {
      const files = fs.readdirSync(DOWNLOADS_DIR).filter(f => f.startsWith(jobId));
      if (files.length > 0) {
        const correctFile = path.join(DOWNLOADS_DIR, files[0]);
        if (correctFile !== outputFile) {
          fs.renameSync(correctFile, outputFile);
        }
      }
    }

    if (format === 'mp3' && thumbnailUrl) {
      const coverFile = path.join(DOWNLOADS_DIR, `${jobId}_cover.jpg`);
      try {
        await runCmd('curl', ['-sL', '-o', coverFile, thumbnailUrl]);
        if (fs.existsSync(coverFile) && fs.statSync(coverFile).size > 0) {
          await embedCoverArt(coverFile, outputFile, format);
        }
      } catch (e) {}
      try { fs.unlinkSync(coverFile); } catch (e) {}
    }

    await job.updateProgress(100);

    console.log(`[Worker:${workerId}] Job ${jobId} completed: ${displayName}`);
    return { jobId, displayName, format };
  } catch (err) {
    console.error(`[Worker:${workerId}] Job ${jobId} failed:`, err.message);
    throw err;
  }
}

const worker = new Worker('conversions', processJob, {
  connection: { host: REDIS_HOST, port: REDIS_PORT },
  concurrency: CONCURRENCY,
  limiter: {
    max: 5,
    duration: 1000,
  },
});

worker.on('completed', (job) => {
  console.log(`[Worker] Job ${job.data.jobId} completed successfully`);
});

worker.on('failed', (job, err) => {
  console.error(`[Worker] Job ${job?.data?.jobId} failed:`, err.message);
});

worker.on('error', (err) => {
  console.error('[Worker] Worker error:', err);
});

worker.on('ready', () => {
  console.log(`[Worker] Ready. Concurrency: ${CONCURRENCY}, Redis: ${REDIS_HOST}:${REDIS_PORT}`);
});

process.on('SIGTERM', async () => {
  console.log('[Worker] Shutting down gracefully...');
  await worker.close();
  process.exit(0);
});

process.on('SIGINT', async () => {
  console.log('[Worker] Interrupted, shutting down...');
  await worker.close();
  process.exit(0);
});
