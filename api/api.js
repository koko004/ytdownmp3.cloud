const express = require('express');
const { Queue } = require('bullmq');
const { v4: uuidv4 } = require('uuid');
const path = require('path');
const fs = require('fs');
const Redis = require('ioredis');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 3000;
const REDIS_HOST = process.env.REDIS_HOST || 'redis';
const REDIS_PORT = process.env.REDIS_PORT || 6379;
const DOWNLOADS_DIR = process.env.DOWNLOADS_DIR || '/app/downloads';

const conversionQueue = new Queue('conversions', {
  connection: { host: REDIS_HOST, port: REDIS_PORT },
  defaultJobOptions: {
    removeOnComplete: { age: 3600 },
    removeOnFail: { age: 86400 },
  },
});

const redisClient = new Redis({ host: REDIS_HOST, port: REDIS_PORT, maxRetriesPerRequest: 1 });

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
    const filename = `${displayName}${ext}`;
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

app.listen(PORT, () => {
  console.log(`[API] Server running on port ${PORT}`);
  console.log(`[API] Redis: ${REDIS_HOST}:${REDIS_PORT}`);
});
