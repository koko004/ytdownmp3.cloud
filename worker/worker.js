const { Worker } = require('bullmq');
const { spawn, execFile } = require('child_process');
const path = require('path');
const fs = require('fs');

const REDIS_HOST = process.env.REDIS_HOST || 'redis';
const REDIS_PORT = process.env.REDIS_PORT || 6379;
const DOWNLOADS_DIR = process.env.DOWNLOADS_DIR || '/app/downloads';
const CONCURRENCY = parseInt(process.env.WORKER_CONCURRENCY || '2', 10);
const JOB_TIMEOUT = parseInt(process.env.JOB_TIMEOUT || '120000', 10);

if (!fs.existsSync(DOWNLOADS_DIR)) {
  fs.mkdirSync(DOWNLOADS_DIR, { recursive: true });
}

function runCmd(cmd, args, timeout = JOB_TIMEOUT) {
  return new Promise((resolve, reject) => {
    execFile(cmd, args, { timeout }, (err, stdout, stderr) => {
      if (err) reject(err);
      else resolve(stdout);
    });
  });
}

function getVideoInfo(url) {
  return new Promise((resolve, reject) => {
    execFile('yt-dlp', [
      '--dump-json', '--no-playlist', '--no-warnings', url
    ], { timeout: 30000 }, (err, stdout) => {
      if (err) return reject(new Error('Could not fetch video info. The video may be private or unavailable.'));
      try {
        resolve(JSON.parse(stdout));
      } catch (e) {
        reject(new Error('Failed to parse video metadata'));
      }
    });
  });
}

function downloadAndConvert(jobId, url, format, job) {
  return new Promise((resolve, reject) => {
    const outputFile = path.join(DOWNLOADS_DIR, `${jobId}.${format}`);

    let args;
    if (format === 'mp3') {
      args = [
        '--no-playlist', '--no-warnings', '--newline',
        '-x', '--audio-format', 'mp3', '--audio-quality', '0',
        '-o', outputFile, url
      ];
    } else {
      args = [
        '--no-playlist', '--no-warnings', '--newline',
        '-f', 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
        '--merge-output-format', 'mp4',
        '-o', outputFile, url
      ];
    }

    const proc = spawn('yt-dlp', args);
    let lastProgress = 10;
    let stderrData = '';

    const progressRegex = /(\d+\.?\d*)%\s+of\s+~?[\d.]+\w+/;

    proc.stdout.on('data', (data) => {
      const text = data.toString();
      stderrData += text;
      const match = text.match(progressRegex) || text.match(/(\d+\.?\d*)%/);
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
      const match = text.match(progressRegex) || text.match(/(\d+\.?\d*)%/);
      if (match) {
        const progress = Math.min(parseFloat(match[1]), 99);
        if (progress > lastProgress) {
          lastProgress = progress;
          job.updateProgress(progress);
        }
      }
    });

    proc.on('close', (code) => {
      if (code === 0) {
        resolve(outputFile);
      } else {
        reject(new Error(`yt-dlp exited with code ${code}: ${stderrData.slice(0, 500)}`));
      }
    });

    proc.on('error', (err) => {
      reject(new Error(`Failed to spawn yt-dlp: ${err.message}`));
    });
  });
}

function embedCoverArt(coverPath, mediaPath, format) {
  if (format !== 'mp3') return Promise.resolve();

  return new Promise((resolve, reject) => {
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
      info = await getVideoInfo(url);
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

    const outputFile = await downloadAndConvert(jobId, url, format, job);

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
