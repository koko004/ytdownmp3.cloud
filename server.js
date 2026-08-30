const express = require('express');
const { execFile, spawn } = require('child_process');
const path = require('path');
const fs = require('fs');
const { v4: uuidv4 } = require('uuid');

const app = express();
const PORT = 3000;

app.use(express.json());

app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('X-Frame-Options', 'DENY');
  res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
  next();
});

app.use(express.static(path.join(__dirname, 'public'), {
  maxAge: '7d',
  setHeaders: (res, filePath) => {
    if (filePath.endsWith('logo.png')) {
      res.setHeader('Cache-Control', 'public, max-age=31536000, immutable');
    }
  }
}));

const OUTPUT_DIR = path.join(__dirname, 'output');
if (!fs.existsSync(OUTPUT_DIR)) fs.mkdirSync(OUTPUT_DIR, { recursive: true });

const jobs = new Map();

function runCmd(cmd, args) {
  return new Promise((resolve, reject) => {
    execFile(cmd, args, { timeout: 120000 }, (err, stdout, stderr) => {
      if (err) reject(err);
      else resolve(stdout);
    });
  });
}

function embedCover(coverPath, mp3Path) {
  return new Promise((resolve, reject) => {
    const tmpOut = mp3Path + '.tmp';
    execFile('ffmpeg', [
      '-y', '-i', mp3Path, '-i', coverPath,
      '-map', '0:a', '-map', '1:v',
      '-c', 'copy',
      '-id3v2_version', '3',
      '-metadata:s:v', 'title=Album cover',
      '-metadata:s:v', 'comment=Cover (front)',
      tmpOut
    ], { timeout: 30000 }, (err) => {
      if (err) return reject(err);
      if (fs.existsSync(tmpOut)) fs.renameSync(tmpOut, mp3Path);
      resolve();
    });
  });
}

app.get('/api/info', (req, res) => {
  const url = req.query.url;
  if (!url) return res.status(400).json({ error: 'URL required' });

  execFile('yt-dlp', [
    '--dump-json', '--no-playlist', '--no-warnings', url
  ], { timeout: 30000 }, (err, stdout) => {
    if (err) return res.status(400).json({ error: 'Could not get video info' });
    try {
      const info = JSON.parse(stdout);
      res.json({
        title: info.title,
        thumbnail: info.thumbnail,
        duration: info.duration,
        channel: info.channel || info.uploader
      });
    } catch (e) {
      res.status(500).json({ error: 'Error parsing info' });
    }
  });
});

app.post('/api/convert', (req, res) => {
  const { url, format } = req.body;
  if (!url) return res.status(400).json({ error: 'URL required' });

  const jobId = uuidv4();
  const ext = format === 'mp3' ? 'mp3' : 'mp4';
  const outputFile = path.join(OUTPUT_DIR, `${jobId}.${ext}`);

  jobs.set(jobId, { status: 'processing', progress: 0 });
  res.json({ jobId, status: 'processing' });

  (async () => {
    try {
      const stdout = await runCmd('yt-dlp', [
        '--dump-json', '--no-playlist', '--no-warnings', url
      ]);
      const info = JSON.parse(stdout);

      const channel = (info.channel || info.uploader || 'Unknown').replace(/[/\\?%*:|"<>]/g, '-');
      const title = (info.title || 'video').replace(/[/\\?%*:|"<>]/g, '-');
      const displayName = `${channel} - ${title}`;
      const thumbnailUrl = info.thumbnail;

      let args;
      if (format === 'mp3') {
        args = [
          '--no-playlist', '--no-warnings',
          '-x', '--audio-format', 'mp3', '--audio-quality', '0',
          '-o', outputFile, url
        ];
      } else {
        args = [
          '--no-playlist', '--no-warnings',
          '-f', 'bestvideo[ext=mp4]+bestaudio[ext=m4a]/best[ext=mp4]/best',
          '--merge-output-format', 'mp4',
          '-o', outputFile, url
        ];
      }

      await new Promise((resolve, reject) => {
        const proc = spawn('yt-dlp', args);
        proc.stderr.on('data', (data) => {
          const match = data.toString().match(/(\d+\.?\d*)%/);
          if (match) jobs.set(jobId, { ...jobs.get(jobId), progress: parseFloat(match[1]) });
        });
        proc.on('close', (code) => code === 0 ? resolve() : reject(new Error('yt-dlp failed')));
      });

      let finalFile = outputFile;
      if (!fs.existsSync(finalFile)) {
        const files = fs.readdirSync(OUTPUT_DIR).filter(f => f.startsWith(jobId));
        if (files.length > 0) finalFile = path.join(OUTPUT_DIR, files[0]);
      }

      if (ext === 'mp3' && thumbnailUrl) {
        const coverFile = path.join(OUTPUT_DIR, `${jobId}_cover.jpg`);
        try {
          await runCmd('curl', ['-sL', '-o', coverFile, thumbnailUrl]);
          if (fs.existsSync(coverFile) && fs.statSync(coverFile).size > 0) {
            await embedCover(coverFile, finalFile);
          }
        } catch (e) {}
        try { fs.unlinkSync(coverFile); } catch (e) {}
      }

      jobs.set(jobId, {
        status: 'done', progress: 100,
        file: path.basename(finalFile),
        displayName
      });
    } catch (e) {
      jobs.set(jobId, { status: 'error', error: e.message || 'Conversion error' });
    }
  })();
});

app.get('/api/status/:jobId', (req, res) => {
  const job = jobs.get(req.params.jobId);
  if (!job) return res.status(404).json({ error: 'Job not found' });
  res.json(job);
});

app.get('/api/download/:jobId', (req, res) => {
  const job = jobs.get(req.params.jobId);
  if (!job || job.status !== 'done') return res.status(404).json({ error: 'File not available' });

  const filePath = path.join(OUTPUT_DIR, job.file);
  if (!fs.existsSync(filePath)) return res.status(404).json({ error: 'File not found' });

  const ext = path.extname(job.file);
  const mimeTypes = { '.mp3': 'audio/mpeg', '.mp4': 'video/mp4' };
  res.setHeader('Content-Type', mimeTypes[ext] || 'application/octet-stream');
  res.setHeader('Content-Disposition', `attachment; filename="${job.displayName}${ext}"`);

  const stream = fs.createReadStream(filePath);
  stream.pipe(res);

  stream.on('end', () => {
    setTimeout(() => {
      try { fs.unlinkSync(filePath); } catch (e) {}
      jobs.delete(req.params.jobId);
    }, 60000);
  });
});

app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
});
