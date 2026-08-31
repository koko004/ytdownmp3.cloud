# Deploy ytdownmp3 on Another Host

## Quick Start (Docker Compose)

```bash
# 1. Clone the repo
git clone https://github.com/koko004/ytdownmp3.cloud.git
cd ytdownmp3.cloud

# 2. Create .env file
cat > .env << 'ENVEOF'
REDIS_HOST=redis
REDIS_PORT=6379
DOWNLOADS_DIR=/app/downloads
WORKER_CONCURRENCY=2
JOB_TIMEOUT=120000
ENVEOF

# 3. Start all services
docker compose up -d
```

## Services & Ports

| Service | Port | Description |
|---------|------|-------------|
| Frontend | 3000 | Main app (nginx) |
| API | 8080 | Backend API |
| Admin | 4000 | Hidden admin panel |
| Redis | 6379 | Queue/cache |

## Production with Nginx Reverse Proxy

```nginx
# /etc/nginx/sites-available/ytdownmp3
server {
    listen 80;
    server_name ytdownmp3.cloud www.ytdownmp3.cloud;

    # Frontend
    location / {
        proxy_pass http://localhost:3000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }

    # API
    location /api/ {
        proxy_pass http://localhost:8080/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }

    # Admin (restrict by IP)
    location /admin/ {
        allow YOUR_ADMIN_IP;
        deny all;
        proxy_pass http://localhost:4000/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

## Scale Workers

```bash
# Scale to N workers
docker compose up -d --scale worker=4
```

## Using Pre-built Images (No Build)

```yaml
# docker-compose.prod.yml
version: "3.8"
services:
  redis:
    image: redis:7-alpine
    volumes:
      - redis-data:/data

  api:
    image: koko004/ytdownmp3-api:latest
    environment:
      - REDIS_HOST=redis
      - DOWNLOADS_DIR=/app/downloads
    volumes:
      - downloads:/app/downloads

  worker:
    image: koko004/ytdownmp3-worker:latest
    environment:
      - REDIS_HOST=redis
      - DOWNLOADS_DIR=/app/downloads
      - WORKER_CONCURRENCY=2
    volumes:
      - downloads:/app/downloads

  frontend:
    image: koko004/ytdownmp3-frontend:latest
    ports:
      - "3000:80"

  admin:
    image: koko004/ytdownmp3-admin:latest
    environment:
      - REDIS_HOST=redis
    ports:
      - "4000:4000"

volumes:
  redis-data:
  downloads:
```

Run: `docker compose -f docker-compose.prod.yml up -d`

## Admin Panel Access

- URL: `http://your-server:4000`
- Restrict via firewall or nginx `allow/deny`
- Shows: CPU, memory, disk, queue stats, visitors by IP/country

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REDIS_HOST` | redis | Redis hostname |
| `REDIS_PORT` | 6379 | Redis port |
| `DOWNLOADS_DIR` | /app/downloads | Shared volume path |
| `WORKER_CONCURRENCY` | 2 | Parallel jobs per worker |
| `JOB_TIMEOUT` | 120000 | Max job time (ms) |
