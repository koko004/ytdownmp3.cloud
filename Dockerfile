FROM node:18-slim

# Instalar ffmpeg, python3 y curl que pide tu flujo
RUN apt-get update && apt-get install -y \
    ffmpeg \
    curl \
    python3 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copiar dependencias e instalar
COPY package*.json ./
RUN npm install

# Copiar el resto del proyecto (incluyendo la carpeta public)
COPY . .

EXPOSE 3000

# Comando para iniciar tu servidor Express
CMD ["node", "server.js"]
