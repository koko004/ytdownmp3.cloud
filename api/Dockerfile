FROM node:18-slim

WORKDIR /app

COPY package*.json ./
RUN npm install --omit=dev

COPY api.js .

RUN mkdir -p /app/downloads

EXPOSE 3000

CMD ["node", "api.js"]
