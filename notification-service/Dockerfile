FROM node:18-alpine

WORKDIR /app

COPY package*.json ./

RUN npm ci --only=production --no-cache

COPY src/ ./src/

CMD ["node", "src/kafka.js"]