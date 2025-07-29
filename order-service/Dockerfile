FROM node:18-alpine

WORKDIR /app

COPY package*.json ./

RUN npm ci --only=production --no-cache

COPY src/ ./src/

ENV KAFKA_BROKER=kafka:9092

EXPOSE 3001

CMD ["node", "src/app.js"]