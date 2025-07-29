FROM node:18-alpine

WORKDIR /app

COPY package*.json ./

RUN npm ci --only=production --no-cache

COPY src/ ./src/

EXPOSE 3002

CMD ["node", "src/app.js"]