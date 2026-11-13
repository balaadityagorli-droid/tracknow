FROM node:22-alpine AS base
WORKDIR /app

# Copy source (no npm install needed — zero dependencies)
COPY package.json ./
COPY src/ ./src/
COPY public/ ./public/
COPY sim/ ./sim/
COPY dev/ ./dev/
COPY infra/ ./infra/

# Create data directory
RUN mkdir -p /data

ENV NODE_ENV=production
ENV DB_PATH=/data/tracknow.db

EXPOSE 3001

HEALTHCHECK --interval=5s --timeout=3s --retries=3 \
  CMD node -e "require('http').get('http://localhost:'+process.env.PORT+'/api/health',(r)=>process.exit(r.statusCode===200?0:1))"

CMD ["node", "--no-warnings", "src/server.js"]
