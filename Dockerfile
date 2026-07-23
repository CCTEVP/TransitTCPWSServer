FROM node:20-bookworm-slim

ENV NODE_ENV=production
WORKDIR /app

# Native build requirements for zeromq when prebuilt binaries are unavailable.
RUN apt-get update && apt-get install -y --no-install-recommends \
  python3 \
  make \
  g++ \
  pkg-config \
  libzmq3-dev \
  && rm -rf /var/lib/apt/lists/*

COPY package*.json ./
RUN npm ci --omit=dev

COPY . .

EXPOSE 8080
CMD ["npm", "start"]
