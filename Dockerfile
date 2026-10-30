FROM oven/bun:1.1-alpine

WORKDIR /app

COPY package.json bun.lockb* ./
RUN bun install --frozen-lockfile || bun install

COPY tsconfig.json ./
COPY src ./src

EXPOSE 3000

CMD ["bun", "run", "src/main.ts"]
