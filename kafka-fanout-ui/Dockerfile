# Multi-stage build: Vite produces a static bundle, served by `vite preview`
# (a tiny Node server, no Nginx needed for local dev).
#
# This is the simplest thing that works for `docker compose up`. For
# production you'd swap the runtime stage for an Nginx image serving
# `dist/`, or run `vite build` at CI time and ship the bundle separately.

# --- Build stage ---
FROM node:20-alpine AS build

WORKDIR /app

# Install deps with the lockfile if present, else fall back to package.json.
COPY package.json ./
COPY package-lock.json* ./
RUN if [ -f package-lock.json ]; then \
        npm ci; \
    else \
        npm install; \
    fi

# Build the production bundle.
COPY . .
RUN npm run build

# --- Runtime stage ---
FROM node:20-alpine AS runtime

WORKDIR /app

# We only need the `dist/` output and a `package.json` declaring the
# `preview` script. Copy the manifest so `npm` finds the scripts and
# the `vite` binary installed at build time is reused here.
COPY --from=build /app/package.json ./package.json
COPY --from=build /app/package-lock.json* ./package-lock.json
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist
COPY --from=build /app/vite.config.js ./vite.config.js

ENV NODE_ENV=production \
    HOST=0.0.0.0 \
    PORT=5173

EXPOSE 5173

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD wget -qO- http://localhost:5173/ >/dev/null || exit 1

CMD ["npx", "vite", "preview", "--host", "0.0.0.0", "--port", "5173"]
