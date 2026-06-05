import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// Shared proxy target for both dev (`vite dev`) and Docker preview (`vite preview`).
// Inside Docker, services talk via service name: `backend:8000`.
// Outside Docker (local dev), the backend is on `localhost:8000`.
// Override with VITE_BACKEND_URL env var when needed.
// Default to localhost:8000 for `npm run dev` outside Docker.
// docker-compose.yml passes VITE_BACKEND_URL=http://backend:8000 for the container.
const BACKEND = process.env.VITE_BACKEND_URL || 'http://localhost:8000';

/**
 * Configure the http-proxy instance so that SSE (Server-Sent Events)
 * connections are never buffered.  Without this the proxy accumulates
 * the entire streamed response before forwarding it, so the browser
 * EventSource never fires until the connection drops.
 */
function configureSseProxy(proxy) {
  proxy.on('proxyReq', (proxyReq, req) => {
    if (req.url && req.url.includes('/stream')) {
      // Disable the proxy socket timeout so long-lived SSE connections
      // are not killed after the default idle timeout.
      proxyReq.setTimeout(0);
    }
  });
  proxy.on('proxyRes', (proxyRes, req) => {
    if (req.url && req.url.includes('/stream')) {
      // Ensure downstream caches and Nginx do not buffer the stream.
      proxyRes.headers['cache-control'] = 'no-cache';
      proxyRes.headers['x-accel-buffering'] = 'no';
    }
  });
}

const apiProxy = {
  target: BACKEND,
  changeOrigin: true,
  configure: configureSseProxy,
};

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: { '/api': apiProxy },
  },
  // `vite preview` (used inside Docker) needs its own proxy section —
  // it does NOT inherit from `server.proxy`.
  preview: {
    port: 5173,
    proxy: { '/api': apiProxy },
  },
});
