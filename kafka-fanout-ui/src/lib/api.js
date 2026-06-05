/**
 * Tiny fetch wrapper for the backend API.
 *
 * - Injects `Content-Type: application/json` for non-GET requests with a body.
 * - Throws a normalized `ApiError` on non-2xx responses.
 * - Uses VITE_API_BASE in production; defaults to '/api' which is
 *   proxied to the FastAPI server in dev.
 */

const BASE = import.meta.env.VITE_API_BASE || '/api';

export class ApiError extends Error {
  constructor(status, code, message, details) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.code = code;
    this.details = details || {};
  }
}

async function request(method, path, body) {
  const opts = { method, headers: {} };
  if (body !== undefined) {
    opts.headers['Content-Type'] = 'application/json';
    opts.body = JSON.stringify(body);
  }
  const res = await fetch(`${BASE}${path}`, opts);
  if (res.status === 204) return null;
  let data = null;
  try {
    data = await res.json();
  } catch {
    // empty body
  }
  if (!res.ok) {
    const err = data && data.error;
    throw new ApiError(
      res.status,
      err?.code || 'http_error',
      err?.message || res.statusText,
      err?.details || {},
    );
  }
  return data;
}

export const api = {
  listEnvs: () => request('GET', '/envs'),
  getEnv: (id) => request('GET', `/envs/${id}`),
  createEnv: (payload) => request('POST', '/envs', payload),
  updateEnv: (id, payload) => request('PUT', `/envs/${id}`, payload),
  deleteEnv: (id) => request('DELETE', `/envs/${id}`),
  duplicateEnv: (id) => request('POST', `/envs/${id}/duplicate`),
  startEnv: (id) => request('POST', `/envs/${id}/start`),
  stopEnv: (id) => request('POST', `/envs/${id}/stop`),
  resetOffsets: (id) => request('POST', `/envs/${id}/reset-offsets`),
  getStatus: (id) => request('GET', `/envs/${id}/status`),
  getLogs: (id, limit = 200) => request('GET', `/envs/${id}/logs?limit=${limit}`),
  testMessage: (id, message) => request('POST', `/envs/${id}/test`, { message }),
  exportAll: () => request('GET', '/export'),
  importAll: (envelope) => request('POST', '/import', envelope),
  getOutboxDeadLetters: (id) => request('GET', `/envs/${id}/outbox/dead-letters`),
  subscribeToEnvStream: (envId, onStatus, onLog, onError) => {
    const url = BASE.startsWith('http')
      ? `${BASE}/envs/${envId}/stream`
      : `${window.location.origin}${BASE}/envs/${envId}/stream`;
    const eventSource = new EventSource(url);
    
    eventSource.addEventListener('status', (e) => {
      try { onStatus(JSON.parse(e.data)); } catch {}
    });
    
    eventSource.addEventListener('log', (e) => {
      try { onLog(JSON.parse(e.data)); } catch {}
    });
    
    if (onError) {
      eventSource.addEventListener('error', (e) => {
        try {
          if (e.data) onError(JSON.parse(e.data));
        } catch {}
      });
    }
    
    return () => eventSource.close();
  },
};
