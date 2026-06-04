import React, { useEffect, useState } from 'react';
import { useEnvs } from '../store/useEnvs.jsx';
import { api } from '../lib/api.js';

/**
 * Start / Stop buttons, status snapshot, log viewer. Polls status while
 * the env is starting or running; back-off when stopped.
 */
export default function RuntimeControls({ env }) {
  const { state, dispatch } = useEnvs();
  const status = state.statuses[env.id];
  const [logsOpen, setLogsOpen] = useState(false);
  const [logs, setLogs] = useState([]);
  const [showLogs, setShowLogs] = useState(false);

  // Poll status: 2s while starting/running, 10s while stopped, 5s on error.
  useEffect(() => {
    let cancelled = false;
    let timer = null;
    async function tick() {
      try {
        const s = await api.getStatus(env.id);
        if (cancelled) return;
        dispatch({ type: 'SET_STATUS', envId: env.id, status: s });
      } catch {
        // ignore
      }
      const next = status?.state;
      const interval = next === 'running' || next === 'starting' ? 2000 : next === 'error' ? 5000 : 10000;
      timer = setTimeout(tick, interval);
    }
    tick();
    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
    };
  }, [env.id, status?.state, dispatch]);

  // Logs: refresh every 3s while open.
  useEffect(() => {
    if (!showLogs) return;
    let cancelled = false;
    let timer = null;
    async function tick() {
      try {
        const rows = await api.getLogs(env.id, 200);
        if (cancelled) return;
        setLogs(rows);
      } catch {
        // ignore
      }
      timer = setTimeout(tick, 3000);
    }
    tick();
    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
    };
  }, [env.id, showLogs]);

  const stateName = status?.state || 'stopped';
  const counters = status
    ? {
        consumed: status.messages_consumed,
        routed: status.messages_routed,
        failed: status.messages_failed,
      }
    : { consumed: 0, routed: 0, failed: 0 };

  return (
    <div className="card mt-4">
      <div className="card-header">
        <span className="card-title">Runtime</span>
        <span className={`pill ${stateName}`}>{stateName}</span>
      </div>
      <div className="row">
        <button
          className="btn btn-primary"
          onClick={() => dispatch({ type: 'RUNTIME_CMD', id: env.id, cmd: 'start' })}
          disabled={stateName === 'running' || stateName === 'starting' || stateName === 'stopping'}
        >
          Start
        </button>
        <button
          className="btn"
          onClick={() => dispatch({ type: 'RUNTIME_CMD', id: env.id, cmd: 'stop' })}
          disabled={stateName === 'stopped' || stateName === 'stopping'}
        >
          Stop
        </button>
        <span className="muted">
          consumed: <strong>{counters.consumed}</strong> · routed: <strong>{counters.routed}</strong> · failed:{' '}
          <strong>{counters.failed}</strong>
        </span>
      </div>
      {status?.last_error ? (
        <div className="mt-2" style={{ color: 'var(--err)', fontSize: 'var(--fs-sm)' }}>
          {status.last_error}
        </div>
      ) : null}
      <div className="mt-3">
        <button className="btn-link" onClick={() => setShowLogs(!showLogs)}>
          {showLogs ? 'Hide' : 'Show'} recent logs
        </button>
        {showLogs ? (
          <div className="logs-panel mt-2">
            {logs.length === 0 ? (
              <div className="muted">No log lines yet.</div>
            ) : (
              logs.map((l) => (
                <div className={`log-line level-${l.level}`} key={l.id}>
                  {l.ts} {l.level} :: {l.message}
                </div>
              ))
            )}
          </div>
        ) : null}
      </div>
    </div>
  );
}
