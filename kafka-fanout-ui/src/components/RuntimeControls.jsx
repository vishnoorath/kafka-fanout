import React, { useEffect, useRef, useState } from 'react';
import { useEnvs } from '../store/useEnvs.jsx';
import { api } from '../lib/api.js';
import MetricsDashboard from './MetricsDashboard.jsx';

/**
 * Start / Stop buttons, status snapshot, log viewer. Polls status while
 * the env is starting or running; back-off when stopped.
 *
 * Polling: one effect per env, started on mount, torn down on unmount.
 * The interval is computed from the *latest* status read from a ref
 * (so updating `statuses` doesn't re-run the effect and create a
 * feedback loop). State changes only re-schedule the *next* tick.
 */
export default function RuntimeControls({ env }) {
  const { state, dispatch } = useEnvs();
  const status = state.statuses[env.id];
  // Mirror the latest status in a ref so the polling closure always
  // sees the freshest state without re-creating the effect.
  const statusRef = useRef(status);
  useEffect(() => {
    statusRef.current = status;
  }, [status]);

  // Optimistic state overlay: when the user clicks Start/Stop, we
  // locally mark the env as "starting" / "stopping" so the buttons
  // disable immediately. The next poll replaces it with the real
  // server-side state.
  const [pendingCmd, setPendingCmd] = useState(null);
  const pendingRef = useRef(null);
  useEffect(() => {
    pendingRef.current = pendingCmd;
  }, [pendingCmd]);

  const [logs, setLogs] = useState([]);
  const [showLogs, setShowLogs] = useState(true);
  const [activeSubTab, setActiveSubTab] = useState('logs');
  const [deadLetters, setDeadLetters] = useState([]);
  const [loadingDLQ, setLoadingDLQ] = useState(false);

  async function fetchDeadLetters() {
    setLoadingDLQ(true);
    try {
      const data = await api.getOutboxDeadLetters(env.id);
      setDeadLetters(data || []);
    } catch (err) {
      console.error('Failed to fetch outbox dead letters', err);
    } finally {
      setLoadingDLQ(false);
    }
  }

  useEffect(() => {
    if (activeSubTab === 'outbox-dlq') {
      fetchDeadLetters();
    }
  }, [env.id, activeSubTab]);
  // Ref to the bottom sentinel for auto-scroll
  const logsBottomRef = useRef(null);

  // Auto-scroll to bottom whenever new log lines arrive and the Logs tab is visible
  useEffect(() => {
    if (activeSubTab === 'logs' && logsBottomRef.current) {
      logsBottomRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [logs, activeSubTab]);

  // Stream status and logs via SSE
  useEffect(() => {
    // Reset logs on environment change
    setLogs([]);

    const unsubscribe = api.subscribeToEnvStream(
      env.id,
      (newStatus) => {
        dispatch({ type: 'SET_STATUS', envId: env.id, status: newStatus });
        if (
          pendingRef.current &&
          (newStatus.state === 'running' ||
            newStatus.state === 'stopped' ||
            newStatus.state === 'error')
        ) {
          setPendingCmd(null);
        }
      },
      (newLogs) => {
        setLogs((prev) => {
          const prevIds = new Set(prev.map((l) => l.id));
          const filteredNew = newLogs.filter((l) => !prevIds.has(l.id));
          const merged = [...prev, ...filteredNew];
          merged.sort((a, b) => a.id - b.id);
          if (merged.length > 200) {
            return merged.slice(merged.length - 200);
          }
          return merged;
        });
      },
      (err) => {
        console.error('SSE Stream Error:', err);
      }
    );

    return () => {
      unsubscribe();
    };
  }, [env.id, dispatch]);

  // Effective state: optimistic "starting" / "stopping" while the
  // command is in flight, otherwise the server's last reported state.
  const serverState = status?.state || 'stopped';
  const stateName = pendingCmd
    ? pendingCmd === 'start'
      ? 'starting'
      : 'stopping'
    : serverState;
  const counters = status
    ? {
        consumed: status.messages_consumed,
        routed: status.messages_routed,
        failed: status.messages_failed,
      }
    : { consumed: 0, routed: 0, failed: 0 };

  async function runCommand(cmd) {
    setPendingCmd(cmd);
    // Clear the local log buffer on start so the "Show recent logs"
    // panel doesn't display lines from the previous run while we wait
    // for the next /logs poll. The backend also wipes its log table on
    // start (manager._reset_status_and_logs) so the next fetch is
    // empty until the new consumer emits its first line.
    if (cmd === 'start') {
      setLogs([]);
    }
    try {
      await new Promise((resolve, reject) => {
        // Wrap dispatch so we can detect failures and clear the
        // optimistic pending state if the API call itself errors out.
        dispatch({ type: 'RUNTIME_CMD', id: env.id, cmd, _resolve: resolve, _reject: reject });
      });
    } catch {
      // API call failed; clear optimistic state so buttons re-enable.
      // The store already pushed an error toast.
      setPendingCmd(null);
    }
  }

  // Clear Run logs older than 5 minutes from the backend log buffer,
  // and prune the locally-streamed log buffer to match.
  const [clearing, setClearing] = useState(false);
  async function handleClearLogs() {
    if (clearing) return;
    setClearing(true);
    const cutoffMs = Date.now() - 5 * 60 * 1000;
    // Optimistic local prune so the UI snaps to the cleared state.
    setLogs((prev) => prev.filter((l) => {
      const t = l.ts ? new Date(l.ts).getTime() : 0;
      return Number.isFinite(t) && t >= cutoffMs;
    }));
    try {
      const res = await api.clearLogs(env.id, 300);
      dispatch({
        type: 'TOAST',
        kind: 'success',
        text: `Cleared ${res?.deleted ?? 0} log line${res?.deleted === 1 ? '' : 's'} older than 5 min`,
      });
    } catch (err) {
      dispatch({ type: 'TOAST', kind: 'error', text: `Clear failed: ${err.message}` });
    } finally {
      setClearing(false);
    }
  }

  return (
    <div className="card mt-4">
      <div className="card-header">
        <span className="card-title">Runtime</span>
        <span className={`pill ${stateName}`}>{stateName}</span>
      </div>
      <div className="row">
        <button
          className="btn btn-primary"
          onClick={() => runCommand('start')}
          disabled={stateName === 'running' || stateName === 'starting' || stateName === 'stopping'}
        >
          Start
        </button>
        <button
          className="btn"
          onClick={() => runCommand('stop')}
          disabled={stateName === 'stopped' || stateName === 'stopping'}
        >
          Stop
        </button>
        <button
          className="btn"
          onClick={handleClearLogs}
          disabled={clearing}
          title="Delete run logs older than 5 minutes"
        >
          {clearing ? 'Clearing…' : 'Clear'}
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
        <div className="tabs sub-tabs mb-3">
          <button
            className={`tab ${activeSubTab === 'metrics' ? 'active' : ''}`}
            onClick={() => setActiveSubTab('metrics')}
          >
            Metrics Dashboard
          </button>
          <button
            className={`tab ${activeSubTab === 'logs' ? 'active' : ''}`}
            onClick={() => {
              setActiveSubTab('logs');
              setShowLogs(true);
            }}
          >
            Recent Logs
            {logs.length > 0 && (
              <span className="tab-count">({logs.length})</span>
            )}
          </button>
          {status?.delivery_mode === 'outbox' && (
            <button
              className={`tab ${activeSubTab === 'outbox-dlq' ? 'active' : ''}`}
              onClick={() => {
                setActiveSubTab('outbox-dlq');
                setShowLogs(false);
              }}
            >
              Outbox DLQ
              {status?.outbox_dead_lettered_total > 0 && (
                <span className="tab-count" style={{ backgroundColor: 'var(--err)', color: 'white', padding: '0.1rem 0.3rem', borderRadius: '3px', marginLeft: '0.35rem' }}>
                  {status.outbox_dead_lettered_total}
                </span>
              )}
            </button>
          )}
        </div>

        {activeSubTab === 'metrics' ? (
          <MetricsDashboard status={status} />
        ) : activeSubTab === 'logs' && showLogs ? (
          <div className="logs-panel mt-2">
            {logs.length === 0 ? (
              <div className="muted">No log lines yet. Start the consumer to see live logs.</div>
            ) : (
              <div className="logs-grid">
                <div className="log-header">Timestamp</div>
                <div className="log-header">Level</div>
                <div className="log-header">Message</div>
                {logs.map((l) => (
                  <React.Fragment key={l.id}>
                    <div className="log-cell ts">{l.ts}</div>
                    <div className={`log-cell level level-${l.level?.toLowerCase()}`}>{l.level}</div>
                    <div className="log-cell message">{l.message}</div>
                  </React.Fragment>
                ))}
                <div ref={logsBottomRef} />
              </div>
            )}
          </div>
        ) : activeSubTab === 'outbox-dlq' ? (
          <div className="logs-panel mt-2" style={{ maxHeight: 'calc(100vh - 380px)', overflowY: 'auto' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.75rem' }}>
              <span className="muted" style={{ fontSize: 'var(--fs-sm)' }}>
                Shows messages that failed to write to target destinations after all retries.
              </span>
              <button className="btn" onClick={fetchDeadLetters} disabled={loadingDLQ} style={{ padding: '0.2rem 0.5rem', fontSize: 'var(--fs-xs)' }}>
                {loadingDLQ ? 'Loading...' : 'Refresh'}
              </button>
            </div>
            {deadLetters.length === 0 ? (
              <div className="muted flex-center" style={{ height: '100px' }}>No outbox dead letters recorded.</div>
            ) : (
              <div className="logs-grid" style={{ gridTemplateColumns: '150px 180px 1fr 220px 80px' }}>
                <div className="log-header">Time Moved</div>
                <div className="log-header">Idempotency Key</div>
                <div className="log-header">Payload</div>
                <div className="log-header">Reason / Error</div>
                <div className="log-header">Attempts</div>
                {deadLetters.map((dl) => (
                  <React.Fragment key={dl.id}>
                    <div className="log-cell ts">{new Date(dl.dead_lettered_at).toLocaleString()}</div>
                    <div className="log-cell message">{dl.idempotency_key}</div>
                    <div className="log-cell message" style={{ fontFamily: 'monospace', fontSize: 'var(--fs-xs)', whiteSpace: 'pre-wrap', overflow: 'auto', maxHeight: '100px' }}>
                      {dl.payload}
                    </div>
                    <div className="log-cell message" style={{ color: 'var(--err)', fontSize: 'var(--fs-xs)', whiteSpace: 'pre-wrap' }}>
                      {dl.last_error}
                    </div>
                    <div className="log-cell message" style={{ textAlign: 'center' }}>{dl.attempts}</div>
                  </React.Fragment>
                ))}
              </div>
            )}
          </div>
        ) : null}
      </div>
    </div>
  );
}
