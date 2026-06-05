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
        </div>

        {activeSubTab === 'metrics' ? (
          <MetricsDashboard status={status} />
        ) : showLogs ? (
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
        ) : null}
      </div>
    </div>
  );
}
