import React, { useEffect, useRef, useState } from 'react';
import { useEnvs } from '../store/useEnvs.jsx';
import { api } from '../lib/api.js';

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
  const [showLogs, setShowLogs] = useState(false);

  // Poll status: 2s while starting/running, 10s while stopped, 5s on error.
  // The effect runs once per env; the interval is read from the ref
  // each tick so a state change only changes the *next* interval, not
  // the currently-pending one.
  useEffect(() => {
    let cancelled = false;
    let timer = null;
    async function tick() {
      let live = statusRef.current?.state;
      try {
        const s = await api.getStatus(env.id);
        if (cancelled) return;
        dispatch({ type: 'SET_STATUS', envId: env.id, status: s });
        // Use the freshly fetched state directly — statusRef.current is
        // only updated after React re-renders (via useEffect), so reading
        // it here would give the *previous* cycle's value.
        live = s?.state;
      } catch {
        // ignore; keep the last-known live value for interval calculation
      }
      if (cancelled) return;
      // Clear any optimistic pending state once the server reports a
      // terminal state (running or stopped).
      if (pendingRef.current && (live === 'running' || live === 'stopped' || live === 'error')) {
        setPendingCmd(null);
      }
      const interval =
        live === 'running' || live === 'starting' ? 2000 : live === 'error' ? 5000 : 10000;
      timer = setTimeout(tick, interval);
    }
    tick();
    return () => {
      cancelled = true;
      if (timer) clearTimeout(timer);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- statusRef is a ref, not a dep;
    // we read it at the top of tick() as a fallback when the fetch fails.
  }, [env.id, dispatch]);

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
        <button className="btn-link" onClick={() => setShowLogs(!showLogs)}>
          {showLogs ? 'Hide' : 'Show'} recent logs
        </button>
        {showLogs ? (
          <div className="logs-panel mt-2">
            {logs.length === 0 ? (
              <div className="muted">No log lines yet.</div>
            ) : (
              <div className="logs-grid">
                <div className="log-header">Timestamp</div>
                <div className="log-header">Level</div>
                <div className="log-header">Message</div>
                {logs.map((l) => (
                  <React.Fragment key={l.id}>
                    <div className="log-cell ts">{l.ts}</div>
                    <div className={`log-cell level level-${l.level}`}>{l.level}</div>
                    <div className="log-cell message">{l.message}</div>
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
