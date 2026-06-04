import React, { useEffect, useRef, useState } from 'react';
import { useEnvs, effectiveEnv } from '../store/useEnvs.jsx';
import { newEnv } from '../utils/factory.js';
import Modal from './Modal.jsx';
import { previewMatchCondition } from '../utils/expression.js';

function EnvMenu({ env, onClose, onConfirm }) {
  const { dispatch } = useEnvs();
  const ref = useRef(null);
  useEffect(() => {
    function onDocMouseDown(e) {
      if (ref.current && !ref.current.contains(e.target)) onClose();
    }
    document.addEventListener('mousedown', onDocMouseDown);
    return () => document.removeEventListener('mousedown', onDocMouseDown);
  }, [onClose]);

  // Menu items dispatch an "intent" to the parent (Sidebar) which
  // opens the appropriate confirmation modal at the sidebar level.
  return (
    <div
      className="menu-container"
      ref={ref}
      onClick={(e) => e.stopPropagation()}
      onMouseDown={(e) => e.stopPropagation()}
    >
      <div className="menu">
        <button
          className="menu-item"
          onClick={() => {
            const name = window.prompt('New name', env.name);
            if (name && name !== env.name) {
              const payload = {
                name,
                description: env.description,
                enabled: env.enabled,
                dlq_topic: env.dlq_topic,
                dlq_brokers: env.dlq_brokers,
                source: env.source,
                domain_groupings: env.domain_groupings,
              };
              dispatch({ type: 'UPDATE_ENV', id: env.id, payload });
            }
            onClose();
          }}
        >
          Rename
        </button>
        <button
          className="menu-item"
          onClick={() => {
            dispatch({ type: 'DUPLICATE_ENV', id: env.id });
            onClose();
          }}
        >
          Duplicate
        </button>
        <button
          className="menu-item"
          onClick={() => {
            onConfirm({ kind: 'reset', envId: env.id });
            onClose();
          }}
        >
          Reset offsets to earliest
        </button>
        <div className="menu-divider" />
        <button
          className="menu-item danger"
          onClick={() => {
            onConfirm({ kind: 'delete', envId: env.id });
            onClose();
          }}
        >
          Delete
        </button>
      </div>
    </div>
  );
}

function StatusPill({ stateName }) {
  return <span className={`pill ${stateName || 'stopped'}`}>{stateName || 'stopped'}</span>;
}

/** Build a one-line summary for a DG node label. */
function dgSummaryLabel(dg) {
  const name = dg.name || '(unnamed)';
  const mcs = dg.match_conditions || [];
  if (mcs.length === 0) return `${name} · (no conditions)`;
  if (mcs.length === 1) return `${name} · ${previewMatchCondition(mcs[0])}`;
  return `${name} · ${mcs.length} match conditions`;
}

function EnvTreeNode({ env, expanded, onToggle, menuOpen, onMenu, onConfirm, statusName }) {
  const { state, dispatch } = useEnvs();
  const eff = effectiveEnv(state, env.id);
  const dgs = eff.domain_groupings || [];
  const isSelected =
    env.id === state.selectedId && state.selectedDGIndex == null;

  return (
    <div className={`tree-node ${isSelected ? 'active' : ''}`}>
      <div
        className="tree-row env-row"
        onClick={() => {
          onToggle();
          dispatch({ type: 'SELECT', id: env.id });
        }}
      >
        <button
          className={`tree-chevron ${expanded ? 'open' : ''}`}
          onClick={(e) => {
            e.stopPropagation();
            onToggle();
          }}
          aria-label={expanded ? 'Collapse' : 'Expand'}
        >
          ▸
        </button>
        <div className="env-name">{env.name || <em className="muted">untitled</em>}</div>
        <StatusPill stateName={statusName} />
        <span className="muted mapping-count">({dgs.length})</span>
        <button
          className="env-menu-btn"
          onClick={(e) => {
            e.stopPropagation();
            onMenu(env.id);
          }}
          aria-label="env menu"
        >
          ⋯
        </button>
        {menuOpen ? (
          <EnvMenu env={env} onClose={() => onMenu(null)} onConfirm={onConfirm} />
        ) : null}
      </div>
      {expanded ? (
        <ul className="tree-children" role="group">
          {dgs.length === 0 ? (
            <li className="tree-child muted">no domain groupings</li>
          ) : (
            dgs.map((dg, idx) => {
              const isActive =
                env.id === state.selectedId && state.selectedDGIndex === idx;
              return (
                <li
                  key={idx}
                  className={`tree-child mapping-row ${isActive ? 'active' : ''}`}
                  onClick={() =>
                    dispatch({ type: 'SELECT_DG', envId: env.id, index: idx })
                  }
                  title={dgSummaryLabel(dg)}
                >
                  <span className="tree-child-bullet">↳</span>
                  <span className="tree-child-label">
                    #{idx + 1} · {dg.name || '(unnamed)'}
                  </span>
                </li>
              );
            })
          )}
        </ul>
      ) : null}
    </div>
  );
}

export default function Sidebar() {
  const { state, dispatch } = useEnvs();
  const [menuFor, setMenuFor] = useState(null);
  const [expanded, setExpanded] = useState(() => {
    if (state.selectedId) return { [state.selectedId]: true };
    return {};
  });
  const [pending, setPending] = useState(null);

  const pendingEnv = pending ? state.envs.find((e) => e.id === pending.envId) : null;

  function toggle(envId) {
    setExpanded((prev) => ({ ...prev, [envId]: !prev[envId] }));
  }

  function createNew() {
    const env = newEnv();
    dispatch({ type: 'CREATE_ENV', payload: env });
    setExpanded((prev) => ({ ...prev, [env.id]: true }));
  }

  if (state.loading) {
    return (
      <aside className="sidebar">
        <div className="sidebar-header">
          <h1 className="sidebar-title">Environments</h1>
        </div>
        <div className="sidebar-empty">Loading…</div>
      </aside>
    );
  }

  return (
    <aside className="sidebar">
      <div className="sidebar-header">
        <h1 className="sidebar-title">Environments</h1>
        <button className="btn btn-primary" onClick={createNew}>
          + New Environment
        </button>
      </div>
      {state.envs.length === 0 ? (
        <div className="sidebar-empty">No environments yet — create one to get started.</div>
      ) : (
        <div className="sidebar-list tree">
          {state.envs.map((env) => (
            <EnvTreeNode
              key={env.id}
              env={env}
              expanded={!!expanded[env.id]}
              onToggle={() => toggle(env.id)}
              statusName={state.statuses[env.id]?.state}
              menuOpen={menuFor === env.id}
              onMenu={(id) => setMenuFor(menuFor === id ? null : id)}
              onConfirm={(intent) => setPending(intent)}
            />
          ))}
        </div>
      )}

      <Modal
        open={pending?.kind === 'delete'}
        title="Delete environment?"
        onClose={() => setPending(null)}
        footer={
          <>
            <button className="btn" onClick={() => setPending(null)}>
              Cancel
            </button>
            <button
              className="btn btn-danger"
              onClick={() => {
                if (pending) {
                  dispatch({ type: 'DELETE_ENV', id: pending.envId });
                  setPending(null);
                }
              }}
            >
              Delete
            </button>
          </>
        }
      >
        This will stop the consumer (if running) and remove the env
        from the database. The action cannot be undone.
      </Modal>

      <Modal
        open={pending?.kind === 'reset'}
        title="Reset offsets to earliest?"
        onClose={() => setPending(null)}
        footer={
          <>
            <button className="btn" onClick={() => setPending(null)}>
              Cancel
            </button>
            <button
              className="btn btn-primary"
              onClick={() => {
                if (pending) {
                  dispatch({ type: 'RUNTIME_CMD', id: pending.envId, cmd: 'reset-offsets' });
                  setPending(null);
                }
              }}
            >
              Reset
            </button>
          </>
        }
      >
        Stops the consumer (if running), deletes the consumer group,
        and lets the next Start pick up from <code>earliest</code>.
      </Modal>
    </aside>
  );
}
