import React, { useEffect, useRef, useState } from 'react';
import { useEnvs } from '../store/useEnvs.jsx';
import { newEnv, newMapping, newDestination } from '../utils/factory.js';
import Modal from './Modal.jsx';

function EnvRow({ env, active, onSelect, onMenu, onConfirm, menuOpen }) {
  return (
    <div
      className={`env-row ${active ? 'active' : ''}`}
      onClick={onSelect}
    >
      <div className="env-name">{env.name || <em className="muted">untitled</em>}</div>
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
  );
}

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
  // opens the appropriate confirmation modal at the sidebar level —
  // NOT inside this menu component. (Earlier the modals lived here
  // and got unmounted when the menu closed, before they could show.)
  //
  // We stop click propagation on the menu container so the document
  // mousedown handler can't accidentally close us (e.g. when the
  // user clicks the ⋯ button to open the menu, the same click bubbles
  // up and the document handler sees a target outside `ref.current`
  // and would call onClose — instant close).
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
              // Build the full payload (preserving existing fields) and PUT.
              const payload = {
                name,
                description: env.description,
                enabled: env.enabled,
                dlq_topic: env.dlq_topic,
                dlq_brokers: env.dlq_brokers,
                source: env.source,
                mappings: env.mappings,
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
            // Tell the parent to open the reset-confirm modal for this env.
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
            // Tell the parent to open the delete-confirm modal for this env.
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

export default function Sidebar() {
  const { state, dispatch } = useEnvs();
  const [menuFor, setMenuFor] = useState(null);
  // The pending confirm modal: { kind: 'delete' | 'reset', envId } or null.
  // Lives at the Sidebar level so the modal survives the menu closing.
  const [pending, setPending] = useState(null);

  const pendingEnv = pending ? state.envs.find((e) => e.id === pending.envId) : null;

  function createNew() {
    const env = newEnv();
    env.mappings = [newMapping()];
    env.mappings[0].destinations = [newDestination()];
    env.mappings[0].destinations[0].topic = 'example.destination';
    env.mappings[0].destinations[0].headers = [];
    env.name = `New environment ${state.envs.length + 1}`;
    dispatch({ type: 'CREATE_ENV', payload: env });
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
        <div className="sidebar-list">
          {state.envs.map((env) => (
            <EnvRow
              key={env.id}
              env={env}
              active={env.id === state.selectedId}
              onSelect={() => dispatch({ type: 'SELECT', id: env.id })}
              onMenu={(id) => setMenuFor(menuFor === id ? null : id)}
              onConfirm={(intent) => setPending(intent)}
              menuOpen={menuFor === env.id}
            />
          ))}
        </div>
      )}

      {/* Confirmation modals — at Sidebar level so they don't get
          unmounted when the menu closes. */}
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
