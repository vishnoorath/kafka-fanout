import React, { useState } from 'react';
import { useEnvs, effectiveEnv, hasDirtyDraft, buildEnvPayload } from '../store/useEnvs.jsx';

/**
 * Env header: name, description, save state, export/import buttons.
 * The actual save button lives in the Source / Mappings panels so the
 * payload is built from the latest effective state.
 */
export default function EnvHeader({ env }) {
  const { state, dispatch } = useEnvs();
  const eff = effectiveEnv(state, env.id);
  const dirty = hasDirtyDraft(state, env.id);
  const [editingName, setEditingName] = useState(false);
  const [editingDesc, setEditingDesc] = useState(false);

  const saveState = dirty ? (
    <span className="save-state dirty">Unsaved changes</span>
  ) : (
    <span className="save-state">Saved</span>
  );

  function patchName(value) {
    dispatch({ type: 'PATCH_DRAFT', envId: env.id, patch: { name: value }, touched: { name: true } });
  }
  function patchDesc(value) {
    dispatch({ type: 'PATCH_DRAFT', envId: env.id, patch: { description: value }, touched: { description: true } });
  }

  async function exportAll() {
    const apiMod = await import('../lib/api.js');
    const data = await apiMod.api.exportAll();
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'kafka-fanout-export.json';
    a.click();
    URL.revokeObjectURL(url);
  }

  async function importFile() {
    const input = document.createElement('input');
    input.type = 'file';
    input.accept = 'application/json';
    input.onchange = async () => {
      const file = input.files?.[0];
      if (!file) return;
      try {
        const text = await file.text();
        const data = JSON.parse(text);
        const apiMod = await import('../lib/api.js');
        const envs = await apiMod.api.importAll(data);
        // Reload the list.
        for (const e of envs) {
          dispatch({ type: 'REPLACE_ENV', env: e });
        }
        dispatch({ type: 'TOAST', kind: 'success', text: `Imported ${envs.length} envs` });
      } catch (exc) {
        dispatch({ type: 'TOAST', kind: 'error', text: `Import failed: ${exc.message}` });
      }
    };
    input.click();
  }

  return (
    <header className="env-header">
      <div>
        {editingName ? (
          <input
            className="input"
            value={eff.name}
            autoFocus
            onChange={(e) => patchName(e.target.value)}
            onBlur={() => setEditingName(false)}
            onKeyDown={(e) => e.key === 'Enter' && setEditingName(false)}
          />
        ) : (
          <h2
            className="env-title"
            onClick={() => setEditingName(true)}
            title="Click to rename"
            style={{ cursor: 'text' }}
          >
            {eff.name || <em className="muted">untitled</em>}
          </h2>
        )}
        {editingDesc ? (
          <input
            className="input"
            value={eff.description}
            autoFocus
            onChange={(e) => patchDesc(e.target.value)}
            onBlur={() => setEditingDesc(false)}
            onKeyDown={(e) => e.key === 'Enter' && setEditingDesc(false)}
          />
        ) : (
          <span className="env-desc" onClick={() => setEditingDesc(true)} style={{ cursor: 'text' }}>
            {eff.description || 'Add a description…'}
          </span>
        )}
      </div>
      <div className="btn-group" style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.35rem' }}>
          <label style={{ fontSize: '0.8125rem', color: 'var(--text-muted)' }}>Delivery Mode:</label>
          <select
            className="select"
            style={{ width: 'auto', padding: '0.15rem 0.4rem', height: 'auto', fontSize: '0.8125rem' }}
            value={eff.delivery_mode || 'at_least_once'}
            onChange={(e) => {
              dispatch({
                type: 'PATCH_DRAFT',
                envId: env.id,
                patch: { delivery_mode: e.target.value },
                touched: { delivery_mode: true }
              });
            }}
          >
            <option value="at_least_once">At-least-once</option>
            <option value="outbox">Outbox (EOS)</option>
          </select>
          <span className="muted" title="Outbox mode ensures exactly-once-like publishing to all matching topics. Note: Downstream consumers must deduplicate messages using X-Source-Coord headers." style={{ cursor: 'help', fontSize: '0.875rem' }}>ⓘ</span>
        </div>
        {saveState}
        <button className="btn" onClick={exportAll}>
          Export
        </button>
        <button className="btn" onClick={importFile}>
          Import
        </button>
      </div>
    </header>
  );
}
