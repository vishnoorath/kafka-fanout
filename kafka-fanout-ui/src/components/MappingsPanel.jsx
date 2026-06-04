import React, { useEffect, useMemo, useState } from 'react';
import { useEnvs, effectiveEnv, hasDirtyDraft, buildEnvPayload } from '../store/useEnvs.jsx';
import { newMapping, newDestination, newHeader, SECRET_PLACEHOLDER } from '../utils/factory.js';
import { previewCondition } from '../utils/expression.js';
import { safeSearch } from '../lib/jmespath-preview.js';

function moveItem(arr, from, to) {
  const next = [...arr];
  const [m] = next.splice(from, 1);
  next.splice(to, 0, m);
  return next;
}

function patchDraftMappings(envId, mappings) {
  // Also touch every field so hasDirtyDraft reports true.
  return { type: 'PATCH_DRAFT', envId, patch: { mappings }, touched: { mappings: true } };
}

// ---------- Headers editor ----------

function HeadersEditor({ destination, onChange, testMessageParsed }) {
  const headers = destination.headers || [];
  function addHeader() {
    onChange([...headers, newHeader()]);
  }
  function removeHeader(idx) {
    onChange(headers.filter((_, i) => i !== idx));
  }
  function updateHeader(idx, field, value) {
    const next = headers.map((h, i) => (i === idx ? { ...h, [field]: value } : h));
    onChange(next);
  }
  return (
    <div className="mt-2">
      <label className="hint">Headers</label>
      {headers.length === 0 ? (
        <div className="muted mb-2">No headers.</div>
      ) : (
        headers.map((h, idx) => {
          let preview = null;
          if (h.mode === 'from_message' && testMessageParsed != null) {
            const r = safeSearch(h.value, testMessageParsed);
            preview = r.ok ? (r.value == null ? '(null)' : String(r.value)) : `(error: ${r.error})`;
          }
          return (
            <div key={idx} className="form-row mb-2" style={{ alignItems: 'flex-end' }}>
              <div className="form-group" style={{ flex: '0 0 30%' }}>
                {idx === 0 ? <label>Name</label> : null}
                <input
                  className="input"
                  value={h.name || ''}
                  onChange={(e) => updateHeader(idx, 'name', e.target.value)}
                />
              </div>
              <div className="form-group" style={{ flex: 1 }}>
                {idx === 0 ? <label>Value</label> : null}
                <input
                  className="input"
                  value={h.value || ''}
                  onChange={(e) => updateHeader(idx, 'value', e.target.value)}
                />
                {preview != null ? (
                  <span className="preview-line">→ {preview}</span>
                ) : null}
              </div>
              <div className="form-group" style={{ flex: '0 0 130px' }}>
                {idx === 0 ? <label>Mode</label> : null}
                <select
                  className="select"
                  value={h.mode || 'static'}
                  onChange={(e) => updateHeader(idx, 'mode', e.target.value)}
                >
                  <option value="static">static</option>
                  <option value="from_message">from_message</option>
                </select>
              </div>
              <button
                className="btn-link"
                onClick={() => removeHeader(idx)}
                title="Remove header"
              >
                Remove
              </button>
            </div>
          );
        })
      )}
      <button className="btn-link" onClick={addHeader}>
        + Add header
      </button>
    </div>
  );
}

// ---------- Destination editor ----------

function DestinationEditor({ destination, onChange, onRemove, testMessageParsed }) {
  const [openAdvanced, setOpenAdvanced] = useState(false);
  function patch(field, value) {
    onChange({ ...destination, [field]: value });
  }
  function patchHeaderList(headers) {
    patch('headers', headers);
  }
  return (
    <div className="card">
      <div className="card-header">
        <span className="card-title">Destination</span>
        <button className="btn-link" onClick={onRemove}>
          Remove
        </button>
      </div>

      <div className="form-group">
        <label className="checkbox">
          <input
            type="checkbox"
            checked={destination.use_source_broker !== false}
            onChange={(e) => patch('use_source_broker', e.target.checked)}
          />
          Use source broker
        </label>
      </div>

      {!destination.use_source_broker ? (
        <div className="form-group">
          <label>Brokers</label>
          <input
            className="input"
            value={destination.brokers || ''}
            onChange={(e) => patch('brokers', e.target.value)}
            placeholder="kafka-b:9092"
          />
        </div>
      ) : (
        <p className="muted">(inherits brokers + security from source)</p>
      )}

      <div className="form-group">
        <label>Topic</label>
        <input
          className="input"
          value={destination.topic || ''}
          onChange={(e) => patch('topic', e.target.value)}
        />
      </div>

      <div
        className={`collapsible-header ${openAdvanced ? 'open' : ''}`}
        onClick={() => setOpenAdvanced(!openAdvanced)}
      >
        Advanced — Security
      </div>
      {openAdvanced ? (
        <div className="collapsible-body">
          <div className="form-group">
            <label>Security Protocol</label>
            <select
              className="select"
              value={destination.security_protocol || 'PLAINTEXT'}
              onChange={(e) => patch('security_protocol', e.target.value)}
            >
              <option value="PLAINTEXT">PLAINTEXT</option>
              <option value="SSL">SSL</option>
              <option value="SASL_PLAINTEXT">SASL_PLAINTEXT</option>
              <option value="SASL_SSL">SASL_SSL</option>
            </select>
          </div>
          {destination.security_protocol === 'SASL_PLAINTEXT' ||
          destination.security_protocol === 'SASL_SSL' ? (
            <>
              <div className="form-group">
                <label>SASL Mechanism</label>
                <select
                  className="select"
                  value={destination.sasl_mechanism || ''}
                  onChange={(e) => patch('sasl_mechanism', e.target.value)}
                >
                  <option value="">—</option>
                  <option value="PLAIN">PLAIN</option>
                  <option value="SCRAM-SHA-256">SCRAM-SHA-256</option>
                  <option value="SCRAM-SHA-512">SCRAM-SHA-512</option>
                </select>
              </div>
              <div className="form-group">
                <label>SASL Username</label>
                <input
                  className="input"
                  value={destination.sasl_username || ''}
                  onChange={(e) => patch('sasl_username', e.target.value)}
                />
              </div>
              <div className="form-group">
                <label>SASL Password</label>
                <input
                  className="input"
                  type="password"
                  autoComplete="off"
                  value={destination.sasl_password === SECRET_PLACEHOLDER ? '' : destination.sasl_password || ''}
                  placeholder={destination.sasl_password === SECRET_PLACEHOLDER ? SECRET_PLACEHOLDER : ''}
                  onChange={(e) => patch('sasl_password', e.target.value === '' ? null : e.target.value)}
                />
              </div>
            </>
          ) : null}
          {destination.security_protocol === 'SSL' ||
          destination.security_protocol === 'SASL_SSL' ? (
            <div className="form-group">
              <label>SSL CA Location</label>
              <input
                className="input"
                value={destination.ssl_ca_location || ''}
                onChange={(e) => patch('ssl_ca_location', e.target.value)}
              />
            </div>
          ) : null}
        </div>
      ) : null}

      <HeadersEditor
        destination={destination}
        onChange={patchHeaderList}
        testMessageParsed={testMessageParsed}
      />
    </div>
  );
}

// ---------- Condition editor ----------

function ConditionEditor({ mapping, onChange }) {
  function patch(field, value) {
    onChange({ ...mapping, [field]: value });
  }
  const preview = previewCondition(mapping);
  return (
    <div>
      <div className="form-group">
        <label>Key path (JMESPath)</label>
        <input
          className="input"
          value={mapping.key_path || ''}
          onChange={(e) => patch('key_path', e.target.value)}
          placeholder="Message.TableName"
        />
        <span className="hint">Use JMESPath syntax, e.g. <code>Message.TableName</code> or <code>items[0].id</code>.</span>
      </div>
      <div className="form-row">
        <div className="form-group" style={{ flex: '0 0 160px' }}>
          <label>Operator</label>
          <select
            className="select"
            value={mapping.operator || 'equals'}
            onChange={(e) => patch('operator', e.target.value)}
          >
            <option value="equals">equals</option>
            <option value="not_equals">not_equals</option>
            <option value="contains">contains</option>
          </select>
        </div>
        <div className="form-group">
          <label>Value</label>
          <input
            className="input"
            value={mapping.value || ''}
            onChange={(e) => patch('value', e.target.value)}
          />
        </div>
        <div className="form-group" style={{ flex: '0 0 200px' }}>
          <label>&nbsp;</label>
          <label className="checkbox">
            <input
              type="checkbox"
              checked={mapping.case_insensitive !== false}
              onChange={(e) => patch('case_insensitive', e.target.checked)}
            />
            Case-insensitive
          </label>
        </div>
      </div>
      <div className="preview-line">Match: {preview}</div>
    </div>
  );
}

// ---------- Mapping card ----------

function MappingCard({ index, mapping, onChange, onRemove, onMove, total, testMessageParsed }) {
  const [collapsed, setCollapsed] = useState(false);
  const [openAdvanced, setOpenAdvanced] = useState(false);
  function patchDest(idx, value) {
    const next = mapping.destinations.map((d, i) => (i === idx ? value : d));
    onChange({ ...mapping, destinations: next });
  }
  function addDest() {
    onChange({ ...mapping, destinations: [...mapping.destinations, newDestination()] });
  }
  function removeDest(idx) {
    onChange({ ...mapping, destinations: mapping.destinations.filter((_, i) => i !== idx) });
  }
  return (
    <div className="card">
      <div className="card-header">
        <span
          className="card-title"
          onClick={() => setCollapsed(!collapsed)}
          style={{ cursor: 'pointer', flex: 1 }}
        >
          #{index + 1} — Match: {previewCondition(mapping)}
        </span>
        <div className="btn-group">
          <button
            className="btn-link"
            onClick={() => onMove(index, index - 1)}
            disabled={index === 0}
            title="Move up"
          >
            ↑
          </button>
          <button
            className="btn-link"
            onClick={() => onMove(index, index + 1)}
            disabled={index === total - 1}
            title="Move down"
          >
            ↓
          </button>
          <button className="btn-link" onClick={() => setCollapsed(!collapsed)}>
            {collapsed ? 'Expand' : 'Collapse'}
          </button>
          <button className="btn-link" onClick={onRemove}>
            Remove
          </button>
        </div>
      </div>
      {collapsed ? null : (
        <>
          <ConditionEditor mapping={mapping} onChange={onChange} />
          <div className="mt-3">
            <label className="hint">Destinations</label>
            {mapping.destinations.length === 0 ? (
              <p className="muted">No destinations yet.</p>
            ) : (
              mapping.destinations.map((d, idx) => (
                <DestinationEditor
                  key={idx}
                  destination={d}
                  onChange={(v) => patchDest(idx, v)}
                  onRemove={() => removeDest(idx)}
                  testMessageParsed={testMessageParsed}
                />
              ))
            )}
            <button className="btn mt-2" onClick={addDest}>
              + Add destination
            </button>
          </div>
        </>
      )}
    </div>
  );
}

// ---------- MappingsPanel ----------

export default function MappingsPanel({ env }) {
  const { state, dispatch } = useEnvs();
  const eff = effectiveEnv(state, env.id);
  const mappings = eff.mappings || [];
  const dirty = hasDirtyDraft(state, env.id);
  const isNew = Boolean(state.drafts[env.id]?._create);
  const testMessageParsed = useMemo(() => {
    try {
      return JSON.parse(state.testMessage);
    } catch {
      return null;
    }
  }, [state.testMessage]);

  function setMappings(next) {
    dispatch(patchDraftMappings(env.id, next));
  }
  function patchMapping(idx, value) {
    setMappings(mappings.map((m, i) => (i === idx ? value : m)));
  }
  function moveMapping(from, to) {
    if (to < 0 || to >= mappings.length) return;
    setMappings(moveItem(mappings, from, to));
  }
  function removeMapping(idx) {
    setMappings(mappings.filter((_, i) => i !== idx));
  }
  function addMapping() {
    setMappings([...mappings, newMapping()]);
  }

  function save() {
    const payload = buildEnvPayload(env, state.drafts[env.id] || {});
    if (isNew) {
      dispatch({ type: 'CREATE_ENV', payload });
    } else {
      dispatch({ type: 'UPDATE_ENV', id: env.id, payload });
    }
  }

  return (
    <div>
      {mappings.length === 0 ? (
        <p className="muted">No mappings yet — add one to start routing.</p>
      ) : (
        mappings.map((m, idx) => (
          <MappingCard
            key={idx}
            index={idx}
            mapping={m}
            onChange={(v) => patchMapping(idx, v)}
            onRemove={() => removeMapping(idx)}
            onMove={moveMapping}
            total={mappings.length}
            testMessageParsed={testMessageParsed}
          />
        ))
      )}
      <div className="row-end mt-3 mb-4">
        <button className="btn" onClick={addMapping}>
          + Add mapping
        </button>
        <button className="btn btn-primary" onClick={save}>
          {isNew ? 'Create environment' : 'Save'}
        </button>
      </div>
    </div>
  );
}
