import React, { useMemo, useState } from 'react';
import { useEnvs, effectiveEnv, hasDirtyDraft, buildEnvPayload } from '../store/useEnvs.jsx';
import {
  newDomainGrouping,
  newDestination,
  newHeader,
  newMatchCondition,
  newMatchConditionValue,
  SECRET_PLACEHOLDER,
} from '../utils/factory.js';
import { previewMatchCondition } from '../utils/expression.js';
import { safeSearch } from '../lib/jmespath-preview.js';
import Modal from './Modal.jsx';

function moveItem(arr, from, to) {
  const next = [...arr];
  const [m] = next.splice(from, 1);
  next.splice(to, 0, m);
  return next;
}

function patchDraftDGs(envId, dgs) {
  return { type: 'PATCH_DRAFT', envId, patch: { domain_groupings: dgs }, touched: { domain_groupings: true } };
}

// ---------- Headers editor (unchanged) ----------

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

// ---------- Destination editor (unchanged shape, retargeted at DG) ----------

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

// ---------- Match condition editor (with values list) ----------

function MatchConditionEditor({ mc, onChange }) {
  function patch(field, value) {
    onChange({ ...mc, [field]: value });
  }
  function patchValue(idx, value) {
    const next = (mc.values || []).map((v, i) => (i === idx ? { value } : v));
    onChange({ ...mc, values: next });
  }
  function addValue() {
    onChange({ ...mc, values: [...(mc.values || []), newMatchConditionValue()] });
  }
  function removeValue(idx) {
    onChange({ ...mc, values: (mc.values || []).filter((_, i) => i !== idx) });
  }
  const preview = previewMatchCondition(mc);
  return (
    <div className="card">
      <div className="form-group">
        <label>Key path (JMESPath)</label>
        <input
          className="input"
          value={mc.key_path || ''}
          onChange={(e) => patch('key_path', e.target.value)}
          placeholder="Message.TableName"
        />
        <span className="hint">
          Use JMESPath syntax, e.g. <code>Message.TableName</code> or <code>items[0].id</code>.
        </span>
      </div>
      <div className="form-row">
        <div className="form-group" style={{ flex: '0 0 160px' }}>
          <label>Operator</label>
          <select
            className="select"
            value={mc.operator || 'equals'}
            onChange={(e) => patch('operator', e.target.value)}
          >
            <option value="equals">equals</option>
            <option value="not_equals">not_equals</option>
            <option value="contains">contains</option>
          </select>
        </div>
        <div className="form-group" style={{ flex: '0 0 200px' }}>
          <label>&nbsp;</label>
          <label className="checkbox">
            <input
              type="checkbox"
              checked={mc.case_insensitive !== false}
              onChange={(e) => patch('case_insensitive', e.target.checked)}
            />
            Case-insensitive
          </label>
        </div>
      </div>
      <div className="form-group">
        <label>Values (any match → fire)</label>
        {(mc.values || []).map((v, idx) => (
          <div key={idx} className="form-row mb-2" style={{ alignItems: 'flex-end' }}>
            <div className="form-group" style={{ flex: 1, marginBottom: 0 }}>
              {idx === 0 ? <label>Value</label> : null}
              <input
                className="input"
                value={v.value || ''}
                onChange={(e) => patchValue(idx, e.target.value)}
                placeholder={idx === 0 ? 'e.g. tbl_Deworming' : ''}
              />
            </div>
            <button
              className="btn-link"
              onClick={() => removeValue(idx)}
              disabled={(mc.values || []).length <= 1}
              title={
                (mc.values || []).length <= 1
                  ? 'A match condition must have at least one value'
                  : 'Remove value'
              }
            >
              Remove
            </button>
          </div>
        ))}
        <button className="btn-link" onClick={addValue}>
          + Add value
        </button>
      </div>
      <div className="preview-line">Match: {preview}</div>
    </div>
  );
}

function DGNameSelector({ value, onChange }) {
  const { state, dispatch } = useEnvs();
  const [showModal, setShowModal] = useState(false);
  const [customName, setCustomName] = useState('');

  function handleSelectChange(e) {
    const v = e.target.value;
    if (v === '__other__') {
      setCustomName('');
      setShowModal(true);
    } else {
      onChange(v);
    }
  }

  function handleAddCustom() {
    const trimmed = customName.trim();
    if (trimmed) {
      dispatch({ type: 'ADD_DG_NAME', name: trimmed });
      onChange(trimmed);
      setShowModal(false);
    }
  }

  return (
    <div className="form-row" style={{ alignItems: 'flex-end' }}>
      <div className="form-group" style={{ flex: '0 0 220px', marginBottom: 0 }}>
        <label>Domain Grouping</label>
        <select
          className="select"
          value={state.dgNames.includes(value) ? value : (value ? '__custom_not_saved__' : '')}
          onChange={handleSelectChange}
        >
          <option value="">— select —</option>
          {state.dgNames.map((n) => (
            <option key={n} value={n}>
              {n}
            </option>
          ))}
          {!state.dgNames.includes(value) && value ? (
            <option value="__custom_not_saved__">{value} (custom)</option>
          ) : null}
          <option value="__other__">Other…</option>
        </select>
      </div>

      <Modal
        open={showModal}
        title="Add Custom Domain Grouping"
        onClose={() => setShowModal(false)}
        footer={
          <div className="btn-group">
            <button className="btn" onClick={() => setShowModal(false)}>
              Cancel
            </button>
            <button className="btn btn-primary" onClick={handleAddCustom}>
              Add
            </button>
          </div>
        }
      >
        <div className="form-group">
          <label>Grouping Name</label>
          <input
            className="input"
            value={customName}
            onChange={(e) => setCustomName(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') {
                handleAddCustom();
              }
            }}
            placeholder="e.g. Sales"
            autoFocus
          />
        </div>
      </Modal>
    </div>
  );
}

// ---------- Domain grouping card ----------

function DomainGroupingCard({
  index,
  dg,
  onChange,
  onRemove,
  onMove,
  total,
  testMessageParsed,
  showHeader = true,
}) {
  const [collapsed, setCollapsed] = useState(false);
  function patchMC(idx, value) {
    const next = (dg.match_conditions || []).map((m, i) => (i === idx ? value : m));
    onChange({ ...dg, match_conditions: next });
  }
  function addMC() {
    onChange({ ...dg, match_conditions: [...(dg.match_conditions || []), newMatchCondition()] });
  }
  function removeMC(idx) {
    onChange({
      ...dg,
      match_conditions: (dg.match_conditions || []).filter((_, i) => i !== idx),
    });
  }
  function patchDest(idx, value) {
    const next = (dg.destinations || []).map((d, i) => (i === idx ? value : d));
    onChange({ ...dg, destinations: next });
  }
  function addDest() {
    onChange({ ...dg, destinations: [...(dg.destinations || []), newDestination()] });
  }
  function removeDest(idx) {
    onChange({
      ...dg,
      destinations: (dg.destinations || []).filter((_, i) => i !== idx),
    });
  }
  return (
    <div className="card">
      {showHeader ? (
        <div className="card-header">
          <span
            className="card-title"
            onClick={() => setCollapsed(!collapsed)}
            style={{ cursor: 'pointer', flex: 1 }}
          >
            #{index + 1} — {(dg.name || '(unnamed)')} ·{' '}
            {(dg.match_conditions || []).length} match condition
            {(dg.match_conditions || []).length === 1 ? '' : 's'} ·{' '}
            {(dg.destinations || []).length} destination
            {(dg.destinations || []).length === 1 ? '' : 's'}
          </span>
          <div className="btn-group">
            {onMove ? (
              <>
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
              </>
            ) : null}
            <button className="btn-link" onClick={() => setCollapsed(!collapsed)}>
              {collapsed ? 'Expand' : 'Collapse'}
            </button>
            {onRemove ? (
              <button className="btn-link" onClick={onRemove}>
                Remove
              </button>
            ) : null}
          </div>
        </div>
      ) : null}
      {collapsed ? null : (
        <>
          <DGNameSelector
            value={dg.name || ''}
            onChange={(v) => onChange({ ...dg, name: v })}
          />

          <div className="mt-3">
            <label className="hint">Match conditions (any matching → fire)</label>
            {(dg.match_conditions || []).length === 0 ? (
              <p className="muted">No match conditions — this DG will never fire.</p>
            ) : (
              (dg.match_conditions || []).map((mc, mcIdx) => (
                <div key={mcIdx} className="mb-2">
                  <MatchConditionEditor
                    mc={mc}
                    onChange={(v) => patchMC(mcIdx, v)}
                  />
                  {(dg.match_conditions || []).length > 1 ? (
                    <div className="row-end">
                      <button className="btn-link" onClick={() => removeMC(mcIdx)}>
                        Remove condition
                      </button>
                    </div>
                  ) : null}
                </div>
              ))
            )}
            <button className="btn mt-2" onClick={addMC}>
              + Add match condition
            </button>
          </div>

          <div className="mt-3">
            <label className="hint">Destinations</label>
            {(dg.destinations || []).length === 0 ? (
              <p className="muted">No destinations yet.</p>
            ) : (
              (dg.destinations || []).map((d, dIdx) => (
                <DestinationEditor
                  key={dIdx}
                  destination={d}
                  onChange={(v) => patchDest(dIdx, v)}
                  onRemove={() => removeDest(dIdx)}
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

// ---------- Single-DG focused panel ----------

export function SingleDomainGroupingPanel({ env, dgIndex }) {
  const { state, dispatch } = useEnvs();
  const eff = effectiveEnv(state, env.id);
  const dgs = eff.domain_groupings || [];
  const dg = dgs[dgIndex];
  const dirty = hasDirtyDraft(state, env.id);
  const isNew = Boolean(state.drafts[env.id]?._create);
  const testMessageParsed = useMemo(() => {
    try {
      return JSON.parse(state.testMessage);
    } catch {
      return null;
    }
  }, [state.testMessage]);

  if (!dg) {
    return (
      <div>
        <p className="muted">
          Domain grouping #{dgIndex + 1} no longer exists. It may have been removed.
        </p>
      </div>
    );
  }

  function patch(value) {
    const next = dgs.map((d, i) => (i === dgIndex ? value : d));
    dispatch(patchDraftDGs(env.id, next));
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
      <DomainGroupingCard
        index={dgIndex}
        dg={dg}
        onChange={patch}
        testMessageParsed={testMessageParsed}
      />
      <div className="row-end mt-3">
        <button
          className="btn-link"
          onClick={() => dispatch({ type: 'SELECT', id: env.id })}
        >
          ← Back to environment
        </button>
        <span className="spacer" />
        <button className="btn btn-primary" onClick={save}>
          {isNew ? 'Create environment' : 'Save'}
        </button>
      </div>
    </div>
  );
}

// ---------- Domain groupings panel (full env view) ----------

export default function DomainGroupingsPanel({ env }) {
  const { state, dispatch } = useEnvs();
  const eff = effectiveEnv(state, env.id);
  const dgs = eff.domain_groupings || [];
  const dirty = hasDirtyDraft(state, env.id);
  const isNew = Boolean(state.drafts[env.id]?._create);
  const testMessageParsed = useMemo(() => {
    try {
      return JSON.parse(state.testMessage);
    } catch {
      return null;
    }
  }, [state.testMessage]);

  function setDGs(next) {
    dispatch(patchDraftDGs(env.id, next));
  }
  function patchDG(idx, value) {
    setDGs(dgs.map((d, i) => (i === idx ? value : d)));
  }
  function moveDG(from, to) {
    if (to < 0 || to >= dgs.length) return;
    setDGs(moveItem(dgs, from, to));
  }
  function removeDG(idx) {
    setDGs(dgs.filter((_, i) => i !== idx));
  }
  function addDG() {
    setDGs([...dgs, newDomainGrouping()]);
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
      {dgs.length === 0 ? (
        <p className="muted">No domain groupings yet — add one to start routing.</p>
      ) : (
        dgs.map((dg, idx) => (
          <DomainGroupingCard
            key={idx}
            index={idx}
            dg={dg}
            onChange={(v) => patchDG(idx, v)}
            onRemove={() => removeDG(idx)}
            onMove={moveDG}
            total={dgs.length}
            testMessageParsed={testMessageParsed}
          />
        ))
      )}
      <div className="row-end mt-3 mb-4">
        <button className="btn" onClick={addDG}>
          + Add domain grouping
        </button>
        <button className="btn btn-primary" onClick={save}>
          {isNew ? 'Create environment' : 'Save'}
        </button>
      </div>
    </div>
  );
}
