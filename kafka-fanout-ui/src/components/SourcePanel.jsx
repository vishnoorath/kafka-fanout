import React, { useMemo, useState } from 'react';
import { useEnvs, effectiveEnv, hasDirtyDraft, buildEnvPayload } from '../store/useEnvs.jsx';
import { SECRET_PLACEHOLDER } from '../utils/factory.js';

function isSasl(protocol) {
  return protocol === 'SASL_PLAINTEXT' || protocol === 'SASL_SSL';
}

function validateSource(src) {
  const errors = {};
  if (!src.brokers || !src.brokers.trim()) errors.brokers = 'Brokers is required.';
  if (!src.topic || !src.topic.trim()) errors.topic = 'Topic is required.';
  if (!src.consumer_group || !src.consumer_group.trim()) {
    errors.consumer_group = 'Consumer group is required.';
  }
  if (isSasl(src.security_protocol)) {
    if (!src.sasl_mechanism) errors.sasl_mechanism = 'SASL mechanism is required.';
    if (!src.sasl_username) errors.sasl_username = 'SASL username is required.';
    // sasl_password: placeholder counts as already-set; only an empty
    // string here is invalid.
    if (src.sasl_password != null && src.sasl_password === '') {
      errors.sasl_password = 'SASL password is required.';
    }
  }
  return errors;
}

function SecretInput({ value, onChange, placeholder = '••••••••' }) {
  // The store has a string for "user-typed value" or null for "no secret
  // set". We render a single text input that, when the user types,
  // becomes a literal value; when the user clears, we send `null`.
  return (
    <input
      className="input"
      type="password"
      autoComplete="off"
      value={value == null ? '' : value === SECRET_PLACEHOLDER ? '' : value}
      placeholder={value === SECRET_PLACEHOLDER ? placeholder : ''}
      onChange={(e) => onChange(e.target.value === '' ? null : e.target.value)}
    />
  );
}

export default function SourcePanel({ env }) {
  const { state, dispatch } = useEnvs();
  const eff = effectiveEnv(state, env.id);
  const [openAdvanced, setOpenAdvanced] = useState(false);
  const [openDlq, setOpenDlq] = useState(Boolean(eff.dlq_topic));
  const errors = useMemo(() => validateSource(eff.source), [eff.source]);
  const hasErrors = Object.keys(errors).length > 0;
  const dirty = hasDirtyDraft(state, env.id);
  const isNew = Boolean(state.drafts[env.id]?._create);

  function patchSource(field, value) {
    const next = { [field]: value };
    dispatch({ type: 'PATCH_DRAFT', envId: env.id, patch: { source: next }, touched: { source: true } });
  }

  function save() {
    if (hasErrors) return;
    const payload = buildEnvPayload(env, state.drafts[env.id] || {});
    if (isNew) {
      dispatch({ type: 'CREATE_ENV', payload });
    } else {
      dispatch({ type: 'UPDATE_ENV', id: env.id, payload });
    }
  }

  // We render the source with the draft overlay applied, but the
  // dispatch path is `patchSource(field, value)` so the draft gets the
  // delta.
  const src = eff.source;

  return (
    <div>
      <div className="card">
        <h3 className="card-title">Connection</h3>

        <div className="form-group">
          <label>Brokers</label>
          <input
            className={`input ${errors.brokers ? 'invalid' : ''}`}
            value={src.brokers || ''}
            onChange={(e) => patchSource('brokers', e.target.value)}
            placeholder="kafka:9092, kafka2:9092"
          />
          {errors.brokers ? <span className="error">{errors.brokers}</span> : null}
          <span className="hint">Comma-separated host:port list.</span>
        </div>

        <div className="form-row">
          <div className="form-group">
            <label>Topic</label>
            <input
              className={`input ${errors.topic ? 'invalid' : ''}`}
              value={src.topic || ''}
              onChange={(e) => patchSource('topic', e.target.value)}
            />
            {errors.topic ? <span className="error">{errors.topic}</span> : null}
          </div>
          <div className="form-group">
            <label>Consumer Group</label>
            <input
              className={`input ${errors.consumer_group ? 'invalid' : ''}`}
              value={src.consumer_group || ''}
              onChange={(e) => patchSource('consumer_group', e.target.value)}
            />
            {errors.consumer_group ? (
              <span className="error">{errors.consumer_group}</span>
            ) : null}
          </div>
        </div>

        <div className="form-group">
          <label>Offset Reset</label>
          <div className="radio-group">
            <label>
              <input
                type="radio"
                name={`offset_reset-${env.id}`}
                checked={src.offset_reset === 'earliest'}
                onChange={() => patchSource('offset_reset', 'earliest')}
              />
              earliest
            </label>
            <label>
              <input
                type="radio"
                name={`offset_reset-${env.id}`}
                checked={src.offset_reset === 'latest'}
                onChange={() => patchSource('offset_reset', 'latest')}
              />
              latest
            </label>
          </div>
          <span className="hint">Used only when the consumer group has no committed offset for the topic.</span>
        </div>
      </div>

      <div className="card">
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
                value={src.security_protocol || 'PLAINTEXT'}
                onChange={(e) => patchSource('security_protocol', e.target.value)}
              >
                <option value="PLAINTEXT">PLAINTEXT</option>
                <option value="SSL">SSL</option>
                <option value="SASL_PLAINTEXT">SASL_PLAINTEXT</option>
                <option value="SASL_SSL">SASL_SSL</option>
              </select>
            </div>

            {isSasl(src.security_protocol) ? (
              <>
                <div className="form-group">
                  <label>SASL Mechanism</label>
                  <select
                    className={`select ${errors.sasl_mechanism ? 'invalid' : ''}`}
                    value={src.sasl_mechanism || ''}
                    onChange={(e) => patchSource('sasl_mechanism', e.target.value)}
                  >
                    <option value="">—</option>
                    <option value="PLAIN">PLAIN</option>
                    <option value="SCRAM-SHA-256">SCRAM-SHA-256</option>
                    <option value="SCRAM-SHA-512">SCRAM-SHA-512</option>
                  </select>
                  {errors.sasl_mechanism ? <span className="error">{errors.sasl_mechanism}</span> : null}
                </div>
                <div className="form-group">
                  <label>SASL Username</label>
                  <input
                    className={`input ${errors.sasl_username ? 'invalid' : ''}`}
                    value={src.sasl_username || ''}
                    onChange={(e) => patchSource('sasl_username', e.target.value)}
                  />
                  {errors.sasl_username ? <span className="error">{errors.sasl_username}</span> : null}
                </div>
                <div className="form-group">
                  <label>SASL Password</label>
                  <SecretInput
                    value={src.sasl_password}
                    onChange={(v) => patchSource('sasl_password', v)}
                  />
                  {errors.sasl_password ? <span className="error">{errors.sasl_password}</span> : null}
                  <span className="hint">
                    Leave empty to keep the existing password; type a new value to replace it.
                  </span>
                </div>
              </>
            ) : null}

            {src.security_protocol === 'SSL' || src.security_protocol === 'SASL_SSL' ? (
              <div className="form-group">
                <label>SSL CA Location</label>
                <input
                  className="input"
                  value={src.ssl_ca_location || ''}
                  onChange={(e) => patchSource('ssl_ca_location', e.target.value)}
                  placeholder="/path/to/ca.pem"
                />
                <span className="hint">Path to CA bundle. Required when using a private CA.</span>
              </div>
            ) : null}
          </div>
        ) : null}
      </div>

      <div className="card">
        <div
          className={`collapsible-header ${openDlq ? 'open' : ''}`}
          onClick={() => setOpenDlq(!openDlq)}
        >
          Dead-Letter Queue (DLQ)
        </div>
        {openDlq ? (
          <div className="collapsible-body">
            <div className="form-group">
              <label>DLQ Topic</label>
              <input
                className="input"
                value={eff.dlq_topic || ''}
                onChange={(e) =>
                  dispatch({
                    type: 'PATCH_DRAFT',
                    envId: env.id,
                    patch: { dlq_topic: e.target.value },
                    touched: { dlq_topic: true },
                  })
                }
                placeholder="env.dlq"
              />
              <span className="hint">
                Messages that fail JSON parsing are forwarded here with an <code>__error</code> header.
                Leave empty to skip poison messages entirely.
              </span>
            </div>
            <div className="form-group">
              <label>DLQ Brokers</label>
              <input
                className="input"
                value={eff.dlq_brokers || ''}
                onChange={(e) =>
                  dispatch({
                    type: 'PATCH_DRAFT',
                    envId: env.id,
                    patch: { dlq_brokers: e.target.value },
                    touched: { dlq_brokers: true },
                  })
                }
                placeholder="(leave empty to use source brokers)"
              />
            </div>
          </div>
        ) : null}
      </div>

      <div className="row-end mt-3">
        <button className="btn btn-primary" onClick={save} disabled={hasErrors}>
          {isNew ? 'Create environment' : 'Save'}
        </button>
      </div>
    </div>
  );
}
