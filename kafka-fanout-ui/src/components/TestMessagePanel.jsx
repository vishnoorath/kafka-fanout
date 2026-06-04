import React, { useMemo, useState } from 'react';
import { useEnvs, effectiveEnv } from '../store/useEnvs.jsx';
import { safeSearch } from '../lib/jmespath-preview.js';

/**
 * Sticky test-message panel. Pure client-side evaluation — the same
 * jmespath library the backend uses, with the same strict-typing rule
 * (non-scalar result -> not matched, error "non-scalar result").
 */
function evaluateClient(mapping, parsedMessage) {
  const r = safeSearch(mapping.key_path, parsedMessage);
  if (!r.ok) {
    return { matched: false, resolved: null, error: `invalid JMESPath: ${r.error}`, kind: 'error' };
  }
  const v = r.value;
  if (v == null) {
    return { matched: false, resolved: null, error: 'key not found', kind: 'unmatched' };
  }
  if (typeof v === 'object') {
    return { matched: false, resolved: v, error: 'non-scalar result', kind: 'unmatched' };
  }
  // Coerce non-strings.
  const vs = String(v);
  let target = mapping.value;
  let a = vs;
  let b = target;
  if (mapping.case_insensitive !== false) {
    a = a.toLowerCase();
    b = b.toLowerCase();
  }
  let matched = false;
  if (mapping.operator === 'equals') matched = a === b;
  else if (mapping.operator === 'not_equals') matched = a !== b;
  else if (mapping.operator === 'contains') matched = a.includes(b);
  return {
    matched,
    resolved: v,
    error: matched ? null : 'resolved value did not match',
    kind: matched ? 'matched' : 'unmatched',
  };
}

function stringify(v) {
  if (v == null) return 'null';
  if (typeof v === 'string') return JSON.stringify(v);
  return String(v);
}

export default function TestMessagePanel({ env }) {
  const { state, dispatch } = useEnvs();
  const [collapsed, setCollapsed] = useState(false);
  const eff = effectiveEnv(state, env.id);
  const mappings = eff.mappings || [];
  const parsed = useMemo(() => {
    try {
      return JSON.parse(state.testMessage);
    } catch {
      return null;
    }
  }, [state.testMessage]);

  return (
    <div className="test-panel">
      <div className="test-panel-header" onClick={() => setCollapsed(!collapsed)}>
        <strong>Test message</strong>
        <span className="muted">{collapsed ? '▸' : '▾'}</span>
      </div>
      {collapsed ? null : (
        <div>
          <textarea
            className="textarea"
            value={state.testMessage}
            onChange={(e) => dispatch({ type: 'SET_TEST_MESSAGE', value: e.target.value })}
            spellCheck={false}
            rows={6}
            style={{ minHeight: 120 }}
          />
          {parsed == null ? (
            <p className="muted mt-2">Invalid JSON — nothing to evaluate.</p>
          ) : (
            <div className="mt-2">
              {mappings.length === 0 ? (
                <p className="muted">Add mappings to see results.</p>
              ) : (
                mappings.map((m, idx) => {
                  const result = evaluateClient(m, parsed);
                  return (
                    <div key={idx} className="mb-2">
                      <div className="match-result">
                        <span className={`match-badge ${result.kind}`}>
                          {result.kind === 'matched' ? '✓' : result.kind === 'error' ? '!' : '·'}
                        </span>
                        <span className="mono">#{idx + 1} {m.key_path}</span>
                        <span className="muted">→ {stringify(result.resolved)}</span>
                        {result.error ? <span className="muted">({result.error})</span> : null}
                      </div>
                      {result.matched ? (
                        <div className="destination-block">
                          {m.destinations.length === 0 ? (
                            <em className="muted">No destinations</em>
                          ) : (
                            m.destinations.map((d, didx) => {
                              const headers = (d.headers || []).map((h) => {
                                if (h.mode === 'from_message') {
                                  const r = safeSearch(h.value, parsed);
                                  return { name: h.name, value: r.ok ? stringify(r.value) : `(error: ${r.error})` };
                                }
                                return { name: h.name, value: h.value };
                              });
                              return (
                                <div key={didx} className="mb-2">
                                  <strong className="mono">{d.topic}</strong>
                                  {headers.length > 0 ? (
                                    <div className="mt-2">
                                      {headers.map((h, hidx) => (
                                        <div className="header-row-preview" key={hidx}>
                                          <span>{h.name}</span>
                                          <span className="arrow">→</span>
                                          <span>{h.value}</span>
                                        </div>
                                      ))}
                                    </div>
                                  ) : null}
                                </div>
                              );
                            })
                          )}
                        </div>
                      ) : null}
                    </div>
                  );
                })
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
