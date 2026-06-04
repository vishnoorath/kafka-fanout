import React, { useMemo, useState } from 'react';
import { useEnvs, effectiveEnv } from '../store/useEnvs.jsx';
import { safeSearch } from '../lib/jmespath-preview.js';

/**
 * Client-side evaluation of a single match condition.
 * Mirrors the backend's `evaluate_match_condition` (OR-list semantics).
 */
function evaluateMatchConditionClient(mc, parsedMessage) {
  const r = safeSearch(mc.key_path, parsedMessage);
  if (!r.ok) {
    return {
      matched: false,
      resolved: null,
      error: `invalid JMESPath: ${r.error}`,
      kind: 'error',
      matchedValueIndex: null,
      matchedValue: null,
    };
  }
  const v = r.value;
  if (v == null) {
    return { matched: false, resolved: null, error: 'key not found', kind: 'unmatched', matchedValueIndex: null, matchedValue: null };
  }
  if (typeof v === 'object') {
    return { matched: false, resolved: v, error: 'non-scalar result', kind: 'unmatched', matchedValueIndex: null, matchedValue: null };
  }
  const values = (mc.values || []).map((x) => (x && typeof x === 'object' ? x.value : x)) || [];
  if (values.length === 0) {
    return { matched: false, resolved: v, error: 'no values to match against', kind: 'unmatched', matchedValueIndex: null, matchedValue: null };
  }
  const caseInsensitive = mc.case_insensitive !== false;
  const vs = String(v);
  for (let i = 0; i < values.length; i++) {
    let target = String(values[i] == null ? '' : values[i]);
    let a = vs;
    let b = target;
    if (caseInsensitive) {
      a = a.toLowerCase();
      b = b.toLowerCase();
    }
    let matched = false;
    if (mc.operator === 'equals') matched = a === b;
    else if (mc.operator === 'not_equals') matched = a !== b;
    else if (mc.operator === 'contains') matched = a.includes(b);
    if (matched) {
      return { matched: true, resolved: v, error: null, kind: 'matched', matchedValueIndex: i, matchedValue: values[i] };
    }
  }
  return { matched: false, resolved: v, error: 'resolved value did not match', kind: 'unmatched', matchedValueIndex: null, matchedValue: null };
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
  const dgs = eff.domain_groupings || [];
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
              {dgs.length === 0 ? (
                <p className="muted">Add a domain grouping to see results.</p>
              ) : (
                dgs.map((dg, dgIdx) => {
                  const mcs = dg.match_conditions || [];
                  const mcResults = mcs.map((mc) => evaluateMatchConditionClient(mc, parsed));
                  const anyMatched = mcResults.some((r) => r.matched);
                  return (
                    <div key={dgIdx} className="mb-3">
                      <div className="match-result">
                        <span className={`match-badge ${anyMatched ? 'matched' : (mcResults.some((r) => r.kind === 'error') ? 'error' : 'unmatched')}`}>
                          {anyMatched ? '✓' : (mcResults.some((r) => r.kind === 'error') ? '!' : '·')}
                        </span>
                        <span className="mono">#{dgIdx + 1} {dg.name || '(unnamed)'}</span>
                        <span className="muted">— {anyMatched ? 'matched' : 'no match'}</span>
                      </div>
                      {mcResults.map((r, mcIdx) => (
                        <div key={mcIdx} className="mb-1" style={{ marginLeft: 24 }}>
                          <div className="match-result">
                            <span className={`match-badge ${r.kind}`}>
                              {r.kind === 'matched' ? '✓' : r.kind === 'error' ? '!' : '·'}
                            </span>
                            <span className="mono">MC#{mcIdx + 1} {mcs[mcIdx].key_path}</span>
                            <span className="muted">→ {stringify(r.resolved)}</span>
                            {r.matched ? (
                              <span className="muted"> matched "{r.matchedValue}"</span>
                            ) : r.error ? (
                              <span className="muted">({r.error})</span>
                            ) : null}
                          </div>
                        </div>
                      ))}
                      {anyMatched ? (
                        <div className="destination-block">
                          {(dg.destinations || []).length === 0 ? (
                            <em className="muted">No destinations</em>
                          ) : (
                            (dg.destinations || []).map((d, didx) => {
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
