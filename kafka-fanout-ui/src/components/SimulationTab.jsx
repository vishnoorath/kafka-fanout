import React, { useMemo, useState } from 'react';
import { useEnvs, effectiveEnv } from '../store/useEnvs.jsx';
import { safeSearch } from '../lib/jmespath-preview.js';

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
    else if (mc.operator === 'contains') matched = a.includes(b) || b.includes(a);
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

export default function SimulationTab({ env }) {
  const { state, dispatch } = useEnvs();
  const eff = effectiveEnv(state, env.id);
  const dgs = eff.domain_groupings || [];
  
  const [formatError, setFormatError] = useState(null);
  const [activeOutcomeTab, setActiveOutcomeTab] = useState('matched'); // 'matched' | 'unmatched'

  const parsed = useMemo(() => {
    try {
      const p = JSON.parse(state.testMessage);
      setFormatError(null);
      return p;
    } catch (exc) {
      setFormatError(exc.message);
      return null;
    }
  }, [state.testMessage]);

  const evaluatedGroupings = useMemo(() => {
    if (!parsed) return [];
    return dgs.map((dg, dgIdx) => {
      const mcs = dg.match_conditions || [];
      const mcResults = mcs.map((mc) => evaluateMatchConditionClient(mc, parsed));
      const anyMatched = mcResults.some((r) => r.matched);
      return {
        dg,
        dgIdx,
        mcResults,
        anyMatched,
      };
    });
  }, [dgs, parsed]);

  const matchedGroupings = evaluatedGroupings.filter((g) => g.anyMatched);
  const unmatchedGroupings = evaluatedGroupings.filter((g) => !g.anyMatched);

  function handleFormat() {
    try {
      const formatted = JSON.stringify(JSON.parse(state.testMessage), null, 2);
      dispatch({ type: 'SET_TEST_MESSAGE', value: formatted });
      setFormatError(null);
    } catch (exc) {
      setFormatError(exc.message);
    }
  }

  return (
    <div className="simulation-tab">
      <div className="simulation-layout">
        {/* Left Column: Editor */}
        <div className="sim-editor-panel">
          <div className="panel-header-simple">
            <span className="panel-title-simple">Sandbox Payload Editor</span>
            <button className="btn btn-sm" onClick={handleFormat}>
              Format JSON
            </button>
          </div>
          <textarea
            className="sim-textarea"
            value={state.testMessage}
            onChange={(e) => dispatch({ type: 'SET_TEST_MESSAGE', value: e.target.value })}
            spellCheck={false}
            placeholder="Paste your JSON test payload here..."
          />
          {formatError ? (
            <div className="sim-error-alert">
              <strong>JSON Parse Error:</strong> {formatError}
            </div>
          ) : (
            <div className="sim-success-alert">
              JSON Structure is Valid ✓
            </div>
          )}
        </div>

        {/* Right Column: Routing Outcomes */}
        <div className="sim-results-panel">
          <div className="panel-header-simple">
            <span className="panel-title-simple">Evaluated Routing Simulation</span>
          </div>

          {parsed == null ? (
            <div className="muted flex-center" style={{ height: '300px' }}>
              Please correct the JSON payload to run the simulation.
            </div>
          ) : dgs.length === 0 ? (
            <div className="muted flex-center" style={{ height: '300px' }}>
              No Domain Groupings configured for this environment.
            </div>
          ) : (
            <>
              <div className="tabs sub-tabs mb-3">
                <button
                  className={`tab ${activeOutcomeTab === 'matched' ? 'active' : ''}`}
                  onClick={() => setActiveOutcomeTab('matched')}
                >
                  Matched <span className="tab-count-pill matched">{matchedGroupings.length}</span>
                </button>
                <button
                  className={`tab ${activeOutcomeTab === 'unmatched' ? 'active' : ''}`}
                  onClick={() => setActiveOutcomeTab('unmatched')}
                >
                  No Match <span className="tab-count-pill unmatched">{unmatchedGroupings.length}</span>
                </button>
              </div>

              <div className="sim-groupings-list">
                {(activeOutcomeTab === 'matched' ? matchedGroupings : unmatchedGroupings).length === 0 ? (
                  <div className="muted flex-center" style={{ height: '150px' }}>
                    No {activeOutcomeTab === 'matched' ? 'matching' : 'unmatched'} domain groupings.
                  </div>
                ) : (
                  (activeOutcomeTab === 'matched' ? matchedGroupings : unmatchedGroupings).map(
                    ({ dg, dgIdx, mcResults, anyMatched }) => {
                      const mcs = dg.match_conditions || [];
                      return (
                        <div
                          key={dgIdx}
                          className={`sim-group-card ${anyMatched ? 'matched-glow' : ''}`}
                        >
                          <div className="sim-group-header">
                            <span className={`sim-badge ${anyMatched ? 'matched' : 'unmatched'}`}>
                              {anyMatched ? 'MATCHED ✓' : 'NO MATCH'}
                            </span>
                            <span className="sim-group-name">
                              #{dgIdx + 1} — {dg.name || '(unnamed)'}
                            </span>
                          </div>

                          <div className="sim-conditions-section">
                            <div className="sim-section-label">Condition Rules</div>
                            {mcResults.map((r, mcIdx) => (
                              <div key={mcIdx} className="sim-condition-row">
                                <span className={`sim-dot ${r.kind}`} />
                                <span className="sim-path">{mcs[mcIdx].key_path}</span>
                                <span className="sim-op">{mcs[mcIdx].operator}</span>
                                <span className="sim-val">
                                  [{mcs[mcIdx].values?.map((v) => v.value).join(', ')}]
                                </span>
                                <span className="sim-arrow">→</span>
                                <span className="sim-resolved">
                                  resolved: <code>{stringify(r.resolved)}</code>
                                </span>
                                {r.error && <span className="sim-error-hint">({r.error})</span>}
                              </div>
                            ))}
                          </div>

                          {anyMatched && (
                            <div className="sim-destinations-section">
                              <div className="sim-section-label">Target Routing Destinations</div>
                              {(dg.destinations || []).length === 0 ? (
                                <div className="muted pl-3">No destination topics specified.</div>
                              ) : (
                                (dg.destinations || []).map((d, didx) => {
                                  const headers = (d.headers || []).map((h) => {
                                    if (h.mode === 'from_message') {
                                      const res = safeSearch(h.value, parsed);
                                      return { name: h.name, value: res.ok ? stringify(res.value) : `(error: ${res.error})` };
                                    }
                                    return { name: h.name, value: h.value };
                                  });

                                  return (
                                    <div key={didx} className="sim-destination-item">
                                      <div className="sim-dest-topic">
                                        Topic: <strong>{d.topic}</strong>
                                      </div>
                                      {headers.length > 0 && (
                                        <div className="sim-dest-headers">
                                          {headers.map((h, hidx) => (
                                            <div key={hidx} className="sim-header-row">
                                              <span className="sim-h-name">{h.name}:</span>
                                              <span className="sim-h-val">{h.value}</span>
                                            </div>
                                          ))}
                                        </div>
                                      )}
                                    </div>
                                  );
                                })
                              )}
                            </div>
                          )}
                        </div>
                      );
                    }
                  )
                )}
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}
