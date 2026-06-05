import React, { useMemo } from 'react';
import { effectiveEnv, useEnvs } from '../store/useEnvs.jsx';

export default function VisualTopology({ env }) {
  const { state } = useEnvs();
  const eff = effectiveEnv(state, env.id);
  const dgs = eff.domain_groupings || [];
  const source = eff.source || { topic: 'source-topic' };

  // Parse test message to highlight active routing paths
  const parsed = useMemo(() => {
    try {
      return JSON.parse(state.testMessage);
    } catch {
      return null;
    }
  }, [state.testMessage]);

  // Client-side rule evaluation to highlight matches
  const matchResults = useMemo(() => {
    if (!parsed) return [];
    return dgs.map((dg) => {
      const mcs = dg.match_conditions || [];
      return mcs.some((mc) => {
        // Simple client evaluation (matching evaluateMatchConditionClient)
        const pathVal = mc.key_path;
        if (!pathVal) return false;
        
        // Inline simple evaluation
        let cleanPath = pathVal;
        if (cleanPath.startsWith('@.')) {
          cleanPath = cleanPath.slice(2);
        } else if (cleanPath.startsWith('@')) {
          cleanPath = cleanPath.slice(1);
        }
        const keys = cleanPath ? cleanPath.split('.') : [];
        let val = parsed;
        for (const k of keys) {
          if (val && typeof val === 'object') {
            val = val[k];
          } else {
            val = undefined;
            break;
          }
        }
        if (val == null || typeof val === 'object') return false;
        
        const values = (mc.values || []).map(x => x?.value ?? x);
        const vs = String(val).toLowerCase();
        const op = mc.operator;
        
        return values.some(target => {
          const b = String(target).toLowerCase();
          if (op === 'equals') return vs === b;
          if (op === 'not_equals') return vs !== b;
          if (op === 'contains') return vs.includes(b) || b.includes(vs);
          return false;
        });
      });
    });
  }, [dgs, parsed]);

  return (
    <div className="topology-view">
      <div className="topology-header-info">
        <p className="muted">
          Visualizing data flow routing. Flow lines highlight in{' '}
          <span style={{ color: 'var(--success)', fontWeight: 'bold' }}>green</span> when rules match the current sandbox payload.
        </p>
      </div>

      <div className="topology-container">
        {/* Source Node Column */}
        <div className="topo-column">
          <div className="topo-node source-node">
            <div className="node-icon">📥</div>
            <div className="node-label">Source Topic</div>
            <div className="node-name">{source.topic}</div>
            <div className="node-details">{source.brokers}</div>
          </div>
        </div>

        {/* Mappings / Condition Nodes Column */}
        <div className="topo-column flex-1">
          {dgs.length === 0 ? (
            <div className="muted topo-empty">No rules configured.</div>
          ) : (
            dgs.map((dg, idx) => {
              const matched = matchResults[idx];
              return (
                <div key={idx} className="topo-row-group">
                  {/* The Rule / Group Node */}
                  <div className={`topo-node dg-node ${matched ? 'matched' : ''}`}>
                    <div className="node-label">Grouping #{idx + 1}</div>
                    <div className="node-name">{dg.name || '(unnamed)'}</div>
                    <div className="dg-conditions-summary">
                      {dg.match_conditions?.map((mc, cIdx) => (
                        <div key={cIdx} className="dg-cond-pill">
                          {mc.key_path} {mc.operator} ...
                        </div>
                      ))}
                    </div>
                  </div>

                  {/* Connecting line helper */}
                  <div className="topo-connector-line">
                    <svg className="connector-svg">
                      <line
                        x1="0"
                        y1="50%"
                        x2="100%"
                        y2="50%"
                        className={matched ? 'line-matched' : 'line-idle'}
                      />
                    </svg>
                  </div>

                  {/* Destination Nodes */}
                  <div className="topo-destinations-col">
                    {dg.destinations?.length === 0 ? (
                      <div className="muted small p-2">No destinations</div>
                    ) : (
                      dg.destinations?.map((d, dIdx) => (
                        <div key={dIdx} className={`topo-node dest-node ${matched ? 'matched' : ''}`}>
                          <div className="node-icon">📤</div>
                          <div className="node-label">Destination</div>
                          <div className="node-name">{d.topic}</div>
                        </div>
                      ))
                    )}
                  </div>
                </div>
              );
            })
          )}
        </div>
      </div>
    </div>
  );
}
