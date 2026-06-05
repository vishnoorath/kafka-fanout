import React, { useEffect, useState } from 'react';

export default function MetricsDashboard({ status }) {
  const [history, setHistory] = useState([]);

  useEffect(() => {
    if (!status) return;
    // Only capture history if running
    if (status.state !== 'running') {
      // Keep history but decay slowly or just push zeros
    }
    const now = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
    setHistory((prev) => {
      const next = [
        ...prev,
        {
          time: now,
          consumed: status.consumed_rate || 0.0,
          routed: status.routed_rate || 0.0,
          failed: status.failed_rate || 0.0,
        },
      ];
      if (next.length > 20) {
        next.shift();
      }
      return next;
    });
  }, [status]);

  const stats = [
    {
      label: 'Consumed',
      value: status?.messages_consumed ?? 0,
      rate: status?.consumed_rate ?? 0.0,
      color: 'var(--accent)',
      bg: 'rgba(99, 102, 241, 0.08)',
    },
    {
      label: 'Routed',
      value: status?.messages_routed ?? 0,
      rate: status?.routed_rate ?? 0.0,
      color: 'var(--ok)',
      bg: 'rgba(34, 197, 94, 0.08)',
    },
    {
      label: 'Failed / DLQ',
      value: status?.messages_failed ?? 0,
      rate: status?.failed_rate ?? 0.0,
      color: 'var(--err)',
      bg: 'rgba(239, 68, 68, 0.08)',
    },
    {
      label: 'Unmatched',
      value: status?.messages_unmatched ?? 0,
      rate: null,
      color: 'var(--warn)',
      bg: 'rgba(234, 179, 8, 0.08)',
    },
  ];

  // SVG Chart Config
  const width = 600;
  const height = 130;
  const paddingLeft = 30;
  const paddingRight = 10;
  const paddingTop = 10;
  const paddingBottom = 20;

  const chartWidth = width - paddingLeft - paddingRight;
  const chartHeight = height - paddingTop - paddingBottom;

  const maxVal = Math.max(
    1.0,
    ...history.map((h) => Math.max(h.consumed, h.routed, h.failed))
  );

  const getPoints = (key) => {
    if (history.length < 2) return '';
    return history
      .map((h, i) => {
        const x = paddingLeft + (i / (history.length - 1)) * chartWidth;
        const y = paddingTop + chartHeight - (h[key] / maxVal) * chartHeight;
        return `${x.toFixed(1)},${y.toFixed(1)}`;
      })
      .join(' ');
  };

  return (
    <div className="metrics-dashboard">
      <div className="metrics-grid">
        {stats.map((s, idx) => (
          <div
            key={idx}
            className="metric-card"
            style={{ borderColor: s.color, backgroundColor: s.bg }}
          >
            <div className="metric-label">{s.label}</div>
            <div className="metric-value" style={{ color: s.color }}>
              {s.value.toLocaleString()}
            </div>
            {s.rate !== null ? (
              <div className="metric-rate">
                <span className="rate-num">{s.rate.toFixed(1)}</span> msg/sec
              </div>
            ) : (
              <div className="metric-rate" style={{ opacity: 0.6 }}>
                unmatched routes
              </div>
            )}
          </div>
        ))}
      </div>

      <div className="chart-card mt-3">
        <div className="chart-header">
          <span className="chart-title">Throughput (last 20s)</span>
          <div className="chart-legend">
            <span className="legend-item"><span className="legend-dot consumed" /> Consumed</span>
            <span className="legend-item"><span className="legend-dot routed" /> Routed</span>
            <span className="legend-item"><span className="legend-dot failed" /> Failed</span>
          </div>
        </div>
        
        {history.length < 2 ? (
          <div className="muted flex-center" style={{ height: '130px' }}>
            Waiting for more data points...
          </div>
        ) : (
          <div className="chart-wrapper">
            <svg viewBox={`0 0 ${width} ${height}`} className="throughput-svg">
              {/* Y Axis Gridlines */}
              {[0, 0.5, 1].map((ratio) => {
                const y = paddingTop + chartHeight * (1 - ratio);
                const val = (ratio * maxVal).toFixed(1);
                return (
                  <g key={ratio} className="grid-line">
                    <line x1={paddingLeft} y1={y} x2={width - paddingRight} y2={y} stroke="var(--border)" strokeDasharray="3 3" />
                    <text x={paddingLeft - 5} y={y + 4} textAnchor="end" fill="var(--text-muted)" fontSize="9">
                      {val}
                    </text>
                  </g>
                );
              })}

              {/* X Axis Time Labels */}
              {history.map((h, i) => {
                // Label every 5th tick
                if (i % 5 !== 0 && i !== history.length - 1) return null;
                const x = paddingLeft + (i / (history.length - 1)) * chartWidth;
                return (
                  <text key={i} x={x} y={height - 4} textAnchor="middle" fill="var(--text-muted)" fontSize="9">
                    {h.time}
                  </text>
                );
              })}

              {/* Smooth line plots */}
              <polyline
                fill="none"
                stroke="var(--accent)"
                strokeWidth="2.5"
                strokeLinecap="round"
                strokeLinejoin="round"
                points={getPoints('consumed')}
              />
              <polyline
                fill="none"
                stroke="var(--ok)"
                strokeWidth="2.5"
                strokeLinecap="round"
                strokeLinejoin="round"
                points={getPoints('routed')}
              />
              <polyline
                fill="none"
                stroke="var(--err)"
                strokeWidth="2.5"
                strokeLinecap="round"
                strokeLinejoin="round"
                points={getPoints('failed')}
              />
            </svg>
          </div>
        )}
      </div>
    </div>
  );
}
