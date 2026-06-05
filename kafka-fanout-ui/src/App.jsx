import React, { useState, useEffect, useRef } from 'react';
import Sidebar from './components/Sidebar.jsx';
import EnvHeader from './components/EnvHeader.jsx';
import SourcePanel from './components/SourcePanel.jsx';
import DomainGroupingsPanel, { SingleDomainGroupingPanel } from './components/MappingsPanel.jsx';
import RuntimeControls from './components/RuntimeControls.jsx';
import Toasts from './components/Toasts.jsx';
import { EnvsProvider, useEnvs } from './store/useEnvs.jsx';
import SimulationTab from './components/SimulationTab.jsx';
import VisualTopology from './components/VisualTopology.jsx';

/**
 * Renders either the env-level view (Source + Mappings + Runtime)
 * or the single-DG focused view, based on the store's `selectedDGIndex`.
 * RuntimeControls is always shown — runtime is per-env, not per-DG.
 */
function MainPane() {
  const { state, dispatch } = useEnvs();
  const env = state.envs.find((e) => e.id === state.selectedId);

  if (!env) {
    return (
      <div className="main-content">
        <p className="muted">No environment selected.</p>
      </div>
    );
  }

  const dgIndex = state.selectedDGIndex;

  const content = dgIndex != null ? (
    <SingleDomainGroupingPanel env={env} dgIndex={dgIndex} />
  ) : state.activeTab === 'source' ? (
    <SourcePanel env={env} />
  ) : state.activeTab === 'simulation' ? (
    <SimulationTab env={env} />
  ) : state.activeTab === 'topology' ? (
    <VisualTopology env={env} />
  ) : state.activeTab === 'runtime' ? (
    <RuntimeControls env={env} />
  ) : (
    <DomainGroupingsPanel env={env} />
  );

  return (
    <>
      <EnvHeader env={env} />
      {dgIndex == null && (
        <div className="tabs">
          <button
            className={`tab ${state.activeTab === 'source' ? 'active' : ''}`}
            onClick={() => dispatch({ type: 'SET_TAB', tab: 'source' })}
          >
            Source
          </button>
          <button
            className={`tab ${state.activeTab === 'mappings' ? 'active' : ''}`}
            onClick={() => dispatch({ type: 'SET_TAB', tab: 'mappings' })}
          >
            Domain Groupings
            <span className="tab-count">({env.domain_groupings.length})</span>
          </button>
          <button
            className={`tab ${state.activeTab === 'simulation' ? 'active' : ''}`}
            onClick={() => dispatch({ type: 'SET_TAB', tab: 'simulation' })}
          >
            Simulation Sandbox
          </button>
          <button
            className={`tab ${state.activeTab === 'topology' ? 'active' : ''}`}
            onClick={() => dispatch({ type: 'SET_TAB', tab: 'topology' })}
          >
            Visual Topology
          </button>
          <button
            className={`tab ${state.activeTab === 'runtime' ? 'active' : ''}`}
            onClick={() => dispatch({ type: 'SET_TAB', tab: 'runtime' })}
          >
            Runtime
          </button>
        </div>
      )}
      <div className="main-content">
        <div className="scrollable-panel">
          {content}
        </div>
      </div>
    </>
  );
}

export default function App() {
  const [sidebarWidth, setSidebarWidth] = useState(() => {
    try {
      const stored = localStorage.getItem('fanout:sidebar_w');
      return stored ? parseInt(stored, 10) : 240;
    } catch {
      return 240;
    }
  });
  const isDragging = useRef(false);

  useEffect(() => {
    function handleMouseMove(e) {
      if (!isDragging.current) return;
      const newWidth = Math.max(160, Math.min(600, e.clientX));
      setSidebarWidth(newWidth);
      try {
        localStorage.setItem('fanout:sidebar_w', String(newWidth));
      } catch {
        // ignore
      }
    }
    function handleMouseUp() {
      isDragging.current = false;
      document.body.style.cursor = '';
      document.body.style.userSelect = '';
    }
    window.addEventListener('mousemove', handleMouseMove);
    window.addEventListener('mouseup', handleMouseUp);
    return () => {
      window.removeEventListener('mousemove', handleMouseMove);
      window.removeEventListener('mouseup', handleMouseUp);
    };
  }, []);

  return (
    <EnvsProvider>
      <div className="app" style={{ gridTemplateColumns: `${sidebarWidth}px 4px 1fr` }}>
        <Sidebar />
        <div
          className="sidebar-resizer"
          onMouseDown={(e) => {
            e.preventDefault();
            isDragging.current = true;
            document.body.style.cursor = 'col-resize';
            document.body.style.userSelect = 'none';
          }}
        />
        <div className="main">
          <MainPane />
        </div>
      </div>
      <Toasts />
    </EnvsProvider>
  );
}
