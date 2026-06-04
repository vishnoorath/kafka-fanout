import React from 'react';
import Sidebar from './components/Sidebar.jsx';
import EnvHeader from './components/EnvHeader.jsx';
import SourcePanel from './components/SourcePanel.jsx';
import DomainGroupingsPanel, { SingleDomainGroupingPanel } from './components/MappingsPanel.jsx';
import TestMessagePanel from './components/TestMessagePanel.jsx';
import RuntimeControls from './components/RuntimeControls.jsx';
import Toasts from './components/Toasts.jsx';
import { EnvsProvider, useEnvs } from './store/useEnvs.jsx';

/**
 * Renders either the env-level view (Source + Mappings + Test + Runtime)
 * or the single-DG focused view, based on the store's `selectedDGIndex`.
 * RuntimeControls + TestMessagePanel are always shown — runtime is
 * per-env, not per-DG.
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

  if (dgIndex != null) {
    return (
      <>
        <EnvHeader env={env} />
        <div className="main-content">
          <SingleDomainGroupingPanel env={env} dgIndex={dgIndex} />
          <TestMessagePanel env={env} />
          <RuntimeControls env={env} />
        </div>
      </>
    );
  }

  return (
    <>
      <EnvHeader env={env} />
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
      </div>
      <div className="main-content">
        {state.activeTab === 'source' ? (
          <SourcePanel env={env} />
        ) : (
          <DomainGroupingsPanel env={env} />
        )}
        <TestMessagePanel env={env} />
        <RuntimeControls env={env} />
      </div>
    </>
  );
}

export default function App() {
  return (
    <EnvsProvider>
      <div className="app">
        <Sidebar />
        <div className="main">
          <MainPane />
        </div>
      </div>
      <Toasts />
    </EnvsProvider>
  );
}
