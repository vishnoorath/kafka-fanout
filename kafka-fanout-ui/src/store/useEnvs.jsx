import React, {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useReducer,
  useRef,
} from 'react';
import { api } from '../lib/api.js';
import { newId } from '../utils/id.js';
import { SECRET_PLACEHOLDER, DOMAIN_GROUPING_NAMES } from '../utils/factory.js';

const DRAFT_KEY = 'fanout:draft:v1';

let initialDGNames = [];
try {
  const stored = localStorage.getItem('fanout:dg_names');
  initialDGNames = stored ? JSON.parse(stored) : DOMAIN_GROUPING_NAMES;
} catch {
  initialDGNames = DOMAIN_GROUPING_NAMES;
}

/**
 * Reducer-based store for envs + transient UI state.
 *
 * - Authoritative env data lives in `envs` (mirrored from the backend).
 * - `drafts` is keyed by env id and contains the in-flight edit state.
 *   On every save the draft is cleared.
 * - `statuses` is keyed by env id; populated by the polling effect.
 * - `toasts` is a stack of {id, kind, text} items.
 */
const initialState = {
  loading: true,
  loadError: null,
  envs: [], // [{id, name, ..., source, domain_groupings:[]}]
  selectedId: null,
  // When set, the right pane shows just this domain grouping (by index
  // in the env's domain_groupings list). When null, the right pane
  // shows the full env view (Source + Mappings + Test + Runtime).
  selectedDGIndex: null,
  activeTab: 'source', // 'source' | 'mappings'
  drafts: {}, // { [envId]: { ...editedFields, _touched: {field: true} } }
  statuses: {}, // { [envId]: { state, last_error, ... } }
  testMessage: '{\n  "Message": { "TableName": "Cattles" }\n}',
  toasts: [],
  dgNames: initialDGNames,
};

function reducer(state, action) {
  switch (action.type) {
    case 'LOAD_START':
      return { ...state, loading: true, loadError: null };
    case 'LOAD_OK':
      return {
        ...state,
        loading: false,
        loadError: null,
        envs: action.envs,
        selectedId: state.selectedId || (action.envs[0]?.id ?? null),
        statuses: Object.fromEntries(
          action.envs.map((e) => [e.id, e._status || state.statuses[e.id] || null]),
        ),
      };
    case 'LOAD_ERROR':
      return { ...state, loading: false, loadError: action.error };
    case 'SELECT':
      // Selecting an env always clears the per-DG selection so
      // the right pane shows the env-level view.
      return { ...state, selectedId: action.id, selectedDGIndex: null };
    case 'SELECT_DG':
      // Selecting a domain grouping also selects its env (parent).
      return {
        ...state,
        selectedId: action.envId,
        selectedDGIndex: action.index,
      };
    case 'SET_TAB':
      return { ...state, activeTab: action.tab };
    case 'ADD_ENV':
      return {
        ...state,
        envs: [...state.envs, action.env],
        selectedId: action.env.id,
        drafts: { ...state.drafts, [action.env.id]: { _create: true } },
      };
    case 'PATCH_DRAFT': {
      const drafts = { ...state.drafts };
      const existing = drafts[action.envId] || {};
      drafts[action.envId] = {
        ...existing,
        ...action.patch,
        _touched: { ...(existing._touched || {}), ...(action.touched || {}) },
        _create: existing._create,
      };
      return { ...state, drafts };
    }
    case 'CLEAR_DRAFT': {
      const drafts = { ...state.drafts };
      delete drafts[action.envId];
      return { ...state, drafts };
    }
    case 'REPLACE_ENV': {
      const envs = state.envs.map((e) => (e.id === action.env.id ? action.env : e));
      const statuses = { ...state.statuses, [action.env.id]: action.env._status || state.statuses[action.env.id] || null };
      return { ...state, envs, statuses };
    }
    case 'REMOVE_ENV': {
      const envs = state.envs.filter((e) => e.id !== action.id);
      const drafts = { ...state.drafts };
      delete drafts[action.id];
      const statuses = { ...state.statuses };
      delete statuses[action.id];
      const wasRemoved =
        state.selectedId === action.id || state.selectedId == null;
      return {
        ...state,
        envs,
        drafts,
        statuses,
        selectedId: wasRemoved ? envs[0]?.id ?? null : state.selectedId,
        selectedDGIndex: wasRemoved ? null : state.selectedDGIndex,
      };
    }
    case 'SET_STATUS':
      return {
        ...state,
        statuses: { ...state.statuses, [action.envId]: action.status },
      };
    case 'SET_TEST_MESSAGE':
      return { ...state, testMessage: action.value };
    case 'PUSH_TOAST':
      return {
        ...state,
        toasts: [...state.toasts, { id: newId(), kind: action.kind, text: action.text }],
      };
    case 'DISMISS_TOAST':
      return { ...state, toasts: state.toasts.filter((t) => t.id !== action.id) };
    case 'ADD_DG_NAME': {
      const dgNames = [...state.dgNames];
      if (action.name && !dgNames.includes(action.name)) {
        dgNames.push(action.name);
        try {
          localStorage.setItem('fanout:dg_names', JSON.stringify(dgNames));
        } catch {
          // ignore
        }
      }
      return { ...state, dgNames };
    }
    default:
      return state;
  }
}

const StoreCtx = createContext(null);

export function EnvsProvider({ children }) {
  const [state, baseDispatch] = useReducer(reducer, initialState);

  // Persist drafts to localStorage (debounced). Skipped for a "create" draft.
  useEffect(() => {
    const t = setTimeout(() => {
      try {
        const persistable = {};
        for (const [id, d] of Object.entries(state.drafts)) {
          if (!d._create) persistable[id] = d;
        }
        localStorage.setItem(DRAFT_KEY, JSON.stringify(persistable));
      } catch {
        // ignore quota / disabled storage
      }
    }, 300);
    return () => clearTimeout(t);
  }, [state.drafts]);

  // Initial load: read drafts from localStorage, then fetch envs.
  const draftHydrated = useRef(false);
  useEffect(() => {
    let cancelled = false;
    (async () => {
      baseDispatch({ type: 'LOAD_START' });
      try {
        let drafts = {};
        if (!draftHydrated.current) {
          try {
            const raw = localStorage.getItem(DRAFT_KEY);
            const parsed = raw ? JSON.parse(raw) : {};
            // Reshape v2: drop any draft still using the old `mappings`
            // top-level key. The shape is incompatible and a stale
            // draft would block Save with cryptic field errors.
            let dropped = false;
            for (const [id, d] of Object.entries(parsed || {})) {
              if (d && Object.prototype.hasOwnProperty.call(d, 'mappings')) {
                delete parsed[id];
                dropped = true;
              }
            }
            if (dropped) {
              try { localStorage.setItem(DRAFT_KEY, JSON.stringify(parsed || {})); } catch {}
              baseDispatch({
                type: 'PUSH_TOAST',
                kind: 'info',
                text: 'Saved drafts were reset (model reshape).',
              });
            }
            drafts = parsed || {};
          } catch {
            drafts = {};
          }
          draftHydrated.current = true;
        }
        const envs = await api.listEnvs();
        if (cancelled) return;
        // Attach drafts to state directly via a custom dispatch path.
        // (We do it by setting state.drafts via a PATCH in a follow-up
        // effect so the reducer stays the single source of truth.)
        window.__pendingDrafts = drafts; // hacky but small
        baseDispatch({ type: 'LOAD_OK', envs });
      } catch (exc) {
        baseDispatch({ type: 'LOAD_ERROR', error: String(exc.message || exc) });
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  // Hydrate drafts from localStorage right after the first LOAD_OK.
  useEffect(() => {
    if (state.loading) return;
    if (window.__pendingDrafts) {
      const pending = window.__pendingDrafts;
      window.__pendingDrafts = null;
      for (const [id, d] of Object.entries(pending)) {
        baseDispatch({ type: 'PATCH_DRAFT', envId: id, patch: d });
      }
    }
  }, [state.loading]);

  // Public action helpers.
  const dispatch = useCallback(
    (action) => {
      // Wrap a few action types with side-effects.
      if (action.type === 'TOAST') {
        baseDispatch({
          type: 'PUSH_TOAST',
          kind: action.kind || 'info',
          text: action.text,
        });
        return;
      }
      if (action.type === 'CREATE_ENV') {
        // POST -> ADD_ENV.
        (async () => {
          try {
            const env = await api.createEnv(action.payload);
            baseDispatch({ type: 'ADD_ENV', env });
            baseDispatch({ type: 'CLEAR_DRAFT', envId: env.id });
            baseDispatch({ type: 'TOAST', kind: 'success', text: `Created "${env.name}"` });
            return env;
          } catch (exc) {
            baseDispatch({ type: 'TOAST', kind: 'error', text: `Create failed: ${exc.message}` });
            throw exc;
          }
        })();
        return;
      }
      if (action.type === 'UPDATE_ENV') {
        (async () => {
          try {
            const env = await api.updateEnv(action.id, action.payload);
            baseDispatch({ type: 'REPLACE_ENV', env });
            baseDispatch({ type: 'CLEAR_DRAFT', envId: action.id });
            baseDispatch({ type: 'TOAST', kind: 'success', text: `Saved "${env.name}"` });
            return env;
          } catch (exc) {
            baseDispatch({ type: 'TOAST', kind: 'error', text: `Save failed: ${exc.message}` });
            throw exc;
          }
        })();
        return;
      }
      if (action.type === 'DELETE_ENV') {
        (async () => {
          try {
            await api.deleteEnv(action.id);
            baseDispatch({ type: 'REMOVE_ENV', id: action.id });
            baseDispatch({ type: 'TOAST', kind: 'success', text: 'Deleted' });
          } catch (exc) {
            baseDispatch({ type: 'TOAST', kind: 'error', text: `Delete failed: ${exc.message}` });
          }
        })();
        return;
      }
      if (action.type === 'DUPLICATE_ENV') {
        (async () => {
          try {
            const env = await api.duplicateEnv(action.id);
            baseDispatch({ type: 'ADD_ENV', env });
            baseDispatch({ type: 'TOAST', kind: 'success', text: `Duplicated as "${env.name}"` });
          } catch (exc) {
            baseDispatch({ type: 'TOAST', kind: 'error', text: `Duplicate failed: ${exc.message}` });
          }
        })();
        return;
      }
      if (action.type === 'RUNTIME_CMD') {
        const fn =
          action.cmd === 'start'
            ? api.startEnv
            : action.cmd === 'stop'
            ? api.stopEnv
            : action.cmd === 'reset-offsets'
            ? api.resetOffsets
            : null;
        if (!fn) {
          action._reject?.(new Error(`Unknown cmd: ${action.cmd}`));
          return;
        }
        (async () => {
          try {
            await fn(action.id);
            baseDispatch({
              type: 'TOAST',
              kind: 'success',
              text:
                action.cmd === 'reset-offsets'
                  ? 'Offsets reset to earliest'
                  : `${action.cmd} OK`,
            });
            // Refresh status after a moment.
            setTimeout(async () => {
              try {
                const status = await api.getStatus(action.id);
                baseDispatch({ type: 'SET_STATUS', envId: action.id, status });
              } catch {
                // ignore
              }
            }, 500);
            action._resolve?.();
          } catch (exc) {
            baseDispatch({ type: 'TOAST', kind: 'error', text: `${action.cmd} failed: ${exc.message}` });
            action._reject?.(exc);
          }
        })();
        return;
      }
      baseDispatch(action);
    },
    [],
  );

  return <StoreCtx.Provider value={{ state, dispatch }}>{children}</StoreCtx.Provider>;
}

export function useEnvs() {
  const ctx = useContext(StoreCtx);
  if (!ctx) throw new Error('useEnvs must be used inside EnvsProvider');
  return ctx;
}

/** Helper: get the effective edit state for an env (draft overlay on top of saved). */
export function effectiveEnv(state, envId) {
  const env = state.envs.find((e) => e.id === envId);
  if (!env) return null;
  const draft = state.drafts[envId];
  if (!draft) return env;
  return mergeDraft(env, draft);
}

function mergeDraft(env, draft) {
  const out = { ...env };
  for (const k of ['name', 'description', 'enabled', 'dlq_topic', 'dlq_brokers']) {
    if (k in draft && !k.startsWith('_')) out[k] = draft[k];
  }
  if (draft.source) {
    out.source = { ...env.source, ...draft.source };
  }
  if (draft.domain_groupings) {
    out.domain_groupings = draft.domain_groupings;
  }
  return out;
}

/** True if any draft field has been touched. */
export function hasDirtyDraft(state, envId) {
  const d = state.drafts[envId];
  if (!d) return false;
  if (d._create) return true;
  return Object.keys(d._touched || {}).length > 0;
}

/** Build the API payload from the effective state.
 *  Translates secret placeholders to `null` (no change) and any
 *  user-typed values to the literal string. */
export function buildEnvPayload(env, draft) {
  const src = { ...env.source, ...(draft?.source || {}) };
  const sourceOut = {
    brokers: src.brokers || '',
    topic: src.topic || '',
    consumer_group: src.consumer_group || '',
    offset_reset: src.offset_reset || 'earliest',
    security_protocol: src.security_protocol || 'PLAINTEXT',
    sasl_mechanism: src.sasl_mechanism || null,
    sasl_username: src.sasl_username || '',
    sasl_password: src.sasl_password === SECRET_PLACEHOLDER ? null : src.sasl_password || null,
    ssl_ca_location: src.ssl_ca_location || '',
  };
  const dgs = (draft?.domain_groupings || env.domain_groupings || []).map((dg) => ({
    name: dg.name || '',
    match_conditions: (dg.match_conditions || []).map((mc) => ({
      key_path: mc.key_path,
      operator: mc.operator,
      case_insensitive: mc.case_insensitive !== false,
      values: (mc.values || []).map((v) => ({ value: v.value || '' })),
    })),
    destinations: (dg.destinations || []).map((d) => ({
      use_source_broker: d.use_source_broker !== false,
      brokers: d.use_source_broker === false ? d.brokers || null : null,
      topic: d.topic || '',
      security_protocol: d.security_protocol || 'PLAINTEXT',
      sasl_mechanism: d.sasl_mechanism || null,
      sasl_username: d.sasl_username || '',
      sasl_password:
        d.sasl_password === SECRET_PLACEHOLDER ? null : d.sasl_password || null,
      ssl_ca_location: d.ssl_ca_location || '',
      headers: (d.headers || []).map((h) => ({
        name: h.name,
        value: h.value,
        mode: h.mode,
      })),
    })),
  }));
  return {
    name: (draft?.name ?? env.name) || '',
    description: (draft?.description ?? env.description) || '',
    enabled: !!(draft?.enabled ?? env.enabled),
    dlq_topic: (draft?.dlq_topic ?? env.dlq_topic) || null,
    dlq_brokers: (draft?.dlq_brokers ?? env.dlq_brokers) || null,
    source: sourceOut,
    domain_groupings: dgs,
  };
}
