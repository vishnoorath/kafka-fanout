import React, { useEffect } from 'react';
import { useEnvs } from '../store/useEnvs.jsx';

export default function Toasts() {
  const { state, dispatch } = useEnvs();
  // Auto-dismiss after 6s; errors stay longer (10s) so the user can
  // read the message. Click a toast to dismiss immediately.
  useEffect(() => {
    if (state.toasts.length === 0) return;
    const timers = state.toasts.map((t) =>
      setTimeout(
        () => dispatch({ type: 'DISMISS_TOAST', id: t.id }),
        t.kind === 'error' ? 10000 : 6000,
      ),
    );
    return () => timers.forEach(clearTimeout);
  }, [state.toasts, dispatch]);
  return (
    <div className="toasts">
      {state.toasts.map((t) => (
        <div
          key={t.id}
          className={`toast ${t.kind}`}
          onClick={() => dispatch({ type: 'DISMISS_TOAST', id: t.id })}
        >
          {t.text}
        </div>
      ))}
    </div>
  );
}
