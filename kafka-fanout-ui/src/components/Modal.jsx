import React from 'react';

/**
 * Reusable modal dialog. Closes on backdrop click unless `dismissible`
 * is false. Renders nothing when `open` is false.
 */
export default function Modal({ open, title, children, onClose, dismissible = true, footer }) {
  if (!open) return null;
  return (
    <div
      className="modal-backdrop"
      onMouseDown={(e) => {
        if (dismissible && e.target === e.currentTarget) onClose?.();
      }}
    >
      <div className="modal" role="dialog" aria-modal="true">
        {title ? <h3>{title}</h3> : null}
        <div className="modal-body">{children}</div>
        <div className="modal-footer">
          {footer ?? (
            <button className="btn" onClick={onClose}>
              Close
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
