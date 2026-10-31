import { useEffect, useRef, type KeyboardEvent, type ReactNode } from "react";
import { createPortal } from "react-dom";

interface Props { label: string; onClose: () => void; children: ReactNode }

// Both workspaces stay above the matrix; nested confirmations retain their own focus/Escape handling.
export function OrderGeniusWorkspacePanel({ label, onClose, children }: Props) {
  const panel = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const previous = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    panel.current?.focus();
    return () => { if (previous?.isConnected) previous.focus(); };
  }, []);
  const keyDown = (event: KeyboardEvent<HTMLDivElement>): void => {
    if (!(event.target instanceof Element) || event.target.closest('[role="dialog"]') !== panel.current) return;
    if (event.key === "Escape") { event.preventDefault(); event.stopPropagation(); onClose(); return; }
    if (event.key !== "Tab") return;
    const focusable = [...event.currentTarget.querySelectorAll<HTMLElement>('button:not(:disabled),a[href],input:not(:disabled),select:not(:disabled),textarea:not(:disabled),summary,[tabindex="0"]')]
      .filter(element => element.checkVisibility());
    const first = focusable[0], last = focusable.at(-1);
    if (!first) { event.preventDefault(); return; }
    if (document.activeElement === panel.current || (event.shiftKey && document.activeElement === first) || (!event.shiftKey && document.activeElement === last)) {
      event.preventDefault(); (event.shiftKey ? last : first)?.focus();
    }
  };
  return createPortal(<div className="og-workspace-overlay" onMouseDown={event => { if (event.target === event.currentTarget) onClose(); }}>
    <div ref={panel} className="og-workspace-surface" role="dialog" aria-modal="true" aria-label={label} tabIndex={-1} onKeyDown={keyDown}>{children}</div>
  </div>, document.body);
}
