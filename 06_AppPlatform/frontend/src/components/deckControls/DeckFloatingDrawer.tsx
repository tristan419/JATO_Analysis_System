import type { ReactNode } from "react";

interface DeckFloatingDrawerProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  triggerPrimary: string;
  triggerSecondaryOpen: string;
  triggerSecondaryClosed: string;
  title: string;
  eyebrow?: string;
  ariaLabel: string;
  closeLabel?: string;
  className?: string;
  triggerClassName?: string;
  panelClassName?: string;
  headerClassName?: string;
  bodyClassName?: string;
  footerClassName?: string;
  children: ReactNode;
  footer?: ReactNode;
}

export function DeckFloatingDrawer({
  open,
  onOpenChange,
  triggerPrimary,
  triggerSecondaryOpen,
  triggerSecondaryClosed,
  title,
  eyebrow,
  ariaLabel,
  closeLabel = "关闭",
  className,
  triggerClassName,
  panelClassName,
  headerClassName,
  bodyClassName,
  footerClassName,
  children,
  footer,
}: DeckFloatingDrawerProps) {
  const drawerClassName = ["deck-floating-drawer", className, open ? "is-open" : ""].filter(Boolean).join(" ");
  const toggleClassName = ["market-scan-export-toggle", "deck-floating-toggle", triggerClassName].filter(Boolean).join(" ");
  const asideClassName = ["deck-floating-panel", panelClassName].filter(Boolean).join(" ");
  const headClassName = ["deck-floating-panel-head", headerClassName].filter(Boolean).join(" ");
  const panelBodyClassName = ["deck-floating-panel-body", bodyClassName].filter(Boolean).join(" ");
  const panelFooterClassName = ["market-scan-toolbar-meta", "deck-floating-panel-meta", footerClassName].filter(Boolean).join(" ");

  return (
    <section className={drawerClassName}>
      <button
        type="button"
        className={toggleClassName}
        onClick={() => onOpenChange(!open)}
        aria-expanded={open}
      >
        <span>{triggerPrimary}</span>
        <span>{open ? triggerSecondaryOpen : triggerSecondaryClosed}</span>
      </button>
      {open ? (
        <aside className={asideClassName} aria-label={ariaLabel}>
          <header className={headClassName}>
            <div>
              {eyebrow ? <span className="market-scan-panel-eyebrow">{eyebrow}</span> : null}
              <h3>{title}</h3>
            </div>
            <button
              type="button"
              className="btn btn-ghost btn-sm"
              onClick={() => onOpenChange(false)}
            >
              {closeLabel}
            </button>
          </header>

          <div className={panelBodyClassName}>{children}</div>

          {footer ? (
            <footer className={panelFooterClassName}>
              {footer}
            </footer>
          ) : null}
        </aside>
      ) : null}
    </section>
  );
}
