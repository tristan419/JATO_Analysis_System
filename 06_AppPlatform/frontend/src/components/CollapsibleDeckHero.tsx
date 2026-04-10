import type { ReactNode } from "react";

interface CollapsibleDeckHeroProps {
  collapsed: boolean;
  onToggle: () => void;
  expandedLabel: string;
  collapsedLabel: string;
  expandedTitle: string;
  collapsedTitle: string;
  head: ReactNode;
  body?: ReactNode;
  className?: string;
  shellClassName?: string;
}

export function CollapsibleDeckHero({
  collapsed,
  onToggle,
  expandedLabel,
  collapsedLabel,
  expandedTitle,
  collapsedTitle,
  head,
  body,
  className = "header-card dashboard-hero",
  shellClassName = "dashboard-hero-shell",
}: CollapsibleDeckHeroProps) {
  return (
    <div className={`${shellClassName}${collapsed ? " is-collapsed" : ""}`}>
      <div className={className}>
        <div className="dashboard-hero-head">{head}</div>
        {body ? (
          <div className="dashboard-hero-body">
            <div className="dashboard-hero-body-inner">{body}</div>
          </div>
        ) : null}

        <button
          type="button"
          className="dashboard-rail-toggle dashboard-hero-toggle"
          aria-expanded={!collapsed}
          aria-label={collapsed ? expandedLabel : collapsedLabel}
          title={collapsed ? expandedTitle : collapsedTitle}
          onClick={onToggle}
        >
          <span aria-hidden="true">{collapsed ? "+" : "-"}</span>
        </button>
      </div>
    </div>
  );
}