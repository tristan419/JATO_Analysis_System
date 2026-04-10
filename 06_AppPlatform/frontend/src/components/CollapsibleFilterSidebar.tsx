import type { ReactNode } from "react";

interface CollapsibleFilterSidebarProps {
  collapsed: boolean;
  onToggle: () => void;
  kicker: string;
  title: string;
  summary: string;
  expandedLabel: string;
  collapsedLabel: string;
  expandedTitle: string;
  collapsedTitle: string;
  children: ReactNode;
  className?: string;
}

export function CollapsibleFilterSidebar({
  collapsed,
  onToggle,
  kicker,
  title,
  summary,
  expandedLabel,
  collapsedLabel,
  expandedTitle,
  collapsedTitle,
  children,
  className = "filter-sidebar",
}: CollapsibleFilterSidebarProps) {
  return (
    <aside className={`${className}${collapsed ? " is-collapsed" : ""}`}>
      <div className="filter-sidebar-rail">
        <div className="filter-sidebar-rail-copy">
          <span className="panel-kicker">{kicker}</span>
          <strong className="filter-sidebar-rail-title">{title}</strong>
          <span className="filter-sidebar-rail-summary">{summary}</span>
        </div>
        <button
          type="button"
          className="dashboard-rail-toggle filter-sidebar-toggle"
          aria-expanded={!collapsed}
          aria-label={collapsed ? expandedLabel : collapsedLabel}
          title={collapsed ? expandedTitle : collapsedTitle}
          onClick={onToggle}
        >
          <span aria-hidden="true">{collapsed ? "+" : "-"}</span>
        </button>
      </div>

      <div className="filter-sidebar-body">{children}</div>
    </aside>
  );
}