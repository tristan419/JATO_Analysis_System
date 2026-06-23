import type { CSSProperties, ReactElement, ReactNode } from "react";
import { EmptyState } from "../common/EmptyState";

export interface SheetGroupedPreviewMetric {
  label: string;
  value: ReactNode;
}

export interface SheetGroupedPreviewColumn {
  key: string;
  label: ReactNode;
  style?: CSSProperties;
}

export interface SheetGroupedPreviewGroup<Row> {
  key: string;
  title: ReactNode;
  metrics: SheetGroupedPreviewMetric[];
  rows: Row[];
  truncated?: boolean;
  previewLimit?: number;
}

interface SheetGroupedPreviewProps<Row, Group extends SheetGroupedPreviewGroup<Row>> {
  title: ReactNode;
  toolbar?: ReactNode;
  groups: Group[];
  columns: SheetGroupedPreviewColumn[];
  expandedGroupKeys: Set<string>;
  previewTouched: boolean;
  emptyText: string;
  onToggleGroup: (group: Group, expanded: boolean) => void;
  renderRow: (row: Row, index: number, group: Group) => ReactElement;
  renderTruncated?: (group: Group) => ReactNode;
}

export function SheetGroupedPreview<
  Row,
  Group extends SheetGroupedPreviewGroup<Row> = SheetGroupedPreviewGroup<Row>,
>({
  title,
  toolbar,
  groups,
  columns,
  expandedGroupKeys,
  previewTouched,
  emptyText,
  onToggleGroup,
  renderRow,
  renderTruncated,
}: SheetGroupedPreviewProps<Row, Group>) {
  return (
    <div style={previewPanelStyle}>
      <div style={previewHeaderStyle}>
        <strong>{title}</strong>
        {toolbar ? <div style={previewToolbarStyle}>{toolbar}</div> : null}
      </div>
      {groups.length === 0 ? <EmptyState text={emptyText} /> : (
        <div style={previewGroupsStyle}>
          {groups.map((group, groupIndex) => {
            const expanded = expandedGroupKeys.has(group.key) || (!previewTouched && groupIndex === 0);
            return (
              <div key={group.key} style={previewGroupStyle}>
                <button
                  type="button"
                  style={previewGroupHeaderButtonStyle}
                  onClick={() => onToggleGroup(group, expanded)}
                >
                  <span aria-hidden="true" style={{ ...previewDisclosureStyle, transform: expanded ? "rotate(90deg)" : "rotate(0deg)" }} />
                  <strong>{group.title}</strong>
                  <span style={previewGroupMetricsStyle}>
                    {group.metrics.map((metric) => (
                      <span key={String(metric.label)} style={previewMetricChipStyle}>
                        {metric.label} {metric.value}
                      </span>
                    ))}
                  </span>
                </button>
                {expanded ? (
                  <div style={previewTableWrapStyle}>
                    <table style={tableStyle}>
                      <thead>
                        <tr>
                          {columns.map((column) => (
                            <th key={column.key} style={{ ...thStyle, ...column.style }}>{column.label}</th>
                          ))}
                        </tr>
                      </thead>
                      <tbody>
                        {group.rows.map((row, index) => renderRow(row, index, group))}
                      </tbody>
                    </table>
                    {group.truncated ? (
                      <div style={previewTruncatedStyle}>
                        {renderTruncated ? renderTruncated(group) : `仅展示前 ${group.previewLimit ?? group.rows.length} 行预览。`}
                      </div>
                    ) : null}
                  </div>
                ) : null}
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}

const previewPanelStyle: CSSProperties = {
  border: "1px solid #e2e8f0",
  borderRadius: 8,
  overflow: "hidden",
  background: "#ffffff",
};

const previewHeaderStyle: CSSProperties = {
  display: "flex",
  justifyContent: "space-between",
  alignItems: "center",
  gap: 10,
  padding: "10px 12px",
  borderBottom: "1px solid #e2e8f0",
  color: "#334155",
  fontSize: 13,
};

const previewToolbarStyle: CSSProperties = {
  display: "flex",
  alignItems: "center",
  justifyContent: "flex-end",
  gap: 6,
  flexWrap: "wrap",
};

const previewTableWrapStyle: CSSProperties = {
  maxHeight: 520,
  overflow: "auto",
};

const previewGroupsStyle: CSSProperties = {
  display: "grid",
};

const previewGroupStyle: CSSProperties = {
  borderTop: "1px solid #e2e8f0",
};

const previewGroupHeaderButtonStyle: CSSProperties = {
  width: "100%",
  display: "grid",
  gridTemplateColumns: "18px minmax(120px, 1fr) auto",
  alignItems: "center",
  gap: 10,
  padding: "10px 12px",
  border: 0,
  background: "#f8fafc",
  color: "#334155",
  cursor: "pointer",
  textAlign: "left",
};

const previewDisclosureStyle: CSSProperties = {
  display: "inline-block",
  width: 0,
  height: 0,
  borderTop: "5px solid transparent",
  borderBottom: "5px solid transparent",
  borderLeft: "8px solid #2563eb",
  transition: "transform 120ms ease",
  transformOrigin: "45% 50%",
};

const previewGroupMetricsStyle: CSSProperties = {
  display: "flex",
  gap: 6,
  flexWrap: "wrap",
  justifyContent: "flex-end",
};

const previewMetricChipStyle: CSSProperties = {
  border: "1px solid #e2e8f0",
  borderRadius: 999,
  padding: "2px 8px",
  background: "#ffffff",
  color: "#475569",
  fontSize: 11,
  fontWeight: 700,
  whiteSpace: "nowrap",
};

const previewTruncatedStyle: CSSProperties = {
  padding: "8px 12px",
  color: "#64748b",
  fontSize: 12,
  borderTop: "1px solid #e2e8f0",
};

const tableStyle: CSSProperties = {
  width: "100%",
  borderCollapse: "collapse",
};

const thStyle: CSSProperties = {
  textAlign: "left",
  padding: "10px 12px",
  fontSize: 11,
  fontWeight: 700,
  color: "#64748b",
  background: "#f8fafc",
  textTransform: "uppercase",
  whiteSpace: "nowrap",
};
