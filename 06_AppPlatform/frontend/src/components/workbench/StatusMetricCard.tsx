import type { CSSProperties, ReactNode } from "react";

export type StatusMetricTone = "neutral" | "success" | "warning" | "danger" | "info";

interface StatusMetricCardProps {
  label: string;
  value: ReactNode;
  tone?: StatusMetricTone;
  active?: boolean;
  onClick?: () => void;
}

export function StatusMetricCard({
  label,
  value,
  tone = "neutral",
  active = false,
  onClick,
}: StatusMetricCardProps) {
  const color = metricToneColor(tone);
  const content = (
    <>
      <span style={{ color: "#64748b", fontSize: 11, fontWeight: 700 }}>{label}</span>
      <strong style={{ color, fontSize: 22, lineHeight: 1 }}>{value}</strong>
    </>
  );

  if (onClick) {
    return (
      <button
        type="button"
        style={{
          ...metricCardStyle,
          ...(active ? metricCardActiveStyle : null),
          cursor: "pointer",
          textAlign: "left",
          font: "inherit",
        }}
        onClick={onClick}
      >
        {content}
      </button>
    );
  }

  return <div style={{ ...metricCardStyle, ...(active ? metricCardActiveStyle : null) }}>{content}</div>;
}

function metricToneColor(tone: StatusMetricTone): string {
  if (tone === "success") return "#16a34a";
  if (tone === "warning") return "#b45309";
  if (tone === "danger") return "#dc2626";
  if (tone === "info") return "#2563eb";
  return "#111827";
}

const metricCardStyle: CSSProperties = {
  display: "grid",
  gap: 5,
  minHeight: 64,
  padding: 10,
  border: "1px solid #e2e8f0",
  borderRadius: 6,
  background: "#f8fafc",
};

const metricCardActiveStyle: CSSProperties = {
  borderColor: "#2563eb",
  background: "#eff6ff",
  boxShadow: "inset 0 0 0 1px #2563eb",
};
