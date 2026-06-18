import type { CSSProperties, ReactNode } from "react";

interface EmptyStateProps {
  text: ReactNode;
}

export function EmptyState({ text }: EmptyStateProps) {
  return <div style={emptyStateStyle}>{text}</div>;
}

const emptyStateStyle: CSSProperties = {
  padding: 24,
  textAlign: "center",
  color: "#94a3b8",
  fontSize: 13,
};
