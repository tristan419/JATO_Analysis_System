interface GovernanceStatusBadgeProps {
  value: string;
  label?: string;
}


export function GovernanceStatusBadge({ value, label }: GovernanceStatusBadgeProps) {
  const normalized = value.trim().toLowerCase().replaceAll("_", "-");
  return (
    <span className={`msrp-governance-badge is-${normalized}`}>
      {label ?? value.replaceAll("_", " ")}
    </span>
  );
}
